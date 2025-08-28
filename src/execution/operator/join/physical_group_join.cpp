
#include "duckdb/execution/operator/join/physical_group_join.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include <thread>
#include <chrono>
#include <iostream>
#include <vector>
#include <atomic>
#include <mutex>

namespace duckdb {

PhysicalGroupJoin::PhysicalGroupJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                     unique_ptr<PhysicalOperator> right, unique_ptr<Expression> condition_p,
                                     JoinType join_type, idx_t estimated_cardinality,
                                     vector<unique_ptr<Expression>> &groups_p,
                                     vector<unique_ptr<Expression>> &aggregates_p)
    : PhysicalJoin(op, PhysicalOperatorType::GROUP_JOIN, join_type, estimated_cardinality),
      condition(std::move(condition_p)), groups(std::move(groups_p)), aggregates(std::move(aggregates_p)) {
    std::cout << "Inside PhysicalGroupJoin()" << std::endl; 
    children.push_back(std::move(left));
    children.push_back(std::move(right));
    D_ASSERT(join_type != JoinType::MARK);
    D_ASSERT(join_type != JoinType::SINGLE);
}

std::unordered_map<int, std::pair<int, int>> PhysicalGroupJoin::aggregation_map;
std::unordered_map<int, int> PhysicalGroupJoin::aggregation_map2;
std::unordered_map<int, double> PhysicalGroupJoin::final_results;

PhysicalGroupJoin::~PhysicalGroupJoin() = default;

class GroupJoinGlobalSinkState : public GlobalSinkState {
public:
    std::atomic<int> active_sink_tasks{0};
    bool finalized = false;
    bool left_scanned = false;
    bool right_scanned = false; 
    unique_ptr<GroupedAggregateHashTable> hash_table;
    vector<LogicalType> group_types;
    vector<idx_t> grouping_indices;
    vector<AggregateObject> aggregate_objects;
    vector<LogicalType> aggregate_return_types;
    ChunkCollection left_data;
    ChunkCollection right_data;
    // For parallel output
    std::vector<std::pair<int, double>> result_vector;
};

class GroupJoinLocalSinkState : public LocalSinkState {
public:
    GroupJoinLocalSinkState(Allocator &allocator, const PhysicalGroupJoin &op, ClientContext &context)
        : aggregate_executor(context) {
        for (auto &aggr_expr : op.aggregates) {
            auto &bound_aggr = aggr_expr->Cast<BoundAggregateExpression>();
            for (auto &child : bound_aggr.children) {
                aggregate_executor.AddExpression(*child);
            }
        }
        vector<LogicalType> aggregate_input_types;
        for (auto &aggr_expr : op.aggregates) {
            auto &bound_aggr = aggr_expr->Cast<BoundAggregateExpression>();
            for (auto &child : bound_aggr.children) {
                aggregate_input_types.push_back(child->return_type);
            }
        }
        aggregate_input_chunk.Initialize(allocator, aggregate_input_types);
    }
    ExpressionExecutor aggregate_executor;
    DataChunk aggregate_input_chunk;
};

unique_ptr<GlobalSinkState> PhysicalGroupJoin::GetGlobalSinkState(ClientContext &context) const {
    auto state = make_uniq<GroupJoinGlobalSinkState>();
    auto &left_types = children[0]->types;
    auto &right_types = children[1]->types;
    for (auto &group : groups) {
        auto &bound_ref = group->Cast<BoundReferenceExpression>();
        idx_t idx = bound_ref.index;
        LogicalType group_type;
        if (idx < left_types.size()) {
            group_type = left_types[idx];
            state->grouping_indices.push_back(idx);
        } else {
            idx_t right_idx = idx - left_types.size();
            if (right_idx >= right_types.size()) {
                throw InternalException("Grouping attribute index out of bounds in right child's types");
            }
            group_type = right_types[right_idx];
            state->grouping_indices.push_back(idx);
        }
        state->group_types.push_back(group_type);
    }
    for (auto &aggr_expr : aggregates) {
        if (aggr_expr->expression_class != ExpressionClass::BOUND_AGGREGATE) {
            throw NotImplementedException("Expected bound aggregate expression");
        }
        auto &bound_aggr = aggr_expr->Cast<BoundAggregateExpression>();
        AggregateObject aggr_obj(&bound_aggr);
        state->aggregate_objects.push_back(aggr_obj);
        state->aggregate_return_types.push_back(bound_aggr.return_type);
    }
    vector<LogicalType> aggregate_input_types;
    for (auto &aggr_expr : aggregates) {
        auto &bound_aggr = aggr_expr->Cast<BoundAggregateExpression>();
        if (!bound_aggr.children.empty()) {
            for (auto &child : bound_aggr.children) {
                aggregate_input_types.push_back(child->return_type);
            }
        } else {
            aggregate_input_types.push_back(LogicalType::INTEGER);
        }
    }
    state->hash_table = make_uniq<GroupedAggregateHashTable>(
        context, Allocator::Get(context),
        state->group_types, aggregate_input_types, state->aggregate_objects
    );
    return std::move(state);
}

unique_ptr<LocalSinkState> PhysicalGroupJoin::GetLocalSinkState(ExecutionContext &context) const {
    return make_uniq<GroupJoinLocalSinkState>(Allocator::Get(context.client), *this, context.client);
}

SinkResultType PhysicalGroupJoin::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
    auto &global_state = input.global_state.Cast<GroupJoinGlobalSinkState>();
    const auto &input_types = chunk.GetTypes();
    auto &left_types = children[0]->types;
    auto &right_types = children[1]->types;
    int left_est_card = children[0]->estimated_cardinality;
    int right_est_card = children[1]->estimated_cardinality;
    if (input_types == left_types) {
        global_state.left_data.Append(chunk);
        if (global_state.left_data.Size() >= left_est_card || chunk.size() < STANDARD_VECTOR_SIZE) {
            global_state.left_scanned = true;
        }
    } else if (input_types == right_types) {
        global_state.right_data.Append(chunk);
        if (global_state.right_data.Size() >= right_est_card || chunk.size() < STANDARD_VECTOR_SIZE) {
            global_state.right_scanned = true;
        }
    } else {
        throw InternalException("PhysicalGroupJoin::Sink: input chunk types do not match any child");
    }
    return SinkResultType::NEED_MORE_INPUT;
}

// Parallel aggregation: step 1 and 2 in parallel, then combine, then store result as vector for parallel output
void duckdb::PhysicalGroupJoin::PerformEqualityAggregation(
    
    duckdb::GroupJoinGlobalSinkState &global_state, std::unordered_map<int, double>& final_results) const {


    auto start = std::chrono::high_resolution_clock::now();

    std::unordered_map<int, double> left_sums;
    std::unordered_map<int, long long int> right_counts;

    auto left_agg = [&]() {
        duckdb::DataChunk lscan_chunk;
        global_state.left_data.InitializeScan();
        while (global_state.left_data.Scan(lscan_chunk)) {
            for (size_t i = 0; i < lscan_chunk.size(); ++i) {
                int key;
                double value;
                try {
                    key = lscan_chunk.data[0].GetValue(i).GetValue<int>();
                    value = lscan_chunk.data[1].GetValue(i).GetValue<double>();
                } catch (const std::exception& e) {
                    continue;
                }
                left_sums[key] += value;
            }
        }
    };

    auto right_agg = [&]() {
        duckdb::DataChunk rscan_chunk;
        global_state.right_data.InitializeScan();
        while (global_state.right_data.Scan(rscan_chunk)) {
            for (size_t i = 0; i < rscan_chunk.size(); ++i) {
                int key;
                try {
                    key = rscan_chunk.data[0].GetValue(i).GetValue<int>();
                } catch (const std::exception& e) {
                    continue;
                }
                right_counts[key]++;
            }
        }
    };

    std::thread left_thread(left_agg);
    std::thread right_thread(right_agg);
    left_thread.join();
    right_thread.join();

    for (const auto &left_entry : left_sums) {
        int key = left_entry.first;
        double sum = left_entry.second;
        long long int matching_rows = right_counts.count(key) ? right_counts.at(key) : 0;
        if (right_counts.count(key)) {
            final_results[key] = sum * matching_rows;
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end - start;
    std::cout << "> Total time taken for PerformEqualityAggregation(): " << elapsed.count() << " seconds" << std::endl;
}

// For parallel output, store result as vector in global_state
void StoreResultsAsVector(GroupJoinGlobalSinkState &global_state) {
    global_state.result_vector.clear();
    for (const auto &entry : PhysicalGroupJoin::final_results) {
        global_state.result_vector.emplace_back(entry.first, entry.second);
    }
}

// Used for parallel GetData
class GroupJoinGlobalSourceState : public GlobalSourceState {
public:
    explicit GroupJoinGlobalSourceState(GroupJoinGlobalSinkState *sink_state_p)
        : sink_state(sink_state_p), next_idx(0) {}
    GroupJoinGlobalSinkState *sink_state;
    std::atomic<size_t> next_idx;
};

struct GroupJoinLocalSourceState : public LocalSourceState {
    size_t my_offset = 0;
    size_t my_end = 0;
};

unique_ptr<GlobalSourceState> PhysicalGroupJoin::GetGlobalSourceState(ClientContext &context) const {
    auto &global_sink = sink_state->Cast<GroupJoinGlobalSinkState>();
    return make_uniq<GroupJoinGlobalSourceState>(&global_sink);
}

unique_ptr<LocalSourceState> PhysicalGroupJoin::GetLocalSourceState(ExecutionContext &context, GlobalSourceState &gstate) const {
    auto &global = gstate.Cast<GroupJoinGlobalSourceState>();
    auto local = make_uniq<GroupJoinLocalSourceState>();
    // Each thread gets a range of the result vector
    size_t total = global.sink_state->result_vector.size();
    size_t chunk_size = STANDARD_VECTOR_SIZE;
    size_t idx = global.next_idx.fetch_add(chunk_size);
    local->my_offset = idx;
    local->my_end = std::min(idx + chunk_size, total);
    return local;
}

SourceResultType PhysicalGroupJoin::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {

    static auto start = std::chrono::high_resolution_clock::now();

    auto &global = input.global_state.Cast<GroupJoinGlobalSourceState>();
    auto &local = input.local_state.Cast<GroupJoinLocalSourceState>();
    auto &result_vector = global.sink_state->result_vector;
    size_t total = result_vector.size();
    if (local.my_offset >= total) {
        return SourceResultType::FINISHED;
    }
    size_t rows_to_output = local.my_end - local.my_offset;
    if (chunk.ColumnCount() == 0) {
        vector<LogicalType> result_types = {LogicalType::INTEGER, LogicalType::HUGEINT};
        chunk.Initialize(Allocator::Get(context.client), result_types);
    }
    chunk.SetCardinality(rows_to_output);
    for (size_t row_idx = 0; row_idx < rows_to_output; ++row_idx) {
        const auto &entry = result_vector[local.my_offset + row_idx];
        chunk.data[0].SetValue(row_idx, Value::INTEGER(entry.first));
        chunk.data[1].SetValue(row_idx, Value::DOUBLE(entry.second));
    }
    // Prepare next range for this thread
    size_t chunk_size = STANDARD_VECTOR_SIZE;
    size_t idx = global.next_idx.fetch_add(chunk_size);
    local.my_offset = idx;
    local.my_end = std::min(idx + chunk_size, total);
    if (local.my_offset >= total) {
        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> elapsed = end - start;
        std::cout << ">> Total time taken for GetData(): " << elapsed.count() << " seconds" << std::endl;
        return SourceResultType::FINISHED;
    }
    return SourceResultType::HAVE_MORE_OUTPUT;
}

SinkFinalizeType PhysicalGroupJoin::Finalize(Pipeline &pipeline, Event &event, ClientContext &context, OperatorSinkFinalizeInput &input) const {
    auto &global_state = input.global_state.Cast<GroupJoinGlobalSinkState>();
    if (!global_state.left_scanned || !global_state.right_scanned) {
        return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
    }
    if (global_state.finalized) {
        return SinkFinalizeType::READY;
    }
    global_state.finalized = true;
    final_results.clear();
    PerformEqualityAggregation(global_state, final_results);
    StoreResultsAsVector(global_state);
    return SinkFinalizeType::READY;
}

InsertionOrderPreservingMap<string> PhysicalGroupJoin::ParamsToString() const {
    InsertionOrderPreservingMap<string> result;
    result["Join Type"] = EnumUtil::ToString(join_type);
    result["Join Condition"] = condition->GetName();
    auto &grps = groups;
    auto &aggr = aggregates;
    string groups_info;
    for (idx_t i = 0; i < groups.size(); i++) {
        if (i > 0) {
            groups_info += "\n";
        }
        groups_info += grps[i]->GetName();
    }
    result["Groups"] = groups_info;
    string aggregate_info;
    for (idx_t i = 0; i < aggr.size(); i++) {
        auto &aggregate = aggr[i]->Cast<BoundAggregateExpression>();
        if (i > 0) {
            aggregate_info += "\n";
        }
        aggregate_info += aggr[i]->GetName();
        if (aggregate.filter) {
            aggregate_info += " Filter: " + aggregate.filter->GetName();
        }
    }
    result["Aggregates"] = aggregate_info;
    return result;
}

class GroupJoinOperatorState : public OperatorState {
public:
    GroupJoinOperatorState(ClientContext &context, const PhysicalGroupJoin &op)
        : group_executor(context) {
        for (auto &group : op.groups) {
            group_executor.AddExpression(*group);
        }
    }

    //! Expression executor for the grouping expression
    ExpressionExecutor group_executor;
    //! DataChunk to store the grouping keys
    DataChunk group_chunk;
    //! DataChunk to store the aggregates fetched from the hash table
    DataChunk aggregate_chunk;
};

unique_ptr<OperatorState> PhysicalGroupJoin::GetOperatorState(ExecutionContext &context) const {

    std::cout << "Inside GetOperatorState()" << std::endl;

    auto &global_state = sink_state->Cast<GroupJoinGlobalSinkState>();
    auto state = make_uniq<GroupJoinOperatorState>(context.client, *this);

    // Initialize the group chunk and aggregate chunk
    state->group_chunk.InitializeEmpty(global_state.group_types);
    state->aggregate_chunk.InitializeEmpty(global_state.aggregate_return_types);

    std::cout << "Outside GetOperatorState()" << std::endl;
    return std::move(state);
}

OperatorResultType PhysicalGroupJoin::ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                      GlobalOperatorState &gstate, OperatorState &state) const {
    // Minimal stub, adjust as needed for your logic
    return OperatorResultType::NEED_MORE_INPUT;
}


void PhysicalGroupJoin::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
    D_ASSERT(children.size() == 2);
    auto &left_child = children[0];
    auto &right_child = children[1];
    auto &state = meta_pipeline.GetState();
    state.SetPipelineSource(current, *this);
    MetaPipeline &left_meta = meta_pipeline.CreateChildMetaPipeline(current, *this);
    left_meta.Build(*left_child);
    MetaPipeline &right_meta = meta_pipeline.CreateChildMetaPipeline(current, *this);
    right_meta.Build(*right_child);
    shared_ptr<Pipeline> current_ptr = current.shared_from_this();
    shared_ptr<Pipeline> &left_ptr = left_meta.GetBasePipeline();
    shared_ptr<Pipeline> &right_ptr = right_meta.GetBasePipeline();
    current_ptr->AddDependency(left_ptr);
    current_ptr->AddDependency(right_ptr);
}

} // namespace duckdb
