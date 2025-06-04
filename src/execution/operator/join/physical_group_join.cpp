#include "duckdb/execution/operator/join/physical_group_join.hpp"

#include "duckdb/execution/aggregate_hashtable.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
// #include "bound_aggregate_expression.hpp" 
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include <iostream>

namespace duckdb {

PhysicalGroupJoin::PhysicalGroupJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                     unique_ptr<PhysicalOperator> right, unique_ptr<Expression> condition_p,
                                     JoinType join_type, idx_t estimated_cardinality,
                                     vector<unique_ptr<Expression>> &groups_p,
                                     vector<unique_ptr<Expression>> &aggregates_p)
    : PhysicalJoin(op, PhysicalOperatorType::GROUP_JOIN, join_type, estimated_cardinality),
      condition(std::move(condition_p)), groups(std::move(groups_p)), aggregates(std::move(aggregates_p)) {

    std::cout << "Inside PhysicalGroupJoin()" << std::endl;    

    children.push_back(std::move(right));
    children.push_back(std::move(left));
    D_ASSERT(join_type != JoinType::MARK);
    D_ASSERT(join_type != JoinType::SINGLE);

    // Extract grouping attributes from the join condition
    if (condition->expression_class == ExpressionClass::BOUND_COMPARISON) {
        auto &comp_expr = condition->Cast<BoundComparisonExpression>();
        if (comp_expr.type == ExpressionType::COMPARE_EQUAL) {
            // For equijoins, we can extract the grouping attributes
            // Ensure both sides are bound references
          
        } else {
            throw NotImplementedException("PhysicalGroupJoin only supports equijoins");
        }
    } else {
        throw NotImplementedException("PhysicalGroupJoin only supports bound comparison expressions");
    }
}

class GroupJoinGlobalSinkState : public GlobalSinkState {
public:
    //! The hash table that will be used to store the grouping attributes and aggregates
    unique_ptr<GroupedAggregateHashTable> hash_table;

    //! The types of the grouping columns
    vector<LogicalType> group_types;

    //! The indices of the grouping columns in the right child's output
    vector<idx_t> grouping_indices;

    //! The aggregate objects
    vector<AggregateObject> aggregate_objects;

    //! The payload types (types of the aggregates)
    vector<LogicalType> aggregate_return_types;

    //! Buffers to store input from left and right children
    ChunkCollection left_data;
    ChunkCollection right_data;
};


class GroupJoinLocalSinkState : public LocalSinkState {
public:
    GroupJoinLocalSinkState(Allocator &allocator, const PhysicalGroupJoin &op, ClientContext &context)
        : aggregate_executor(context) {

        std::cout << "Inside GroupJoinLocalSinkState()" << std::endl;
        // Prepare the aggregate executor
        // for (auto &aggr_expr : op.aggregates) {
        //     aggregate_executor.AddExpression(*aggr_expr);
        // }

        for (auto &aggr_expr : op.aggregates) {
            auto &bound_aggr = aggr_expr->Cast<BoundAggregateExpression>();
            for (auto &child : bound_aggr.children) {
                aggregate_executor.AddExpression(*child);
            }
        }


        // Initialize the aggregate input chunk
        vector<LogicalType> aggregate_input_types;
        for (auto &aggr_expr : op.aggregates) {
            auto &bound_aggr = aggr_expr->Cast<BoundAggregateExpression>();
            for (auto &child : bound_aggr.children) {
                aggregate_input_types.push_back(child->return_type);
            }
        }
        aggregate_input_chunk.Initialize(allocator, aggregate_input_types);
        
        std::cout << "Outside GroupJoinLocalSinkState()" << std::endl;

    }

    //! Expression executor for the aggregates
    ExpressionExecutor aggregate_executor;
    //! DataChunk to hold the evaluated aggregate inputs
    DataChunk aggregate_input_chunk;
};

unique_ptr<GlobalSinkState> PhysicalGroupJoin::GetGlobalSinkState(ClientContext &context) const {

    std::cout << "Inside GetGlobalSinkState()" << std::endl;

    auto state = make_uniq<GroupJoinGlobalSinkState>();

    // Get the types from the left and right children
    auto &left_types = children[0]->types;
    std::cout << children[0]->ToString() << std::endl;
    std::cout << "Inside GetGlobalSinkState()  1" << std::endl;

    auto &right_types = children[1]->types;
    std::cout << children[1]->ToString() << std::endl;
    std::cout << "Inside GetGlobalSinkState()  2" << std::endl;

    std::cout << right_types[0].ToString() << std::endl;

    // Extract the bound comparison expression
    auto &comp_expr = condition->Cast<BoundComparisonExpression>();
    auto &left_expr = comp_expr.left->Cast<BoundReferenceExpression>();
    auto &right_expr = comp_expr.right->Cast<BoundReferenceExpression>();

    std::cout << "SIZE:"<< groups.size() << std::endl;
    // Adjust the grouping indices to be relative to the right child's output
    for (auto &group : groups) {
        // if (group->expression_class != ExpressionClass::BOUND_REFERENCE) {
        //     throw NotImplementedException("PhysicalGroupJoin only supports bound reference grouping expressions");
        // }
        auto &bound_ref = group->Cast<BoundReferenceExpression>();
        idx_t idx = bound_ref.index;
        // if (idx >= right_types.size()) {
        //     throw InternalException("Grouping attribute index out of bounds in right child's types");
        // }
        
        state->grouping_indices.push_back(idx);
        std::cout << "Inside GetGlobalSinkState()  2.seclast" << std::endl;
        std::cout << idx << std::endl;
        state->group_types.push_back(right_types[idx]);
        std::cout << "Inside GetGlobalSinkState()  2.last" << std::endl;
    }

    std::cout << "Inside GetGlobalSinkState()  3" << std::endl;
    // Prepare the aggregate objects
    for (auto &aggr_expr : aggregates) {
        if (aggr_expr->expression_class != ExpressionClass::BOUND_AGGREGATE) {
            throw NotImplementedException("Expected bound aggregate expression");
        }
        auto &bound_aggr = aggr_expr->Cast<BoundAggregateExpression>();
        AggregateObject aggr_obj(&bound_aggr);
        state->aggregate_objects.push_back(aggr_obj);
        state->aggregate_return_types.push_back(bound_aggr.return_type);
    }

    // Initialize the hash table
    state->hash_table = make_uniq<GroupedAggregateHashTable>(context, Allocator::Get(context),
                                                             state->group_types, vector<LogicalType>(),
                                                             state->aggregate_objects);
    
    return std::move(state);
    std::cout << "Inside GetGlobalSinkState() END" << std::endl;

}

unique_ptr<LocalSinkState> PhysicalGroupJoin::GetLocalSinkState(ExecutionContext &context) const {
    return make_uniq<GroupJoinLocalSinkState>(Allocator::Get(context.client), *this, context.client);
}

SinkResultType PhysicalGroupJoin::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
    
    std::cout << "Inside Sink()" << std::endl;

    auto &global_state = input.global_state.Cast<GroupJoinGlobalSinkState>();
    auto &local_state = input.local_state.Cast<GroupJoinLocalSinkState>();

    // Prepare the grouping keys
    DataChunk groups;
    groups.InitializeEmpty(global_state.group_types);
    for (idx_t i = 0; i < global_state.grouping_indices.size(); i++) {
        groups.data[i].Reference(chunk.data[global_state.grouping_indices[i]]);
    }
    groups.SetCardinality(chunk.size());

    // Evaluate the aggregate inputs
    local_state.aggregate_executor.Execute(chunk, local_state.aggregate_input_chunk);

    // Add to hash table
    unsafe_vector<idx_t> filter; // No filter
    global_state.hash_table->AddChunk(groups, local_state.aggregate_input_chunk, filter);

    return SinkResultType::NEED_MORE_INPUT;
}

// SinkFinalizeType PhysicalGroupJoin::Finalize(Pipeline &pipeline, Event &event, ClientContext &context, OperatorSinkFinalizeInput &input) const {
//     auto &global_state = input.global_state.Cast<GroupJoinGlobalSinkState>();

//     std::cout << "PhysicalGroupJoin::Finalize — Performing nested loop join and group-by" << std::endl;

//     // For simplicity, get the total column count and prepare type vector
//     const auto &left_types = children[0]->types;
//     const auto &right_types = children[1]->types;

//     vector<LogicalType> join_types = left_types;
//     join_types.insert(join_types.end(), right_types.begin(), right_types.end());

//     // Buffers for scan
//     DataChunk left_chunk, right_chunk;

//     global_state.left_data.InitializeScan();
//     while (global_state.left_data.Scan(left_chunk)) {
//         global_state.right_data.InitializeScan();
//         while (global_state.right_data.Scan(right_chunk)) {
//             // Nested loop join
//             for (idx_t i = 0; i < left_chunk.size(); ++i) {
//                 for (idx_t j = 0; j < right_chunk.size(); ++j) {
//                     // Join one row from left and right into joined_chunk
//                     DataChunk joined_chunk;
//                     joined_chunk.Initialize(Allocator::Get(context), join_types);
//                     for (idx_t col = 0; col < left_chunk.ColumnCount(); ++col) {
//                         joined_chunk.data[col].Reference(left_chunk.data[col]);
//                         joined_chunk.data[col].Slice(i, 1);
//                     }
//                     for (idx_t col = 0; col < right_chunk.ColumnCount(); ++col) {
//                         idx_t out_col = left_chunk.ColumnCount() + col;
//                         joined_chunk.data[out_col].Reference(right_chunk.data[col]);
//                         joined_chunk.data[out_col].Slice(j, 1);
//                     }
//                     joined_chunk.SetCardinality(1);

//                     // Optionally: evaluate the join condition (if not always true)
//                     if (condition) {
//                         ExpressionExecutor condition_executor(context);
//                         condition_executor.AddExpression(*condition);
//                         DataChunk condition_result;
//                         condition_result.Initialize(Allocator::Get(context), {LogicalType::BOOLEAN});
//                         condition_executor.Execute(joined_chunk, condition_result);
//                         auto result_ptr = FlatVector::GetData<bool>(condition_result.data[0]);
//                         if (!result_ptr[0]) {
//                             continue;
//                         }
//                     }

//                     // Extract group keys
//                     DataChunk group_chunk;
//                     group_chunk.InitializeEmpty(global_state.group_types);
//                     for (idx_t g = 0; g < global_state.grouping_indices.size(); ++g) {
//                         idx_t idx = global_state.grouping_indices[g];
//                         group_chunk.data[g].Reference(joined_chunk.data[idx]);
//                     }
//                     group_chunk.SetCardinality(1);

//                     // Extract aggregate inputs
//                     DataChunk aggr_input_chunk;
//                     aggr_input_chunk.Initialize(Allocator::Get(context), global_state.hash_table->GetTypes());
//                     ExpressionExecutor aggr_exec(context);
//                     for (auto &aggr_expr : aggregates) {
//                         aggr_exec.AddExpression(*aggr_expr);
//                     }
//                     aggr_exec.Execute(joined_chunk, aggr_input_chunk);

//                     // Add to hash table
//                     unsafe_vector<idx_t> filter;
//                     global_state.hash_table->AddChunk(group_chunk, aggr_input_chunk, filter);
//                 }
//             }
//         }
//     }

//     std::cout << "PhysicalGroupJoin::Finalize — Completed nested loop and aggregation" << std::endl;

//     return SinkFinalizeType::READY;
// }


SinkFinalizeType PhysicalGroupJoin::Finalize(Pipeline &pipeline, Event &event, ClientContext &context, OperatorSinkFinalizeInput &input) const {
    auto &global_state = input.global_state.Cast<GroupJoinGlobalSinkState>();

    std::cout << "PhysicalGroupJoin::Finalize — Performing nested loop join and group-by" << std::endl;

    const auto &left_types = children[0]->types;
    const auto &right_types = children[1]->types;

    vector<LogicalType> join_types = left_types;
    join_types.insert(join_types.end(), right_types.begin(), right_types.end());

    DataChunk left_chunk, right_chunk;

    global_state.left_data.InitializeScan();
    while (global_state.left_data.Scan(left_chunk)) {
        global_state.right_data.InitializeScan();
        while (global_state.right_data.Scan(right_chunk)) {

            DataChunk joined_chunk;
            joined_chunk.Initialize(Allocator::Get(context), join_types);
            joined_chunk.SetCardinality(1);

            for (idx_t i = 0; i < left_chunk.size(); ++i) {
                for (idx_t j = 0; j < right_chunk.size(); ++j) {
                    // Manually copy single row from left_chunk into joined_chunk
                    for (idx_t col = 0; col < left_chunk.ColumnCount(); ++col) {
                        joined_chunk.data[col].SetValue(0, left_chunk.data[col].GetValue(i));
                    }

                    // Manually copy single row from right_chunk into joined_chunk
                    for (idx_t col = 0; col < right_chunk.ColumnCount(); ++col) {
                        idx_t out_col = left_chunk.ColumnCount() + col;
                        joined_chunk.data[out_col].SetValue(0, right_chunk.data[col].GetValue(j));
                    }

                    // Evaluate join condition if any
                    if (condition) {
                        ExpressionExecutor condition_executor(context);
                        condition_executor.AddExpression(*condition);
                        DataChunk condition_result;
                        condition_result.Initialize(Allocator::Get(context), {LogicalType::BOOLEAN});
                        condition_executor.Execute(joined_chunk, condition_result);
                        auto result_ptr = FlatVector::GetData<bool>(condition_result.data[0]);
                        if (!result_ptr[0]) {
                            continue;
                        }
                    }

                    // Extract group keys
                    DataChunk group_chunk;
                    group_chunk.InitializeEmpty(global_state.group_types);
                    group_chunk.SetCardinality(1);
                    for (idx_t g = 0; g < global_state.grouping_indices.size(); ++g) {
                        idx_t idx = global_state.grouping_indices[g];
                        group_chunk.data[g].SetValue(0, joined_chunk.data[idx].GetValue(0));
                    }

                    // Extract aggregate inputs
                    DataChunk aggr_input_chunk;
                    // aggr_input_chunk.Initialize(Allocator::Get(context), global_state.hash_table->GetTypes());
                    
                vector<LogicalType> aggr_input_types;
                for (auto &aggr_expr : aggregates) {
                    auto *agg = dynamic_cast<BoundAggregateExpression*>(aggr_expr.get());
                    if (agg && !agg->children.empty()) {
                        // Use the return type of the first child as input type to aggregate
                        aggr_input_types.push_back(agg->children[0]->return_type);
                    } else {
                        // If not aggregate or no children, fallback to the expression's return type
                        aggr_input_types.push_back(aggr_expr->return_type);
                    }
                }

                aggr_input_chunk.Initialize(Allocator::Get(context), aggr_input_types);

                    ExpressionExecutor aggr_exec(context);
                    for (auto &aggr_expr : aggregates) {
                        aggr_exec.AddExpression(*aggr_expr);
                    }
                    aggr_exec.Execute(joined_chunk, aggr_input_chunk);

                    // Add to hash table
                    unsafe_vector<idx_t> filter;
                    global_state.hash_table->AddChunk(group_chunk, aggr_input_chunk, filter);
                }
            }
        }
    }

    std::cout << "PhysicalGroupJoin::Finalize — Completed nested loop and aggregation" << std::endl;

    return SinkFinalizeType::READY;
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
                                                      GlobalOperatorState &gstate_p, OperatorState &state_p) const {

    // auto &global_state = sink_state->Cast<GroupJoinGlobalSinkState>();
    // auto &local_state = input.local_state.Cast<GroupJoinLocalSinkState>();

    // // Probe join hash table with left input
    // DataChunk join_result;
    // global_state.join_hash_table->Probe(input, join_result);

    // if (join_result.size() == 0) {
    //     return OperatorResultType::NEED_MORE_INPUT; // no matches for this chunk
    // }

    // // Extract grouping keys and aggregate inputs from join result
    // DataChunk groups_chunk;
    // groups_chunk.Initialize(global_state.group_types);

    // for (idx_t i = 0; i < global_state.grouping_indices.size(); i++) {
    //     groups_chunk.data[i].Reference(join_result.data[global_state.grouping_indices[i]]);
    // }
    // groups_chunk.SetCardinality(join_result.size());

    // // Evaluate aggregates input expressions on join result (reuse local_state.aggregate_executor)
    // local_state.aggregate_executor.Execute(join_result, local_state.aggregate_input_chunk);

    // // Add to aggregate hash table
    // vector<idx_t> no_filter; // empty filter vector
    // global_state.hash_table->AddChunk(groups_chunk, local_state.aggregate_input_chunk, no_filter);

    // // Output columns: joined columns + aggregates after grouping
    // // For simplicity, output the joined columns only here or aggregated output in GetData()

    // // Just forward joined columns for now
    // chunk.Reference(join_result);

    return OperatorResultType::NEED_MORE_INPUT;

}

unique_ptr<GlobalSourceState> PhysicalGroupJoin::GetGlobalSourceState(ClientContext &context) const {
    return nullptr;
}

unique_ptr<LocalSourceState> PhysicalGroupJoin::GetLocalSourceState(ExecutionContext &context,
                                                                    GlobalSourceState &gstate) const {
    return nullptr;
}

// SourceResultType PhysicalGroupJoin::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
//     auto &global_state = sink_state->Cast<GroupJoinGlobalSinkState>();

//     if (!input.state) {
//         input.state = make_uniq<AggregateHTScanState>(global_state.hash_table->GetScanState());
//     }
//     // input.state is unique_ptr<AggregateHTScanState>, so dereference to get reference
//     auto &scan_state = *input.state;

//     idx_t count = global_state.hash_table->Scan(scan_state, chunk);

//     if (count == 0) {
//         return SourceResultType::FINISHED;
//     }
//     chunk.SetCardinality(count);
//     return SourceResultType::HAVE_MORE_OUTPUT;
// }

SourceResultType PhysicalGroupJoin::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
    // Retrieve the global sink state from input or context
    auto &global_state = input.global_state.Cast<GroupJoinGlobalSinkState>();

    // if (!input.state) {
    //     // Initialize scan state for the hash table
    //     input.state = make_uniq<GroupedAggregateHashTableScanState>(global_state.hash_table->GetScanState());
    // }
    auto &scan_state = *make_uniq<GroupedAggregateHashTableScanState>();

    // Scan the hash table to fill the chunk
    idx_t count = global_state.hash_table->Scan(chunk, scan_state, STANDARD_VECTOR_SIZE);

    if (count == 0) {
        return SourceResultType::FINISHED;
    }

    chunk.SetCardinality(count);
    return SourceResultType::HAVE_MORE_OUTPUT;
}

// SourceResultType PhysicalGroupJoin::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
//     // auto &global_state = input.global_state->Cast<GroupJoinGlobalSinkState>();
//     auto &global_state = input.global_state.Cast<GroupJoinGlobalSinkState>();

//     if (!input.local_state) {
//         input.local_state = make_uniq<GroupedAggregateHashTableScanState>();
//     }
//     auto &scan_state = input.local_state;

//     idx_t count = global_state.hash_table->Scan(chunk, scan_state, STANDARD_VECTOR_SIZE);

//     if (count == 0) {
//         return SourceResultType::FINISHED;
//     }

//     chunk.SetCardinality(count);
//     return SourceResultType::HAVE_MORE_OUTPUT;
// }





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

} // namespace duckdb

