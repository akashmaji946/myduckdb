#include "duckdb/execution/operator/join/physical_group_join.hpp"

#include "duckdb/execution/aggregate_hashtable.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/function/aggregate_function.hpp"

#include <iostream>

namespace duckdb {

PhysicalGroupJoin::PhysicalGroupJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                     unique_ptr<PhysicalOperator> right, unique_ptr<Expression> condition_p,
                                     JoinType join_type, idx_t estimated_cardinality,
                                     vector<unique_ptr<Expression>> &groups_p,
                                     vector<unique_ptr<Expression>> &aggregates_p)
    : PhysicalJoin(op, PhysicalOperatorType::GROUP_JOIN, join_type, estimated_cardinality),
      condition(std::move(condition_p)), groups(std::move(groups_p)), aggregates(std::move(aggregates_p)) {
    children.push_back(std::move(left));
    children.push_back(std::move(right));
    D_ASSERT(join_type != JoinType::MARK);
    D_ASSERT(join_type != JoinType::SINGLE);

    // Extract grouping attributes from the join condition
    if (condition->expression_class == ExpressionClass::BOUND_COMPARISON) {
        auto &comp_expr = condition->Cast<BoundComparisonExpression>();
        if (comp_expr.type == ExpressionType::COMPARE_EQUAL) {
            // For equijoins, we can extract the grouping attributes
            // Ensure both sides are bound references
            // if (comp_expr.left->expression_class == ExpressionClass::BOUND_REFERENCE &&
            //     comp_expr.right->expression_class == ExpressionClass::BOUND_REFERENCE) {
            //     // Valid equijoin on bound references
            // } else {
            //     throw NotImplementedException("PhysicalGroupJoin only supports equijoins on bound references");
            // }
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
};

class GroupJoinLocalSinkState : public LocalSinkState {
public:
    GroupJoinLocalSinkState(Allocator &allocator, const PhysicalGroupJoin &op, ClientContext &context)
        : aggregate_executor(context) {
        // Prepare the aggregate executor
        for (auto &aggr_expr : op.aggregates) {
            aggregate_executor.AddExpression(*aggr_expr);
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
    }

    //! Expression executor for the aggregates
    ExpressionExecutor aggregate_executor;
    //! DataChunk to hold the evaluated aggregate inputs
    DataChunk aggregate_input_chunk;
};

unique_ptr<GlobalSinkState> PhysicalGroupJoin::GetGlobalSinkState(ClientContext &context) const {
    auto state = make_uniq<GroupJoinGlobalSinkState>();

    // Get the types from the left and right children
    auto &left_types = children[0]->types;
    auto &right_types = children[1]->types;

    // Extract the bound comparison expression
    auto &comp_expr = condition->Cast<BoundComparisonExpression>();
    auto &left_expr = comp_expr.left->Cast<BoundReferenceExpression>();
    auto &right_expr = comp_expr.right->Cast<BoundReferenceExpression>();

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
        state->group_types.push_back(right_types[idx]);
    }

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
}

unique_ptr<LocalSinkState> PhysicalGroupJoin::GetLocalSinkState(ExecutionContext &context) const {
    return make_uniq<GroupJoinLocalSinkState>(Allocator::Get(context.client), *this, context.client);
}

SinkResultType PhysicalGroupJoin::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
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

SinkFinalizeType PhysicalGroupJoin::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                             OperatorSinkFinalizeInput &input) const {
    // No finalization required
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
    auto &global_state = sink_state->Cast<GroupJoinGlobalSinkState>();
    auto state = make_uniq<GroupJoinOperatorState>(context.client, *this);

    // Initialize the group chunk and aggregate chunk
    state->group_chunk.InitializeEmpty(global_state.group_types);
    state->aggregate_chunk.InitializeEmpty(global_state.aggregate_return_types);

    return std::move(state);
}

OperatorResultType PhysicalGroupJoin::ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                      GlobalOperatorState &gstate_p, OperatorState &state_p) const {
    D_ASSERT(input.size() > 0);
    auto &state = state_p.Cast<GroupJoinOperatorState>();
    auto &global_state = sink_state->Cast<GroupJoinGlobalSinkState>();

    // Evaluate the grouping keys from the input
    DataChunk &groups = state.group_chunk;
    groups.Reset();
    state.group_executor.Execute(input, groups);

    // Fetch the aggregates from the hash table
    DataChunk &aggregates = state.aggregate_chunk;
    aggregates.Reset();

    global_state.hash_table->FetchAggregates(groups, aggregates);

    // Construct the result chunk
    idx_t input_cols = input.ColumnCount();
    idx_t aggregate_cols = aggregates.ColumnCount();

    vector<LogicalType> result_types;
    result_types.reserve(input_cols + aggregate_cols);

    // Append input columns to the result
    for (idx_t col_idx = 0; col_idx < input_cols; col_idx++) {
        result_types.push_back(input.data[col_idx].GetType());
    }

    // Append aggregates to the result
    for (idx_t col_idx = 0; col_idx < aggregate_cols; col_idx++) {
        result_types.push_back(aggregates.data[col_idx].GetType());
    }

    chunk.InitializeEmpty(result_types);
    chunk.SetCardinality(input.size());

    // Set data for input columns
    for (idx_t col_idx = 0; col_idx < input_cols; col_idx++) {
        chunk.data[col_idx].Reference(input.data[col_idx]);
    }

    // Set data for aggregates
    for (idx_t col_idx = 0; col_idx < aggregate_cols; col_idx++) {
        chunk.data[input_cols + col_idx].Reference(aggregates.data[col_idx]);
    }

    return OperatorResultType::NEED_MORE_INPUT;
}

unique_ptr<GlobalSourceState> PhysicalGroupJoin::GetGlobalSourceState(ClientContext &context) const {
    return nullptr;
}

unique_ptr<LocalSourceState> PhysicalGroupJoin::GetLocalSourceState(ExecutionContext &context,
                                                                    GlobalSourceState &gstate) const {
    return nullptr;
}

SourceResultType PhysicalGroupJoin::GetData(ExecutionContext &context, DataChunk &chunk,
                                            OperatorSourceInput &input) const {
    return SourceResultType::FINISHED;
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

} // namespace duckdb