
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/common/enums/operator_result_type.hpp"
#include "duckdb/common/enums/physical_operator_type.hpp"
#include "duckdb/common/enums/explain_format.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/optimizer/join_order/join_node.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/execution/physical_operator_states.hpp"
#include "duckdb/common/enums/order_preservation_type.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/execution/operator/join/physical_am_us_join.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
// #include "duckdb/common/field_writer.hpp"
#include <mutex>
#include <iostream>

namespace duckdb {


class JoinGroupByGlobalState : public GlobalSourceState {
public:
    JoinGroupByGlobalState() : finished(false) {}

    //! Tracks if all data has been processed
    bool finished;
};

class PhysicalAmUsGroupBy : public PhysicalOperator {
public:
    PhysicalAmUsGroupBy(LogicalOperator &op, unique_ptr<PhysicalOperator> left, unique_ptr<PhysicalOperator> right,
                        vector<JoinCondition> join_conditions, vector<unique_ptr<Expression>> group_expressions,
                        vector<unique_ptr<Expression>> aggregate_expressions, JoinType join_type,
                        idx_t estimated_cardinality)
        : PhysicalOperator(PhysicalOperatorType::GROUPJOIN_GROUP_BY, op.types, estimated_cardinality),
          join_conditions(std::move(join_conditions)), group_expressions(std::move(group_expressions)),
          aggregate_expressions(std::move(aggregate_expressions)), join_type(join_type) {
        children.push_back(std::move(left));
        children.push_back(std::move(right));
    }

    vector<JoinCondition> join_conditions;
    vector<unique_ptr<Expression>> group_expressions;
    vector<unique_ptr<Expression>> aggregate_expressions;
    JoinType join_type;

    // Execute the operator
    SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override {
    // Cast to the custom global state
    auto &state = input.global_state.Cast<JoinGroupByGlobalState>();

    // If already finished, return no more output
    if (state.finished) {
        chunk.SetCardinality(0);
        return SourceResultType::FINISHED;
    }

    // Perform the join operation
    DataChunk join_result;
    PerformJoin(context, chunk, join_result);

    // Perform the group-by operation on the join result
    DataChunk grouped_result;
    PerformGroupBy(context, join_result, grouped_result);

    // Move the grouped result to the output chunk
    chunk.Move(grouped_result);

    // Mark as finished after producing this chunk
    state.finished = true;
    return SourceResultType::FINISHED;
    }


    unique_ptr<GlobalOperatorState> GetGlobalOperatorState(ClientContext &context) const override {
        return make_uniq<GlobalOperatorState>();
    }

    unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override {
      return make_uniq<JoinGroupByGlobalState>();
    }

private:
    // Step 1: Perform AMUS_JOIN
    void PerformJoin(ExecutionContext &context, DataChunk &input_chunk, DataChunk &join_result) const {
        ExpressionExecutor join_executor(context.client);
        for (auto &condition : join_conditions) {
            join_executor.AddExpression(*condition.right);
        }
        join_result.Initialize(Allocator::Get(context.client), this->types);

        join_executor.Execute(input_chunk, join_result);
    }

    // Step 2: Perform GROUP BY on the result of the join
    void PerformGroupBy(ExecutionContext &context, DataChunk &join_result, DataChunk &grouped_result) const {
        ExpressionExecutor group_executor(context.client);
        for (auto &group_expr : group_expressions) {
            group_executor.AddExpression(*group_expr);
        }

        DataChunk group_keys;
        group_keys.Initialize(Allocator::Get(context.client), GetGroupKeyTypes());
        group_executor.Execute(join_result, group_keys);

        AggregateResult(context, group_keys, join_result, grouped_result);
    }

    // Step 3: Compute aggregates for each group
    void AggregateResult(ExecutionContext &context, DataChunk &group_keys, DataChunk &join_result,
                         DataChunk &grouped_result) const {
        // Placeholder: Replace with actual aggregation logic
        grouped_result.Initialize(Allocator::Get(context.client), this->types);

        // TODO: Perform aggregation on `join_result` grouped by `group_keys`
        // Use a hash table or similar structure to store group-wise aggregates
    }

    // Utility to extract group key types
    vector<LogicalType> GetGroupKeyTypes() const {
        vector<LogicalType> group_key_types;
        for (auto &expr : group_expressions) {
            group_key_types.push_back(expr->return_type);
        }
        return group_key_types;
    }
};

} // namespace duckdb
