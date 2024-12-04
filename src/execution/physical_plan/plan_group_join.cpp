#include "duckdb/execution/operator/join/perfect_hash_join_executor.hpp"
#include "duckdb/execution/operator/join/physical_cross_product.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/execution/operator/join/physical_group_join.hpp"
#include "duckdb/execution/operator/join/physical_iejoin.hpp"
#include "duckdb/execution/operator/join/physical_nested_loop_join.hpp"
#include "duckdb/execution/operator/join/physical_piecewise_merge_join.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/execution/operator/join/physical_blockwise_nl_join.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

#include <iostream>

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::PlanGroupJoin(LogicalAggregate &op) {
    // Visit the children
    auto &join = op.children[0]->children[0]->Cast<LogicalComparisonJoin>();

    idx_t lhs_cardinality = join.children[0]->EstimateCardinality(context);
    idx_t rhs_cardinality = join.children[1]->EstimateCardinality(context);

    auto left = CreatePlan(*join.children[0]);
    auto right = CreatePlan(*join.children[1]);
    left->estimated_cardinality = lhs_cardinality;
    right->estimated_cardinality = rhs_cardinality;
    D_ASSERT(left && right);

    if (join.conditions.empty()) {
        // No conditions: insert a cross product
        return make_uniq<PhysicalCrossProduct>(op.types, std::move(left), std::move(right), op.estimated_cardinality);
    }

    unique_ptr<PhysicalOperator> plan;

    for (auto &cond : join.conditions) {
        RewriteJoinCondition(*cond.right, left->types.size());
    }
    auto condition = JoinCondition::CreateExpression(std::move(join.conditions));

    std::cout << "Group Join Everytime" << std::endl;

    // Pass the grouping and aggregate expressions from the LogicalAggregate operator
    plan = make_uniq<PhysicalGroupJoin>(op, std::move(left), std::move(right), std::move(condition),
                                        join.join_type, op.estimated_cardinality, op.groups, op.expressions);

    return plan;
}

} // namespace duckdb
