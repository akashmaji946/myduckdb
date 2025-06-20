#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include<iostream>
namespace duckdb {

bool canReplaceByGroupJoin1(LogicalOperator &op){
	if(op.type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) 
		return false;

	auto &groupby = op.Cast<LogicalAggregate>();
	
	if( groupby.groups.size() > 0 && 
	   	(
			( groupby.children[0] && 
			  groupby.children[0]->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) ||
			( groupby.children[0]->children[0] && 
			  groupby.children[0]->children[0]->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN)
		)
	  ){
		return true;
	}
	return false;
}

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalProjection &op) {
	D_ASSERT(op.children.size() == 1);

	if (canReplaceByGroupJoin1(*op.children[0])) {
        std::cout << ">>>>>>Group Join Candidate Found!<<<<<<" << std::endl;

        // Generate the GroupJoin plan
        auto group_join_plan = PlanGroupJoin((*op.children[0]).Cast<LogicalAggregate>());
        group_join_plan->estimated_cardinality = (*op.children[0]).estimated_cardinality;

        // Ensure the Projection operator uses the same schema as GroupJoin
        auto projection_types = group_join_plan->types; // Use the same types as GroupJoin
        auto projection_plan = make_uniq<PhysicalProjection>(op.types, vector<unique_ptr<Expression>>(), op.estimated_cardinality);

        // Connect the Projection operator to the GroupJoin output
        projection_plan->children.push_back(std::move(group_join_plan));
        return projection_plan;
    }

	// std::cout << "=======================Showing children for " << op.GetName() << std::endl;
	for(auto &child:  op.children){
			// std::cout << child->GetName () << std::endl;
	}
	// std::cout << "=======================\n";
	auto plan = CreatePlan(*op.children[0]);
	// return plan;

#ifdef DEBUG
	for (auto &expr : op.expressions) {
		D_ASSERT(!expr->IsWindow());
		D_ASSERT(!expr->IsAggregate());
	}
#endif
	if (plan->types.size() == op.types.size()) {
		// std::cout << "INSIDE===========================================================>\n";
		// check if this projection can be omitted entirely
		// this happens if a projection simply emits the columns in the same order
		// e.g. PROJECTION(#0, #1, #2, #3, ...)
		bool omit_projection = true;
		for (idx_t i = 0; i < op.types.size(); i++) {
			if (op.expressions[i]->type == ExpressionType::BOUND_REF) {
				auto &bound_ref = op.expressions[i]->Cast<BoundReferenceExpression>();
				if (bound_ref.index == i) {
					continue;
				}
			}
			omit_projection = false;
			break;
		}
		if (omit_projection) {
			// the projection only directly projects the child' columns: omit it entirely
			// std::cout << "INNER RETURN===========================================================>\n";
			return plan;
		}
	}

	auto projection = make_uniq<PhysicalProjection>(op.types, std::move(op.expressions), op.estimated_cardinality);
	projection->children.push_back(std::move(plan));
	// std::cout << "OUTER RETURN===========================================================>\n";
	return std::move(projection);
}

} // namespace duckdb
