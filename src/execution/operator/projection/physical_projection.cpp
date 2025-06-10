#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

#include <iostream>
namespace duckdb {

class ProjectionState : public OperatorState {
public:
	explicit ProjectionState(ExecutionContext &context, const vector<unique_ptr<Expression>> &expressions)
	    : executor(context.client, expressions) {
	}

	ExpressionExecutor executor;

public:
	void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
		context.thread.profiler.Flush(op);
	}
};

PhysicalProjection::PhysicalProjection(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list,
                                       idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::PROJECTION, std::move(types), estimated_cardinality),
      select_list(std::move(select_list)) {
}

// OperatorResultType PhysicalProjection::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
//                                                GlobalOperatorState &gstate, OperatorState &state_p) const {
// 	auto &state = state_p.Cast<ProjectionState>();
// 	// chunk.Reset();							
// 	// chunk.Initialize(Allocator::Get(context.client), input.GetTypes());
// 	state.executor.Execute(input, chunk);

// 	std::cout << "Projection Input Chunk:=>" << input.ToString() << std::endl;
// 	std::cout << "Projection Output Chunk:=>" << chunk.ToString() << std::endl;
// 	std::cout << "Input Chunk Types:\n";
// 	for (auto &type : input.GetTypes()) {
// 		std::cout << type.ToString() << std::endl;
// 	}
// 	std::cout << "Output Chunk Types:\n";
// 	for (auto &type : chunk.GetTypes()) {
// 		std::cout << type.ToString() << std::endl;
// 	}											
// 	// std::cout << "=================================I am called=============================================\n";
// 	return OperatorResultType::NEED_MORE_INPUT;
// }

OperatorResultType PhysicalProjection::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                               GlobalOperatorState &gstate, OperatorState &state_p) const {
    auto &state = state_p.Cast<ProjectionState>();

    // Reset the output chunk
    chunk.Reset();

    // Initialize the output chunk with the same schema as the input chunk
    // chunk.Initialize(Allocator::Get(context.client), input.GetTypes());
	chunk.SetCardinality(input.size());


    // Copy values from the input chunk to the output chunk
    for (idx_t col_idx = 0; col_idx < input.ColumnCount(); ++col_idx) {
        for (idx_t row_idx = 0; row_idx < input.size(); ++row_idx) {
            chunk.data[col_idx].SetValue(row_idx, input.data[col_idx].GetValue(row_idx));
        }
    }

    // Debug the input and output chunks
    std::cout << "Projection Input Chunk:\n" << input.ToString() << std::endl;
    std::cout << "Projection Output Chunk:\n" << chunk.ToString() << std::endl;

	chunk.Verify();
    return OperatorResultType::NEED_MORE_INPUT;
}

unique_ptr<OperatorState> PhysicalProjection::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<ProjectionState>(context, select_list);
}


unique_ptr<PhysicalOperator>
PhysicalProjection::CreateJoinProjection(vector<LogicalType> proj_types, const vector<LogicalType> &lhs_types,
                                         const vector<LogicalType> &rhs_types, const vector<idx_t> &left_projection_map,
                                         const vector<idx_t> &right_projection_map, const idx_t estimated_cardinality) {
    vector<unique_ptr<Expression>> proj_selects;
    proj_selects.reserve(proj_types.size());

    // Directly reference columns from the input chunk
    for (idx_t i = 0; i < proj_types.size(); ++i) {
        proj_selects.emplace_back(make_uniq<BoundReferenceExpression>(proj_types[i], i));
    }

    return make_uniq<PhysicalProjection>(std::move(proj_types), std::move(proj_selects), estimated_cardinality);
}

// unique_ptr<PhysicalOperator>
// PhysicalProjection::CreateJoinProjection(vector<LogicalType> proj_types, const vector<LogicalType> &lhs_types,
//                                          const vector<LogicalType> &rhs_types, const vector<idx_t> &left_projection_map,
//                                          const vector<idx_t> &right_projection_map, const idx_t estimated_cardinality) {

// 	vector<unique_ptr<Expression>> proj_selects;
// 	proj_selects.reserve(proj_types.size());

// 	if (left_projection_map.empty()) {
// 		for (storage_t i = 0; i < lhs_types.size(); ++i) {
// 			proj_selects.emplace_back(make_uniq<BoundReferenceExpression>(lhs_types[i], i));
// 		}
// 	} else {
// 		for (auto i : left_projection_map) {
// 			proj_selects.emplace_back(make_uniq<BoundReferenceExpression>(lhs_types[i], i));
// 		}
// 	}
// 	const auto left_cols = lhs_types.size();

// 	if (right_projection_map.empty()) {
// 		for (storage_t i = 0; i < rhs_types.size(); ++i) {
// 			proj_selects.emplace_back(make_uniq<BoundReferenceExpression>(rhs_types[i], left_cols + i));
// 		}

// 	} else {
// 		for (auto i : right_projection_map) {
// 			proj_selects.emplace_back(make_uniq<BoundReferenceExpression>(rhs_types[i], left_cols + i));
// 		}
// 	}

// 	return make_uniq<PhysicalProjection>(std::move(proj_types), std::move(proj_selects), estimated_cardinality);
// }

InsertionOrderPreservingMap<string> PhysicalProjection::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	string projections;
	for (idx_t i = 0; i < select_list.size(); i++) {
		if (i > 0) {
			projections += "\n";
		}
		auto &expr = select_list[i];
		projections += expr->GetName();
	}
	result["__projections__"] = projections;
	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

} // namespace duckdb
