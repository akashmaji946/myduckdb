#include "duckdb/execution/operator/join/physical_blockwise_nl_join.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/operator/join/outer_join_marker.hpp"
#include "duckdb/execution/operator/join/physical_comparison_join.hpp"
#include "duckdb/execution/operator/join/physical_cross_product.hpp"
#include "duckdb/common/enum_util.hpp"
#include <iostream>
namespace duckdb {

PhysicalBlockwiseNLJoin::PhysicalBlockwiseNLJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                                 unique_ptr<PhysicalOperator> right, unique_ptr<Expression> condition,
                                                 JoinType join_type, idx_t estimated_cardinality)
    : PhysicalJoin(op, PhysicalOperatorType::BLOCKWISE_NL_JOIN, join_type, estimated_cardinality),
      condition(std::move(condition)) {
	children.push_back(std::move(left));
	children.push_back(std::move(right));
	// MARK and SINGLE joins not handled
	D_ASSERT(join_type != JoinType::MARK);
	D_ASSERT(join_type != JoinType::SINGLE);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class BlockwiseNLJoinLocalState : public LocalSinkState {
public:
	BlockwiseNLJoinLocalState() {
	}
};

class BlockwiseNLJoinGlobalState : public GlobalSinkState {
public:
	explicit BlockwiseNLJoinGlobalState(ClientContext &context, const PhysicalBlockwiseNLJoin &op)
	    : right_chunks(context, op.children[1]->GetTypes()), right_outer(PropagatesBuildSide(op.join_type)) {
	}

	mutex lock;
	ColumnDataCollection right_chunks;
	OuterJoinMarker right_outer;
};

unique_ptr<GlobalSinkState> PhysicalBlockwiseNLJoin::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<BlockwiseNLJoinGlobalState>(context, *this);
}

unique_ptr<LocalSinkState> PhysicalBlockwiseNLJoin::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<BlockwiseNLJoinLocalState>();
}

SinkResultType PhysicalBlockwiseNLJoin::Sink(ExecutionContext &context, DataChunk &chunk,
                                             OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<BlockwiseNLJoinGlobalState>();
	lock_guard<mutex> nl_lock(gstate.lock);
	gstate.right_chunks.Append(chunk);
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType PhysicalBlockwiseNLJoin::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                   OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<BlockwiseNLJoinGlobalState>();
	gstate.right_outer.Initialize(gstate.right_chunks.Count());

	if (gstate.right_chunks.Count() == 0 && EmptyResultIfRHSIsEmpty()) {
		return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
	}
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//
class BlockwiseNLJoinState : public CachingOperatorState {
public:
	explicit BlockwiseNLJoinState(ExecutionContext &context, ColumnDataCollection &rhs,
	                              const PhysicalBlockwiseNLJoin &op)
	    : cross_product(rhs), left_outer(IsLeftOuterJoin(op.join_type)), match_sel(STANDARD_VECTOR_SIZE),
	      executor(context.client, *op.condition) {
		left_outer.Initialize(STANDARD_VECTOR_SIZE);
	}

	CrossProductExecutor cross_product;
	OuterJoinMarker left_outer;
	SelectionVector match_sel;
	ExpressionExecutor executor;
	DataChunk intermediate_chunk;
};

unique_ptr<OperatorState> PhysicalBlockwiseNLJoin::GetOperatorState(ExecutionContext &context) const {
	auto &gstate = sink_state->Cast<BlockwiseNLJoinGlobalState>();
	auto result = make_uniq<BlockwiseNLJoinState>(context, gstate.right_chunks, *this);
	if (join_type == JoinType::SEMI || join_type == JoinType::ANTI) {
		vector<LogicalType> intermediate_types;
		for (auto &type : children[0]->types) {
			intermediate_types.emplace_back(type);
		}
		for (auto &type : children[1]->types) {
			intermediate_types.emplace_back(type);
		}
		result->intermediate_chunk.Initialize(Allocator::DefaultAllocator(), intermediate_types);
	}
	if (join_type == JoinType::RIGHT_ANTI || join_type == JoinType::RIGHT_SEMI) {
		throw NotImplementedException("physical blockwise RIGHT_SEMI/RIGHT_ANTI join not yet implemented");
	}
	return std::move(result);
}

// OperatorResultType PhysicalBlockwiseNLJoin::ExecuteInternal(ExecutionContext &context, DataChunk &input,
//                                                             DataChunk &chunk, GlobalOperatorState &gstate_p,
//                                                             OperatorState &state_p) const {
// 	D_ASSERT(input.size() > 0);
// 	auto &state = state_p.Cast<BlockwiseNLJoinState>();
// 	auto &gstate = sink_state->Cast<BlockwiseNLJoinGlobalState>();

// 	if (gstate.right_chunks.Count() == 0) {
// 		// empty RHS
// 		if (!EmptyResultIfRHSIsEmpty()) {
// 			PhysicalComparisonJoin::ConstructEmptyJoinResult(join_type, false, input, chunk);
// 			return OperatorResultType::NEED_MORE_INPUT;
// 		} else {
// 			return OperatorResultType::FINISHED;
// 		}
// 	}

// 	DataChunk *intermediate_chunk = &chunk;
// 	if (join_type == JoinType::SEMI || join_type == JoinType::ANTI) {
// 		intermediate_chunk = &state.intermediate_chunk;
// 		intermediate_chunk->Reset();
// 	}

// 	// now perform the actual join
// 	// we perform a cross product, then execute the expression directly on the cross product result
// 	idx_t result_count = 0;
// 	bool found_match[STANDARD_VECTOR_SIZE] = {false};

// 	do {
// 		auto result = state.cross_product.Execute(input, *intermediate_chunk);
// 		if (result == OperatorResultType::NEED_MORE_INPUT) {
// 			// exhausted input, have to pull new LHS chunk
// 			if (state.left_outer.Enabled()) {
// 				// left join: before we move to the next chunk, see if we need to output any vectors that didn't
// 				// have a match found
// 				state.left_outer.ConstructLeftJoinResult(input, *intermediate_chunk);
// 				state.left_outer.Reset();
// 			}

// 			if (join_type == JoinType::SEMI) {
// 				PhysicalJoin::ConstructSemiJoinResult(input, chunk, found_match);
// 			}
// 			if (join_type == JoinType::ANTI) {
// 				PhysicalJoin::ConstructAntiJoinResult(input, chunk, found_match);
// 			}

// 			return OperatorResultType::NEED_MORE_INPUT;
// 		}

// 		// now perform the computation
// 		result_count = state.executor.SelectExpression(*intermediate_chunk, state.match_sel);

// 		// handle anti and semi joins with different logic
// 		if (result_count > 0) {
// 			// found a match!
// 			// handle anti semi join conditions first
// 			if (join_type == JoinType::ANTI || join_type == JoinType::SEMI) {
// 				if (state.cross_product.ScanLHS()) {
// 					found_match[state.cross_product.PositionInChunk()] = true;
// 				} else {
// 					for (idx_t i = 0; i < result_count; i++) {
// 						found_match[state.match_sel.get_index(i)] = true;
// 					}
// 				}
// 				intermediate_chunk->Reset();
// 				// trick the loop to continue as semi and anti joins will never produce more output than
// 				// the LHS cardinality
// 				result_count = 0;
// 			} else {
// 				// check if the cross product is scanning the LHS or the RHS in its entirety
// 				if (!state.cross_product.ScanLHS()) {
// 					// set the match flags in the LHS
// 					state.left_outer.SetMatches(state.match_sel, result_count);
// 					// set the match flag in the RHS
// 					gstate.right_outer.SetMatch(state.cross_product.ScanPosition() +
// 					                            state.cross_product.PositionInChunk());
// 				} else {
// 					// set the match flag in the LHS
// 					state.left_outer.SetMatch(state.cross_product.PositionInChunk());
// 					// set the match flags in the RHS
// 					gstate.right_outer.SetMatches(state.match_sel, result_count, state.cross_product.ScanPosition());
// 				}
// 				intermediate_chunk->Slice(state.match_sel, result_count);
// 			}
// 		} else {
// 			// no result: reset the chunk
// 			intermediate_chunk->Reset();
// 		}
// 	} while (result_count == 0);

// 	return OperatorResultType::HAVE_MORE_OUTPUT;
// }

OperatorResultType PhysicalBlockwiseNLJoin::ExecuteInternal(ExecutionContext &context, DataChunk &input,
                                                            DataChunk &chunk, GlobalOperatorState &gstate_p,
                                                            OperatorState &state_p) const {
    auto &state = state_p.Cast<BlockwiseNLJoinState>();
    auto &gstate = sink_state->Cast<BlockwiseNLJoinGlobalState>();

    if (gstate.right_chunks.Count() == 0) {
        // Handle empty RHS case
        if (!EmptyResultIfRHSIsEmpty()) {
            PhysicalComparisonJoin::ConstructEmptyJoinResult(join_type, false, input, chunk);
            return OperatorResultType::NEED_MORE_INPUT;
        } else {
            return OperatorResultType::FINISHED;
        }
    }

	vector<LogicalType> combined_types;
	// Add LHS types
	for (const auto &type : children[0]->types) {
		combined_types.push_back(type);
	}
	// Add RHS types
	for (const auto &type : children[1]->types) {
		combined_types.push_back(type);
	}


    idx_t result_count = 0;
    bool found_match[STANDARD_VECTOR_SIZE] = {false};
    DataChunk *intermediate_chunk = &chunk;
    // intermediate_chunk->Initialize(Allocator::DefaultAllocator(), combined_types); // Initialize based on expected schema

    // Loop through RHS chunks in reverse order
	std :: cout << "Counter Size NLJ:" << gstate.right_chunks.Count() << std::endl;
    for (idx_t chunk_idx = gstate.right_chunks.ChunkCount(); chunk_idx > 0; chunk_idx--) {
		// std::cout << "===================Loop Backwards=======================\n";
        DataChunk rhs_chunk;
        gstate.right_chunks.FetchChunk(chunk_idx - 1, rhs_chunk); // Fetch chunk in reverse order

        // Set the current RHS chunk for the cross product
        state.cross_product.SetRightChunk(rhs_chunk);

        // Perform the cross product for this RHS chunk
        auto result = state.cross_product.Execute(input, *intermediate_chunk);
        if (result == OperatorResultType::NEED_MORE_INPUT) {
            // Handle left outer joins, semi/anti join, etc.
            return OperatorResultType::NEED_MORE_INPUT;
        }

        // Evaluate join condition and process result
        result_count = state.executor.SelectExpression(*intermediate_chunk, state.match_sel);
        if (result_count > 0) {
            // Determine if we are scanning the LHS or RHS in the cross product
            bool scanning_lhs = state.cross_product.ScanLHS();

            // Set match flags based on the scanning direction
            if (scanning_lhs) {
                state.left_outer.SetMatch(state.cross_product.PositionInChunk());
                gstate.right_outer.SetMatches(state.match_sel, result_count, state.cross_product.ScanPosition());
            } else {
                gstate.right_outer.SetMatch(state.cross_product.ScanPosition() + state.cross_product.PositionInChunk());
                state.left_outer.SetMatches(state.match_sel, result_count);
            }

            // Slice the intermediate chunk based on matched selection vector
            intermediate_chunk->Slice(state.match_sel, result_count);

            // Prepare the output chunk with the matched results
            if (join_type == JoinType::INNER || join_type == JoinType::LEFT || join_type == JoinType::RIGHT) {
                // Populate the output chunk with selected data from both LHS and RHS
                // chunk.Reference(intermediate_chunk);
            }

            // Handle SEMI joins
            if (join_type == JoinType::SEMI) {
                PhysicalJoin::ConstructSemiJoinResult(input, chunk, found_match);
            }

            // Handle ANTI joins
            if (join_type == JoinType::ANTI) {
                PhysicalJoin::ConstructAntiJoinResult(input, chunk, found_match);
            }

            // Additional handling for FULL OUTER joins
            if (join_type == JoinType::OUTER) {
                // Collect unmatched rows from LHS and RHS to append to output chunk
                // auto unmatched_lhs = state.left_outer.CollectUnmatchedRows(input);
                // for (const auto &row : unmatched_lhs) {
                //     DataChunk unmatched_row_chunk;
                //     unmatched_row_chunk.Initialize(Allocator::DefaultAllocator(), chunk.GetTypes());
                //     unmatched_row_chunk.SetRow(0, row); // Fill in unmatched LHS row
                //     unmatched_row_chunk.SetNullsForRHSColumns(); // Set NULLs for RHS columns
                //     chunk.Append(unmatched_row_chunk); // Append to output
                // }

                // auto unmatched_rhs = gstate.right_outer.CollectUnmatchedRows(chunk);
                // for (const auto &row : unmatched_rhs) {
                //     DataChunk unmatched_row_chunk;
                //     unmatched_row_chunk.Initialize(Allocator::DefaultAllocator(), chunk.GetTypes());
                //     unmatched_row_chunk.SetNullsForLHSColumns(); // Set NULLs for LHS columns
                //     unmatched_row_chunk.SetRow(0, row); // Fill in unmatched RHS row
                //     chunk.Append(unmatched_row_chunk); // Append to output
                // }
            }
        }
    }

    return OperatorResultType::FINISHED; // Indicate that processing is complete
}



InsertionOrderPreservingMap<string> PhysicalBlockwiseNLJoin::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Join Type"] = EnumUtil::ToString(join_type);
	result["Condition"] = condition->GetName();
	return result;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class BlockwiseNLJoinGlobalScanState : public GlobalSourceState {
public:
	explicit BlockwiseNLJoinGlobalScanState(const PhysicalBlockwiseNLJoin &op) : op(op) {
		D_ASSERT(op.sink_state);
		auto &sink = op.sink_state->Cast<BlockwiseNLJoinGlobalState>();
		sink.right_outer.InitializeScan(sink.right_chunks, scan_state);
	}

	const PhysicalBlockwiseNLJoin &op;
	OuterJoinGlobalScanState scan_state;

public:
	idx_t MaxThreads() override {
		auto &sink = op.sink_state->Cast<BlockwiseNLJoinGlobalState>();
		return sink.right_outer.MaxThreads();
	}
};

class BlockwiseNLJoinLocalScanState : public LocalSourceState {
public:
	explicit BlockwiseNLJoinLocalScanState(const PhysicalBlockwiseNLJoin &op, BlockwiseNLJoinGlobalScanState &gstate) {
		D_ASSERT(op.sink_state);
		auto &sink = op.sink_state->Cast<BlockwiseNLJoinGlobalState>();
		sink.right_outer.InitializeScan(gstate.scan_state, scan_state);
	}

	OuterJoinLocalScanState scan_state;
};

unique_ptr<GlobalSourceState> PhysicalBlockwiseNLJoin::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<BlockwiseNLJoinGlobalScanState>(*this);
}

unique_ptr<LocalSourceState> PhysicalBlockwiseNLJoin::GetLocalSourceState(ExecutionContext &context,
                                                                          GlobalSourceState &gstate) const {
	return make_uniq<BlockwiseNLJoinLocalScanState>(*this, gstate.Cast<BlockwiseNLJoinGlobalScanState>());
}

SourceResultType PhysicalBlockwiseNLJoin::GetData(ExecutionContext &context, DataChunk &chunk,
                                                  OperatorSourceInput &input) const {
	D_ASSERT(PropagatesBuildSide(join_type));
	// check if we need to scan any unmatched tuples from the RHS for the full/right outer join
	auto &sink = sink_state->Cast<BlockwiseNLJoinGlobalState>();
	auto &gstate = input.global_state.Cast<BlockwiseNLJoinGlobalScanState>();
	auto &lstate = input.local_state.Cast<BlockwiseNLJoinLocalScanState>();

	// if the LHS is exhausted in a FULL/RIGHT OUTER JOIN, we scan chunks we still need to output
	sink.right_outer.Scan(gstate.scan_state, lstate.scan_state, chunk);

	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

} // namespace duckdb
