//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/physical_group_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/join/physical_join.hpp"

namespace duckdb {
class GroupJoinGlobalSinkState;
class GroupJoinGlobalSourceState;


//! PhysicalGroupJoin represents a nested loop join between two tables on arbitrary expressions. This is different
//! from the PhysicalNestedLoopJoin in that it does not require expressions to be comparisons between the LHS and the
//! RHS.
class PhysicalGroupJoin : public PhysicalJoin{
public:
	static std::unordered_map<int, std::pair<int, int>> aggregation_map;
	static std::unordered_map<int, int> aggregation_map2;

	static std::unordered_map<int, double> final_results;

	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::GROUP_JOIN;

public:
	PhysicalGroupJoin(
        LogicalOperator &op,
        unique_ptr<PhysicalOperator> left,
        unique_ptr<PhysicalOperator> right,
        unique_ptr<Expression> condition,
        JoinType join_type,
        idx_t estimated_cardinality,
        vector<unique_ptr<Expression>> &groups,
        vector<unique_ptr<Expression>> &aggregates
    );
	virtual ~PhysicalGroupJoin();

    unique_ptr<Expression> condition;
    vector<unique_ptr<Expression>> groups;
    vector<unique_ptr<Expression>> aggregates;

public:
	// Operator Interface
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;

	bool ParallelOperator() const override {
		return true;
	}

protected:
	// CachingOperatorState Interface
	OperatorResultType ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                                   GlobalOperatorState &gstate, OperatorState &state) const;

public:
	// Source interface
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
	                                                 GlobalSourceState &gstate) const override;
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
	bool ParallelSource() const override {
		return true;
	}

	// int PerformEqualityAggregation(GroupJoinGlobalSinkState &global_state, DataChunk &left_chunk, DataChunk &right_chunk,
    //                                 std::unordered_map<int, std::pair<int, int>> &aggregation_map,
    //                                 std::unordered_map<int, int> &aggregation_map2, int &countlc, int &countrc) const;

	// int PerformInEqualityAggregation(GroupJoinGlobalSinkState &global_state, DataChunk &left_chunk, DataChunk &right_chunk,
    //                                 std::unordered_map<int, std::pair<int, int>> &aggregation_map,
    //                                 std::unordered_map<int, int> &aggregation_map2, int &countlc, int &countrc) const;
	
	void  PerformEqualityAggregation(
    duckdb::GroupJoinGlobalSinkState &global_state, std::unordered_map<int, double>& final_results) const;

	void PerformInEqualityAggregation(
    duckdb::GroupJoinGlobalSinkState &global_state, std::unordered_map<int, double>& final_results) const;

public:
	// Sink interface
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	bool IsSink() const {
		return true;
	}
	bool ParallelSink() const {
		return true;
	}

public:
	InsertionOrderPreservingMap<string> ParamsToString() const override;
	// bool SinkOrderDependent() const override;
	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;

	OrderPreservationType SourceOrder() const {
		return OrderPreservationType::FIXED_ORDER;
	}
	OrderPreservationType OperatorOrder() const {
		return OrderPreservationType::NO_ORDER;
	}
	bool SinkOrderDependent() const {
		return true;
	}

};

} // namespace duckdb
