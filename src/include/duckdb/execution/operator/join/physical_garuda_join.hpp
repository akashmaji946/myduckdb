//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/garuda/physical_garuda_join.hpp
//
//
//===----------------------------------------------------------------------===//
 
#pragma once
 
#include "duckdb/execution/operator/join/physical_comparison_join.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
 
namespace duckdb {
 
class PhysicalGarudaJoin : public PhysicalOperator {
public:
    static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::GARUDA_JOIN;
 
public:
    PhysicalGarudaJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left, unique_ptr<PhysicalOperator> right,
                     vector<JoinCondition> cond, JoinType join_type, const vector<idx_t> &left_projection_map, const vector<idx_t> &right_projection_map, idx_t estimated_cardinality);
 
    //! The types of the join keys
    vector<LogicalType> condition_types;
 
    //! The indices for getting the payload columns
    vector<idx_t> payload_column_idxs;
    //! The types of the payload columns
    vector<LogicalType> payload_types;
 
    //! Positions of the RHS columns that need to output
    vector<idx_t> rhs_output_columns;
    //! The types of the output
    vector<LogicalType> rhs_output_types;
 
public:
    // Join interface
    JoinType join_type;
    vector<JoinCondition> conditions;
 
    vector<const_reference<PhysicalOperator>> GetSources() const override;
    void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;
    InsertionOrderPreservingMap<string> ParamsToString() const override;
 
public:
    // Operator Interface
    unique_ptr<GlobalOperatorState> GetGlobalOperatorState(ClientContext &context) const override;
 
    bool ParallelOperator() const override {
        return false;
    }
 
    OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                       GlobalOperatorState &gstate, OperatorState &state) const override;
 
    OperatorFinalizeResultType FinalExecute(ExecutionContext &context, DataChunk &chunk, GlobalOperatorState &gstate,
                                            OperatorState &state) const override;
 
    bool RequiresFinalExecute() const override {
        return true;
    }
 
public:
    // Source interface
    SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;
 
    // HD_TODO: check what this is for!
    //! Becomes a source when it is an external join
    bool IsSource() const override {
        return true;
    }
    bool ParallelSource() const override {
        return false;
    }
 
public:
    // Sink Interface
    unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
    unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
    SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
    SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
    void PrepareFinalize(ClientContext &context, GlobalSinkState &global_state) const override;
    SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                              OperatorSinkFinalizeInput &input) const override;
 
    bool IsSink() const override {
        return true;
    }
    bool ParallelSink() const override {
        return true;
    }
 
 
public:
    static bool IsSupported(const vector<JoinCondition> &conditions, JoinType join_type);
 
    //! Returns a list of the types of the join conditions
    vector<LogicalType> GetJoinTypes() const;
};
 
 
} // namespace duckdb