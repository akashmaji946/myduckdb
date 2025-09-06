// #include "duckdb/execution/operator/join/physical_garuda_join.hpp"
 
// #include "duckdb/parallel/thread_context.hpp"
// #include "duckdb/planner/expression/bound_reference_expression.hpp"
 
// #define NOMINMAX
// // #include <interface/garuda.h>
 
// #include <iostream>
 
// namespace duckdb {
 
// /*************************************************************
//  * Local Sink State : Gets the right (HD: why is right the build?) relation of the Join
//  **************************************************************/
// class GarudaJoinLocalSinkState : public LocalSinkState {
// public:
//     explicit GarudaJoinLocalSinkState(ClientContext &context, const vector<JoinCondition> &conditions, const PhysicalGarudaJoin &op) : build_executor(context) {
//         vector<LogicalType> condition_types;
//         for (auto &cond : conditions) {
//             build_executor.AddExpression(*cond.right);
//             condition_types.push_back(cond.right->return_type);
//         }
//         build_join_column.Initialize(Allocator::Get(context), condition_types);
//         tmp_chunk.Initialize(Allocator::Get(context), condition_types);
//         build_relation.Initialize(Allocator::Get(context), op.payload_types);
//         tmp_payload_chunk.Initialize(Allocator::Get(context), op.payload_types);
//         n_rows = 0;
//     }
 
//     idx_t n_rows;
//     // store partial data
//     DataChunk build_relation, build_join_column, tmp_chunk, tmp_payload_chunk;
//     // The executor of the build side
//     ExpressionExecutor build_executor;
// };
 
// /*************************************************************
//  * Global Sink State
//  **************************************************************/
// class GarudaJoinGlobalSinkState : public GlobalSinkState {
// public:
//     explicit GarudaJoinGlobalSinkState(ClientContext &context, const PhysicalGarudaJoin &op)
//         /*: build_relation(context, op.children[1]->types)*/ {
 
//         build_relation.Initialize(Allocator::Get(context), op.payload_types);
//     }
 
//     mutex lock;
 
//     // all columns necessary to create output
//     DataChunk build_relation;
//     // Join column
//     DataChunk build_join_column;
// };
 
 
// /*************************************************************
//  * Operator State
//  **************************************************************/
// class GarudaJoinGlobalState : public GlobalOperatorState {
// public:
//     GarudaJoinGlobalState(ClientContext &context, const PhysicalGarudaJoin &op, const vector<JoinCondition> &conditions)
//         : probe_executor(context) , /*probe_relation(context, op.children[0]->types),*/ started(false), completed(false) {
//         vector<LogicalType> condition_types;
//         for (auto &cond : conditions) {
//             probe_executor.AddExpression(*cond.left);
//             condition_types.push_back(cond.left->return_type);
//         }
//         auto &allocator = Allocator::Get(context);
//         probe_join_column.Initialize(allocator, condition_types);
//         tmp_chunk.Initialize(allocator, condition_types);
//         probe_relation.Initialize(allocator, op.children[0]->types);
//     }
 
//     // all columns necessary to create output
//     DataChunk probe_relation;
//     // Join column
//     DataChunk probe_join_column, tmp_chunk;
 
//     ExpressionExecutor probe_executor;
 
//     garuda::PageVector<garuda::Tuple> *joinResult;
//     idx_t remaining, cur;
//     bool started, completed;
// };
 
// PhysicalGarudaJoin::PhysicalGarudaJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
//                                        unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, JoinType join_type,
//                                        const vector<idx_t> &left_projection_map, const vector<idx_t> &right_projection_map,
//                                        idx_t estimated_cardinality)
//     : PhysicalOperator(PhysicalOperatorType::GARUDA_JOIN, std::move(op.types), estimated_cardinality),
//         conditions(std::move(cond)), join_type(join_type)  {
//     children.push_back(std::move(left));
//     children.push_back(std::move(right));
 
//     // Collect condition types, and which conditions are just references (so we won't duplicate them in the payload)
//     unordered_map<idx_t, idx_t> build_columns_in_conditions;
//     for (idx_t cond_idx = 0; cond_idx < conditions.size(); cond_idx++) {
//         auto &condition = conditions[cond_idx];
//         condition_types.push_back(condition.left->return_type);
//         if (condition.right->GetExpressionClass() == ExpressionClass::BOUND_REF) {
//             build_columns_in_conditions.emplace(condition.right->Cast<BoundReferenceExpression>().index, cond_idx);
//         }
//     }
 
//     auto &rhs_input_types = children[1]->GetTypes();
 
//     // Create a projection map for the RHS (if it was empty), for convenience
//     auto right_projection_map_copy = right_projection_map;
//     if (right_projection_map_copy.empty()) {
//         right_projection_map_copy.reserve(rhs_input_types.size());
//         for (idx_t i = 0; i < rhs_input_types.size(); i++) {
//             right_projection_map_copy.emplace_back(i);
//         }
//     }
 
//     // Now fill payload expressions/types and RHS columns/types
//     for (auto &rhs_col : right_projection_map_copy) {
//         auto &rhs_col_type = rhs_input_types[rhs_col];
 
//         auto it = build_columns_in_conditions.find(rhs_col);
//         if (it == build_columns_in_conditions.end()) {
//             // This rhs column is not a join key
//             rhs_output_columns.push_back(condition_types.size() + payload_types.size() - 1);
//         } else {
//             // This rhs column is a join key
//             rhs_output_columns.push_back(it->second);
//         }
//         payload_column_idxs.push_back(rhs_col);
//         payload_types.push_back(rhs_col_type);
//         rhs_output_types.push_back(rhs_col_type);
//     }
// }
 
// vector<const_reference<PhysicalOperator> > PhysicalGarudaJoin::GetSources() const {
//     auto result = children[0]->GetSources();
//     if (IsSource()) {
//         result.push_back(*this);
//     }
//     return result;
// }
 
// void PhysicalGarudaJoin::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
//     PhysicalJoin::BuildJoinPipelines(current, meta_pipeline, *this);
// }
 
// InsertionOrderPreservingMap<string> PhysicalGarudaJoin::ParamsToString() const {
//     InsertionOrderPreservingMap<string> result;
//     result["Join Type"] = EnumUtil::ToString(join_type);
//     string condition_info;
//     for (idx_t i = 0; i < conditions.size(); i++) {
//         auto &join_condition = conditions[i];
//         if (i > 0) {
//             condition_info += "\n";
//         }
//         condition_info +=
//             StringUtil::Format("%s %s %s", join_condition.left->GetName(),
//                                ExpressionTypeToOperator(join_condition.comparison), join_condition.right->GetName());
//         // string op = ExpressionTypeToOperator(it.comparison);
//         // extra_info += it.left->GetName() + " " + op + " " + it.right->GetName() + "\n";
//     }
//     result["Conditions"] = condition_info;
//     SetEstimatedCardinality(result, estimated_cardinality);
//     return result;
// }
 
// unique_ptr<GlobalOperatorState> PhysicalGarudaJoin::GetGlobalOperatorState(ClientContext &context) const {
//     return make_uniq<GarudaJoinGlobalState>(context, *this, conditions);
// }
 
// OperatorResultType PhysicalGarudaJoin::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk, GlobalOperatorState &gstate, OperatorState &state) const {
//     // First collect all of probe relations
//     auto &opstate = gstate.Cast<GarudaJoinGlobalState>();
 
//     opstate.tmp_chunk.Reset();
//     opstate.probe_executor.Execute(input,opstate.tmp_chunk);
//     opstate.probe_join_column.Append(opstate.tmp_chunk,true);
 
//     if(opstate.probe_relation.ColumnCount() != 1) {
//         opstate.probe_relation.Append(input, true);
//     }
//     return OperatorResultType::NEED_MORE_INPUT;
// }
 
 
// inline void cpuLargeJoinSTL(int32_t *left, idx_t lsize, int32_t *right, idx_t rsize, std::vector<int32_t> &lid, std::vector<int32_t> &rid) {
//     std::unordered_map<int32_t,std::vector<int32_t> > hash1;
//     lid.clear();
//     rid.clear();
 
//     for(int i = 0;i <lsize;i ++) {
//         if(hash1.find(left[i]) == hash1.end()) {
//             hash1[left[i]] = std::vector<int32_t>();
//         }
//         hash1[left[i]].push_back(i);
//     }
 
//     for(int i = 0;i < rsize;i ++) {
//         if(hash1.find(right[i]) != hash1.end()) {
//             for(int j = 0;j < hash1[right[i]].size();j ++) {
//                 lid.push_back(hash1[right[i]][j]);
//                 rid.push_back(i);
//             }
//         }
//     }
// }
 
// OperatorFinalizeResultType PhysicalGarudaJoin::FinalExecute(ExecutionContext &context, DataChunk &chunk, GlobalOperatorState &gstate, OperatorState &state) const {
//     auto &bstate = this->sink_state->Cast<GarudaJoinGlobalSinkState>();
//     auto &pstate = gstate.Cast<GarudaJoinGlobalState>();
 
//     // HD: hack: We know this is a integer column.... so harcoding the read!
//     idx_t bsize = bstate.build_join_column.size();
//     uint32_t * build_col = reinterpret_cast<uint32_t *>(bstate.build_join_column.data[0].GetData());
 
//     idx_t psize = pstate.probe_join_column.size();
//     uint32_t * probe_col = reinterpret_cast<uint32_t *>(pstate.probe_join_column.data[0].GetData());
 
//     // Do the actual join.
// //    cpuLargeJoinSTL(build_col,bsize,probe_col,psize,pstate.lid,pstate.rid);
//     pstate.joinResult = garuda::performJoin(build_col,bsize,probe_col,psize);
//     pstate.completed = true;
 
//     return OperatorFinalizeResultType::FINISHED;
// }
 
// SourceResultType PhysicalGarudaJoin::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
//     auto &pstate = this->op_state->Cast<GarudaJoinGlobalState>();
//     if(!pstate.completed) {
//         return SourceResultType::BLOCKED;
//     }
//     auto &bstate = sink_state->Cast<GarudaJoinGlobalSinkState>();
 
//     if(chunk.ColumnCount() != (pstate.probe_relation.ColumnCount() + bstate.build_relation.ColumnCount()) ) {
//         std::cerr << "column mismatch!!! \n";
//         exit(0);
//     }
 
//     if(!pstate.started) {
//         pstate.started = true;
//         pstate.remaining = pstate.joinResult->size();
//         pstate.cur = 0;
//     }
 
//     if(pstate.remaining == 0) {
//         pstate.joinResult->clear();
//         delete pstate.joinResult;
//         return SourceResultType::FINISHED;
//     }
 
//     // HD: TODO handle parallel!
 
//     int32_t n = std::min(pstate.remaining,idx_t(STANDARD_VECTOR_SIZE));
//     SelectionVector sel_build, sel_probe;
//     sel_build.Initialize(n);
//     sel_probe.Initialize(n);
//     chunk.SetCardinality(n);
//     for(idx_t i = pstate.cur; i < pstate.cur + n;i ++) {
//         sel_build.set_index(i - pstate.cur,(*pstate.joinResult)[i].x);
//         sel_probe.set_index(i - pstate.cur,(*pstate.joinResult)[i].y);
//     }
//     // HD Double check... RHS is build relation, so build should be last few columns?
//     if(pstate.probe_relation.ColumnCount() == 1) {
//         chunk.Slice(pstate.probe_join_column,sel_probe,n);
//     } else {
//         chunk.Slice(pstate.probe_relation,sel_probe,n);
//     }
//     chunk.Slice(bstate.build_relation,sel_build,n,pstate.probe_relation.ColumnCount());
//     pstate.cur += n;
//     pstate.remaining -= n;
//     if(pstate.remaining == 0) {
//         pstate.joinResult->clear();
//         delete pstate.joinResult;
//         return SourceResultType::FINISHED;
//     }
//     return SourceResultType::HAVE_MORE_OUTPUT;
// }
 
// unique_ptr<GlobalSinkState> PhysicalGarudaJoin::GetGlobalSinkState(ClientContext &context) const {
//     return make_uniq<GarudaJoinGlobalSinkState>(context, *this);
// }
 
// unique_ptr<LocalSinkState> PhysicalGarudaJoin::GetLocalSinkState(ExecutionContext &context) const {
//     return make_uniq<GarudaJoinLocalSinkState>(context.client, conditions, *this);
// }
 
// SinkResultType PhysicalGarudaJoin::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
//     // HD: TODO can setup partitioning here itself?
//     auto &lstate = input.local_state.Cast<GarudaJoinLocalSinkState>();
 
//     // HD TODO check correctness
//     // need to do a reference and then an append!
//     lstate.tmp_payload_chunk.Reset();
//     lstate.tmp_payload_chunk.SetCardinality(chunk);
//     for (idx_t i = 0; i < payload_column_idxs.size(); i++) {
//         lstate.tmp_payload_chunk.data[i].Reference(chunk.data[payload_column_idxs[i]]);
//     }
//     lstate.build_relation.Append(lstate.tmp_payload_chunk,true);
 
//     lstate.tmp_chunk.Reset();
//     lstate.build_executor.Execute(chunk,lstate.tmp_chunk);
//     lstate.build_join_column.Append(lstate.tmp_chunk,true);
 
//     return SinkResultType::NEED_MORE_INPUT;
// }
 
// SinkCombineResultType PhysicalGarudaJoin::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
//     auto &gstate = input.global_state.Cast<GarudaJoinGlobalSinkState>();
//     auto &lstate = input.local_state.Cast<GarudaJoinLocalSinkState>();
 
//     // HD: TODO check single thread performance!
//     lock_guard<mutex> guard(gstate.lock);
//     if(gstate.build_join_column.ColumnCount() == 0) {
//         gstate.build_join_column.Initialize(Allocator::Get(context.client), lstate.build_join_column.GetTypes());
//     }
//     gstate.build_join_column.Append(lstate.build_join_column, true);
//     if(gstate.build_relation.ColumnCount() != 1) {
//         gstate.build_relation.Append(lstate.build_relation, true);
//     }
 
//     return SinkCombineResultType::FINISHED;
// }
 
// void PhysicalGarudaJoin::PrepareFinalize(ClientContext &context, GlobalSinkState &global_state) const {
//     // HD: TODO
// }
 
// SinkFinalizeType PhysicalGarudaJoin::Finalize(Pipeline &pipeline, Event &event, ClientContext &context, OperatorSinkFinalizeInput &input) const {
//     // HD: TODO
//     return SinkFinalizeType::READY;
// }
 
// bool PhysicalGarudaJoin::IsSupported(const vector<JoinCondition> &conditions, JoinType join_type) {
//     if(conditions.size() > 1) {
//         return false;
//     }
//     if(join_type != JoinType::INNER) {
//         return false;
//     }
//     if(conditions[0].comparison != ExpressionType::COMPARE_EQUAL) {
//         return false;
//     }
//     // for now support joins only on integers
//     if(conditions[0].left->return_type != LogicalType::INTEGER || conditions[0].right->return_type != LogicalType::INTEGER) {
//         return false;
//     }
//     return true;
// }
 
// vector<LogicalType> PhysicalGarudaJoin::GetJoinTypes() const {
//     vector<LogicalType> result;
//     for (auto &op : conditions) {
//         result.push_back(op.right->return_type);
//     }
//     return result;
// }

// } // namespace duckdb