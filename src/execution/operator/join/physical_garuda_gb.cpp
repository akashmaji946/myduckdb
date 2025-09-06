// #include "duckdb/execution/operator/garuda/physical_garuda_gb.hpp"
 
// #include "duckdb/parallel/thread_context.hpp"
// #include "duckdb/planner/expression/bound_reference_expression.hpp"
 
// #include <iostream>
 
// namespace duckdb {
 
 
// // HD TODO Does not support distinct
 
// /*************************************************************
//  * Local Sink State
//  **************************************************************/
// class GarudaGBLocalSinkState : public LocalSinkState {
// public:
//     explicit GarudaGBLocalSinkState(const PhysicalGarudaAggregate &op, ExecutionContext &context) {
//         group_data.Initialize(Allocator::Get(context.client), op.input_group_types);
//         tmp_group_chunk.Initialize(Allocator::Get(context.client), op.input_group_types);
 
//         auto &payload_types = op.grouped_aggregate_data.payload_types;
//         if (!payload_types.empty()) {
//             agg_data.Initialize(Allocator::Get(context.client), payload_types);
//             tmp_agg_chunk.Initialize(Allocator::Get(context.client), payload_types);
//         }
//     }
//     DataChunk group_data, tmp_group_chunk;
//     DataChunk agg_data, tmp_agg_chunk;
// };
 
// /*************************************************************
//  * Global Sink State
//  **************************************************************/
// class GarudaGBGlobalSinkState : public GlobalSinkState {
// public:
//     explicit GarudaGBGlobalSinkState(const PhysicalGarudaAggregate &op, ClientContext &context) {
//         group_data.Initialize(Allocator::Get(context), op.input_group_types);
 
//         auto &payload_types = op.grouped_aggregate_data.payload_types;
//         if (!payload_types.empty()) {
//             agg_data.Initialize(Allocator::Get(context), payload_types);
//         }
//     }
 
//     DataChunk group_data, agg_data;
//     mutex lock;
// };
 
// /*************************************************************
//  * Local Source State
//  **************************************************************/
// class GarudaGBLocalSourceState : public LocalSourceState {
// public:
//     explicit GarudaGBLocalSourceState(ExecutionContext &context, const PhysicalGarudaAggregate &op) {
//     }
// };
 
// /*************************************************************
//  * Global Source State
//  **************************************************************/
// class GarudaGBGlobalSourceState : public GlobalSourceState {
// public:
//     explicit GarudaGBGlobalSourceState(ClientContext &context, const PhysicalGarudaAggregate &op) {
//     }
// };
 
// static vector<LogicalType> CreateGroupChunkTypes(vector<unique_ptr<Expression>> &groups) {
//     set<idx_t> group_indices;
 
//     if (groups.empty()) {
//         return {};
//     }
 
//     for (auto &group : groups) {
//         D_ASSERT(group->type == ExpressionType::BOUND_REF);
//         auto &bound_ref = group->Cast<BoundReferenceExpression>();
//         group_indices.insert(bound_ref.index);
//     }
//     idx_t highest_index = *group_indices.rbegin();
//     vector<LogicalType> types(highest_index + 1, LogicalType::SQLNULL);
//     for (auto &group : groups) {
//         auto &bound_ref = group->Cast<BoundReferenceExpression>();
//         types[bound_ref.index] = bound_ref.return_type;
//     }
//     return types;
// }
 
// PhysicalGarudaAggregate::PhysicalGarudaAggregate(ClientContext &context, vector<LogicalType> types,
//                                                  vector<unique_ptr<Expression> > expressions, idx_t estimated_cardinality)
//      : PhysicalGarudaAggregate(context, std::move(types), std::move(expressions), {}, estimated_cardinality) {
 
// }
 
// PhysicalGarudaAggregate::PhysicalGarudaAggregate(ClientContext &context, vector<LogicalType> types,
//                                                  vector<unique_ptr<Expression> > expressions,
//                                                  vector<unique_ptr<Expression> > groups, idx_t estimated_cardinality)
//     : PhysicalGarudaAggregate(context, std::move(types), std::move(expressions), std::move(groups), {}, {},
//                             estimated_cardinality) {
 
// }
 
// PhysicalGarudaAggregate::PhysicalGarudaAggregate(ClientContext &context, vector<LogicalType> types,
//                                                  vector<unique_ptr<Expression> > expressions,
//                                                  vector<unique_ptr<Expression> > groups_p,
//                                                  vector<GroupingSet> grouping_sets_p,
//                                                  vector<unsafe_vector<idx_t> > grouping_functions_p,
//                                                  idx_t estimated_cardinality)
//     : PhysicalOperator(PhysicalOperatorType::GARUDA_GROUP_BY, std::move(types), estimated_cardinality),
//       grouping_sets(std::move(grouping_sets_p)) {
 
//     // get a list of all aggregates to be computed
//     const idx_t group_count = groups_p.size();
//     if (grouping_sets.empty()) {
//         GroupingSet set;
//         for (idx_t i = 0; i < group_count; i++) {
//             set.insert(i);
//         }
//         grouping_sets.push_back(std::move(set));
//     }
 
//     input_group_types = CreateGroupChunkTypes(groups_p);
 
//     grouped_aggregate_data.InitializeGroupby(std::move(groups_p), std::move(expressions),
//                                              std::move(grouping_functions_p));
 
 
//     auto &aggregates = grouped_aggregate_data.aggregates;
 
//     // HD hack!!!!
//     for (idx_t i = 0; i < aggregates.size(); i++) {
//         auto &aggregate = aggregates[i];
//         auto &aggr = aggregate->Cast<BoundAggregateExpression>();
//         if(aggr.function.name == "sum_no_overflow" || aggr.function.name == "sum") {
//             agg_fns.push_back(AggregateFunctionType::SUM);
//         } else if(aggr.function.name == "avg") {
//             agg_fns.push_back(AggregateFunctionType::AVG);
//         } else if(aggr.function.name == "min") {
//             agg_fns.push_back(AggregateFunctionType::MIN);
//         } else if(aggr.function.name == "max") {
//             agg_fns.push_back(AggregateFunctionType::MAX);
//         } else if(aggr.function.name == "count_star") {
//             agg_fns.push_back(AggregateFunctionType::COUNT);
//         } else {
//             std::cout << "can this happen????\n";
//         }
//         std::cout << aggr.function.name;
//         if(aggr.children.size() == 0) {
//             agg_idx.push_back(-1);
//             std::cout << ", -1";
//         } else {
//             D_ASSERT(aggr.children.size() == 1);
//             auto &child_expr = aggr.children[0];
//             D_ASSERT(child_expr->type == ExpressionType::BOUND_REF);
//             auto &bound_ref_expr = child_expr->Cast<BoundReferenceExpression>();
//             agg_idx.push_back(bound_ref_expr.index - input_group_types.size());
//             std::cout << ", " << bound_ref_expr.index;
//         }
//         std::cout << "\n";
//     }
 
//     std::cout << "initialized gb: " << input_group_types.size() << "," << this->types.size() << "," << agg_fns.size() << std::endl;
// }
 
// unique_ptr<GlobalSourceState> PhysicalGarudaAggregate::GetGlobalSourceState(ClientContext &context) const {
//     return make_uniq<GarudaGBGlobalSourceState>(context, *this);
// }
 
// unique_ptr<LocalSourceState> PhysicalGarudaAggregate::GetLocalSourceState(ExecutionContext &context, GlobalSourceState &gstate) const {
//     return make_uniq<GarudaGBLocalSourceState>(context, *this);
// }
 
// SourceResultType PhysicalGarudaAggregate::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
//     // HD TODO: figure out return column indexes.
//     return SourceResultType::FINISHED;
// }
 
// SinkResultType PhysicalGarudaAggregate::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
//     auto &local_state = input.local_state.Cast<GarudaGBLocalSinkState>();
 
//     // Populate the aggregate columns
//     {
//         DataChunk &tmp = local_state.tmp_agg_chunk;
//         auto &aggregates = grouped_aggregate_data.aggregates;
//         idx_t agg_idx = 0;
//         for (auto &aggregate : aggregates) {
//             auto &aggr = aggregate->Cast<BoundAggregateExpression>();
//             for (auto &child_expr : aggr.children) {
//                 D_ASSERT(child_expr->type == ExpressionType::BOUND_REF);
//                 auto &bound_ref_expr = child_expr->Cast<BoundReferenceExpression>();
//                 D_ASSERT(bound_ref_expr.index < chunk.data.size());
//                 tmp.data[agg_idx++].Reference(chunk.data[bound_ref_expr.index]);
//             }
//         }
//         tmp.SetCardinality(chunk.size());
//         local_state.agg_data.Append(tmp,true);
//     }
 
//     // populate the group columns
//     {
//         DataChunk &tmp = local_state.tmp_group_chunk;
//         for(idx_t i = 0;i < local_state.group_data.ColumnCount();i ++) {
//             tmp.data[i].Reference(chunk.data[i]);
//         }
//         tmp.SetCardinality(chunk.size());
//         local_state.group_data.Append(tmp,true);
//     }
// //    std::cout << "no. of chunk columns: " << chunk.ColumnCount() << std::endl;
//     return SinkResultType::NEED_MORE_INPUT;
// }
 
// SinkCombineResultType PhysicalGarudaAggregate::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
//     auto &gstate = input.global_state.Cast<GarudaGBGlobalSinkState>();
//     auto &lstate = input.local_state.Cast<GarudaGBLocalSinkState>();
 
//     // HD: TODO check single thread performance!
//     lock_guard<mutex> guard(gstate.lock);
//     if(gstate.agg_data.ColumnCount() > 0) {
//         gstate.agg_data.Append(lstate.agg_data, true);
//     }
//     gstate.group_data.Append(lstate.group_data, true);
 
//     return SinkCombineResultType::FINISHED;
// }
 
// void PhysicalGarudaAggregate::PrepareFinalize(ClientContext &context, GlobalSinkState &global_state) const {
 
// }
 
// SinkFinalizeType PhysicalGarudaAggregate::Finalize(Pipeline &pipeline, Event &event, ClientContext &context, OperatorSinkFinalizeInput &input) const {
//     auto &gstate = input.global_state.Cast<GarudaGBGlobalSinkState>();
//     std::cout << "performing gb on " << gstate.group_data.ColumnCount() << " groups with " << gstate.group_data.size() << " rows \n";
//     return SinkFinalizeType::READY;
// }
 
// unique_ptr<GlobalSinkState> PhysicalGarudaAggregate::GetGlobalSinkState(ClientContext &context) const {
//     return make_uniq<GarudaGBGlobalSinkState>(*this, context);
// }
 
// unique_ptr<LocalSinkState> PhysicalGarudaAggregate::GetLocalSinkState(ExecutionContext &context) const {
//     return make_uniq<GarudaGBLocalSinkState>(*this, context);
// }
 
// bool PhysicalGarudaAggregate::IsSupported(const vector<LogicalType> &types, const vector<unique_ptr<Expression> > &expressions, const vector<unique_ptr<Expression> > &groups) {
//     // For now support only int32 grouping
//     for (auto &group : groups) {
//         D_ASSERT(group->type == ExpressionType::BOUND_REF);
//         auto &bound_ref = group->Cast<BoundReferenceExpression>();
//         if(types[bound_ref.index].InternalType() != PhysicalType::INT32 && types[bound_ref.index].InternalType() != PhysicalType::UINT32) {
//             return false;
//         }
//     }
//     for (auto &expr : expressions) {
//         D_ASSERT(expr->expression_class == ExpressionClass::BOUND_AGGREGATE);
//         D_ASSERT(expr->IsAggregate());
//         auto &aggr = expr->Cast<BoundAggregateExpression>();
 
//         if(!(aggr.function.name == "sum_no_overflow" || aggr.function.name == "sum"
//                || aggr.function.name == "avg" || aggr.function.name == "min"
//                || aggr.function.name == "max" || aggr.function.name == "count_star")) {
//             return false;
//         }
//         // HD TODO: chek type of aggr.children (there should be at most 1) do we support decimal, or double or both?
//     }
//     return true;
// }
 
 
 
// } // namespace duckdb