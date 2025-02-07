#include "duckdb/execution/operator/aggregate/physical_groupjoin_aggregate.hpp"

#include "duckdb/execution/perfect_aggregate_hashtable.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include <iostream>
namespace duckdb {

PhysicalGroupJoinAggregate::PhysicalGroupJoinAggregate(ClientContext &context, vector<LogicalType> types_p,
                                                           vector<unique_ptr<Expression>> aggregates_p,
                                                           vector<unique_ptr<Expression>> groups_p,
                                                           const vector<unique_ptr<BaseStatistics>> &group_stats,
                                                           vector<idx_t> required_bits_p, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::GROUPJOIN_GROUP_BY, std::move(types_p), estimated_cardinality),
      groups(std::move(groups_p)), aggregates(std::move(aggregates_p)), required_bits(std::move(required_bits_p)) {
	D_ASSERT(groups.size() == group_stats.size());
	group_minima.reserve(group_stats.size());
	for (auto &stats : group_stats) {
		D_ASSERT(stats);
		auto &nstats = *stats;
		D_ASSERT(NumericStats::HasMin(nstats));
		group_minima.push_back(NumericStats::Min(nstats));
	}
	for (auto &expr : groups) {
		group_types.push_back(expr->return_type);
	}

	vector<BoundAggregateExpression *> bindings;
	vector<LogicalType> payload_types_filters;
	for (auto &expr : aggregates) {
		D_ASSERT(expr->expression_class == ExpressionClass::BOUND_AGGREGATE);
		D_ASSERT(expr->IsAggregate());
		auto &aggr = expr->Cast<BoundAggregateExpression>();
		bindings.push_back(&aggr);

		D_ASSERT(!aggr.IsDistinct());
		D_ASSERT(aggr.function.combine);
		for (auto &child : aggr.children) {
			payload_types.push_back(child->return_type);
		}
		if (aggr.filter) {
			payload_types_filters.push_back(aggr.filter->return_type);
		}
	}
	for (const auto &pay_filters : payload_types_filters) {
		payload_types.push_back(pay_filters);
	}
	aggregate_objects = AggregateObject::CreateAggregateObjects(bindings);

	// filter_indexes must be pre-built, not lazily instantiated in parallel...
	idx_t aggregate_input_idx = 0;
	for (auto &aggregate : aggregates) {
		auto &aggr = aggregate->Cast<BoundAggregateExpression>();
		aggregate_input_idx += aggr.children.size();
	}
	for (auto &aggregate : aggregates) {
		auto &aggr = aggregate->Cast<BoundAggregateExpression>();
		if (aggr.filter) {
			auto &bound_ref_expr = aggr.filter->Cast<BoundReferenceExpression>();
			auto it = filter_indexes.find(aggr.filter.get());
			if (it == filter_indexes.end()) {
				filter_indexes[aggr.filter.get()] = bound_ref_expr.index;
				bound_ref_expr.index = aggregate_input_idx++;
			} else {
				++aggregate_input_idx;
			}
		}
	}
}

unique_ptr<PerfectAggregateHashTable> PhysicalGroupJoinAggregate::CreateHT(Allocator &allocator,
                                                                             ClientContext &context) const {
	return make_uniq<PerfectAggregateHashTable>(context, allocator, group_types, payload_types, aggregate_objects,
	                                            group_minima, required_bits);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class PhysicalGroupJoinAggregateGlobalState : public GlobalSinkState {
public:
	PhysicalGroupJoinAggregateGlobalState(const PhysicalGroupJoinAggregate &op, ClientContext &context)
	    : ht(op.CreateHT(Allocator::Get(context), context)) {
	}

	//! The lock for updating the global aggregate state
	mutex lock;
	//! The global aggregate hash table
	unique_ptr<PerfectAggregateHashTable> ht;
};

class PhysicalGroupJoinAggregateLocalState : public LocalSinkState {
public:
	PhysicalGroupJoinAggregateLocalState(const PhysicalGroupJoinAggregate &op, ExecutionContext &context)
	    : ht(op.CreateHT(Allocator::Get(context.client), context.client)) {
		group_chunk.InitializeEmpty(op.group_types);
		if (!op.payload_types.empty()) {
			aggregate_input_chunk.InitializeEmpty(op.payload_types);
		}
	}

	//! The local aggregate hash table
	unique_ptr<PerfectAggregateHashTable> ht;
	DataChunk group_chunk;
	DataChunk aggregate_input_chunk;
};

unique_ptr<GlobalSinkState> PhysicalGroupJoinAggregate::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<PhysicalGroupJoinAggregateGlobalState>(*this, context);
}

unique_ptr<LocalSinkState> PhysicalGroupJoinAggregate::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<PhysicalGroupJoinAggregateLocalState>(*this, context);
}

SinkResultType PhysicalGroupJoinAggregate::Sink(ExecutionContext &context, DataChunk &chunk,
                                                  OperatorSinkInput &input) const {
	auto &lstate = input.local_state.Cast<PhysicalGroupJoinAggregateLocalState>();
	DataChunk &group_chunk = lstate.group_chunk;
	DataChunk &aggregate_input_chunk = lstate.aggregate_input_chunk;
	// std::cout << "Agg:\n"<< aggregate_input_chunk.ToString() << std::endl;
	// std::cout << "Inside phy perf hash:\n"<< chunk.ToString() << std::endl;
	for (idx_t group_idx = 0; group_idx < groups.size(); group_idx++) {
		auto &group = groups[group_idx];
		D_ASSERT(group->type == ExpressionType::BOUND_REF);
		auto &bound_ref_expr = group->Cast<BoundReferenceExpression>();
		group_chunk.data[group_idx].Reference(chunk.data[bound_ref_expr.index]);
	}
	idx_t aggregate_input_idx = 0;
	for (auto &aggregate : aggregates) {
		auto &aggr = aggregate->Cast<BoundAggregateExpression>();
		for (auto &child_expr : aggr.children) {
			D_ASSERT(child_expr->type == ExpressionType::BOUND_REF);
			auto &bound_ref_expr = child_expr->Cast<BoundReferenceExpression>();
			aggregate_input_chunk.data[aggregate_input_idx++].Reference(chunk.data[bound_ref_expr.index]);
		}
	}
	for (auto &aggregate : aggregates) {
		auto &aggr = aggregate->Cast<BoundAggregateExpression>();
		if (aggr.filter) {
			auto it = filter_indexes.find(aggr.filter.get());
			D_ASSERT(it != filter_indexes.end());
			aggregate_input_chunk.data[aggregate_input_idx++].Reference(chunk.data[it->second]);
		}
	}
	// std::cout << "Inside phy perf hash:\n"<< chunk.ToString() << std::endl;
	group_chunk.SetCardinality(chunk.size());

	aggregate_input_chunk.SetCardinality(chunk.size());
	// std::cout << "Inside phy perf hash: group chunk: \n"<< group_chunk.ToString() << std::endl;
	group_chunk.Verify();
	aggregate_input_chunk.Verify();
	D_ASSERT(aggregate_input_chunk.ColumnCount() == 0 || group_chunk.size() == aggregate_input_chunk.size());

	
	lstate.ht->AddChunk(group_chunk, aggregate_input_chunk);
	
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Combine
//===--------------------------------------------------------------------===//
SinkCombineResultType PhysicalGroupJoinAggregate::Combine(ExecutionContext &context,
                                                            OperatorSinkCombineInput &input) const {
	auto &lstate = input.local_state.Cast<PhysicalGroupJoinAggregateLocalState>();
	auto &gstate = input.global_state.Cast<PhysicalGroupJoinAggregateGlobalState>();

	lock_guard<mutex> l(gstate.lock);
	gstate.ht->Combine(*lstate.ht);

	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class GroupJoinAggregateState : public GlobalSourceState {
public:
	GroupJoinAggregateState() : ht_scan_position(0) {
	}

	//! The current position to scan the HT for output tuples
	idx_t ht_scan_position;
};

unique_ptr<GlobalSourceState> PhysicalGroupJoinAggregate::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<GroupJoinAggregateState>();
}

SourceResultType PhysicalGroupJoinAggregate::GetData(ExecutionContext &context, DataChunk &chunk,
                                                       OperatorSourceInput &input) const {
	auto &state = input.global_state.Cast<GroupJoinAggregateState>();
	auto &gstate = sink_state->Cast<PhysicalGroupJoinAggregateGlobalState>();
	// std::cout << "Before Scanned:\n";
	// std::cout << chunk.ToString() << std::endl;
	gstate.ht->Scan(state.ht_scan_position, chunk);
	// std::cout << "Scanned:\n";
	// std::cout << chunk.ToString() << std::endl;
	if (chunk.size() > 0) {
		return SourceResultType::HAVE_MORE_OUTPUT;
	} else {
		return SourceResultType::FINISHED;
	}
}

InsertionOrderPreservingMap<string> PhysicalGroupJoinAggregate::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	string groups_info;
	for (idx_t i = 0; i < groups.size(); i++) {
		if (i > 0) {
			groups_info += "\n";
		}
		groups_info += groups[i]->GetName();
	}
	result["Groups"] = groups_info;

	string aggregate_info;
	for (idx_t i = 0; i < aggregates.size(); i++) {
		if (i > 0) {
			aggregate_info += "\n";
		}
		aggregate_info += aggregates[i]->GetName();
		auto &aggregate = aggregates[i]->Cast<BoundAggregateExpression>();
		if (aggregate.filter) {
			aggregate_info += " Filter: " + aggregate.filter->GetName();
		}
	}
	result["Aggregates"] = aggregate_info;
	return result;
}

} // namespace duckdb
