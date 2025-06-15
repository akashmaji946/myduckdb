//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/aggregate_hashtable.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/row_operations/row_matcher.hpp"
#include "duckdb/common/types/row/partitioned_tuple_data.hpp"
#include "duckdb/execution/base_aggregate_hashtable.hpp"
#include "duckdb/execution/ht_entry.hpp"
#include "duckdb/storage/arena_allocator.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/types/row/tuple_data_layout.hpp"
#include "duckdb/execution/operator/aggregate/aggregate_object.hpp"
#include<iostream>

namespace duckdb {

class BlockHandle;
class BufferHandle;

struct FlushMoveState;

//! GroupedAggregateHashTable is a linear probing HT that is used for computing
//! aggregates
/*!
    GroupedAggregateHashTable is a HT that is used for computing aggregates. It takes
   as input the set of groups and the types of the aggregates to compute and
   stores them in the HT. It uses linear probing for collision resolution.
*/
struct GroupedAggregateHashTableScanState {
    idx_t partition_idx = 0;
    idx_t local_offset = 0;
    TupleDataScanState scan_state;
    bool scan_state_initialized = false;
};

class GroupedAggregateHashTable : public BaseAggregateHashTable {
public:
	GroupedAggregateHashTable(ClientContext &context, Allocator &allocator, vector<LogicalType> group_types,
	                          vector<LogicalType> payload_types, const vector<BoundAggregateExpression *> &aggregates,
	                          idx_t initial_capacity = InitialCapacity(), idx_t radix_bits = 0);
	GroupedAggregateHashTable(ClientContext &context, Allocator &allocator, vector<LogicalType> group_types,
	                          vector<LogicalType> payload_types, vector<AggregateObject> aggregates,
	                          idx_t initial_capacity = InitialCapacity(), idx_t radix_bits = 0);
	GroupedAggregateHashTable(ClientContext &context, Allocator &allocator, vector<LogicalType> group_types);
	~GroupedAggregateHashTable() override;

public:
	//! The hash table load factor, when a resize is triggered
	constexpr static double LOAD_FACTOR = 1.5;

	//! Get the layout of this HT
	const TupleDataLayout &GetLayout() const;
	//! Number of groups in the HT
	idx_t Count() const;
	//! Initial capacity of the HT
	static idx_t InitialCapacity();
	//! Capacity that can hold 'count' entries without resizing
	static idx_t GetCapacityForCount(idx_t count);
	//! Current capacity of the HT
	idx_t Capacity() const;
	//! Threshold at which to resize the HT
	idx_t ResizeThreshold() const;

	//! Add the given data to the HT, computing the aggregates grouped by the
	//! data in the group chunk. When resize = true, aggregates will not be
	//! computed but instead just assigned.
	idx_t AddChunk(DataChunk &groups, DataChunk &payload, const unsafe_vector<idx_t> &filter);
	idx_t AddChunk(DataChunk &groups, Vector &group_hashes, DataChunk &payload, const unsafe_vector<idx_t> &filter);
	idx_t AddChunk(DataChunk &groups, DataChunk &payload, AggregateType filter);

	//! Fetch the aggregates for specific groups from the HT and place them in the result
	void FetchAggregates(DataChunk &groups, DataChunk &result);

	//! Finds or creates groups in the hashtable using the specified group keys. The addresses vector will be filled
	//! with pointers to the groups in the hash table, and the new_groups selection vector will point to the newly
	//! created groups. The return value is the amount of newly created groups.
	idx_t FindOrCreateGroups(DataChunk &groups, Vector &group_hashes, Vector &addresses_out,
	                         SelectionVector &new_groups_out);
	idx_t FindOrCreateGroups(DataChunk &groups, Vector &addresses_out, SelectionVector &new_groups_out);
	void FindOrCreateGroups(DataChunk &groups, Vector &addresses_out);

	unique_ptr<PartitionedTupleData> &GetPartitionedData();
	shared_ptr<ArenaAllocator> GetAggregateAllocator();

	//! Resize the HT to the specified size. Must be larger than the current size.
	void Resize(idx_t size);
	//! Resets the pointer table of the HT to all 0's
	void ClearPointerTable();
	//! Resets the group count to 0
	void ResetCount();
	//! Set the radix bits for this HT
	void SetRadixBits(idx_t radix_bits);
	//! Initializes the PartitionedTupleData
	void InitializePartitionedData();

	//! Executes the filter(if any) and update the aggregates
	void Combine(GroupedAggregateHashTable &other);
	void Combine(TupleDataCollection &other_data, optional_ptr<atomic<double>> progress = nullptr);

	//! Unpins the data blocks
	void UnpinData();

private:
	//! Efficiently matches groups
	RowMatcher row_matcher;

	//! Append state
	struct AggregateHTAppendState {
		AggregateHTAppendState();

		PartitionedTupleDataAppendState append_state;

		Vector ht_offsets;
		Vector hash_salts;
		SelectionVector group_compare_vector;
		SelectionVector no_match_vector;
		SelectionVector empty_vector;
		SelectionVector new_groups;
		Vector addresses;
		unsafe_unique_array<UnifiedVectorFormat> group_data;
		DataChunk group_chunk;
	} state;

	//! The number of radix bits to partition by
	idx_t radix_bits;
	//! The data of the HT
	unique_ptr<PartitionedTupleData> partitioned_data;

	//! Predicates for matching groups (always ExpressionType::COMPARE_EQUAL)
	vector<ExpressionType> predicates;

	//! The number of groups in the HT
	idx_t count;
	//! The capacity of the HT. This can be increased using GroupedAggregateHashTable::Resize
	idx_t capacity;
	//! The hash map (pointer table) of the HT: allocated data and pointer into it
	AllocatedData hash_map;
	ht_entry_t *entries;
	//! Offset of the hash column in the rows
	idx_t hash_offset;
	//! Bitmask for getting relevant bits from the hashes to determine the position
	hash_t bitmask;

	//! The active arena allocator used by the aggregates for their internal state
	shared_ptr<ArenaAllocator> aggregate_allocator;
	//! Owning arena allocators that this HT has data from
	vector<shared_ptr<ArenaAllocator>> stored_allocators;

private:
	//! Disabled the copy constructor
	GroupedAggregateHashTable(const GroupedAggregateHashTable &) = delete;
	//! Destroy the HT
	void Destroy();

	//! Apply bitmask to get the entry in the HT
	inline idx_t ApplyBitMask(hash_t hash) const;

	//! Does the actual group matching / creation
	idx_t FindOrCreateGroupsInternal(DataChunk &groups, Vector &group_hashes, Vector &addresses,
	                                 SelectionVector &new_groups);

	//! Verify the pointer table of the HT
	void Verify();

	public:

idx_t Scan(DataChunk &result, GroupedAggregateHashTableScanState &state, idx_t max_rows) {
    if (!partitioned_data) {
        return 0;
    }
    auto &pdata = *partitioned_data;
    result.Reset();

    auto &layout = const_cast<TupleDataLayout &>(GetLayout());
    idx_t group_col_count = layout.ColumnCount() - layout.AggregateCount() - 1; // -1 for hash column
    idx_t aggr_count = layout.AggregateCount();

    // Prepare output types if not already done
    if (result.ColumnCount() == 0) {
        vector<LogicalType> output_types;
        for (idx_t i = 0; i < group_col_count; i++) {
            output_types.push_back(layout.GetTypes()[i]);
        }
        for (idx_t i = 0; i < aggr_count; i++) {
            // Use the aggregate return type, not the state type!
            output_types.push_back(layout.GetAggregates()[i].function.return_type);
        }
        result.Initialize(Allocator::DefaultAllocator(), output_types);
    }

    idx_t out_row = 0;
    while (state.partition_idx < pdata.PartitionCount() && out_row < max_rows) {
        auto &partition = *pdata.GetPartitions()[state.partition_idx];
        if (!state.scan_state_initialized) {
            partition.InitializeScan(state.scan_state);
            state.scan_state_initialized = true;
            state.local_offset = 0;
        }

        DataChunk temp_chunk;
        partition.InitializeScanChunk(state.scan_state, temp_chunk);

        while (out_row < max_rows) {
            idx_t count = partition.Scan(state.scan_state, temp_chunk);

            if (count == 0) {
                // Move to next partition
                state.partition_idx++;
                state.scan_state_initialized = false;
                break;
            }

            // 1. Copy group columns from temp_chunk to result
            for (idx_t col = 0; col < group_col_count; col++) {
                for (idx_t row = 0; row < count; row++) {
                    result.data[col].SetValue(out_row + row, temp_chunk.data[col].GetValue(row));
                }
            }

            // 2. Finalize aggregate columns into result (after group columns)
            auto &row_locations = state.scan_state.chunk_state.row_locations;
            RowOperationsState row_state(*aggregate_allocator);

            // Create a temporary chunk for finalized aggregates
            DataChunk finalized_chunk;
            vector<LogicalType> finalized_types;
            for (idx_t i = 0; i < aggr_count; i++) {
                finalized_types.push_back(layout.GetAggregates()[i].function.return_type);
            }
            finalized_chunk.Initialize(Allocator::DefaultAllocator(), finalized_types);
            finalized_chunk.SetCardinality(count);

            // Finalize aggregate states into finalized_chunk (always at offset 0)
            RowOperations::FinalizeStates(row_state, layout, row_locations, finalized_chunk, 0);

            // Copy finalized aggregate values into result
            for (idx_t col = 0; col < aggr_count; col++) {
                for (idx_t row = 0; row < count; row++) {
                    result.data[group_col_count + col].SetValue(out_row + row, finalized_chunk.data[col].GetValue(row));
                }
            }

            out_row += count;
            result.SetCardinality(out_row);

            if (out_row >= max_rows) {
                return out_row;
            }
        }
    }
    result.SetCardinality(out_row);
    return out_row;
}




};

} // namespace duckdb
