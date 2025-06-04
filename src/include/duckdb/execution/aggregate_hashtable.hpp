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
	idx_t current_offset = 0;
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

	// Finds the partition index and local offset inside that partition for a global offset
		static void GetPartitionAndLocalOffset(PartitionedTupleData &data, idx_t global_offset,
											idx_t &partition_idx, idx_t &local_offset) {
			partition_idx = 0;
			idx_t running_offset = 0;

			for (idx_t i = 0; i < data.PartitionCount(); i++) {
				auto &partition = *data.GetPartitions()[i]; // Dereference unique_ptr to get TupleDataCollection&
				idx_t partition_count = partition.Count();  // Use . not -> here
				if (global_offset < running_offset + partition_count) {
					partition_idx = i;
					local_offset = global_offset - running_offset;
					return;
				}
				running_offset += partition_count;
			}
			// If offset beyond total count (should not happen), assign to last partition
			partition_idx = data.PartitionCount() - 1;
			local_offset = data.GetPartitions()[partition_idx]->Count();
		}

static void FetchRowFromPartition(TupleDataCollection &partition, row_t row_id, DataChunk &result) {
	// 1. Create a vector of row locations
	Vector row_locations(LogicalType::ROW_TYPE, 1);
	FlatVector::GetData<row_t>(row_locations)[0] = row_id;

	// 2. Selection vector to choose the single row
	SelectionVector sel(1);
	sel.set_index(0, 0);

	// 3. Initialize the result chunk if needed
	if (result.ColumnCount() == 0) {
		partition.InitializeChunk(result);
	}

	// 4. Empty cache for potential list/struct casts
	vector<unique_ptr<Vector>> cached_cast_vectors;

	// 5. Gather the row
	partition.Gather(row_locations, sel, 1, result, sel, cached_cast_vectors);
}

void FetchRowFromPartition(TupleDataCollection &partition, idx_t local_offset, DataChunk &result, idx_t result_row_idx) {
    TupleDataParallelScanState gstate;
    TupleDataLocalScanState lstate;
    
    // Initialize parallel scan state
    partition.InitializeScan(gstate, TupleDataPinProperties::UNPIN_AFTER_DONE);
    
    // Initialize local scan state indexes
    lstate.segment_index = 0;
    lstate.chunk_index = 0;
    
    idx_t rows_scanned = 0;
    DataChunk chunk;
    
    // Loop until we find the chunk containing local_offset
    while (true) {
        bool has_data = partition.Scan(gstate, lstate, chunk);
        if (!has_data) {
            throw InternalException("Failed to scan enough rows to reach local_offset");
        }

        idx_t chunk_size = chunk.size();

        if (rows_scanned + chunk_size > local_offset) {
            // The desired row is in this chunk
            idx_t index_in_chunk = local_offset - rows_scanned;

            // Copy single row from chunk to result at result_row_idx
            for (idx_t col = 0; col < chunk.ColumnCount(); col++) {
                result.data[col].SetValue(result_row_idx, chunk.data[col].GetValue(index_in_chunk));
            }
            return;
        }
        rows_scanned += chunk_size;
    }
}



idx_t Scan(DataChunk &result, GroupedAggregateHashTableScanState &state, idx_t max_rows) {
	auto &data = *partitioned_data;
	idx_t total_count = data.Count();

	if (state.current_offset >= total_count) {
		return 0; // No more rows to scan
	}

	idx_t rows_to_scan = std::min(max_rows, total_count - state.current_offset);
	idx_t rows_scanned = 0;
	result.Reset();

	// Start scan from the appropriate partition and offset
	idx_t partition_idx, local_offset;
	GetPartitionAndLocalOffset(data, state.current_offset, partition_idx, local_offset);

	auto &partitions = data.GetPartitions();

	while (rows_scanned < rows_to_scan && partition_idx < partitions.size()) {
		auto &partition = *partitions[partition_idx];
		idx_t partition_count = partition.Count();

		while (local_offset < partition_count && rows_scanned < rows_to_scan) {
			// Fetch a single row into result
			FetchRowFromPartition(partition, local_offset, result, rows_scanned);

			local_offset++;
			state.current_offset++;
			rows_scanned++;
		}

		// Move to the next partition
		partition_idx++;
		local_offset = 0;
	}

	// Set the result cardinality
	result.SetCardinality(rows_scanned);
	return rows_scanned;
}


};

} // namespace duckdb
