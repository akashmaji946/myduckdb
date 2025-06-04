#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

class ChunkCollection {
public:
	ChunkCollection() : total_chunk_count(0) {}

	// Append a new chunk (copies the data)
	void Append(const DataChunk &chunk) {
		auto new_chunk = make_uniq<DataChunk>();
		// new_chunk->Initialize(chunk.GetTypes());
        new_chunk->Initialize(Allocator::DefaultAllocator(), chunk.GetTypes(), STANDARD_VECTOR_SIZE);
		new_chunk->Append(chunk);
		chunks.push_back(std::move(new_chunk));
		total_chunk_count += chunk.size();
	}

	// Clear all stored chunks
	void Reset() {
		chunks.clear();
		total_chunk_count = 0;
	}

	// Initialize a scan
	void InitializeScan() {
		scan_chunk_idx = 0;
		scan_pos = 0;
	}

	// Scan the next chunk
	bool Scan(DataChunk &out_chunk) {
		while (scan_chunk_idx < chunks.size()) {
			auto &chunk = *chunks[scan_chunk_idx];
			if (scan_pos < chunk.size()) {
				idx_t remaining = chunk.size() - scan_pos;
				idx_t fetch_count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, remaining);

				// out_chunk.Initialize(chunk.GetTypes());
				// for (idx_t col = 0; col < chunk.ColumnCount(); ++col) {
				// 	auto &src = chunk.data[col];
				// 	auto &dst = out_chunk.data[col];
				// 	src.Slice(scan_pos, fetch_count).Copy(dst);
				// }
				// out_chunk.SetCardinality(fetch_count);
                out_chunk.Initialize(Allocator::DefaultAllocator(), chunk.GetTypes(), STANDARD_VECTOR_SIZE);

                for (idx_t col = 0; col < chunk.ColumnCount(); ++col) {
                    auto &src = chunk.data[col];
                    auto &dst = out_chunk.data[col];
                    VectorOperations::Copy(src, dst, fetch_count, scan_pos, 0);
                }

                out_chunk.SetCardinality(fetch_count);

				scan_pos += fetch_count;

				if (scan_pos >= chunk.size()) {
					scan_chunk_idx++;
					scan_pos = 0;
				}
				return true;
			}
			scan_chunk_idx++;
		}
		return false;
	}

	idx_t Size() const {
		return total_chunk_count;
	}

private:
	vector<unique_ptr<DataChunk>> chunks;
	idx_t scan_chunk_idx = 0;
	idx_t scan_pos = 0;
	idx_t total_chunk_count = 0;
};

} // namespace duckdb
