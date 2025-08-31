
#include "duckdb/execution/operator/join/physical_group_join.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include <thread>
#include <chrono>
#include <iostream>
#include <vector>
#include <atomic>
#include <mutex>

namespace duckdb {

PhysicalGroupJoin::PhysicalGroupJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                     unique_ptr<PhysicalOperator> right, unique_ptr<Expression> condition_p,
                                     JoinType join_type, idx_t estimated_cardinality,
                                     vector<unique_ptr<Expression>> &groups_p,
                                     vector<unique_ptr<Expression>> &aggregates_p)
    : PhysicalJoin(op, PhysicalOperatorType::GROUP_JOIN, join_type, estimated_cardinality),
      condition(std::move(condition_p)), groups(std::move(groups_p)), aggregates(std::move(aggregates_p)) {
    std::cout << "Inside PhysicalGroupJoin()" << std::endl; 
    children.push_back(std::move(left));
    children.push_back(std::move(right));
    D_ASSERT(join_type != JoinType::MARK);
    D_ASSERT(join_type != JoinType::SINGLE);
}

std::unordered_map<int, std::pair<int, int>> PhysicalGroupJoin::aggregation_map;
std::unordered_map<int, int> PhysicalGroupJoin::aggregation_map2;
std::unordered_map<int, double> PhysicalGroupJoin::final_results;

PhysicalGroupJoin::~PhysicalGroupJoin() = default;

class GroupJoinGlobalSinkState : public GlobalSinkState {
public:
    std::atomic<int> active_sink_tasks{0};
    bool finalized = false;
    bool left_scanned = false;
    bool right_scanned = false; 
    unique_ptr<GroupedAggregateHashTable> hash_table;
    vector<LogicalType> group_types;
    vector<idx_t> grouping_indices;
    vector<AggregateObject> aggregate_objects;
    vector<LogicalType> aggregate_return_types;
    ChunkCollection left_data;
    ChunkCollection right_data;
    // For parallel output
    std::vector<std::pair<int, double>> result_vector;
    ChunkCollection result_chunks;

};

class GroupJoinLocalSinkState : public LocalSinkState {
public:
    GroupJoinLocalSinkState(Allocator &allocator, const PhysicalGroupJoin &op, ClientContext &context)
        : aggregate_executor(context) {
        for (auto &aggr_expr : op.aggregates) {
            auto &bound_aggr = aggr_expr->Cast<BoundAggregateExpression>();
            for (auto &child : bound_aggr.children) {
                aggregate_executor.AddExpression(*child);
            }
        }
        vector<LogicalType> aggregate_input_types;
        for (auto &aggr_expr : op.aggregates) {
            auto &bound_aggr = aggr_expr->Cast<BoundAggregateExpression>();
            for (auto &child : bound_aggr.children) {
                aggregate_input_types.push_back(child->return_type);
            }
        }
        aggregate_input_chunk.Initialize(allocator, aggregate_input_types);
    }
    ExpressionExecutor aggregate_executor;
    DataChunk aggregate_input_chunk;
};

unique_ptr<GlobalSinkState> PhysicalGroupJoin::GetGlobalSinkState(ClientContext &context) const {
    auto state = make_uniq<GroupJoinGlobalSinkState>();
    auto &left_types = children[0]->types;
    auto &right_types = children[1]->types;
    for (auto &group : groups) {
        auto &bound_ref = group->Cast<BoundReferenceExpression>();
        idx_t idx = bound_ref.index;
        LogicalType group_type;
        if (idx < left_types.size()) {
            group_type = left_types[idx];
            state->grouping_indices.push_back(idx);
        } else {
            idx_t right_idx = idx - left_types.size();
            if (right_idx >= right_types.size()) {
                throw InternalException("Grouping attribute index out of bounds in right child's types");
            }
            group_type = right_types[right_idx];
            state->grouping_indices.push_back(idx);
        }
        state->group_types.push_back(group_type);
    }
    for (auto &aggr_expr : aggregates) {
        if (aggr_expr->expression_class != ExpressionClass::BOUND_AGGREGATE) {
            throw NotImplementedException("Expected bound aggregate expression");
        }
        auto &bound_aggr = aggr_expr->Cast<BoundAggregateExpression>();
        AggregateObject aggr_obj(&bound_aggr);
        state->aggregate_objects.push_back(aggr_obj);
        state->aggregate_return_types.push_back(bound_aggr.return_type);
    }
    vector<LogicalType> aggregate_input_types;
    for (auto &aggr_expr : aggregates) {
        auto &bound_aggr = aggr_expr->Cast<BoundAggregateExpression>();
        if (!bound_aggr.children.empty()) {
            for (auto &child : bound_aggr.children) {
                aggregate_input_types.push_back(child->return_type);
            }
        } else {
            aggregate_input_types.push_back(LogicalType::INTEGER);
        }
    }
    state->hash_table = make_uniq<GroupedAggregateHashTable>(
        context, Allocator::Get(context),
        state->group_types, aggregate_input_types, state->aggregate_objects
    );
    return std::move(state);
}

unique_ptr<LocalSinkState> PhysicalGroupJoin::GetLocalSinkState(ExecutionContext &context) const {
    return make_uniq<GroupJoinLocalSinkState>(Allocator::Get(context.client), *this, context.client);
}

static int chunkCount = 0;
SinkResultType PhysicalGroupJoin::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
    auto &global_state = input.global_state.Cast<GroupJoinGlobalSinkState>();
    const auto &input_types = chunk.GetTypes();
    auto &left_types = children[0]->types;
    auto &right_types = children[1]->types;
    int left_est_card = children[0]->estimated_cardinality;
    int right_est_card = children[1]->estimated_cardinality;

    chunkCount++;
    // std::cout << ">> PhysicalGroupJoin::Sink - Processing chunk number: " << chunkCount << std::endl;

    if (input_types == left_types) {
        global_state.left_data.Append(chunk);
        if (global_state.left_data.Size() >= left_est_card || chunk.size() < STANDARD_VECTOR_SIZE) {
            global_state.left_scanned = true;
        }
    } else if (input_types == right_types) {
        global_state.right_data.Append(chunk);
        if (global_state.right_data.Size() >= right_est_card || chunk.size() < STANDARD_VECTOR_SIZE) {
            global_state.right_scanned = true;
        }
    } else {
        throw InternalException("PhysicalGroupJoin::Sink: input chunk types do not match any child");
    }
    return SinkResultType::NEED_MORE_INPUT;
}

// Parallel aggregation: step 1 and 2 in parallel, then combine, then store result as vector for parallel output
// Idea:1
// void duckdb::PhysicalGroupJoin::PerformEqualityAggregation(
    
//     duckdb::GroupJoinGlobalSinkState &global_state, std::unordered_map<int, double>& final_results) const {


//     auto start = std::chrono::high_resolution_clock::now();

//     std::unordered_map<int, double> left_sums;
//     std::unordered_map<int, long long int> right_counts;

//     auto left_agg = [&]() {
//         duckdb::DataChunk lscan_chunk;
//         global_state.left_data.InitializeScan();
//         while (global_state.left_data.Scan(lscan_chunk)) {
//             for (size_t i = 0; i < lscan_chunk.size(); ++i) {
//                 int key;
//                 double value;
//                 try {
//                     key = lscan_chunk.data[0].GetValue(i).GetValue<int>();
//                     value = lscan_chunk.data[1].GetValue(i).GetValue<double>();
//                 } catch (const std::exception& e) {
//                     continue;
//                 }
//                 left_sums[key] += value;
//             }
//         }
//     };

//     auto right_agg = [&]() {
//         duckdb::DataChunk rscan_chunk;
//         global_state.right_data.InitializeScan();
//         while (global_state.right_data.Scan(rscan_chunk)) {
//             for (size_t i = 0; i < rscan_chunk.size(); ++i) {
//                 int key;
//                 try {
//                     key = rscan_chunk.data[0].GetValue(i).GetValue<int>();
//                 } catch (const std::exception& e) {
//                     continue;
//                 }
//                 right_counts[key]++;
//             }
//         }
//     };

//     std::thread left_thread(left_agg);
//     std::thread right_thread(right_agg);
//     left_thread.join();
//     right_thread.join();

//     auto end = std::chrono::high_resolution_clock::now();
//     std::chrono::duration<double> elapsed = end - start;
//     std::cout << ">> Time taken for scanning and local aggregation: " << elapsed.count() << " seconds" << std::endl;
//     start = std::chrono::high_resolution_clock::now();
//     for (const auto &left_entry : left_sums) {
//         int key = left_entry.first;
//         double sum = left_entry.second;
//         long long int matching_rows = right_counts.count(key) ? right_counts.at(key) : 0;
//         if (right_counts.count(key)) {
//             final_results[key] = sum * matching_rows;
//         }
//     }

//     end = std::chrono::high_resolution_clock::now();
//     elapsed = end - start;
//     std::cout << "Left Table Size:" << left_sums.size() << std::endl;
//     std::cout << "Right Table Size:" << right_counts.size() << std::endl;
//     std::cout << "Final Table Size:" << final_results.size() << std::endl;
//     std::cout << ">>> Total time taken for PerformEqualityAggregation(): " << elapsed.count() << " seconds" << std::endl;
// }


// Idea:2 - Parallel partitioning, then parallel aggregation per partition, then merge partitions
// works well - but still slower than single-threaded version for small datasets due to overhead
// void duckdb::PhysicalGroupJoin::PerformEqualityAggregation(
//     duckdb::GroupJoinGlobalSinkState &global_state, std::unordered_map<int, double>& final_results) const {

//     auto start = std::chrono::high_resolution_clock::now();

//     const int NUM_PARTITIONS = std::thread::hardware_concurrency();
//     int radix_mask = NUM_PARTITIONS - 1;

//     // Partitioned hash tables for left and right
//     std::vector<std::unordered_map<int, double>> left_partitions(NUM_PARTITIONS);
//     std::vector<std::unordered_map<int, long long int>> right_partitions(NUM_PARTITIONS);

//     // Parallel partitioning of left and right tables
//     std::thread left_thread([&]() {
//         duckdb::DataChunk lscan_chunk;
//         global_state.left_data.InitializeScan();
//         while (global_state.left_data.Scan(lscan_chunk)) {
//             for (size_t i = 0; i < lscan_chunk.size(); ++i) {
//                 int key;
//                 double value;
//                 try {
//                     key = lscan_chunk.data[0].GetValue(i).GetValue<int>();
//                     value = lscan_chunk.data[1].GetValue(i).GetValue<double>();
//                 } catch (const std::exception& e) {
//                     continue;
//                 }
//                 size_t partition = std::hash<int>{}(key) & radix_mask;
//                 left_partitions[partition][key] += value;
//             }
//         }
//     });

//     std::thread right_thread([&]() {
//         duckdb::DataChunk rscan_chunk;
//         global_state.right_data.InitializeScan();
//         while (global_state.right_data.Scan(rscan_chunk)) {
//             for (size_t i = 0; i < rscan_chunk.size(); ++i) {
//                 int key;
//                 try {
//                     key = rscan_chunk.data[0].GetValue(i).GetValue<int>();
//                 } catch (const std::exception& e) {
//                     continue;
//                 }
//                 size_t partition = std::hash<int>{}(key) & radix_mask;
//                 right_partitions[partition][key]++;
//             }
//         }
//     });

//     left_thread.join();
//     right_thread.join();

//     auto end = std::chrono::high_resolution_clock::now();
//     std::chrono::duration<double> elapsed = end - start;
//     std::cout << ">> Time taken for parallel scanning and partitioning: " << elapsed.count() << " seconds" << std::endl;
//     start = std::chrono::high_resolution_clock::now();

//     // Parallel aggregation per partition
//     std::vector<std::unordered_map<int, double>> partition_results(NUM_PARTITIONS);
//     std::vector<std::thread> threads;
//     for (int p = 0; p < NUM_PARTITIONS; ++p) {
//         threads.emplace_back([&, p]() {
//             for (const auto &left_entry : left_partitions[p]) {
//                 int key = left_entry.first;
//                 double sum = left_entry.second;
//                 long long int matching_rows = right_partitions[p].count(key) ? right_partitions[p].at(key) : 0;
//                 if (matching_rows > 0) {
//                     partition_results[p][key] = sum * matching_rows;
//                 }
//             }
//         });
//     }
//     for (auto &t : threads) t.join();

//     // Merge partition results
//     for (int p = 0; p < NUM_PARTITIONS; ++p) {
//         for (const auto &entry : partition_results[p]) {
//             final_results[entry.first] = entry.second;
//         }
//     }

//     end = std::chrono::high_resolution_clock::now();
//     elapsed = end - start;
//     std::cout << "Final Table Size:" << final_results.size() << std::endl;
//     std::cout << "> Total time taken for PerformEqualityAggregation() with parallel partitioning: " << elapsed.count() << " seconds" << std::endl;
// }


// // Idea:3 - Parallel scan + thread-local aggregation, then global merge, then final join
// // Buggy - needs fixing
// void duckdb::PhysicalGroupJoin::PerformEqualityAggregation(
//     duckdb::GroupJoinGlobalSinkState &global_state,
//     std::unordered_map<int, double>& final_results) const {

//     auto start = std::chrono::steady_clock::now();

//     // Number of worker threads (you can tune this)
//     const unsigned NUM_THREADS = std::thread::hardware_concurrency();

//     // Thread-local maps: each thread writes into its own
//     std::vector<std::unordered_map<int, double>> left_thread_maps(NUM_THREADS);
//     std::vector<std::unordered_map<int, long long>> right_thread_maps(NUM_THREADS);

//     // Lambda to scan a relation in parallel
//     auto scan_relation = [&](ChunkCollection &scan_state, bool is_left, unsigned tid) {
//         duckdb::DataChunk chunk;
//         while (scan_state.Scan(chunk)) {
//             for (size_t i = 0; i < chunk.size(); ++i) {
//                 try {
//                     int key = chunk.data[0].GetValue(i).GetValue<int>();
//                     if (is_left) {
//                         double val = chunk.data[1].GetValue(i).GetValue<double>();
//                         left_thread_maps[tid][key] += val;
//                     } else {
//                         right_thread_maps[tid][key]++;
//                     }
//                 } catch (...) {
//                     continue;
//                 }
//             }
//         }
//     };

//     // Launch parallel scans
//     std::vector<std::thread> threads;
//     for (unsigned t = 0; t < NUM_THREADS; t++) {
//         threads.emplace_back([&, t]() {
//             if (t % 2 == 0) { // half threads for left, half for right
//                 global_state.left_data.InitializeScan();
//                 scan_relation(global_state.left_data, true, t);
//             } else {
//                 global_state.right_data.InitializeScan();
//                 scan_relation(global_state.right_data, false, t);
//             }
//         });
//     }
//     for (auto &th : threads) th.join();

//     auto end = std::chrono::steady_clock::now();
//     std::cout << ">> Parallel scan + thread-local aggregation: "
//               << std::chrono::duration<double>(end - start).count() << " sec\n";

//     start = std::chrono::steady_clock::now();

//     // === Global Merge Phase ===
//     std::unordered_map<int, double> left_sums;
//     std::unordered_map<int, long long> right_counts;

//     // Merge left thread-local maps
//     for (auto &map : left_thread_maps) {
//         for (auto &kv : map) {
//             left_sums[kv.first] += kv.second;
//         }
//     }
//     // Merge right thread-local maps
//     for (auto &map : right_thread_maps) {
//         for (auto &kv : map) {
//             right_counts[kv.first] += kv.second;
//         }
//     }

//     // === Final join ===
//     for (auto &kv : left_sums) {
//         int key = kv.first;
//         double sum = kv.second;
//         auto it = right_counts.find(key);
//         if (it != right_counts.end()) {
//             final_results[key] = sum * it->second;
//         }
//     }

//     end = std::chrono::steady_clock::now();
//     std::cout << ">>> Merge + final join: "
//               << std::chrono::duration<double>(end - start).count() << " sec\n";
// }


// Idea:4 
// Idea:3 - Parallel scan + thread-local aggregation, then global merge, then final join
void duckdb::PhysicalGroupJoin::PerformEqualityAggregation(
    duckdb::GroupJoinGlobalSinkState &global_state,
    std::unordered_map<int, double> &final_results) const {

    auto start = std::chrono::steady_clock::now();

    const unsigned NUM_THREADS = std::thread::hardware_concurrency();
    std::cout << "Using " << NUM_THREADS << " threads for parallel aggregation" << std::endl;
    const unsigned HALF_THREADS = NUM_THREADS / 2; // left and right equally split

    std::vector<std::unordered_map<int, double>> left_thread_maps(HALF_THREADS);
    std::vector<std::unordered_map<int, long long>> right_thread_maps(NUM_THREADS - HALF_THREADS);

    // Lambda: strided parallel scan of a relation
    auto scan_relation = [&](ChunkCollection &scan_state, bool is_left, int tid, int stride) {
        duckdb::DataChunk chunk;
        size_t idx = tid;
        while (scan_state.PScan(chunk, idx)) {
            for (size_t i = 0; i < chunk.size(); ++i) {
                try {
                    int key = chunk.data[0].GetValue(i).GetValue<int>();
                    if (is_left) {
                        double val = chunk.data[1].GetValue(i).GetValue<double>();
                        left_thread_maps[tid][key] += val;
                    } else {
                        right_thread_maps[tid][key]++; // offset index for right
                    }
                } catch (...) {
                    continue;
                }
            }
            idx += stride; // jump by stride to interleave scans
        }
    };

    global_state.left_data.InitializeScan();
    global_state.right_data.InitializeScan();

    // Launch threads
    std::vector<std::thread> threads;
    for (unsigned t = 0; t < NUM_THREADS; t++) {
        threads.emplace_back([&, t]() {
            if (t < HALF_THREADS) {
                // Left relation threads
                scan_relation(global_state.left_data, true, t, HALF_THREADS);
            } else {
                // Right relation threads
                scan_relation(global_state.right_data, false, t - HALF_THREADS, NUM_THREADS - HALF_THREADS);
            }
        });
    }
    for (auto &th : threads)
        th.join();

    auto end = std::chrono::steady_clock::now();
    std::cout << ">> Parallel scan + thread-local aggregation : "
              << std::chrono::duration<double>(end - start).count() << " sec\n";

    start = std::chrono::steady_clock::now();

    // === Global Merge Phase ===
    std::unordered_map<int, double> left_sums;
    std::unordered_map<int, long long> right_counts;

        // run left and right merges in parallel
        std::thread t1([&] {
            for (auto &map : left_thread_maps) {
                for (auto &kv : map) {
                    left_sums[kv.first] += kv.second;
                }
            }
        });

        std::thread t2([&] {
            for (auto &map : right_thread_maps) {
                for (auto &kv : map) {
                    right_counts[kv.first] += kv.second;
                }
            }
        });

        t1.join();
        t2.join();

    // === Final join ===
    for (auto &kv : left_sums) {
        int key = kv.first;
        double sum = kv.second;
        auto it = right_counts.find(key);
        if (it != right_counts.end()) {
            final_results[key] = sum * it->second;
        }
    }

    end = std::chrono::steady_clock::now();
    std::cout << "Left Table Size:" << left_sums.size() << std::endl;
    std::cout << "Right Table Size:" << right_counts.size() << std::endl;
    std::cout << "Final Table Size:" << final_results.size() << std::endl;
    std::cout << ">>> Merge + final join using PScan: "
              << std::chrono::duration<double>(end - start).count() << " sec\n";
}

void StoreResultsAsChunksParallel(GroupJoinGlobalSinkState &global_state) {
    global_state.result_chunks.Reset();
    const auto &result_vector = global_state.result_vector;
    vector<LogicalType> result_types = {LogicalType::BIGINT, LogicalType::DECIMAL(38, 2)};
    idx_t total = result_vector.size();
    idx_t chunk_size = STANDARD_VECTOR_SIZE;

    // Calculate number of chunks
    idx_t num_chunks = (total + chunk_size - 1) / chunk_size;
    std::vector<std::unique_ptr<DataChunk>> chunks(num_chunks);

    const unsigned NUM_THREADS = std::thread::hardware_concurrency();

    // Parallel chunk building with strided assignment
    std::vector<std::thread> threads;
    for (unsigned t = 0; t < NUM_THREADS; ++t) {
        threads.emplace_back([&, t]() {
            for (idx_t c = t; c < num_chunks; c += NUM_THREADS) {
                idx_t offset = c * chunk_size;
                idx_t this_chunk_size = std::min<idx_t>(chunk_size, total - offset);
                auto chunk = make_uniq<DataChunk>();
                chunk->Initialize(Allocator::DefaultAllocator(), result_types);
                chunk->SetCardinality(this_chunk_size);
                for (idx_t i = 0; i < this_chunk_size; ++i) {
                    const auto &entry = result_vector[offset + i];
                    chunk->data[0].SetValue(i, Value::INTEGER(entry.first));
                    chunk->data[1].SetValue(i, Value::DOUBLE(entry.second));
                }
                chunks[c] = std::move(chunk);
            }
        });
    }
    for (auto &t : threads) t.join();

    // Append all chunks to the collection
    int total_rows = 0;
    int chunks_created = 0;
    for (auto &chunk : chunks) {
        if (chunk) {
            total_rows += chunk->size();
            chunks_created++;
        }
        global_state.result_chunks.Append(*chunk);
    }
    std::cout << "Stored " << total_rows << " rows in result_chunks using " << num_chunks << " chunks." << std::endl;
    std::cout << "Chunks created: " << chunks_created << std::endl;
}




// For parallel output, store result as vector in global_state
void StoreResultsAsVector(GroupJoinGlobalSinkState &global_state) {
    global_state.result_vector.clear();
    for (const auto &entry : PhysicalGroupJoin::final_results) {
        global_state.result_vector.emplace_back(entry.first, entry.second);
    }
}

// Used for parallel GetData
class GroupJoinGlobalSourceState : public GlobalSourceState {
public:
    explicit GroupJoinGlobalSourceState(GroupJoinGlobalSinkState *sink_state_p)
        : sink_state(sink_state_p), next_idx(0) {}
    GroupJoinGlobalSinkState *sink_state;
    std::atomic<size_t> next_idx;
};

struct GroupJoinLocalSourceState : public LocalSourceState {
    size_t my_offset = 0;
    size_t my_end = 0;
};

unique_ptr<GlobalSourceState> PhysicalGroupJoin::GetGlobalSourceState(ClientContext &context) const {
    auto &global_sink = sink_state->Cast<GroupJoinGlobalSinkState>();
    return make_uniq<GroupJoinGlobalSourceState>(&global_sink);
}

unique_ptr<LocalSourceState> PhysicalGroupJoin::GetLocalSourceState(ExecutionContext &context, GlobalSourceState &gstate) const {
    auto &global = gstate.Cast<GroupJoinGlobalSourceState>();
    auto local = make_uniq<GroupJoinLocalSourceState>();
    // Each thread gets a range of the result vector
    size_t total = global.sink_state->result_vector.size();
    size_t chunk_size = STANDARD_VECTOR_SIZE;
    size_t idx = global.next_idx.fetch_add(chunk_size);
    local->my_offset = idx;
    local->my_end = std::min(idx + chunk_size, total);
    return local;
}

static int getdatacnt = 0;

// SourceResultType PhysicalGroupJoin::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {

//     static auto start = std::chrono::high_resolution_clock::now();
    
//     getdatacnt++;
//     std::cout << "Get data called:" << getdatacnt << std::endl;

//     auto &global = input.global_state.Cast<GroupJoinGlobalSourceState>();
//     auto &local = input.local_state.Cast<GroupJoinLocalSourceState>();
//     auto &result_vector = global.sink_state->result_vector;
//     size_t total = result_vector.size();
//     if (local.my_offset >= total) {
//          auto end = std::chrono::high_resolution_clock::now();
//         std::chrono::duration<double> elapsed = end - start;
//         std::cout << ">>> Total time taken for GetData(): " << elapsed.count() << " seconds" << std::endl;
//         return SourceResultType::FINISHED;
//     }
//     size_t rows_to_output = local.my_end - local.my_offset;
//     if (chunk.ColumnCount() == 0) {
//         vector<LogicalType> result_types = {LogicalType::INTEGER, LogicalType::HUGEINT};
//         chunk.Initialize(Allocator::Get(context.client), result_types);
//     }
//     chunk.SetCardinality(rows_to_output);
//     for (size_t row_idx = 0; row_idx < rows_to_output; ++row_idx) {
//         const auto &entry = result_vector[local.my_offset + row_idx];
//         chunk.data[0].SetValue(row_idx, Value::INTEGER(entry.first));
//         chunk.data[1].SetValue(row_idx, Value::DOUBLE(entry.second));
//     }
//     // Prepare next range for this thread
//     size_t chunk_size = STANDARD_VECTOR_SIZE;
//     size_t idx = global.next_idx.fetch_add(chunk_size);
//     local.my_offset = idx;
//     local.my_end = std::min(idx + chunk_size, total);
//     if (local.my_offset >= total) {
//         auto end = std::chrono::high_resolution_clock::now();
//         std::chrono::duration<double> elapsed = end - start;
//         std::cout << ">>> Total time taken for GetData(): " << elapsed.count() << " seconds" << std::endl;

//         // chunk types
//         auto v = chunk.GetTypes();
//         for(auto &t : v) {
//             std::cout << t.ToString() << "**";
//         }   
//         std::cout << std::endl;

//         return SourceResultType::FINISHED;
//     }
//     return SourceResultType::HAVE_MORE_OUTPUT;
// }

// SourceResultType PhysicalGroupJoin::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {

//     static auto start = std::chrono::high_resolution_clock::now();

//     // std::cout << "Get data called:" << getdatacnt++ << std::endl;
//     getdatacnt++;

//     auto &global = input.global_state.Cast<GroupJoinGlobalSourceState>();
//     auto &local = input.local_state.Cast<GroupJoinLocalSourceState>();
//     auto &result_chunks = global.sink_state->result_chunks;
//     size_t total_chunks = result_chunks.NumChunks();

//     std::cout << "Total chunks in result_chunks: " << total_chunks << std::endl;

//     if (local.my_offset >= total_chunks) {
//          auto end = std::chrono::high_resolution_clock::now();
//         std::chrono::duration<double> elapsed = end - start;
//         std::cout << ">> Total time taken for GetData(): " << elapsed.count() << " seconds" << std::endl;
//         std::cout << ">> Total chunks processed in GetData(): " << getdatacnt << std::endl;
//         return SourceResultType::FINISHED;
//     }
//     // Fetch the chunk at my_offset
//     result_chunks.ScanAtIndex(chunk, local.my_offset);
//     // Prepare next range for this thread
//     size_t chunk_size = 1; // one chunk at a time
//     size_t idx = global.next_idx.fetch_add(chunk_size);
//     local.my_offset = idx;
//     local.my_end = std::min(idx + chunk_size, total_chunks);
//     if (local.my_offset >= total_chunks) {
//         auto end = std::chrono::high_resolution_clock::now();
//         std::chrono::duration<double> elapsed = end - start;
//         std::cout << ">>> Total time taken for GetData(): " << elapsed.count() << " seconds" << std::endl;
//         std::cout << ">> Total chunks processed in GetData(): " << getdatacnt << std::endl;
//         return SourceResultType::FINISHED;
//     }
//     return SourceResultType::HAVE_MORE_OUTPUT;
// }

SourceResultType PhysicalGroupJoin::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
    auto &global = input.global_state.Cast<GroupJoinGlobalSourceState>();
    auto &local = input.local_state.Cast<GroupJoinLocalSourceState>();
    auto &result_chunks = global.sink_state->result_chunks;
    size_t total_chunks = result_chunks.NumChunks();

    if (local.my_offset >= total_chunks) {
            // chunk types
            auto v = chunk.GetTypes();
            for(auto &t : v) {
                std::cout << t.ToString() << "**";
            }   
            std::cout << std::endl;
        return SourceResultType::FINISHED;
    }
    // Output the next chunk
    result_chunks.ScanAtIndex(chunk, local.my_offset);

    // Advance to next chunk for this thread
    local.my_offset++;
    if (local.my_offset >= total_chunks) {
        // chunk types
        auto v = chunk.GetTypes();
        for(auto &t : v) {
            std::cout << t.ToString() << "**";
        }   
        std::cout << std::endl;
        return SourceResultType::FINISHED;
    }
    return SourceResultType::HAVE_MORE_OUTPUT;
}

SinkFinalizeType PhysicalGroupJoin::Finalize(Pipeline &pipeline, Event &event, ClientContext &context, OperatorSinkFinalizeInput &input) const {
    auto &global_state = input.global_state.Cast<GroupJoinGlobalSinkState>();
    if (!global_state.left_scanned || !global_state.right_scanned) {
        return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
    }
    if (global_state.finalized) {
        return SinkFinalizeType::READY;
    }
    global_state.finalized = true;
    final_results.clear();
    PerformEqualityAggregation(global_state, final_results);
    StoreResultsAsVector(global_state);
    // StoreResultsAsChunksParallel(global_state);
    std::cout << "Final result size: " << global_state.result_vector.size() << std::endl;

    StoreResultsAsChunksParallel(global_state);
    std::cout << "Final result chunk size: " << global_state.result_chunks.NumChunks() << std::endl;

    return SinkFinalizeType::READY;
}

InsertionOrderPreservingMap<string> PhysicalGroupJoin::ParamsToString() const {
    InsertionOrderPreservingMap<string> result;
    result["Join Type"] = EnumUtil::ToString(join_type);
    result["Join Condition"] = condition->GetName();
    auto &grps = groups;
    auto &aggr = aggregates;
    string groups_info;
    for (idx_t i = 0; i < groups.size(); i++) {
        if (i > 0) {
            groups_info += "\n";
        }
        groups_info += grps[i]->GetName();
    }
    result["Groups"] = groups_info;
    string aggregate_info;
    for (idx_t i = 0; i < aggr.size(); i++) {
        auto &aggregate = aggr[i]->Cast<BoundAggregateExpression>();
        if (i > 0) {
            aggregate_info += "\n";
        }
        aggregate_info += aggr[i]->GetName();
        if (aggregate.filter) {
            aggregate_info += " Filter: " + aggregate.filter->GetName();
        }
    }
    result["Aggregates"] = aggregate_info;
    return result;
}

class GroupJoinOperatorState : public OperatorState {
public:
    GroupJoinOperatorState(ClientContext &context, const PhysicalGroupJoin &op)
        : group_executor(context) {
        for (auto &group : op.groups) {
            group_executor.AddExpression(*group);
        }
    }

    //! Expression executor for the grouping expression
    ExpressionExecutor group_executor;
    //! DataChunk to store the grouping keys
    DataChunk group_chunk;
    //! DataChunk to store the aggregates fetched from the hash table
    DataChunk aggregate_chunk;
};

unique_ptr<OperatorState> PhysicalGroupJoin::GetOperatorState(ExecutionContext &context) const {

    std::cout << "Inside GetOperatorState()" << std::endl;

    auto &global_state = sink_state->Cast<GroupJoinGlobalSinkState>();
    auto state = make_uniq<GroupJoinOperatorState>(context.client, *this);

    // Initialize the group chunk and aggregate chunk
    state->group_chunk.InitializeEmpty(global_state.group_types);
    state->aggregate_chunk.InitializeEmpty(global_state.aggregate_return_types);

    std::cout << "Outside GetOperatorState()" << std::endl;
    return std::move(state);
}

OperatorResultType PhysicalGroupJoin::ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                      GlobalOperatorState &gstate, OperatorState &state) const {
  
    return OperatorResultType::NEED_MORE_INPUT;
}


void PhysicalGroupJoin::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
    D_ASSERT(children.size() == 2);
    auto &left_child = children[0];
    auto &right_child = children[1];
    auto &state = meta_pipeline.GetState();
    state.SetPipelineSource(current, *this);
    MetaPipeline &left_meta = meta_pipeline.CreateChildMetaPipeline(current, *this);
    left_meta.Build(*left_child);
    MetaPipeline &right_meta = meta_pipeline.CreateChildMetaPipeline(current, *this);
    right_meta.Build(*right_child);
    shared_ptr<Pipeline> current_ptr = current.shared_from_this();
    shared_ptr<Pipeline> &left_ptr = left_meta.GetBasePipeline();
    shared_ptr<Pipeline> &right_ptr = right_meta.GetBasePipeline();
    current_ptr->AddDependency(left_ptr);
    current_ptr->AddDependency(right_ptr);
}

} // namespace duckdb
