// #include "duckdb/execution/operator/join/physical_group_join.hpp"
// #include "duckdb/execution/aggregate_hashtable.hpp"
// #include "duckdb/execution/expression_executor.hpp"
// #include "duckdb/common/types/column/column_data_collection.hpp"
// #include "duckdb/planner/expression/bound_comparison_expression.hpp"
// #include "duckdb/planner/expression/bound_reference_expression.hpp"
// #include "duckdb/planner/expression/bound_aggregate_expression.hpp"
// #include "duckdb/function/aggregate_function.hpp"
// #include "duckdb/common/types/chunk_collection.hpp"
// #include "duckdb/planner/expression/bound_aggregate_expression.hpp"
// #include "duckdb/parallel/meta_pipeline.hpp"
// #include <thread>
// #include <chrono>
// #include <iostream>


// namespace duckdb {

// PhysicalGroupJoin::PhysicalGroupJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
//                                      unique_ptr<PhysicalOperator> right, unique_ptr<Expression> condition_p,
//                                      JoinType join_type, idx_t estimated_cardinality,
//                                      vector<unique_ptr<Expression>> &groups_p,
//                                      vector<unique_ptr<Expression>> &aggregates_p)
//     : PhysicalJoin(op, PhysicalOperatorType::GROUP_JOIN, join_type, estimated_cardinality),
//       condition(std::move(condition_p)), groups(std::move(groups_p)), aggregates(std::move(aggregates_p)) {

//     std::cout << "Inside PhysicalGroupJoin()" << std::endl; 
    
//     // aggregation_map = std::unordered_map<int, std::pair<int, int>>(); // Initialize map
//     // aggregation_map2 = std::unordered_map<int, int>(); // Initialize map

//     children.push_back(std::move(left));
//     children.push_back(std::move(right));

//     // children.push_back(std::move(left));

//     D_ASSERT(join_type != JoinType::MARK);
//     D_ASSERT(join_type != JoinType::SINGLE);
// }

// std::unordered_map<int, std::pair<int, int>> PhysicalGroupJoin::aggregation_map;
// std::unordered_map<int, int> PhysicalGroupJoin::aggregation_map2;

// std::unordered_map<int, double> PhysicalGroupJoin::final_results;


// PhysicalGroupJoin::~PhysicalGroupJoin() = default;

// class GroupJoinGlobalSinkState : public GlobalSinkState {
// public:
//     std::atomic<int> active_sink_tasks{0};
//     bool finalized = false;

//     bool left_scanned = false;
//     bool right_scanned = false; 
//     //! The hash table that will be used to store the grouping attributes and aggregates
//     unique_ptr<GroupedAggregateHashTable> hash_table;

//     //! The types of the grouping columns
//     vector<LogicalType> group_types;

//     //! The indices of the grouping columns in the right child's output
//     vector<idx_t> grouping_indices;

//     //! The aggregate objects
//     vector<AggregateObject> aggregate_objects;

//     //! The payload types (types of the aggregates)
//     vector<LogicalType> aggregate_return_types;

//     //! Buffers to store input from left and right children
//     ChunkCollection left_data;
//     ChunkCollection right_data;
// };


// class GroupJoinLocalSinkState : public LocalSinkState {
// public:
//     GroupJoinLocalSinkState(Allocator &allocator, const PhysicalGroupJoin &op, ClientContext &context)
//         : aggregate_executor(context) {

//         std::cout << "Inside GroupJoinLocalSinkState()" << std::endl;

//         for (auto &aggr_expr : op.aggregates) {
//             auto &bound_aggr = aggr_expr->Cast<BoundAggregateExpression>();
//             for (auto &child : bound_aggr.children) {
//                 aggregate_executor.AddExpression(*child);
//             }
//         }

//         // Initialize the aggregate input chunk
//         vector<LogicalType> aggregate_input_types;
//         for (auto &aggr_expr : op.aggregates) {
//             auto &bound_aggr = aggr_expr->Cast<BoundAggregateExpression>();
//             for (auto &child : bound_aggr.children) {
//                 aggregate_input_types.push_back(child->return_type);
//             }
//         }
//         aggregate_input_chunk.Initialize(allocator, aggregate_input_types);
        
//         std::cout << "Outside GroupJoinLocalSinkState()" << std::endl;

//     }

//     //! Expression executor for the aggregates
//     ExpressionExecutor aggregate_executor;
//     //! DataChunk to hold the evaluated aggregate inputs
//     DataChunk aggregate_input_chunk;
// };



// unique_ptr<GlobalSinkState> PhysicalGroupJoin::GetGlobalSinkState(ClientContext &context) const {
//     std::cout << "Inside GetGlobalSinkState() START" << std::endl;

//     auto state = make_uniq<GroupJoinGlobalSinkState>();

//     // Get the types from the left and right children
//     auto &left_types = children[0]->types;
//     auto &right_types = children[1]->types;

//     // Set up group types and grouping indices
//     for (auto &group : groups) {
//         auto &bound_ref = group->Cast<BoundReferenceExpression>();
//         idx_t idx = bound_ref.index;
//         LogicalType group_type;
//         if (idx < left_types.size()) {
//             // Grouping column from left child
//             group_type = left_types[idx];
//             state->grouping_indices.push_back(idx); // index in joined chunk
//         } else {
//             // Grouping column from right child
//             idx_t right_idx = idx - left_types.size();
//             if (right_idx >= right_types.size()) {
//                 throw InternalException("Grouping attribute index out of bounds in right child's types");
//             }
//             group_type = right_types[right_idx];
//             state->grouping_indices.push_back(idx); // index in joined chunk
//         }
//         state->group_types.push_back(group_type);
//     }

//     // Prepare the aggregate objects and return types
//     for (auto &aggr_expr : aggregates) {
//         if (aggr_expr->expression_class != ExpressionClass::BOUND_AGGREGATE) {
//             throw NotImplementedException("Expected bound aggregate expression");
//         }
//         auto &bound_aggr = aggr_expr->Cast<BoundAggregateExpression>();
//         AggregateObject aggr_obj(&bound_aggr);
//         state->aggregate_objects.push_back(aggr_obj);
//         state->aggregate_return_types.push_back(bound_aggr.return_type);
//     }

//     // Prepare aggregate input types (types of aggregate arguments, not return types)
//     vector<LogicalType> aggregate_input_types;
//     for (auto &aggr_expr : aggregates) {
//         auto &bound_aggr = aggr_expr->Cast<BoundAggregateExpression>();
//         if (!bound_aggr.children.empty()) {
//             for (auto &child : bound_aggr.children) {
//                 aggregate_input_types.push_back(child->return_type);
//             }
//         } else {
//             // COUNT(*) or similar: use INTEGER as dummy
//             aggregate_input_types.push_back(LogicalType::INTEGER);
//         }
//     }

//     // Initialize the hash table with group types and aggregate input types
//     state->hash_table = make_uniq<GroupedAggregateHashTable>(
//         context, Allocator::Get(context),
//         state->group_types, aggregate_input_types, state->aggregate_objects
//     );

//     std::cout << "Inside GetGlobalSinkState() END" << std::endl;
//     return std::move(state);
// }

// unique_ptr<LocalSinkState> PhysicalGroupJoin::GetLocalSinkState(ExecutionContext &context) const {
//     return make_uniq<GroupJoinLocalSinkState>(Allocator::Get(context.client), *this, context.client);
// }

// static int leftc = 1;
// static int rightc = 1;

// SinkResultType PhysicalGroupJoin::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
//     // std::cout << "Inside Sink()" << std::endl;
//     auto &global_state = input.global_state.Cast<GroupJoinGlobalSinkState>();

//     // Use types to distinguish left/right child
//     const auto &input_types = chunk.GetTypes();
//     auto &left_types = children[0]->types;
//     auto &right_types = children[1]->types;

//     int left_est_card = children[0]->estimated_cardinality;
//     int right_est_card = children[1]->estimated_cardinality;

//     // std::cout << "ESTIMATED CARD: " << left_est_card << " " << right_est_card << std::endl;


//     if (input_types == left_types) {
//         // std::cout << "Appending chunk to LEFT table: " << leftc++ << std::endl;
//         // std::cout << chunk.ToString() << std::endl;
//         global_state.left_data.Append(chunk);
//         //  std::cout << ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>LEFT CHUNK SIZE: " << chunk.size() << std::endl;
//         if (global_state.left_data.Size() >= left_est_card || chunk.size() < STANDARD_VECTOR_SIZE) {
        
//             global_state.left_scanned = true; // Mark left table as fully scanned
//             // std::cout << "SETTING LEFT=============>" << std::endl;
//             // std::cout << chunk.ToString() << std::endl;

//         }
//     } else if (input_types == right_types) {
//         // std::cout << "Appending chunk to RIGHT table: " << rightc++ << std::endl;
//         // std::cout << chunk.ToString() << std::endl;
//         // std::cout << ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>RIGHT CHUNK SIZE: " << chunk.size() << std::endl;
//         global_state.right_data.Append(chunk);

//         if (global_state.right_data.Size() >= right_est_card || chunk.size() < STANDARD_VECTOR_SIZE) {
            
//             global_state.right_scanned = true; // Mark right table as fully scanned
//             // std::cout << "SETTING RIGHT ===============>" << std::endl;
//             // std::cout << chunk.ToString() << std::endl;
//         }

//     } else {
//         std::cout << "ERROR: input chunk types do not match any child" << std::endl;
//         throw InternalException("PhysicalGroupJoin::Sink: input chunk types do not match any child");
//     }
//     // std::cout << "Outside Sink()" << std::endl;
//     //  std::cout << "CURR CARD: " << global_state.left_data.Size() << " " << global_state.right_data.Size() << std::endl;
//     //  std::cout << (global_state.left_data.Size() == left_est_card) << " " << (global_state.right_data.Size() == right_est_card) << std::endl;
  
//     return SinkResultType::NEED_MORE_INPUT;
// }

// void duckdb::PhysicalGroupJoin::PerformEqualityAggregation(
//     duckdb::GroupJoinGlobalSinkState &global_state, std::unordered_map<int, double>& final_results) const {

//     std::cout << "Inside Parallel PerformEqualityAggregation()........." << std::endl;

//     // Map to store pre-aggregated sums from the left table: Key -> SUM(v)
//     std::unordered_map<int, double> left_sums;
//     // Map to store key counts from the right table: Key -> COUNT(*)
//     std::unordered_map<int, long long int> right_counts;


//     // measure time for step 1, 2, 3
//     auto start = std::chrono::high_resolution_clock::now();

//     // Step 1: Pre-aggregate the left table (A) in a thread
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
//                     std::cout << e.what() << std::endl;
//                     continue;
//                 }
//                 left_sums[key] += value;
//             }
//         }
//     };

//     // Step 2: Count keys in the right table (B) in a thread
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

//     // Launch both threads
//     std::thread left_thread(left_agg);
//     std::thread right_thread(right_agg);


//     // Wait for both to finish
//     left_thread.join();
//     right_thread.join();

//     auto end = std::chrono::high_resolution_clock::now();
//     std::chrono::duration<double> elapsed = end - start;
//     std::cout << "Time taken for Step 1 and Step 2: " << elapsed.count() << " seconds" << std::endl;
   
//     start = std::chrono::high_resolution_clock::now();
//     // Step 3: Combine results using equality logic
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
//     std::cout << "Time taken for Step 3: " << elapsed.count() << " seconds" << std::endl;
// }

// // void duckdb::PhysicalGroupJoin::PerformEqualityAggregation(
// //     duckdb::GroupJoinGlobalSinkState &global_state, std::unordered_map<int, double>& final_results) const {

// //     std::cout << "Inside PerformEqualityAggregation()........." << std::endl;

// //     // Map to store pre-aggregated sums from the left table: Key -> SUM(v)
// //     std::unordered_map<int, double> left_sums;
// //     // Map to store key counts from the right table: Key -> COUNT(*)
// //     std::unordered_map<int, long long int> right_counts;
// //     duckdb::DataChunk lscan_chunk;
// //     duckdb::DataChunk rscan_chunk;

// //     // --- 1. Pre-aggregate the left table (A) ---
// //     global_state.left_data.InitializeScan();
// //     while (global_state.left_data.Scan(lscan_chunk)) {
// //         for (size_t i = 0; i < lscan_chunk.size(); ++i) {
// //             int key;
// //             double value;
// //             try{
// //                              key = lscan_chunk.data[0].GetValue(i).GetValue<int>();
// //                              value = lscan_chunk.data[1].GetValue(i).GetValue<double>();
// //             }catch(const std::exception& e){
// //                 std::cout << e.what() << std::endl;
// //                 continue;
// //             }
                
// //             left_sums[key] += value;
// //         }
// //     }

// //     int times = 10;
// //     int t = 0;
// //     // std::cout << "______________________Inside PerformEqualityAggregation() 1__________________" << std::endl;
// //     // for(const auto& entry : left_sums) {
// //     //     std::cout << "Key: " << entry.first << ", Sum: " << entry.second << std::endl;
// //     //     t++;
// //     //     if(t > times) break;
// //     // }

// //     // --- 2. Count keys in the right table (B) ---
// //     global_state.right_data.InitializeScan();
// //     while (global_state.right_data.Scan(rscan_chunk)) {
// //         for (size_t i = 0; i < rscan_chunk.size(); ++i) {
// //             int key;
// //             try{
// //                 key = rscan_chunk.data[0].GetValue(i).GetValue<int>();
                            
// //             }catch(const std::exception& e){
// //                 continue;
// //             }
// //             right_counts[key]++;
// //         }
// //     }
// //     t = 0;
// //     // std::cout << "______________________Inside PerformEqualityAggregation() 2__________________" << std::endl;
// //     // for(const auto& entry : right_counts) {
// //     //     std::cout << "Key: " << entry.first << ", Count: " << entry.second << std::endl;
// //     //      t++;
// //     //     if(t > times) break;
// //     // }

// //     // --- 3. Combine results using equality logic ---
// //     for (const auto &left_entry : left_sums) {
// //         int key = left_entry.first;
// //         double sum = left_entry.second;
        
// //         // Find the number of matching rows in the right table.
// //         long long int matching_rows = right_counts.count(key) ? right_counts.at(key) : 0;
        
// //         // Final sum is SUM(v) * COUNT(matching rows in B).
// //         if(right_counts.count(key)) {
// //             final_results[key] =  sum * matching_rows;
// //         }
        
// //     }

// //     // return final_results;
// // }


// void duckdb::PhysicalGroupJoin::PerformInEqualityAggregation(
//     duckdb::GroupJoinGlobalSinkState &global_state, std::unordered_map<int, double>& final_results) const {

//     std::cout << "Inside PerformInEqualityAggregation()........." << std::endl;

//     // Map to store pre-aggregated sums from the left table: Key -> SUM(v)
//     std::unordered_map<int, double> left_sums;
//     // Map to store key counts from the right table: Key -> COUNT(*)
//     std::unordered_map<int, long long int> right_counts;
//     duckdb::DataChunk lscan_chunk;
//     duckdb::DataChunk rscan_chunk;

//     int j = 0;
//     // --- 1. Pre-aggregate the left table (A) ---
//     global_state.left_data.InitializeScan();
//     while (global_state.left_data.Scan(lscan_chunk)) {
//         if(j == 0){
//             // std::cout << lscan_chunk.ToString() << std::endl;
//             // j++;
//         }
//         for (size_t i = 0; i < lscan_chunk.size(); ++i) {
//             int key;
//             double value;
//             try{
//                              key = lscan_chunk.data[0].GetValue(i).GetValue<int>();
//                              value = lscan_chunk.data[1].GetValue(i).GetValue<double>();
//             }catch(const std::exception& e){
//                 std::cout << e.what() << std::endl;
//                 continue;
//             }
//             // if(i == 0)
//                 // std::cout << "======> "<<value << std::endl;   
//             left_sums[key] += value;
//         }
//     }

//     // --- 2. Count keys and total rows in the right table (B) ---
//     long long total_right_rows = 0;
//     global_state.right_data.InitializeScan();
//     while (global_state.right_data.Scan(rscan_chunk)) {
//         // std::cout << "rscan_chunk(): " << rscan_chunk.ToString() << std::endl;
//         total_right_rows += rscan_chunk.size();
//         for (size_t i = 0; i < rscan_chunk.size(); ++i) {
//             int key;
//             try{
//                 key = rscan_chunk.data[0].GetValue(i).GetValue<int>();
                            
//             }catch(const std::exception& e){
//                 continue;
//             }
//             right_counts[key]++;
//         }
//     }

//     // --- 3. Combine results using inequality logic ---
//     for (const auto &left_entry : left_sums) {
//         int key = left_entry.first;
//         double sum = left_entry.second;
        
//         // Find the number of matching rows in the right table.
//         int matching_rows = right_counts.count(key) ? right_counts.at(key) : 0;
        
//         // Non-matching rows = total rows - matching rows.
//         long long non_matching_rows = total_right_rows - matching_rows;

//         // Final sum is SUM(v) * COUNT(non-matching rows in B).
//         if(non_matching_rows > 0)
//             final_results[key] = sum * non_matching_rows;
//     }

//     // for(const auto& entry : left_sums) {
//     //     printf("| %-3d | %-5d |\n", entry.first, entry.second);
//     // }
//     //     for(const auto& entry : right_counts) {
//     //     printf("| %-3d | %-5d |\n", entry.first, entry.second);
//     // }

//     // for(const auto& entry : final_results) {
//     //     printf("| %-3d | %-5d |\n", entry.first, entry.second);
//     // }

//     // return final_results;
// }


// SinkFinalizeType PhysicalGroupJoin::Finalize(Pipeline &pipeline, Event &event, ClientContext &context, OperatorSinkFinalizeInput &input) const {
//     auto &global_state = input.global_state.Cast<GroupJoinGlobalSinkState>();
//     // global_state.active_sink_tasks.load() > 0
//     try_again:

//     // Ensure both tables are fully scanned before finalizing
//     if (!global_state.left_scanned || !global_state.right_scanned) {
//         // std::cout << ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Finalize delayed: Waiting for both tables to be scanned" << std::endl;
//         // std::this_thread::sleep_for(std::chrono::milliseconds(10));
//         // goto try_again;
//         return SinkFinalizeType::NO_OUTPUT_POSSIBLE; // Delay finalization
//     }


//     if (global_state.finalized) {
//         // std::cout << ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Finalize ALREDAY completed for PhysicalGroupJoin" << std::endl;
//         return SinkFinalizeType::READY;
//     }
//     global_state.finalized = true;
    
//     const auto &left_types = children[0]->types;
//     const auto &right_types = children[1]->types;

//     std::cout << children[0]->GetName() << std::endl;
//     std::cout << children[1]->GetName() << std::endl;

//     DataChunk left_chunk, right_chunk;

//     int countlc = 1;
//     int countrc = 1;

//     // clear the maps

//     // print the total number of left and right rows
   

//     // Define the map to store keys and aggregation results
//     // std::unordered_map<int, std::pair<int, int>> aggregation_map; // Key -> (Count, Sum)
//     std::cout << "PhysicalGroupJoin::Finalize — Performing nested loop join and group-by" << std::endl;

//     // std::cout << "----------------------Going To Enter---------------------\n";
//     // std::cout << global_state.left_data.Size() << std::endl;
//     // std::cout << global_state.right_data.Size() << std::endl;
//     // std::cout << global_state.left_data.NumChunks() << std::endl;
//     // std::cout << global_state.right_data.NumChunks() << std::endl;
//     // std::cout << leftc << ":" << rightc << std::endl;
    


//     // std::unordered_map<int, int> final_results;
//     final_results.clear();
//     // PerformInEqualityAggregation(global_state, final_results);

//     PerformEqualityAggregation(global_state, final_results);
    

//     // std::vector<int> sorted_keys;
//     // for(const auto& pair : final_results) sorted_keys.push_back(pair.first);
//     // std::sort(sorted_keys.begin(), sorted_keys.end());
    
//     // std::cout << "--------------------------------------------------\n\n";
//     // for (int key : sorted_keys) {
//     //      std::cout << "Key: " << key << ", Count: " << final_results[key] << std::endl;
//     // }
//     // std::cout << "--------------------------------------------------\n\n";

//     // // Print the aggregation results
//     // std::cout << "Aggregation Results:\n";
//     // for (const auto &entry : aggregation_map) {
//     //     std::cout << "Key: " << entry.first << ", Count: " << entry.second.first << ", Sum: " << entry.second.second << std::endl;
//     // }


//     // // Print the aggregation results
//     // std::cout << "Aggregation Results:\n";
//     // for (const auto &entry : aggregation_map2) {
//     //     std::cout << "Key : " << entry.first << ", Count : " << entry.second << std::endl;
//     // }

//     // // Print the aggregation results
//     // std::cout << "Aggregation Results:\n";
//     // for(const auto& entry : aggregation_map) {
//     //     int key = entry.first;
//     //     int count = entry.second.first;
//     //     int sum = entry.second.second;
//     //     std::cout << "Key : " << key << ", Count : " << count << ", Sum : " << sum << std::endl;
//     // }


//     // std::cout << ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Finalize completed for PhysicalGroupJoin" << std::endl;
//     return SinkFinalizeType::READY;
// }







// class GroupJoinOperatorState : public OperatorState {
// public:
//     GroupJoinOperatorState(ClientContext &context, const PhysicalGroupJoin &op)
//         : group_executor(context) {
//         for (auto &group : op.groups) {
//             group_executor.AddExpression(*group);
//         }
//     }

//     //! Expression executor for the grouping expression
//     ExpressionExecutor group_executor;
//     //! DataChunk to store the grouping keys
//     DataChunk group_chunk;
//     //! DataChunk to store the aggregates fetched from the hash table
//     DataChunk aggregate_chunk;
// };

// unique_ptr<OperatorState> PhysicalGroupJoin::GetOperatorState(ExecutionContext &context) const {

//     std::cout << "Inside GetOperatorState()" << std::endl;

//     auto &global_state = sink_state->Cast<GroupJoinGlobalSinkState>();
//     auto state = make_uniq<GroupJoinOperatorState>(context.client, *this);

//     // Initialize the group chunk and aggregate chunk
//     state->group_chunk.InitializeEmpty(global_state.group_types);
//     state->aggregate_chunk.InitializeEmpty(global_state.aggregate_return_types);

//     std::cout << "Outside GetOperatorState()" << std::endl;
//     return std::move(state);
// }

// OperatorResultType PhysicalGroupJoin::ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
//                                                       GlobalOperatorState &gstate_p, OperatorState &state_p) const {

//     // auto &global_state = sink_state->Cast<GroupJoinGlobalSinkState>();
//     // auto &local_state = input.local_state.Cast<GroupJoinLocalSinkState>();

//     // // Probe join hash table with left input
//     // DataChunk join_result;
//     // global_state.join_hash_table->Probe(input, join_result);

//     // if (join_result.size() == 0) {
//     //     return OperatorResultType::NEED_MORE_INPUT; // no matches for this chunk
//     // }

//     // // Extract grouping keys and aggregate inputs from join result
//     // DataChunk groups_chunk;
//     // groups_chunk.Initialize(global_state.group_types);

//     // for (idx_t i = 0; i < global_state.grouping_indices.size(); i++) {
//     //     groups_chunk.data[i].Reference(join_result.data[global_state.grouping_indices[i]]);
//     // }
//     // groups_chunk.SetCardinality(join_result.size());

//     // // Evaluate aggregates input expressions on join result (reuse local_state.aggregate_executor)
//     // local_state.aggregate_executor.Execute(join_result, local_state.aggregate_input_chunk);

//     // // Add to aggregate hash table
//     // vector<idx_t> no_filter; // empty filter vector
//     // global_state.hash_table->AddChunk(groups_chunk, local_state.aggregate_input_chunk, no_filter);

//     // // Output columns: joined columns + aggregates after grouping
//     // // For simplicity, output the joined columns only here or aggregated output in GetData()

//     // // Just forward joined columns for now
//     // chunk.Reference(join_result);
    
//     std::cout << "(((((((((((((( INSIDE EXECUTE INTERNAL )))))))))))))))))))))" << std::endl;
//     return OperatorResultType::NEED_MORE_INPUT;

// }

// class GroupJoinGlobalSourceState : public GlobalSourceState {
// public:
//     explicit GroupJoinGlobalSourceState(GroupJoinGlobalSinkState *sink_state_p) : sink_state(sink_state_p) {}

//     GroupJoinGlobalSinkState *sink_state;
// };
// struct GroupJoinLocalSourceState : public LocalSourceState {
//     GroupedAggregateHashTableScanState scan_state;
// };

// unique_ptr<GlobalSourceState> PhysicalGroupJoin::GetGlobalSourceState(ClientContext &context) const {
//     auto state = make_uniq<GroupJoinGlobalSourceState>(&sink_state->Cast<GroupJoinGlobalSinkState>());
//     return std::move(state);
// }

// unique_ptr<LocalSourceState> PhysicalGroupJoin::GetLocalSourceState(ExecutionContext &context, GlobalSourceState &gstate) const {
//     return make_uniq<GroupJoinLocalSourceState>();
// }



// SourceResultType PhysicalGroupJoin::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
//     static idx_t scan_offset = 0; // Track the current position in the aggregation results

//     // std::cout << "INSIDE GETDATA\n" << std::endl;

//     //  std::cout << "Chunk BEFORE::::\n" << chunk.ToString() << std::endl;

//     static auto start = std::chrono::high_resolution_clock::now();




//     // Define the schema for the result (Key, Count, Sum)
//     vector<LogicalType> result_types = {LogicalType::INTEGER, LogicalType::HUGEINT};
//     if (chunk.ColumnCount() == 0) {
//         chunk.Initialize(Allocator::Get(context.client), result_types);
//     }
//     //  std::cout << "Chunk BEFORE::::\n" << chunk.ToString() << std::endl;
//     // Set the cardinality based on the remaining entries
//     idx_t remaining_entries = final_results.size() - scan_offset;
//     idx_t rows_to_output = remaining_entries < STANDARD_VECTOR_SIZE ? remaining_entries : STANDARD_VECTOR_SIZE;
//     chunk.SetCardinality(rows_to_output);

//     // Populate the chunk with aggregation results
//     idx_t row_idx = 0;
//     for (auto it = std::next(final_results.begin(), scan_offset); row_idx < rows_to_output && it != final_results.end(); ++it, ++row_idx) {
//         chunk.data[0].SetValue(row_idx, Value::INTEGER(it->first));       // Key
//         // chunk.data[1].SetValue(row_idx, Value::INTEGER(it->second.first)); // Count
//         chunk.data[1].SetValue(row_idx, Value::DOUBLE(it->second)); // Sum
//     }

//     // Update the scan offset
//     scan_offset += rows_to_output;

//     // Print the chunk for debugging
//     // std::cout << "Chunk contents::::\n" << chunk.ToString() << std::endl;

//     // std::cout << "__________________SIZES____________________" << std::endl;
//     // auto &global_state = input.global_state.Cast<GroupJoinGlobalSinkState>();
//     // std::cout << global_state.left_data.Size() << std::endl;
//     // std::cout << global_state.right_data.Size() << std::endl;

//     // Return FINISHED if all entries have been sent out
//     if (scan_offset >= final_results.size()) {
//         scan_offset = 0; // Reset for future calls
//         auto end = std::chrono::high_resolution_clock::now();
//         std::chrono::duration<double> elapsed = end - start;
//         std::cout << "Total time taken for GetData(): " << elapsed.count() << " seconds" << std::endl;
//         return SourceResultType::FINISHED;
//     }


//     // std::cout << "OUTSIDE GETDATA\n" << std::endl;
//     return SourceResultType::HAVE_MORE_OUTPUT;
// }


// InsertionOrderPreservingMap<string> PhysicalGroupJoin::ParamsToString() const {
//     InsertionOrderPreservingMap<string> result;
//     result["Join Type"] = EnumUtil::ToString(join_type);
//     result["Join Condition"] = condition->GetName();
// 	auto &grps = groups;
// 	auto &aggr = aggregates;
// 	string groups_info;
// 	for (idx_t i = 0; i < groups.size(); i++) {
// 		if (i > 0) {
// 			groups_info += "\n";
// 		}
// 		groups_info += grps[i]->GetName();
// 	}
// 	result["Groups"] = groups_info;

// 	string aggregate_info;
// 	for (idx_t i = 0; i < aggr.size(); i++) {
// 		auto &aggregate = aggr[i]->Cast<BoundAggregateExpression>();
// 		if (i > 0) {
// 			aggregate_info += "\n";
// 		}
// 		aggregate_info += aggr[i]->GetName();
// 		if (aggregate.filter) {
// 			aggregate_info += " Filter: " + aggregate.filter->GetName();
// 		}
// 	}
// 	result["Aggregates"] = aggregate_info;
//     return result;
// }



// void PhysicalGroupJoin::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
//     D_ASSERT(children.size() == 2); // Ensure binary join
//     // std::cout << "PIPELINE $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$\n";
//     auto &left_child = children[0];
//     auto &right_child = children[1];

//     // The current pipeline will be the one that runs the Finalize() of this join.
//     auto &state = meta_pipeline.GetState();
//     state.SetPipelineSource(current, *this); // Set self as sink

//     // --- Build left child MetaPipeline ---
//     MetaPipeline &left_meta = meta_pipeline.CreateChildMetaPipeline(current, *this);
//     left_meta.Build(*left_child);

//     // --- Build right child MetaPipeline ---
//     MetaPipeline &right_meta = meta_pipeline.CreateChildMetaPipeline(current, *this);
//     right_meta.Build(*right_child);

//     // --- Get shared_ptr to Pipelines ---
//     // Current pipeline shared_ptr via enable_shared_from_this
//     shared_ptr<Pipeline> current_ptr = current.shared_from_this();

//     // Base pipelines of left and right MetaPipelines
//     shared_ptr<Pipeline> &left_ptr = left_meta.GetBasePipeline();
//     shared_ptr<Pipeline> &right_ptr = right_meta.GetBasePipeline();

//     // --- Add dependencies: current depends on left and right ---
//     // right_ptr->AddDependency(left_ptr);
//     current_ptr->AddDependency(left_ptr);
//     current_ptr->AddDependency(right_ptr);

//     // std::cout << current_ptr->ToString() << std::endl;
//     // std::cout << left_ptr->ToString() << std::endl;
//     // std::cout << right_ptr->ToString() << std::endl;

// }

// } // namespace duckdb
