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
#include "duckdb/common/types/row/partitioned_tuple_data.hpp"
#include "duckdb/common/types/row/tuple_data_collection.hpp"
#include "duckdb/common/types/row/tuple_data_allocator.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"

#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/radix_partitioning.hpp"
#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/row/tuple_data_iterator.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/ht_entry.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"


#include <iostream>

namespace duckdb {

PhysicalGroupJoin::PhysicalGroupJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                     unique_ptr<PhysicalOperator> right, unique_ptr<Expression> condition_p,
                                     JoinType join_type, idx_t estimated_cardinality,
                                     vector<unique_ptr<Expression>> &groups_p,
                                     vector<unique_ptr<Expression>> &aggregates_p)
    : PhysicalJoin(op, PhysicalOperatorType::GROUP_JOIN, join_type, estimated_cardinality),
      condition(std::move(condition_p)), groups(std::move(groups_p)), aggregates(std::move(aggregates_p)) {

    std::cout << "Inside PhysicalGroupJoin()" << std::endl; 
    
    // aggregation_map = std::unordered_map<int, std::pair<int, int>>(); // Initialize map
    // aggregation_map2 = std::unordered_map<int, int>(); // Initialize map

    children.push_back(std::move(left));
    children.push_back(std::move(right));
    D_ASSERT(join_type != JoinType::MARK);
    D_ASSERT(join_type != JoinType::SINGLE);
}

std::unordered_map<int, std::pair<int, float>> PhysicalGroupJoin::aggregation_map;
std::unordered_map<int, int> PhysicalGroupJoin::aggregation_map2;

// std::unordered_map<std::vector<Value>, std::vector<Value>, VectorHash> aggregation_map;

PhysicalGroupJoin::~PhysicalGroupJoin() = default;

class GroupJoinGlobalSinkState : public GlobalSinkState {
public:
    bool finalized = false;
    //! The hash table that will be used to store the grouping attributes and aggregates
    unique_ptr<GroupedAggregateHashTable> hash_table;

    //! The types of the grouping columns
    vector<LogicalType> group_types;

    //! The indices of the grouping columns in the right child's output
    vector<idx_t> grouping_indices;

    //! The aggregate objects
    vector<AggregateObject> aggregate_objects;

    //! The payload types (types of the aggregates)
    vector<LogicalType> aggregate_return_types;

    //! Buffers to store input from left and right children
    ChunkCollection left_data;
    ChunkCollection right_data;
};


class GroupJoinLocalSinkState : public LocalSinkState {
public:
    GroupJoinLocalSinkState(Allocator &allocator, const PhysicalGroupJoin &op, ClientContext &context)
        : aggregate_executor(context) {

        std::cout << "Inside GroupJoinLocalSinkState()" << std::endl;

        for (auto &aggr_expr : op.aggregates) {
            auto &bound_aggr = aggr_expr->Cast<BoundAggregateExpression>();
            for (auto &child : bound_aggr.children) {
                aggregate_executor.AddExpression(*child);
            }
        }

        // Initialize the aggregate input chunk
        vector<LogicalType> aggregate_input_types;
        for (auto &aggr_expr : op.aggregates) {
            auto &bound_aggr = aggr_expr->Cast<BoundAggregateExpression>();
            for (auto &child : bound_aggr.children) {
                aggregate_input_types.push_back(child->return_type);
            }
        }
        aggregate_input_chunk.Initialize(allocator, aggregate_input_types);
        
        std::cout << "Outside GroupJoinLocalSinkState()" << std::endl;

    }

    //! Expression executor for the aggregates
    ExpressionExecutor aggregate_executor;
    //! DataChunk to hold the evaluated aggregate inputs
    DataChunk aggregate_input_chunk;
};

// unique_ptr<GlobalSinkState> PhysicalGroupJoin::GetGlobalSinkState(ClientContext &context) const {

//     std::cout << "Inside GetGlobalSinkState() START" << std::endl;

//     auto state = make_uniq<GroupJoinGlobalSinkState>();

//     // Get the types from the left and right children
//     auto &left_types = children[0]->types;
//     std::cout << children[0]->ToString() << std::endl;
//     std::cout << "Inside GetGlobalSinkState()  1" << std::endl;

//     auto &right_types = children[1]->types;
//     std::cout << children[1]->ToString() << std::endl;
//     std::cout << "Inside GetGlobalSinkState()  2" << std::endl;

//     std::cout << right_types[0].ToString() << std::endl;

//     // Extract the bound comparison expression
//     auto &comp_expr = condition->Cast<BoundComparisonExpression>();
//     auto &left_expr = comp_expr.left->Cast<BoundReferenceExpression>();
//     auto &right_expr = comp_expr.right->Cast<BoundReferenceExpression>();

//     std::cout << "GROUPS SIZE:"<< groups.size() << std::endl;
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

//     std::cout << "Inside GetGlobalSinkState()  4" << std::endl;
//     // Prepare the aggregate objects
//     for (auto &aggr_expr : aggregates) {
//         if (aggr_expr->expression_class != ExpressionClass::BOUND_AGGREGATE) {
//             throw NotImplementedException("Expected bound aggregate expression");
//         }
//         auto &bound_aggr = aggr_expr->Cast<BoundAggregateExpression>();
//         AggregateObject aggr_obj(&bound_aggr);
//         state->aggregate_objects.push_back(aggr_obj);
//         state->aggregate_return_types.push_back(bound_aggr.return_type);
//     }

//     // Initialize the hash table
//     // state->hash_table = make_uniq<GroupedAggregateHashTable>(context, Allocator::Get(context),
//     //                                                          state->group_types, vector<LogicalType>(),
//     //                                                          state->aggregate_objects);

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
//     state->hash_table = make_uniq<GroupedAggregateHashTable>(
//         context, Allocator::Get(context),
//         state->group_types, aggregate_input_types, state->aggregate_objects
//     );

//     return std::move(state);
//     std::cout << "Inside GetGlobalSinkState() END" << std::endl;

// }


unique_ptr<GlobalSinkState> PhysicalGroupJoin::GetGlobalSinkState(ClientContext &context) const {
    std::cout << "Inside GetGlobalSinkState() START" << std::endl;

    auto state = make_uniq<GroupJoinGlobalSinkState>();

    // Get the types from the left and right children
    auto &left_types = children[0]->types;
    auto &right_types = children[1]->types;

    // Set up group types and grouping indices
    for (auto &group : groups) {
        auto &bound_ref = group->Cast<BoundReferenceExpression>();
        idx_t idx = bound_ref.index;
        LogicalType group_type;
        if (idx < left_types.size()) {
            // Grouping column from left child
            group_type = left_types[idx];
            state->grouping_indices.push_back(idx); // index in joined chunk
        } else {
            // Grouping column from right child
            idx_t right_idx = idx - left_types.size();
            if (right_idx >= right_types.size()) {
                throw InternalException("Grouping attribute index out of bounds in right child's types");
            }
            group_type = right_types[right_idx];
            state->grouping_indices.push_back(idx); // index in joined chunk
        }
        state->group_types.push_back(group_type);
    }

    // Prepare the aggregate objects and return types
    for (auto &aggr_expr : aggregates) {
        if (aggr_expr->expression_class != ExpressionClass::BOUND_AGGREGATE) {
            throw NotImplementedException("Expected bound aggregate expression");
        }
        auto &bound_aggr = aggr_expr->Cast<BoundAggregateExpression>();
        AggregateObject aggr_obj(&bound_aggr);
        state->aggregate_objects.push_back(aggr_obj);
        state->aggregate_return_types.push_back(bound_aggr.return_type);
    }

    // Prepare aggregate input types (types of aggregate arguments, not return types)
    vector<LogicalType> aggregate_input_types;
    for (auto &aggr_expr : aggregates) {
        auto &bound_aggr = aggr_expr->Cast<BoundAggregateExpression>();
        if (!bound_aggr.children.empty()) {
            for (auto &child : bound_aggr.children) {
                aggregate_input_types.push_back(child->return_type);
            }
        } else {
            // COUNT(*) or similar: use INTEGER as dummy
            aggregate_input_types.push_back(LogicalType::INTEGER);
        }
    }

    // Initialize the hash table with group types and aggregate input types
    state->hash_table = make_uniq<GroupedAggregateHashTable>(
        context, Allocator::Get(context),
        state->group_types, aggregate_input_types, state->aggregate_objects
    );

    std::cout << "Inside GetGlobalSinkState() END" << std::endl;
    return std::move(state);
}

unique_ptr<LocalSinkState> PhysicalGroupJoin::GetLocalSinkState(ExecutionContext &context) const {
    return make_uniq<GroupJoinLocalSinkState>(Allocator::Get(context.client), *this, context.client);
}


SinkResultType PhysicalGroupJoin::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
    std::cout << "Inside Sink()" << std::endl;
    auto &global_state = input.global_state.Cast<GroupJoinGlobalSinkState>();

    // Use types to distinguish left/right child
    const auto &input_types = chunk.GetTypes();
    auto &left_types = children[0]->types;
    auto &right_types = children[1]->types;
    if (input_types == left_types) {
        // std::cout << "Appending chunk to LEFT table" << std::endl;
        // std::cout << chunk.ToString() << std::endl;
        global_state.left_data.Append(chunk);
    } else if (input_types == right_types) {
        // std::cout << "Appending chunk to RIGHT table" << std::endl;
        // std::cout << chunk.ToString() << std::endl;
        global_state.right_data.Append(chunk);
    } else {
        std::cout << "ERROR: input chunk types do not match any child" << std::endl;
        throw InternalException("PhysicalGroupJoin::Sink: input chunk types do not match any child");
    }
    std::cout << "Outside Sink()" << std::endl;
    return SinkResultType::NEED_MORE_INPUT;
}





SinkFinalizeType PhysicalGroupJoin::Finalize(Pipeline &pipeline, Event &event, ClientContext &context, OperatorSinkFinalizeInput &input) const {

    auto &global_state = input.global_state.Cast<GroupJoinGlobalSinkState>();
    if (global_state.finalized) {
        return SinkFinalizeType::READY;
    }
    global_state.finalized = true;

    std::cout << "PhysicalGroupJoin::Finalize — Performing nested loop join and group-by" << std::endl;

    const auto &left_types = children[0]->types;
    const auto &right_types = children[1]->types;

    std::cout << children[0]->GetName() << std::endl;
    std::cout << children[1]->GetName() << std::endl;


    DataChunk left_chunk, right_chunk;

    int joined_chunk_count = 1;

    global_state.left_data.InitializeScan();

    std::cout << "Just outside while.............\n";
    while (global_state.left_data.Scan(left_chunk)) {
        std::cout << "Just inside while.\n";
        global_state.right_data.InitializeScan();
        while (global_state.right_data.Scan(right_chunk)) {
            
            DataChunk joined_chunk;
            vector<LogicalType> join_types;
            // Use the actual types from left_chunk and right_chunk
            for (idx_t col = 0; col < left_chunk.ColumnCount(); ++col) {
                join_types.push_back(left_chunk.data[col].GetType());
            }
            for (idx_t col = 0; col < right_chunk.ColumnCount(); ++col) {
                join_types.push_back(right_chunk.data[col].GetType());
            }
            joined_chunk.Initialize(Allocator::Get(context), join_types);
            joined_chunk.SetCardinality(1);

            std::cout << "_______________________Left Chunk_____________________________________" << std::endl;
            std::cout << left_chunk.ToString() << std::endl;
            std::cout << "_______________________Right Chunk____________________________________" << std::endl;
            std::cout << right_chunk.ToString() << std::endl;


            for (idx_t i = 0; i < left_chunk.size(); ++i) {
                for (idx_t j = 0; j < right_chunk.size(); ++j) {
                    // Manually copy single row from left_chunk into joined_chunk
                    for (idx_t col = 0; col < left_chunk.ColumnCount(); ++col) {
                        joined_chunk.data[col].SetValue(0, left_chunk.data[col].GetValue(i));
                    }

                    // Manually copy single row from right_chunk into joined_chunk
                    for (idx_t col = 0; col < right_chunk.ColumnCount(); ++col) {
                        idx_t out_col = left_chunk.ColumnCount() + col;

                        // std::cout << ">>>>"<< out_col << std::endl;
                        // std::cout << joined_chunk.ColumnCount() << std::endl;
                        // std::cout << ">>>>"<< col << std::endl;
                        //  std::cout << right_chunk.ColumnCount() << std::endl;
                        // std::cout << ">>>>"<< j << std::endl;

                        joined_chunk.data[out_col].SetValue(0, right_chunk.data[col].GetValue(j));

                    }

                    // std::cout << "_________JOINED CHUNK:___________" << std::endl;
                    // std::cout << joined_chunk.ToString() << std::endl;


                    // Evaluate join condition if any
                    if (condition) {
                        ExpressionExecutor condition_executor(context);
                        condition_executor.AddExpression(*condition);
                        DataChunk condition_result;
                        condition_result.Initialize(Allocator::Get(context), {LogicalType::BOOLEAN});
                        condition_executor.Execute(joined_chunk, condition_result);
                        auto result_ptr = FlatVector::GetData<bool>(condition_result.data[0]);
                        if (!result_ptr[0]) {
                            continue;
                        }
                    }

                    std::cout << "______________JOINED CHUNK:" << joined_chunk_count++ << "________________"<< std::endl;
                    std::cout << joined_chunk.ToString() << std::endl;

                    // Extract group keys
                    DataChunk group_chunk;
                    group_chunk.InitializeEmpty(global_state.group_types);
                    group_chunk.SetCardinality(1);

                    std::cout << "group_chunk.ColumnCount(): " << group_chunk.ColumnCount() << std::endl;
                    std::cout << "global_state.grouping_indices.size(): " << global_state.grouping_indices.size() << std::endl;
                    for (idx_t g = 0; g < global_state.grouping_indices.size(); ++g) {
                        idx_t idx = global_state.grouping_indices[g];
                        std::cout << "grouping index: " << idx << std::endl;
                        if (idx >= joined_chunk.ColumnCount()) {
                            std::cout << "ERROR: grouping index out of bounds!" << std::endl;
                            continue;
                        }
                        if (g >= group_chunk.ColumnCount()) {
                            std::cout << "ERROR: group_chunk index out of bounds!" << std::endl;
                            continue;
                        }
                        // std::cout << "Hello there 111\n";
                        // group_chunk.data[g].SetValue(0, joined_chunk.data[idx].GetValue(0));
                        group_chunk.data[g].Reference(joined_chunk.data[idx]);
                        // std::cout << "Hello there 222\n";
                    }

                    // Extract aggregate inputs
                    DataChunk aggr_input_chunk;
                    // aggr_input_chunk.Initialize(Allocator::Get(context), global_state.hash_table->GetTypes());
                


                vector<LogicalType> aggr_input_types;
                for (auto &aggr_expr : aggregates) {
                    auto *agg = dynamic_cast<BoundAggregateExpression*>(aggr_expr.get());
                    
                    std::cout << "Aggregate: " << aggr_expr->GetName() << ", children: " << (agg ? agg->children.size() : 0) << std::endl;
                        if (agg) {
                            for (auto &child : agg->children) {
                                std::cout << "  Child type: " << child->return_type.ToString() << std::endl;
                            }
                        }

                    if (agg && !agg->children.empty()) {
                        aggr_input_types.push_back(agg->children[0]->return_type);
                    } else if (agg && agg->children.empty()) {
                        // COUNT(*) expects no input, but DataChunk must have at least one column, so use INTEGER
                        aggr_input_types.push_back(LogicalType::INTEGER);
                    } else {
                        aggr_input_types.push_back(aggr_expr->return_type);
                    }
                }

                for (auto &t : aggr_input_types) {
                    std::cout << "Aggregate input type: " << t.ToString() << std::endl;
                }

                aggr_input_chunk.Initialize(Allocator::Get(context), aggr_input_types);

                    // ExpressionExecutor aggr_exec(context);
                    // for (auto &aggr_expr : aggregates) {
                    //     aggr_exec.AddExpression(*aggr_expr);
                    // }
                    // aggr_exec.Execute(joined_chunk, aggr_input_chunk);

                    // Prepare executors for each aggregate argument (child)
                    std::vector<std::unique_ptr<ExpressionExecutor>> aggr_executors;
                    for (auto &aggr_expr : aggregates) {
                        auto *agg = dynamic_cast<BoundAggregateExpression*>(aggr_expr.get());
                        if (agg && !agg->children.empty()) {
                            // For each child (argument) of the aggregate, create an executor
                            aggr_executors.push_back(make_uniq<ExpressionExecutor>(context, *agg->children[0]));
                        } else if (agg && agg->children.empty()) {
                            // COUNT(*) or similar: no executor needed, just push a nullptr
                            aggr_executors.push_back(nullptr);
                        } else {
                            // Fallback: not expected, but push nullptr
                            aggr_executors.push_back(nullptr);
                        }
                    }

                    for (idx_t k = 0; k < aggr_executors.size(); ++k) {
                        if (aggr_executors[k]) {
                            // Prepare a single-column result chunk for this argument
                            DataChunk arg_result;
                            arg_result.Initialize(Allocator::Get(context), {aggr_input_types[k]});
                            arg_result.SetCardinality(1);
                            aggr_executors[k]->Execute(joined_chunk, arg_result);
                            // Copy the result into the aggregate input chunk
                            std::cout << "Aggregate Input for Column " << k << ": " << arg_result.ToString() << std::endl;
                            aggr_input_chunk.data[k].Reference(arg_result.data[0]);
                        } else {
                            // For COUNT(*), set value to 1 (or any dummy value, as the aggregate ignores input)
                            aggr_input_chunk.data[k].SetValue(0, Value::INTEGER(1));
                        }
                    }
                    aggr_input_chunk.SetCardinality(1);
                    // aggr_input_chunk.SetCardinality(1);

                    // Add to hash table
                    unsafe_vector<idx_t> filter;
                    std::cout << "Aggregation Input Chunk:\n" << aggr_input_chunk.ToString() << std::endl;
                    std::cout << "Group Chunk:\n" << group_chunk.ToString() << std::endl;
                    global_state.hash_table->AddChunk(group_chunk, aggr_input_chunk, filter);
                }
            }
        }
    }

    std::cout << "Just outside while.\n";
    std::cout << "PhysicalGroupJoin::Finalize — Completed nested loop and aggregation" << std::endl;
    std::cout << "Hash table pointer after finalize: " << global_state.hash_table.get() << std::endl;



    if (global_state.hash_table->GetPartitionedData()) {


        auto &pdata = *global_state.hash_table->GetPartitionedData();

        for (idx_t i = 0; i < pdata.PartitionCount(); i++) {

            auto &partition = *pdata.GetPartitions()[i];
            TupleDataScanState scan_state;
            partition.InitializeScan(scan_state);

            DataChunk result_chunk;
    
            partition.InitializeScanChunk(scan_state, result_chunk);

            while (partition.Scan(scan_state, result_chunk)) {
                std::cout << "Partition " << i << ":\n" << result_chunk.ToString() << std::endl;
            }
        }







        // auto &pdata = *global_state.hash_table->GetPartitionedData();
        // std::cout << "Partitioned data OK, partitions: " << pdata.PartitionCount() << ", total rows: " << pdata.Count() << std::endl;

        // for (idx_t i = 0; i < pdata.PartitionCount(); i++) {
        //     auto &partition = *pdata.GetPartitions()[i];
        //     std::cout << "Partition " << i << " rows: " << partition.Count() << std::endl;

        //     if (partition.Count() == 0) {
        //         std::cout << "Partition is empty!" << std::endl;
        //         continue;
        //     }

        //     // Initialize scan state
        //     TupleDataScanState scan_state;
        //     partition.InitializeScan(scan_state);

        //     // Scan rows
        //     DataChunk result_chunk;
        //     partition.InitializeScanChunk(scan_state, result_chunk);

        //     while (partition.Scan(scan_state, result_chunk)) {
        //         for (idx_t row_idx = 0; row_idx < result_chunk.size(); row_idx++) {
        //             std::cout << "Row " << row_idx << ": ";
        //             for (idx_t col_idx = 0; col_idx < result_chunk.ColumnCount(); col_idx++) {
        //                 auto value = result_chunk.data[col_idx].GetValue(row_idx);
        //                 std::cout << value.ToString() << " ";
        //             }
        //             std::cout << std::endl;
        //         }
        //     }
        // }


    } else {
        std::cout << "Partitioned data is NULL after finalize!" << std::endl;
    }



    // if (global_state.hash_table) {
    //     GroupedAggregateHashTableScanState scan_state;
    //     DataChunk result_chunk;
    //     // Initialize result_chunk with correct types
    //     while (true) {
    //         idx_t count = global_state.hash_table->Scan(result_chunk, scan_state, STANDARD_VECTOR_SIZE);
    //         std::cout << "----------------->count: " << count << std::endl;
    //         if (count == 0) break;
    //         std::cout << "Result:" << count << std::endl;
    //         std::cout << result_chunk.ToString() << std::endl;
    //     }
    // }








    return SinkFinalizeType::READY;
}




// SinkFinalizeType PhysicalGroupJoin::Finalize(Pipeline &pipeline, Event &event, ClientContext &context, OperatorSinkFinalizeInput &input) const {
//     auto &global_state = input.global_state.Cast<GroupJoinGlobalSinkState>();
//     if (global_state.finalized) {
//         return SinkFinalizeType::READY;
//     }
//     global_state.finalized = true;

//     DataChunk left_chunk, right_chunk;
//     global_state.left_data.InitializeScan();
//     global_state.right_data.InitializeScan();

//     ExpressionExecutor join_condition_executor(context);
//     if (condition) {
//         join_condition_executor.AddExpression(*condition);
//     }

//     std::cout << "________________________________________________________________________\n";
//     while (global_state.left_data.Scan(left_chunk)) {
//         while (global_state.right_data.Scan(right_chunk)) {
//             for (idx_t left_row = 0; left_row < left_chunk.size(); ++left_row) {
//                 for (idx_t right_row = 0; right_row < right_chunk.size(); ++right_row) {
//                    vector<LogicalType> join_types;

//                     DataChunk joined_chunk;
//                     // Add types from left_chunk
//                     for (idx_t col = 0; col < left_chunk.ColumnCount(); ++col) {
//                         join_types.push_back(left_chunk.data[col].GetType());
//                     }
//                     // Add types from right_chunk
//                     for (idx_t col = 0; col < right_chunk.ColumnCount(); ++col) {
//                         join_types.push_back(right_chunk.data[col].GetType());
//                     }

//                     // Initialize joined_chunk with combined types
//                     joined_chunk.Initialize(Allocator::Get(context), join_types);

//                     // Copy rows from left_chunk
//                     for (idx_t col = 0; col < left_chunk.ColumnCount(); ++col) {
//                         joined_chunk.data[col].SetValue(0, left_chunk.data[col].GetValue(left_row));
//                     }

//                     // Copy rows from right_chunk
//                     for (idx_t col = 0; col < right_chunk.ColumnCount(); ++col) {
//                         idx_t out_col = left_chunk.ColumnCount() + col; // Offset by left_chunk column count
//                         joined_chunk.data[out_col].SetValue(0, right_chunk.data[col].GetValue(right_row));
//                     }

//                     // Set cardinality
//                     joined_chunk.SetCardinality(1);

//                     if (condition) {
//                         DataChunk condition_result;
//                         condition_result.Initialize(Allocator::Get(context), {LogicalType::BOOLEAN});
//                         join_condition_executor.Execute(joined_chunk, condition_result);
//                         auto result_ptr = FlatVector::GetData<bool>(condition_result.data[0]);
//                         if (!result_ptr[0]) {
//                             continue;
//                         }
//                     }

//                     std::vector<Value> group_keys;
//                     for (auto &group_expr : groups) {
//                         ExpressionExecutor executor(context);
//                         executor.AddExpression(*group_expr);
//                         DataChunk result_chunk;
//                         result_chunk.Initialize(Allocator::Get(context), {group_expr->return_type});
//                         executor.Execute(joined_chunk, result_chunk);
//                         group_keys.push_back(result_chunk.data[0].GetValue(0));
//                     }

//                     std::vector<Value> aggregate_inputs;
//                     for (size_t i = 0; i < aggregates.size(); ++i) {
//                         DataChunk aggregate_result;
//                         aggregate_result.Initialize(Allocator::Get(context), {aggregates[i]->return_type});
//                         aggregate_inputs.push_back(aggregate_result.data[0].GetValue(0));
//                     }

//                     if (aggregation_map.find(group_keys) == aggregation_map.end()) {
//                         aggregation_map[group_keys] = aggregate_inputs;
//                     } else {
//                         for (size_t i = 0; i < aggregate_inputs.size(); ++i) {
//                             aggregation_map[group_keys][i] = Value::DOUBLE(
//                                 aggregation_map[group_keys][i].GetValue<double>() + aggregate_inputs[i].GetValue<double>());
//                         }
//                     }
//                 }
//             }
//         }
//     }
//      std::cout << "________________________________________________________________________\n";

//     return SinkFinalizeType::READY;
// }









// works
// SinkFinalizeType PhysicalGroupJoin::Finalize(Pipeline &pipeline, Event &event, ClientContext &context, OperatorSinkFinalizeInput &input) const {
//     auto &global_state = input.global_state.Cast<GroupJoinGlobalSinkState>();
//     if (global_state.finalized) {
//         return SinkFinalizeType::READY;
//     }
//     global_state.finalized = true;

//     std::cout << "PhysicalGroupJoin::Finalize — Performing nested loop join and group-by" << std::endl;

//     const auto &left_types = children[0]->types;
//     const auto &right_types = children[1]->types;

//     std::cout << children[0]->GetName() << std::endl;
//     std::cout << children[1]->GetName() << std::endl;

//     DataChunk left_chunk, right_chunk;

//     int countlc = 1;
//     int countrc = 1;

//     aggregation_map.clear();
//     aggregation_map2.clear();

//     // find out the aggregation and type
//     for(auto &aggr_expr : aggregates) {
//         std::cout << "Agg:" << aggr_expr.get()->ToString() << std::endl;

//     }

//     for(auto &group : groups) {
//         std::cout << "Group:" << group.get()->ToString() << std::endl;
//     }
        



//     // Define the map to store keys and aggregation results
//     // std::unordered_map<int, std::pair<int, int>> aggregation_map; // Key -> (Count, Sum)

//     std::cout << "----------------------Going To Enter---------------------\n";

//     global_state.left_data.InitializeScan();
//     while (global_state.left_data.Scan(left_chunk)) {
//         std::cout << "LEFT CHUNK :" << countlc++ << std::endl;
//         std::cout << left_chunk.ToString() << std::endl;


//         // Insert keys from the left chunk into the map
//         for (idx_t row_idx = 0; row_idx < left_chunk.size(); ++row_idx) {
//             int key = left_chunk.data[0].GetValue(row_idx).GetValue<int>(); // Assuming the key is in the first column
//             float value = left_chunk.data[1].GetValue(row_idx).GetValue<float>(); // Assuming the value is in the second column

//             if (aggregation_map.find(key) == aggregation_map.end()) {
//                 aggregation_map[key] = {1, value}; // Initialize count and sum
//             } else {
//                 aggregation_map[key].first += 1; // Increment count
//                 aggregation_map[key].second += value; // Add to sum
//             }
//         }
//     }

//     // Print the aggregation results
//     std::cout << "Aggregation Results:\n";
//     for (const auto &entry : aggregation_map) {
//         std::cout << "Key: " << entry.first << ", Count: " << entry.second.first << ", Sum: " << entry.second.second << std::endl;
//     }

//     // std::unordered_map<int, int> aggregation_map2;
//     global_state.right_data.InitializeScan();
//     while (global_state.right_data.Scan(right_chunk)) {
//         std::cout << "RIGHT CHUNK :" << countrc++ << std::endl;
//          std::cout << right_chunk.ToString() << std::endl;

//             // Update keys in the map with matching values from the right chunk
//         for (idx_t row_idx = 0; row_idx < right_chunk.size(); ++row_idx) {
//                 int key = right_chunk.data[0].GetValue(row_idx).GetValue<int>(); // Assuming the key is in the first column

//                 if (aggregation_map2.find(key) != aggregation_map2.end()) {
//                     aggregation_map2[key] += 1;
//                 }else{
//                     aggregation_map2[key] = 1;
//                 }
//         }
//     }

//     for(const auto& entry : aggregation_map2) {
//         int key = entry.first;
//         int value = entry.second;

//         if (aggregation_map.find(key) != aggregation_map.end()) {
//             aggregation_map[key].second *= value;
//             aggregation_map[key].first *= value;
//         }
//     }

//     // Print the aggregation results
//     std::cout << "Aggregation Results:\n";
//     for (const auto &entry : aggregation_map2) {
//         std::cout << "Key : " << entry.first << ", Count : " << entry.second << std::endl;
//     }

//       // Print the aggregation results
//     std::cout << "Aggregation Results:\n";
//     for(const auto& entry : aggregation_map) {
//         int key = entry.first;
//         int count = entry.second.first;
//         int sum = entry.second.second;
//         std::cout << "Key : " << key << ", Count : " << count << ", Sum : " << sum << std::endl;
//     }

    

//     return SinkFinalizeType::READY;
// }






















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
                                                      GlobalOperatorState &gstate_p, OperatorState &state_p) const {

    // auto &global_state = sink_state->Cast<GroupJoinGlobalSinkState>();
    // auto &local_state = input.local_state.Cast<GroupJoinLocalSinkState>();

    // // Probe join hash table with left input
    // DataChunk join_result;
    // global_state.join_hash_table->Probe(input, join_result);

    // if (join_result.size() == 0) {
    //     return OperatorResultType::NEED_MORE_INPUT; // no matches for this chunk
    // }

    // // Extract grouping keys and aggregate inputs from join result
    // DataChunk groups_chunk;
    // groups_chunk.Initialize(global_state.group_types);

    // for (idx_t i = 0; i < global_state.grouping_indices.size(); i++) {
    //     groups_chunk.data[i].Reference(join_result.data[global_state.grouping_indices[i]]);
    // }
    // groups_chunk.SetCardinality(join_result.size());

    // // Evaluate aggregates input expressions on join result (reuse local_state.aggregate_executor)
    // local_state.aggregate_executor.Execute(join_result, local_state.aggregate_input_chunk);

    // // Add to aggregate hash table
    // vector<idx_t> no_filter; // empty filter vector
    // global_state.hash_table->AddChunk(groups_chunk, local_state.aggregate_input_chunk, no_filter);

    // // Output columns: joined columns + aggregates after grouping
    // // For simplicity, output the joined columns only here or aggregated output in GetData()

    // // Just forward joined columns for now
    // chunk.Reference(join_result);

    return OperatorResultType::NEED_MORE_INPUT;

}

struct GroupJoinGlobalSourceState : public GlobalSourceState {
    GroupJoinGlobalSinkState *sink_state = nullptr;
};
struct GroupJoinLocalSourceState : public LocalSourceState {
    GroupedAggregateHashTableScanState scan_state;
};

unique_ptr<GlobalSourceState> PhysicalGroupJoin::GetGlobalSourceState(ClientContext &context) const {
    auto state = make_uniq<GroupJoinGlobalSourceState>();
    
    state->sink_state = &sink_state->Cast<GroupJoinGlobalSinkState>();
    return std::move(state);
}

unique_ptr<LocalSourceState> PhysicalGroupJoin::GetLocalSourceState(ExecutionContext &context, GlobalSourceState &gstate) const {
    return make_uniq<GroupJoinLocalSourceState>();
}

// unique_ptr<GlobalSourceState> PhysicalGroupJoin::GetGlobalSourceState(ClientContext &context) const {
//     return nullptr;
// }

// unique_ptr<LocalSourceState> PhysicalGroupJoin::GetLocalSourceState(ExecutionContext &context,
//                                                                     GlobalSourceState &gstate) const {
//     return nullptr;
// }

// SourceResultType PhysicalGroupJoin::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
//     auto &global_state = sink_state->Cast<GroupJoinGlobalSinkState>();

//     if (!input.state) {
//         input.state = make_uniq<AggregateHTScanState>(global_state.hash_table->GetScanState());
//     }
//     // input.state is unique_ptr<AggregateHTScanState>, so dereference to get reference
//     auto &scan_state = *input.state;

//     idx_t count = global_state.hash_table->Scan(scan_state, chunk);

//     if (count == 0) {
//         return SourceResultType::FINISHED;
//     }
//     chunk.SetCardinality(count);
//     return SourceResultType::HAVE_MORE_OUTPUT;
// }




// SourceResultType PhysicalGroupJoin::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
//     std::cout << "Inside GET Data\n";
//     auto &source_state = input.global_state.Cast<GroupJoinGlobalSourceState>();
//     auto &global_state = *source_state.sink_state;
//     // auto &global_state = input.global_state.Cast<GroupJoinGlobalSinkState>();
//     auto &local_state = input.local_state.Cast<GroupJoinLocalSourceState>();

//     // Only reset scan state on first call
//     if (!local_state.scan_state.initialized) {
//         local_state.scan_state.current_offset = 0;
//         local_state.scan_state.initialized = true;
//     }

//     std::cout << "I am dancing.\n";

//     std::cout << "In GetData: hash_table=" << std::endl;
//     std::cout << global_state.hash_table.get();
//     if (global_state.hash_table) {
//         std::cout << ", partitioned_data=" << global_state.hash_table->GetPartitionedData().get();
//     }
//     std::cout << std::endl;
        
//     if (!global_state.hash_table) {
//         throw InternalException("Hash table is null in PhysicalGroupJoin::GetData");
//     }
//     if (!global_state.hash_table->GetPartitionedData()) {
//         throw InternalException("partitioned_data is null in PhysicalGroupJoin::GetData");
//     }

//     std::cout << "Hash table pointer: " << global_state.hash_table.get() << std::endl;
//     std::cout << "Scan state offset: " << local_state.scan_state.current_offset << std::endl;
//     std::cout << "Output chunk columns: " << chunk.ColumnCount() << std::endl;


//     std::cout << "Hash table partitioned_data: " << (global_state.hash_table->GetPartitionedData() ? "OK" : "NULL") << std::endl;
        
//     // Before scanning, ensure chunk is initialized
//     if (chunk.ColumnCount() == 0) {
//         std::cout << "I am Initialaizing\n";
//         auto &layout = global_state.hash_table->GetLayout();
//         vector<LogicalType> output_types;
//         for (idx_t i = 0; i < layout.ColumnCount(); i++) {
//             output_types.push_back(layout.GetTypes()[i]);
//         }
//         chunk.Initialize(Allocator::Get(context.client), output_types);
//     }

//     idx_t count = global_state.hash_table->Scan(chunk, local_state.scan_state, STANDARD_VECTOR_SIZE);
//     std::cout << "I am crying\n";
//     if (count == 0) {
//         return SourceResultType::FINISHED;
//     }

//     chunk.SetCardinality(count);

//     std::cout << "=============Inside GetData()===========" << std::endl;
//     std::cout << chunk.ToString() << std::endl;
//     std::cout << "=============Outside GetData()===========" << std::endl;

//     return SourceResultType::HAVE_MORE_OUTPUT;
// }



// SourceResultType PhysicalGroupJoin::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
//     // Get scan state and sink state
//     auto &source_state = input.global_state.Cast<GroupJoinGlobalSourceState>();
//     auto &gstate = *source_state.sink_state;
//     auto &scan_state = input.local_state.Cast<GroupJoinLocalSourceState>().scan_state;

//     // Scan the hash table for output
//     std::cout << "Going to scan\n";
//     idx_t count = gstate.hash_table->Scan(chunk, scan_state, STANDARD_VECTOR_SIZE);

//     if (count > 0) {
//         return SourceResultType::HAVE_MORE_OUTPUT;
//     } else {
//         return SourceResultType::FINISHED;
//     }
// }






// SourceResultType PhysicalGroupJoin::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
//     static idx_t scan_offset = 0;

//     vector<LogicalType> result_types = {LogicalType::VARCHAR, LogicalType::FLOAT};
//     if (chunk.ColumnCount() == 0) {
//         chunk.Initialize(Allocator::Get(context.client), result_types);
//     }

//     idx_t remaining_entries = aggregation_map.size() - scan_offset;
//     idx_t rows_to_output = remaining_entries < STANDARD_VECTOR_SIZE ? remaining_entries : STANDARD_VECTOR_SIZE;
//     chunk.SetCardinality(rows_to_output);

//     idx_t row_idx = 0;
//     for (auto it = std::next(aggregation_map.begin(), scan_offset); row_idx < rows_to_output && it != aggregation_map.end(); ++it, ++row_idx) {
//         std::string key_str;
//         for (const auto &key : it->first) {
//             key_str += key.ToString() + " ";
//         }
//         chunk.data[0].SetValue(row_idx, Value(key_str));
//         chunk.data[1].SetValue(row_idx, it->second[1]);
//     }

//     scan_offset += rows_to_output;

//     if (scan_offset >= aggregation_map.size()) {
//         scan_offset = 0;
//         return SourceResultType::FINISHED;
//     }

//     return SourceResultType::HAVE_MORE_OUTPUT;
// }




// SourceResultType PhysicalGroupJoin::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
    
//     std::cout << "Returning temporary result with two rows of 0s\n";
//     std::cout << chunk.size() << " = " << chunk.ColumnCount() << std::endl;

//     // Define the schema for the result (single column of type UINT8)
//     for (idx_t i = 0; i < chunk.ColumnCount(); ++i) {
//         std::cout << chunk.data[i].GetType().ToString() << std::endl;
//     }
//     vector<LogicalType> result_types = {LogicalType::USMALLINT};
//     // chunk.Initialize(Allocator::Get(context.client), result_types);

//      for (idx_t i = 0; i < chunk.ColumnCount(); ++i) {
//         std::cout << chunk.data[i].GetType().ToString() << std::endl;
//     }

//     // Set the cardinality to 2 rows
//     chunk.SetCardinality(2);
//      std::cout << chunk.size() << " = " << chunk.ColumnCount() << std::endl;
//     // Populate the chunk with 0s
//     for (idx_t row_idx = 0; row_idx < chunk.size(); row_idx++) {
//         for(idx_t col_idx = 0; col_idx < chunk.ColumnCount(); col_idx++){
//             std::cout << row_idx << ":" << col_idx << std::endl;
//             chunk.data[col_idx].SetValue(row_idx, Value::UINTEGER(col_idx * 100 + 50));
//         } 
//     }

//     // Print the chunk for debugging
//     std::cout << "Chunk contents:\n" << chunk.ToString() << std::endl;

//     return SourceResultType::FINISHED; // Indicate that the operation is complete
// }


// works
SourceResultType PhysicalGroupJoin::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
    static idx_t scan_offset = 0; // Track the current position in the aggregation results

    // Define the schema for the result (Key, Count, Sum)
    vector<LogicalType> result_types = {LogicalType::INTEGER, LogicalType::FLOAT};
    if (chunk.ColumnCount() == 0) {
        chunk.Initialize(Allocator::Get(context.client), result_types);
    }

    // Set the cardinality based on the remaining entries
    idx_t remaining_entries = aggregation_map.size() - scan_offset;
    idx_t rows_to_output = remaining_entries < STANDARD_VECTOR_SIZE ? remaining_entries : STANDARD_VECTOR_SIZE;
    chunk.SetCardinality(rows_to_output);

    // Populate the chunk with aggregation results
    idx_t row_idx = 0;
    for (auto it = std::next(aggregation_map.begin(), scan_offset); row_idx < rows_to_output && it != aggregation_map.end(); ++it, ++row_idx) {
        chunk.data[0].SetValue(row_idx, Value::INTEGER(it->first));       // Key
        // chunk.data[1].SetValue(row_idx, Value::INTEGER(it->second.first)); // Count
        chunk.data[1].SetValue(row_idx, Value::FLOAT(it->second.second)); // Sum
    }

    // Update the scan offset
    scan_offset += rows_to_output;

    // Print the chunk for debugging
    std::cout << "Chunk contents:\n" << chunk.ToString() << std::endl;

    // Return FINISHED if all entries have been sent out
    if (scan_offset >= aggregation_map.size()) {
        scan_offset = 0; // Reset for future calls
        return SourceResultType::FINISHED;
    }

    return SourceResultType::HAVE_MORE_OUTPUT;
}



// SourceResultType PhysicalGroupJoin::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
//     // Retrieve the global sink state from input or context

//     std::cout << "Inside GET Data\n";
//     auto &global_state = input.global_state.Cast<GroupJoinGlobalSinkState>();

//     // if (!input.state) {
//     //     // Initialize scan state for the hash table
//     //     input.state = make_uniq<GroupedAggregateHashTableScanState>(global_state.hash_table->GetScanState());
//     // }

//     auto &scan_state = *make_uniq<GroupedAggregateHashTableScanState>();

//     // Scan the hash table to fill the chunk
//     idx_t count = global_state.hash_table->Scan(chunk, scan_state, STANDARD_VECTOR_SIZE);

//     if (count == 0) {
//         return SourceResultType::FINISHED;
//     }

//     chunk.SetCardinality(count);

//     std::cout << "=============Inside GetData()===========" << std::endl;
//     std::cout << chunk.ToString() << std::endl;
//     std::cout << "=============Outside GetData()===========" << std::endl;


//     return SourceResultType::HAVE_MORE_OUTPUT;
// }



// SourceResultType PhysicalGroupJoin::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
//     // auto &global_state = input.global_state->Cast<GroupJoinGlobalSinkState>();
//     auto &global_state = input.global_state.Cast<GroupJoinGlobalSinkState>();

//     if (!input.local_state) {
//         input.local_state = make_uniq<GroupedAggregateHashTableScanState>();
//     }
//     auto &scan_state = input.local_state;

//     idx_t count = global_state.hash_table->Scan(chunk, scan_state, STANDARD_VECTOR_SIZE);

//     if (count == 0) {
//         return SourceResultType::FINISHED;
//     }

//     chunk.SetCardinality(count);
//     return SourceResultType::HAVE_MORE_OUTPUT;
// }

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


bool PhysicalGroupJoin::SinkOrderDependent() const  {
	return true;
}


void PhysicalGroupJoin::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
    D_ASSERT(children.size() == 2); // Ensure binary join
    // std::cout << "JIYO $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$\n";
    auto &left_child = children[0];
    auto &right_child = children[1];

    // The current pipeline will be the one that runs the Finalize() of this join.
    auto &state = meta_pipeline.GetState();
    state.SetPipelineSource(current, *this); // Set self as sink

    // --- Build left child MetaPipeline ---
    MetaPipeline &left_meta = meta_pipeline.CreateChildMetaPipeline(current, *this);
    left_meta.Build(*left_child);

    // --- Build right child MetaPipeline ---
    MetaPipeline &right_meta = meta_pipeline.CreateChildMetaPipeline(current, *this);
    right_meta.Build(*right_child);

    // --- Get shared_ptr to Pipelines ---
    // Current pipeline shared_ptr via enable_shared_from_this
    shared_ptr<Pipeline> current_ptr = current.shared_from_this();

    // Base pipelines of left and right MetaPipelines
    shared_ptr<Pipeline> &left_ptr = left_meta.GetBasePipeline();
    shared_ptr<Pipeline> &right_ptr = right_meta.GetBasePipeline();

    // --- Add dependencies: current depends on left and right ---
    current_ptr->AddDependency(left_ptr);
    current_ptr->AddDependency(right_ptr);
}

} // namespace duckdb






















































































// #include "duckdb/execution/operator/join/physical_group_join.hpp"
// #include "duckdb/execution/aggregate_hashtable.hpp"
// #include "duckdb/execution/expression_executor.hpp"
// #include "duckdb/common/types/chunk_collection.hpp"
// #include "duckdb/planner/expression/bound_comparison_expression.hpp"
// #include "duckdb/planner/expression/bound_reference_expression.hpp"
// #include "duckdb/planner/expression/bound_aggregate_expression.hpp"
// #include "duckdb/function/aggregate_function.hpp"
// #include <iostream>

// namespace duckdb {

// // --- State classes ---
// class GroupJoinGlobalSinkState : public GlobalSinkState {
// public:
//     ChunkCollection left_data;
//     ChunkCollection right_data;
//     unique_ptr<GroupedAggregateHashTable> hash_table;
//     vector<LogicalType> group_types;
//     vector<idx_t> grouping_indices; // indices in right child's output
//     vector<AggregateObject> aggregate_objects;
//     vector<LogicalType> aggregate_return_types;
// };

// class GroupJoinLocalSinkState : public LocalSinkState {
// public:
//     GroupJoinLocalSinkState(Allocator &allocator, const PhysicalGroupJoin &op, ClientContext &context)
//         : aggregate_executor(context) {
//         std::cout << "[IN] GroupJoinLocalSinkState constructor" << std::endl;
//         vector<LogicalType> aggregate_input_types;
//         for (auto &aggr_expr : op.aggregates) {
//             auto &bound_aggr = aggr_expr->Cast<BoundAggregateExpression>();
//             for (auto &child : bound_aggr.children) {
//                 aggregate_executor.AddExpression(*child);
//                 aggregate_input_types.push_back(child->return_type);
//             }
//         }
//         aggregate_input_chunk.Initialize(allocator, aggregate_input_types);
//         std::cout << "[OUT] GroupJoinLocalSinkState constructor" << std::endl;
//     }
//     ExpressionExecutor aggregate_executor;
//     DataChunk aggregate_input_chunk;
// };

// PhysicalGroupJoin::PhysicalGroupJoin(
//     LogicalOperator &op,
//     unique_ptr<PhysicalOperator> left,
//     unique_ptr<PhysicalOperator> right,
//     unique_ptr<Expression> condition_p,
//     JoinType join_type,
//     idx_t estimated_cardinality,
//     vector<unique_ptr<Expression>> &groups_p,
//     vector<unique_ptr<Expression>> &aggregates_p
// ) : PhysicalJoin(op, PhysicalOperatorType::GROUP_JOIN, join_type, estimated_cardinality),
//     condition(std::move(condition_p)),
//     groups(std::move(groups_p)),
//     aggregates(std::move(aggregates_p)) {

//     std::cout << "[IN] PhysicalGroupJoin constructor" << std::endl;
//     children.push_back(std::move(left));
//     children.push_back(std::move(right));
//     std::cout << "[OUT] PhysicalGroupJoin constructor" << std::endl;
// }

// PhysicalGroupJoin::~PhysicalGroupJoin() = default;

// // --- Operator implementation ---

// unique_ptr<GlobalSinkState> PhysicalGroupJoin::GetGlobalSinkState(ClientContext &context) const {
//     std::cout << "[IN] GetGlobalSinkState" << std::endl;
//     auto state = make_uniq<GroupJoinGlobalSinkState>();
//     auto &left_types = children[0]->types;
//     auto &right_types = children[1]->types;

//     std::cout << "Left table types: ";
//     for (auto &t : left_types) std::cout << t.ToString() << " ";
//     std::cout << std::endl;
//     std::cout << "Right table types: ";
//     for (auto &t : right_types) std::cout << t.ToString() << " ";
//     std::cout << std::endl;

//     for (auto &group : groups) {
//         auto &bound_ref = group->Cast<BoundReferenceExpression>();
//         idx_t idx = bound_ref.index;
//         state->grouping_indices.push_back(idx);
//         state->group_types.push_back(right_types[idx]);
//     }
//     for (auto &aggr_expr : aggregates) {
//         auto &bound_aggr = aggr_expr->Cast<BoundAggregateExpression>();
//         state->aggregate_objects.emplace_back(&bound_aggr);
//         state->aggregate_return_types.push_back(bound_aggr.return_type);
//     }
//     state->hash_table = make_uniq<GroupedAggregateHashTable>(
//         context, Allocator::Get(context), state->group_types, vector<LogicalType>(), state->aggregate_objects
//     );
//     std::cout << "[OUT] GetGlobalSinkState" << std::endl;
//     return std::move(state);
// }

// unique_ptr<LocalSinkState> PhysicalGroupJoin::GetLocalSinkState(ExecutionContext &context) const {
//     std::cout << "[IN] GetLocalSinkState" << std::endl;
//     auto result = make_uniq<GroupJoinLocalSinkState>(Allocator::Get(context.client), *this, context.client);
//     std::cout << "[OUT] GetLocalSinkState" << std::endl;
//     return result;
// }

// SinkResultType PhysicalGroupJoin::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
//     std::cout << "[IN] Sink" << std::endl;
//     auto &global_state = input.global_state.Cast<GroupJoinGlobalSinkState>();
//     const auto &input_types = chunk.GetTypes();
//     auto &left_types = children[0]->types;
//     auto &right_types = children[1]->types;
//     if (input_types == left_types) {
//         std::cout << "Appending chunk to LEFT table" << std::endl;
//         std::cout << chunk.ToString() << std::endl;
//         global_state.left_data.Append(chunk);
//     } else if (input_types == right_types) {
//         std::cout << "Appending chunk to RIGHT table" << std::endl;
//         std::cout << chunk.ToString() << std::endl;
//         global_state.right_data.Append(chunk);
//     } else {
//         std::cout << "ERROR: input chunk types do not match any child" << std::endl;
//         throw InternalException("PhysicalGroupJoin::Sink: input chunk types do not match any child");
//     }
//     std::cout << "[OUT] Sink" << std::endl;
//     return SinkResultType::NEED_MORE_INPUT;
// }

// SinkFinalizeType PhysicalGroupJoin::Finalize(Pipeline &pipeline, Event &event, ClientContext &context, OperatorSinkFinalizeInput &input) const {
//     std::cout << "[IN] Finalize" << std::endl;
//     auto &global_state = input.global_state.Cast<GroupJoinGlobalSinkState>();
//     const auto &left_types = children[0]->types;
//     const auto &right_types = children[1]->types;
//     vector<LogicalType> join_types = left_types;
//     join_types.insert(join_types.end(), right_types.begin(), right_types.end());

//     // Print all scanned chunks for left table
//     std::cout << "=== All LEFT table chunks ===" << std::endl;
//     DataChunk left_chunk;
//     global_state.left_data.InitializeScan();
//     while (global_state.left_data.Scan(left_chunk)) {
//         std::cout << left_chunk.ToString() << std::endl;
//     }

//     // Print all scanned chunks for right table
//     std::cout << "=== All RIGHT table chunks ===" << std::endl;
//     DataChunk right_chunk;
//     global_state.right_data.InitializeScan();
//     while (global_state.right_data.Scan(right_chunk)) {
//         std::cout << right_chunk.ToString() << std::endl;
//     }

//     // Now do the join and group-by
//     std::cout << "=== Performing in-memory join and group-by ===" << std::endl;
//     global_state.left_data.InitializeScan();
//     while (global_state.left_data.Scan(left_chunk)) {
//         global_state.right_data.InitializeScan();
//         while (global_state.right_data.Scan(right_chunk)) {
//             for (idx_t i = 0; i < left_chunk.size(); ++i) {
//                 for (idx_t j = 0; j < right_chunk.size(); ++j) {
//                     DataChunk joined_chunk;
//                     joined_chunk.Initialize(Allocator::Get(context), join_types);
//                     for (idx_t col = 0; col < left_chunk.ColumnCount(); ++col) {
//                         joined_chunk.data[col].SetValue(0, left_chunk.data[col].GetValue(i));
//                     }
//                     for (idx_t col = 0; col < right_chunk.ColumnCount(); ++col) {
//                         idx_t out_col = left_chunk.ColumnCount() + col;
//                         joined_chunk.data[out_col].SetValue(0, right_chunk.data[col].GetValue(j));
//                     }
//                     joined_chunk.SetCardinality(1);

//                     // Print joined row before group-by
//                     std::cout << "[JOINED ROW] " << joined_chunk.ToString() << std::endl;

//                     // Evaluate join condition if any
//                     if (condition) {
//                         ExpressionExecutor condition_executor(context);
//                         condition_executor.AddExpression(*condition);
//                         DataChunk condition_result;
//                         condition_result.Initialize(Allocator::Get(context), {LogicalType::BOOLEAN});
//                         condition_executor.Execute(joined_chunk, condition_result);
//                         auto result_ptr = FlatVector::GetData<bool>(condition_result.data[0]);
//                         if (!result_ptr[0]) continue;
//                     }

//                     // Extract group keys (offset by left columns)
//                     DataChunk group_chunk;
//                     group_chunk.InitializeEmpty(global_state.group_types);
//                     group_chunk.SetCardinality(1);
//                     for (idx_t g = 0; g < global_state.grouping_indices.size(); ++g) {
//                         idx_t idx = global_state.grouping_indices[g] + left_chunk.ColumnCount();
//                         group_chunk.data[g].SetValue(0, joined_chunk.data[idx].GetValue(0));
//                     }

//                     // Aggregate input
//                     vector<LogicalType> aggr_input_types;
//                     for (auto &aggr_expr : aggregates) {
//                         auto *agg = dynamic_cast<BoundAggregateExpression*>(aggr_expr.get());
//                         if (agg && !agg->children.empty()) {
//                             aggr_input_types.push_back(agg->children[0]->return_type);
//                         } else {
//                             aggr_input_types.push_back(aggr_expr->return_type);
//                         }
//                     }
//                     DataChunk aggr_input_chunk;
//                     aggr_input_chunk.Initialize(Allocator::Get(context), aggr_input_types);
//                     ExpressionExecutor aggr_exec(context);
//                     for (auto &aggr_expr : aggregates) {
//                         aggr_exec.AddExpression(*aggr_expr);
//                     }
//                     aggr_exec.Execute(joined_chunk, aggr_input_chunk);

//                     unsafe_vector<idx_t> filter;
//                     global_state.hash_table->AddChunk(group_chunk, aggr_input_chunk, filter);
//                 }
//             }
//         }
//     }
//     std::cout << "[OUT] Finalize" << std::endl;
//     return SinkFinalizeType::READY;
// }

// SourceResultType PhysicalGroupJoin::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
//     std::cout << "[IN] GetData" << std::endl;
//     auto &global_state = input.global_state.Cast<GroupJoinGlobalSinkState>();
//     static GroupedAggregateHashTableScanState scan_state;
//     idx_t count = global_state.hash_table->Scan(chunk, scan_state, STANDARD_VECTOR_SIZE);
//     if (count == 0) {
//         std::cout << "[OUT] GetData (FINISHED)" << std::endl;
//         return SourceResultType::FINISHED;
//     }
//     chunk.SetCardinality(count);
//     std::cout << "[OUT] GetData (HAVE_MORE_OUTPUT)" << std::endl;
//     std::cout << "Output chunk: " << chunk.ToString() << std::endl;
//     return SourceResultType::HAVE_MORE_OUTPUT;
// }

// unique_ptr<OperatorState> PhysicalGroupJoin::GetOperatorState(ExecutionContext &context) const {
//     std::cout << "[IN] GetOperatorState" << std::endl;
//     std::cout << "[OUT] GetOperatorState" << std::endl;
//     return nullptr;
// }

// OperatorResultType PhysicalGroupJoin::ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
//                                                       GlobalOperatorState &gstate, OperatorState &state) const {
//     std::cout << "[IN] ExecuteInternal" << std::endl;
//     std::cout << "[OUT] ExecuteInternal" << std::endl;
//     return OperatorResultType::NEED_MORE_INPUT;
// }

// unique_ptr<GlobalSourceState> PhysicalGroupJoin::GetGlobalSourceState(ClientContext &context) const {
//     std::cout << "[IN] GetGlobalSourceState" << std::endl;
//     std::cout << "[OUT] GetGlobalSourceState" << std::endl;
//     return nullptr;
// }

// unique_ptr<LocalSourceState> PhysicalGroupJoin::GetLocalSourceState(ExecutionContext &context, GlobalSourceState &gstate) const {
//     std::cout << "[IN] GetLocalSourceState" << std::endl;
//     std::cout << "[OUT] GetLocalSourceState" << std::endl;
//     return nullptr;
// }

// InsertionOrderPreservingMap<string> PhysicalGroupJoin::ParamsToString() const {
//     std::cout << "[IN] ParamsToString" << std::endl;
//     InsertionOrderPreservingMap<string> result;
//     result["Join Type"] = EnumUtil::ToString(join_type);
//     result["Join Condition"] = condition ? condition->GetName() : "None";
//     string groups_info;
//     for (idx_t i = 0; i < groups.size(); i++) {
//         if (i > 0) groups_info += "\n";
//         groups_info += groups[i]->GetName();
//     }
//     result["Groups"] = groups_info;
//     string aggregate_info;
//     for (idx_t i = 0; i < aggregates.size(); i++) {
//         auto &aggregate = aggregates[i]->Cast<BoundAggregateExpression>();
//         if (i > 0) aggregate_info += "\n";
//         aggregate_info += aggregates[i]->GetName();
//         if (aggregate.filter) {
//             aggregate_info += " Filter: " + aggregate.filter->GetName();
//         }
//     }
//     result["Aggregates"] = aggregate_info;
//     std::cout << "[OUT] ParamsToString" << std::endl;
//     return result;
// }

// bool PhysicalGroupJoin::SinkOrderDependent() const {
//     std::cout << "[IN] SinkOrderDependent" << std::endl;
//     std::cout << "[OUT] SinkOrderDependent" << std::endl;
//     return true;
// }

// void PhysicalGroupJoin::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
//     std::cout << "[IN] BuildPipelines" << std::endl;
//     // No-op or implement as needed
//     std::cout << "[OUT] BuildPipelines" << std::endl;
// }

// } // namespace duckdb