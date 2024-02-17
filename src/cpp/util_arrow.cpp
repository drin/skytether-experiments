// ------------------------------
// License
//
// Copyright 2023 Aldrin Montana
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


// ------------------------------
// Dependencies
#include "util_arrow.hpp"


// ------------------------------
// Functions


// >> Templated

template<typename ElementType, typename BuilderType>
Result<shared_ptr<Array>>
VecToArrow(const std::vector<ElementType> &src_data) {
    BuilderType array_builder;
    ARROW_RETURN_NOT_OK(array_builder.Resize(src_data.size()));
    ARROW_RETURN_NOT_OK(array_builder.AppendValues(src_data));

    shared_ptr<Array> vec_as_arrow;
    ARROW_RETURN_NOT_OK(array_builder.Finish(&vec_as_arrow));

    return Result<shared_ptr<Array>>(vec_as_arrow);
}


// >> Convenience wrappers

shared_ptr<ChunkedArray>
VecAdd(shared_ptr<ChunkedArray> left_op, shared_ptr<ChunkedArray> right_op) {
  if (left_op == nullptr or right_op == nullptr) { return nullptr; }

  Result<Datum> op_result = Add(left_op, right_op);
  if (not op_result.ok()) { return nullptr; }

  return std::make_shared<ChunkedArray>(
    std::move(op_result.ValueOrDie()).chunks()
  );
}


shared_ptr<ChunkedArray>
VecSub(shared_ptr<ChunkedArray> left_op, shared_ptr<ChunkedArray> right_op) {
  if (left_op == nullptr or right_op == nullptr) { return nullptr; }

  Result<Datum> op_result = Subtract(left_op, right_op);
  if (not op_result.ok()) { return nullptr; }

  return std::make_shared<ChunkedArray>(
    std::move(op_result.ValueOrDie()).chunks()
  );
}


shared_ptr<ChunkedArray>
VecDiv(shared_ptr<ChunkedArray> left_op, shared_ptr<ChunkedArray> right_op) {
  if (left_op == nullptr or right_op == nullptr) { return nullptr; }

  Result<Datum> op_result = Divide(left_op, right_op);
  if (not op_result.ok()) { return nullptr; }

  return std::make_shared<ChunkedArray>(
    std::move(op_result.ValueOrDie()).chunks()
  );
}


shared_ptr<ChunkedArray>
VecDiv(shared_ptr<ChunkedArray> left_op, Datum right_op) {
  if (left_op == nullptr) { return nullptr; }

  Result<Datum> op_result = Divide(left_op, right_op);
  if (not op_result.ok()) { return nullptr; }

  return std::make_shared<ChunkedArray>(
    std::move(op_result.ValueOrDie()).chunks()
  );
}


shared_ptr<ChunkedArray>
VecMul(shared_ptr<ChunkedArray> left_op, shared_ptr<ChunkedArray> right_op) {
  if (left_op == nullptr or right_op == nullptr) { return nullptr; }

  Result<Datum> op_result = Multiply(left_op, right_op);
  if (not op_result.ok()) { return nullptr; }

  return std::make_shared<ChunkedArray>(
    std::move(op_result.ValueOrDie()).chunks()
  );
}


shared_ptr<ChunkedArray>
VecMul(shared_ptr<ChunkedArray> left_op, Datum right_op) {
  if (left_op == nullptr) { return nullptr; }

  Result<Datum> op_result = Multiply(left_op, right_op);
  if (not op_result.ok()) { return nullptr; }

  return std::make_shared<ChunkedArray>(
    std::move(op_result.ValueOrDie()).chunks()
  );
}


shared_ptr<ChunkedArray>
VecPow(shared_ptr<ChunkedArray> left_op, Datum right_op) {
  if (left_op == nullptr) { return nullptr; }

  Result<Datum> op_result = Power(left_op, right_op);
  if (not op_result.ok()) { return nullptr; }

  return std::make_shared<ChunkedArray>(
    std::move(op_result.ValueOrDie()).chunks()
  );
}


shared_ptr<ChunkedArray>
VecAbs(shared_ptr<ChunkedArray> unary_op) {
  if (unary_op == nullptr) { return nullptr; }

  Result<Datum> op_result = AbsoluteValue(unary_op);
  if (not op_result.ok()) { return nullptr; }

  return std::make_shared<ChunkedArray>(
    std::move(op_result.ValueOrDie()).chunks()
  );
}

// TODO: eventually decompose this so that each chunk can be individually updated
Result<shared_ptr<Table>>
SelectTableByRow(shared_ptr<Table> input_table, Int64Vec &row_indices) {
    // Add indices of encodedIDs to selection indices
    auto convert_result = VecToArrow<int64_t, arrow::Int64Builder>(row_indices);
    if (not convert_result.ok()) {
        return arrow::Status::Invalid("Could not convert vector to array");
    }

    ARROW_ASSIGN_OR_RAISE(
         Datum selection_result
        ,Take(input_table, convert_result.ValueOrDie())
    );

    return Result<shared_ptr<Table>>(
        arrow::util::get<shared_ptr<Table>>(selection_result.value)
    );
}


// >> Utility functions for complex operations

// NOTE: this uses string_view, because CopyMatchedRows does. Maybe figure out how to generalize
// this later
shared_ptr<RecordBatch>
CreateEmptyRecordBatch(shared_ptr<Schema> batch_schema, StrVec &row_ids) {
    // can be parameterized later; if desired
    double              default_fillval = 0;
    ArrayVec            batch_data;
    std::vector<double> fill_values(row_ids.size(), default_fillval);

    // First, add the IDs (assumed to be gene IDs for now)
    auto rowid_result = VecToArrow<std::string, arrow::StringBuilder>(row_ids);
    if (not rowid_result.ok()) {
        std::cerr << "Unable to set IDs for empty RecordBatch" << std::endl;
        return nullptr;
    }

    batch_data.emplace_back(rowid_result.ValueOrDie());

    // Then, fill all of the <empty> values
    for (int col_ndx = 1; col_ndx < batch_schema->num_fields(); ++col_ndx) {
        auto fill_result = VecToArrow<double, arrow::DoubleBuilder>(fill_values);
        if (not fill_result.ok()) {
            std::cerr << "Unable to fill column for empty RecordBatch"
                      << " [" << col_ndx << "]"
                      << std::endl
            ;

            return nullptr;
        }

        batch_data.emplace_back(fill_result.ValueOrDie());
    }

    return RecordBatch::Make(batch_schema, row_ids.size(), batch_data);
}

Result<shared_ptr<Table>>
CopyMatchedRows(shared_ptr<ChunkedArray> ordered_ids, shared_ptr<Table> src_table) {
    // hash primary keys (gene IDs) in src_table to match against later
    int64_t src_rowndx = 0;
    auto    src_keys   = src_table->column(0);

    unordered_map<string_view, int64_t> src_keymap;
    src_keymap.reserve((size_t) src_keys->length());

    // >> TODO: get start time here
    for (int chunk_ndx = 0; chunk_ndx < src_keys->num_chunks(); ++chunk_ndx) {
        shared_ptr<StringArray> chunk_srckeys = std::static_pointer_cast<StringArray>(
            src_keys->chunk(chunk_ndx)
        );

        for (const auto src_key : *chunk_srckeys) {
            if (not src_key.has_value()) { continue; }

            // add a mapping of this value (gene ID) to it's row index
            src_keymap.insert({ src_key.value(), src_rowndx });
            ++src_rowndx;
        }
    }

    // Construct a vector of RecordBatches to convert into a result table
    StrVec         missing_ids;
    Int64Vec       match_indices;
    RecordBatchVec result_batches;

    // >> TODO: interval time: source keymap <--> populate missing rows
    for (int chunk_ndx = 0; chunk_ndx < ordered_ids->num_chunks(); ++chunk_ndx) {
        shared_ptr<StringArray> chunk_srckeys = std::static_pointer_cast<StringArray>(
            ordered_ids->chunk(chunk_ndx)
        );

        // time stamp each iteration: find variability in processing each chunk
        auto timestamp = std::time(nullptr);

        /* NOTE: put_time comes from <iomanip>
        std::cout << "[" << std::put_time(std::localtime(&timestamp), "%T") << "] "
                  << "chunk: " << chunk_ndx
                  << std::endl
        ;
        */

        for (const auto src_key : *chunk_srckeys) {
            auto src_keyval = src_key.value_or("");

            // If the source value is found, add the matched index
            if (src_key.has_value() and src_keymap.count(src_keyval)) {
                // Since we have a match, drain all mismatches
                if (missing_ids.size() > 0) {
                    result_batches.push_back(
                        CreateEmptyRecordBatch(src_table->schema(), missing_ids)
                    );

                    missing_ids.clear();
                }

                match_indices.push_back(src_keymap[src_keyval]);

                // use `continue` to reduce code indentation for the "else" case
                continue;
            }

            // Otherwise, drain matches by appending `take` selection as RecordBatches
            if (match_indices.size() > 0) {
                ARROW_ASSIGN_OR_RAISE(
                     auto table_selection
                    ,SelectTableByRow(src_table, match_indices)
                )

                // push RecordBatches onto the end of result_batches
                TableBatchReader batch_converter(*table_selection);
                ARROW_RETURN_NOT_OK(batch_converter.ReadAll(&result_batches));

                match_indices.clear();
            }

            // if we're here, then the ID didn't have a hit in src_keymap
            missing_ids.push_back(std::string { src_keyval });
        }
    }

    // std::cout << std::endl;

    // drain any remaining missing IDs
    if (missing_ids.size() > 0) {
        result_batches.push_back(
            CreateEmptyRecordBatch(src_table->schema(), missing_ids)
        );

        missing_ids.clear();
    }

    // drain any remaining matched IDs
    if (match_indices.size() > 0) {
        ARROW_ASSIGN_OR_RAISE(
             auto table_selection
            ,SelectTableByRow(src_table, match_indices)
        )

        // push RecordBatches onto the end of result_batches
        TableBatchReader batch_converter(*table_selection);
        ARROW_RETURN_NOT_OK(batch_converter.ReadAll(&result_batches));

        match_indices.clear();
    }

    // >> TODO: get finish time for copying source rows into dense table

    return Table::FromRecordBatches(src_table->schema(), result_batches);
}

