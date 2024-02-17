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
#include "experiments.hpp"
#include "adapter_arrow.hpp"


// ------------------------------
// Aliases
using arrow::fs::FileSystemFromUri;


// ------------------------------
// Convenience Functions

void
PrintSchema(shared_ptr<Schema> schema, int64_t offset, int64_t length) {
  std::cout << "Schema:" << std::endl;

  // >> Print some attributes (columns)
  PrintSchemaAttributes(schema, offset, length);

  // >> Print some metadata key-values (if there are any)
  if (schema->HasMetadata()) {
    PrintSchemaMetadata(schema->metadata()->Copy(), offset, length);
  }
}


void
PrintTable(shared_ptr<Table> table_data, int64_t offset, int64_t length) {
  shared_ptr<Table> table_slice;
  int64_t  row_count = table_data->num_rows();

  std::cout << "Table Excerpt ";

  if (length > 0) {
    int64_t max_rowndx = length < row_count ? length : row_count;
    table_slice = table_data->Slice(offset, max_rowndx);
    std::cout << "(" << max_rowndx << " of " << row_count << ")";
  }

  else {
    table_slice = table_data->Slice(offset);
    std::cout << "(" << row_count << " of " << row_count << ")";
  }

  std::cout << std::endl
            << "--------------" << std::endl
            << table_slice->ToString()
            << std::endl
  ;
}


void
PrintBatch(shared_ptr<RecordBatch> batch_data, int64_t offset, int64_t length) {
  shared_ptr<RecordBatch> batch_slice;
  int64_t row_count = batch_data->num_rows();

  std::cout << "Table Excerpt ";

  if (length > 0) {
    batch_slice = batch_data->Slice(offset, length);
    std::cout << "(" << length << " of " << row_count << " rows)";
  }

  else {
    batch_slice = batch_data->Slice(offset);
    std::cout << "(" << row_count << " of " << row_count << " rows)";
  }

  std::cout << std::endl
            << "--------------" << std::endl
            << batch_slice->ToString()
            << std::endl
  ;
}


// this Reader is arrow::ipc::feather::Reader
Result<shared_ptr<Reader>>
ReaderForIPCFile(const string &path_as_uri) {
  string path_to_file;

  // get a `FileSystem` instance (local fs scheme is "file://")
  ARROW_ASSIGN_OR_RAISE(auto localfs, FileSystemFromUri(path_as_uri, &path_to_file));

  // use the `FileSystem` instance to open a handle to the file
  ARROW_ASSIGN_OR_RAISE(auto input_file_stream, localfs->OpenInputFile(path_to_file));

  return Reader::Open(input_file_stream);
}


Result<shared_ptr<Table>>
ReadIPCFile(const string& path_to_file) {
  shared_ptr<Table> data_table;

  ARROW_ASSIGN_OR_RAISE(auto feather_reader, ReaderForIPCFile(path_to_file));
  ARROW_RETURN_NOT_OK(feather_reader->Read(&data_table));

  return data_table;
}


Result<shared_ptr<Table>>
ReadBatchesFromTable(arrow::TableBatchReader &reader, size_t batch_count) {
  vector<shared_ptr<RecordBatch>> batch_list;

  shared_ptr<RecordBatch> curr_batch;
  for (size_t batch_ndx = 0; batch_ndx < batch_count; ++batch_ndx) {
    ARROW_RETURN_NOT_OK(reader.ReadNext(&curr_batch));
    if (curr_batch == nullptr) { break; }

    batch_list.push_back(curr_batch);
  }

  ARROW_ASSIGN_OR_RAISE(auto table_chunk, Table::FromRecordBatches(batch_list));

  return table_chunk->CombineChunks();
}
