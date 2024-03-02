// ------------------------------
// LICENSE
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
#pragma once

#include <string>

// >> Arrow dependencies
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/dataset/api.h>
#include <arrow/compute/api.h>
#include <arrow/filesystem/api.h>


// ------------------------------
// Type Aliases

// >> Core types
using std::shared_ptr;
using std::unique_ptr;

// >> Arrow result (and wrapper) types
using arrow::Datum;
using arrow::Result;
using arrow::Status;

// >> Arrow core types
using arrow::Array;
using arrow::ChunkedArray;
using arrow::Field;
using arrow::Schema;
using KVMetadata = arrow::KeyValueMetadata;
using arrow::Table;
using arrow::RecordBatch;

// >> Arrow vector aliases
using arrow::ArrayVector;
using arrow::RecordBatchVector;

// >> Arrow low-level and meta types
using arrow::DataType;
using arrow::Buffer;

// >> Arrow array types
using arrow::Int32Array;
using arrow::Int64Array;
using arrow::StringArray;
using arrow::DictionaryArray;

// >> Arrow IO types
using arrow::io::InputStream;
using arrow::io::BufferReader;
using arrow::io::OutputStream;
using arrow::io::BufferOutputStream;

// >> Arrow filesystem types
using arrow::io::RandomAccessFile;
using arrow::fs::FileSystem;
using arrow::ipc::RecordBatchStreamReader;
using arrow::ipc::IpcReadOptions;
using arrow::ipc::feather::Reader;


// ------------------------------
// Functions

void PrintSchemaAttributes(shared_ptr<Schema> schema, int64_t offset, int64_t length);
void PrintSchemaMetadata(shared_ptr<KVMetadata> schema_meta, int64_t offset, int64_t length);

void PrintSchema(shared_ptr<Schema>     schema    , int64_t offset, int64_t length);
void PrintTable(shared_ptr<Table>       table_data, int64_t offset, int64_t length);
void PrintBatch(shared_ptr<RecordBatch> batch_data, int64_t offset, int64_t length);

Result<shared_ptr<Reader>> ReaderForIPCFile(const std::string &path_as_uri);
Result<shared_ptr<Table>>  ReadIPCFile(const std::string& path_to_file);

Result<shared_ptr<Table>>
ReadBatchesFromTable(arrow::TableBatchReader &reader, size_t batch_count);
