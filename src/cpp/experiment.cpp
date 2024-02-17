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
#include "experiment.hpp"


// ------------------------------
// Variables

static std::string help_message {
    "simple <absolute path to current dir> <'table' | 'vector'>"
};


// ------------------------------
// Functions

// Result<shared_ptr<RecordBatchStreamReader>>
Result<shared_ptr<Reader>>
ReaderForIPCFile(const std::string &path_as_uri) {
    #if DEBUG != 0
        std::cout << "Reading arrow IPC-formatted file: " << path_as_uri << std::endl;
    #endif

    std::string path_to_file;

    // get a `FileSystem` instance (local fs scheme is "file://")
    ARROW_ASSIGN_OR_RAISE(auto localfs, arrow::fs::FileSystemFromUri(path_as_uri, &path_to_file));

    // use the `FileSystem` instance to open a handle to the file
    ARROW_ASSIGN_OR_RAISE(auto input_file_stream, localfs->OpenInputFile(path_to_file));

    // read from the handle using `RecordBatchStreamReader`
    /*
    return RecordBatchStreamReader::Open(
         input_file_stream
        ,IpcReadOptions::Defaults()
    );
    */

    // return Reader::Open(input_file_stream, IpcReadOptions::Defaults());
    return Reader::Open(input_file_stream);
}


Result<shared_ptr<Table>>
ReadIPCFile(const std::string& path_to_file) {
    #if DEBUG != 0
        std::cout << "Parsing file: '" << path_to_file << "'" << std::endl;
    #endif

    // Declares and initializes `batch_reader`
    // ARROW_ASSIGN_OR_RAISE(auto batch_reader, ReaderForIPCFile(path_to_file));
    // return arrow::Table::FromRecordBatchReader(batch_reader.get());

    shared_ptr<Table> data_table = shared_ptr<Table>(nullptr);

    ARROW_ASSIGN_OR_RAISE(auto feather_reader, ReaderForIPCFile(path_to_file));
    ARROW_RETURN_NOT_OK(feather_reader->Read(&data_table));

    return data_table;
}


/**
 * Compute partial aggregate from `src_table`, and then return the time to calculate
 * the aggregate. Store the result in the shared_ptr pointed to by `aggr_result`.
 */
std::chrono::milliseconds
AggrTable( shared_ptr<Table>  src_table
          ,int64_t            col_startndx
          ,int64_t            col_limit
          ,shared_ptr<Table> *aggr_result) {

    Aggr partial_aggr;
    int64_t col_stopndx = 0;

    if (col_limit > 0) {
        col_stopndx = (
            ((col_startndx + col_limit) < src_table->num_columns()) ?
                  col_startndx + col_limit
                : src_table->num_columns()
        );
    }

    // NOTE: we are *only* timing the computation on each slice
    auto aggr_tstart = std::chrono::steady_clock::now();

    // does the remainder (last partition) or the whole thing (vsplit_count == 1)
    partial_aggr.Accumulate(src_table, col_startndx, col_stopndx);

    auto aggr_tstop = std::chrono::steady_clock::now();

    (*aggr_result) = partial_aggr.TakeResult();

    return std::chrono::duration_cast<std::chrono::milliseconds>(aggr_tstop - aggr_tstart);
}


Result<shared_ptr<Table>>
ReadBatchesFromTable(arrow::TableBatchReader &reader, size_t batch_count) {
    std::vector<shared_ptr<RecordBatch>> batch_list;

    shared_ptr<RecordBatch> curr_batch;
    for (size_t batch_ndx = 0; batch_ndx < batch_count; ++batch_ndx) {
        ARROW_RETURN_NOT_OK(reader.ReadNext(&curr_batch));
        if (curr_batch == nullptr) { break; }

        batch_list.push_back(curr_batch);
    }

    ARROW_ASSIGN_OR_RAISE(
         auto table_chunk
        ,Table::FromRecordBatches(batch_list)
    );

    return table_chunk->CombineChunks();
}


int main(int argc, char **argv) {
    // ----------
    // Process CLI args for convenience
    if (argc < 3) {
      // we need this to make it easier to run on various systems
      std::cerr << "Error: too few arguments."
                << std::endl
                << "Usage: <experiment> <path-to-resource-root> <aggregation scope> [batch-count] [col-count]"
                << std::endl
      ;
      return 1;
    }

    else if (argc > 5) {
      std::cerr << "Error: too many arguments."
                << std::endl
                << "Usage: <experiment> <path-to-resource-root> <aggregation scope> [batch-count] [col-count]"
                << std::endl
      ;
      return 2;
    }

    std::string work_dirpath     { argv[1] };
    std::string should_aggrtable { argv[2] };
    size_t      batch_count      { 1       };
    int64_t     col_limit        { 0       };

    std::string test_fpath {
        "file://"
      + work_dirpath
      + "/resources/E-GEOD-76312.48-2152.x565.feather"
      // + "/resources/E-GEOD-76312.48-2152.x300.feather"
    };
    bool aggr_table { should_aggrtable == "table" };

    if (argc >= 4) { batch_count  = std::stoull(argv[3]); }
    if (argc == 5) { col_limit    = std::stoll(argv[4]);  }

    #if DEBUG != 0
      std::cout << "Using directory  [" << work_dirpath << "]" << std::endl;
      std::cout << "Aggregating over ["
                << (aggr_table ? "table" : "vector")
                << "]" << std::endl
      ;

      std::cout << "Batch count to concat [" << batch_count  << "]" << std::endl;
      std::cout << "Column limit to aggr  [" << col_limit << "]" << std::endl;
    #endif


    // ----------
    // Read sample data from file
    auto reader_result = ReadIPCFile(test_fpath);
    if (not reader_result.ok()) {
      std::cerr << "Couldn't read file:"                     << std::endl
                << "\t" << reader_result.status().ToString() << std::endl
      ;

      return 1;
    }

    // ----------
    // Peek at the data
    auto data_table = *reader_result;
    auto pkey_col   = data_table->column(0);

    #if DEBUG != 0
      std::cout << "Table dimensions ["
                <<     data_table->num_rows() << ", " << data_table->num_columns()
                << "]"
                << std::endl
                << "\t" << "[" << pkey_col->num_chunks()       << "] chunks"
                << std::endl
                << "\t" << "[" << pkey_col->chunk(0)->length() << "] chunk_size"
                << std::endl
      ;
    #endif

    // >> The computation to be benchmarked
    std::chrono::milliseconds aggr_ttime { 0 };
    int                       aggr_count { 0 };
    shared_ptr<Table>         aggr_result;

    //  |> case for applying computation to whole table
    if (aggr_table) {
      aggr_ttime += AggrTable(data_table, 1, col_limit, &aggr_result);
      aggr_count  = 1;

      #if DEBUG == 0
        std::cout <<         data_table->num_rows()
                  << "," << ((col_limit > 0) ? col_limit : data_table->num_columns())
        ;
      #endif
    }

    //  |> case for applying computation to each table chunk (i.e. RecordBatch)
    else {
      std::vector<shared_ptr<Table>> aggrs_by_slice;
      aggrs_by_slice.reserve(pkey_col->num_chunks());

      // Use `TableBatchReader` to grab chunks from the table
      auto   table_batcher = arrow::TableBatchReader(*data_table);
      auto   slice_result  = ReadBatchesFromTable(table_batcher, batch_count);

      if (slice_result.ok()) {
        #if DEBUG != 0
          std::cout << "Row count for most slices: ["
                    << (*slice_result)->num_rows()
                    << "]"
                    << std::endl
          ;

          std::cout << "Row count for final slice: ["
                    << data_table->num_rows() % (*slice_result)->num_rows()
                    << "]"
                    << std::endl
          ;

        #else
          std::cout << "\""
                       << (*slice_result)->num_rows()
                       << ":"
                       << data_table->num_rows() % (*slice_result)->num_rows()
                    << "\""
                    << ","
                    << ((col_limit > 0) ? col_limit : (*slice_result)->num_columns())
          ;

        #endif
      }

      // Iterate over each chunk and time the aggregation
      while (slice_result.ok()) {
        // Aggregate table and save result
        auto tslice = *slice_result;
        aggr_ttime  += AggrTable(tslice, 1, col_limit, &aggr_result);
        aggrs_by_slice.push_back(aggr_result);

        // Read next batch
        slice_result = ReadBatchesFromTable(table_batcher, batch_count);

        // Count iterations
        ++aggr_count;
      }

      // Aggregate results but this doesn't directly affect the experiment
      auto concat_result = arrow::ConcatenateTables(aggrs_by_slice);
      if (not concat_result.ok()) {
        std::cerr << "Failed to concatenate partial aggr results" << std::endl
                  << "\t" << concat_result.status().ToString()    << std::endl
        ;

        return 3;
      }

      aggr_result = *concat_result;
    }

    #if DEBUG != 0
      std::cout << "Aggr Time:"                                           << std::endl
                << "\tTotal: " << aggr_ttime.count()              << "ms" << std::endl
                << "\tAvg  : " << aggr_ttime.count() / aggr_count << "ms" << std::endl
      ;

    #else
      std::cout << "," << "\"" << aggr_ttime.count()              << "ms" << "\""
                << "," << "\"" << aggr_ttime.count() / aggr_count << "ms" << "\""
                << std::endl
      ;

    #endif

    return 0;
}
