// ------------------------------
// LICENSE
//
// Copyright 2024 Aldrin Montana
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


// ------------------------------
// Variables

static std::string help_message {
    "experiment_one <absolute path to current dir> <'table' | 'vector'>"
};


// ------------------------------
// Functions

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
