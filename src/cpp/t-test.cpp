/**
 * Copyright 2024 Aldrin Montana

 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 *     http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


// ------------------------------
// Dependencies

#include <unistd.h>

#include "operators.hpp"

using std::string;
using Skytether::Partition;


// ------------------------------
// Globals

const char *option_template = "l:r:o:d:h:q:";


// ------------------------------
// Functions

Status
RunMappedTTest(KineticConn &kconn, uint8_t qdepth, string result_key) {
    std::cout << "Running T-test for metaclusters 12 and 13!" << std::endl;

    // ----------
    // >> Get the gene order to normalize to
    std::cout << "Reading gene annotations from kinetic..." << std::endl;
    ARROW_ASSIGN_OR_RAISE(auto gene_partition, kconn.GetPartitionData("annotations/genes"));
    ARROW_ASSIGN_OR_RAISE(auto gene_table, gene_partition->AsTable());
    auto gene_order = gene_table->column(0);

    // release gene table data
    // gene_table.reset();

    // ----------
    // >> Get data for metacluster 12
    std::cout << "Calculating partial aggregates for metacluster 12" << std::endl;
    MeanAggr left_aggr;

    // Prep the first statistics
    //      |> E-GEOD-100618
    std::string left_meta1     { "E-GEOD-100618" };
    // std::string left_meta1_key { "/" + left_meta1 };
    std::string left_meta1_key { kconn.kinetic_domain->domain_key + "/" + left_meta1 };

    std::cout << "\t" << left_meta1     << std::endl;
    ARROW_ASSIGN_OR_RAISE(auto left_meta1_table, kconn.MapPartitionData(left_meta1_key, qdepth));
    ARROW_ASSIGN_OR_RAISE(auto left_meta1_norm, CopyMatchedRows(gene_order, left_meta1_table));

    // replace the old means and variances structures
    left_aggr.means     = std::move(left_meta1_norm->column(1));
    left_aggr.variances = std::move(left_meta1_norm->column(2));

    auto left_meta1_kvmeta = left_meta1_table->schema()->metadata()->Copy();
    ARROW_ASSIGN_OR_RAISE(left_aggr.count, Skytether::GetAggrCount(left_meta1_kvmeta));

    // release unneeded pointers
    /*
    left_meta1_kvmeta.reset();
    left_meta1_table.reset();
    left_meta1_norm.reset();
    */


    //      |> E-GEOD-76312
    MeanAggr    temp_aggr;
    std::string left_meta2     { "E-GEOD-76312" };
    // std::string left_meta2_key { "/" + left_meta2 };
    std::string left_meta2_key { kconn.kinetic_domain->domain_key + "/" + left_meta2 };

    std::cout << "\t" << left_meta2 << std::endl;
    ARROW_ASSIGN_OR_RAISE(auto left_meta2_table, kconn.MapPartitionData(left_meta2_key, qdepth));
    ARROW_ASSIGN_OR_RAISE(auto left_meta2_norm, CopyMatchedRows(gene_order, left_meta2_table));

    // replace the old means and variances structures
    temp_aggr.means     = std::move(left_meta2_norm->column(1));
    temp_aggr.variances = std::move(left_meta2_norm->column(2));

    auto left_meta2_kvmeta = left_meta2_table->schema()->metadata()->Copy();
    ARROW_ASSIGN_OR_RAISE(temp_aggr.count, Skytether::GetAggrCount(left_meta2_kvmeta));
    // left_meta2_kvmeta.reset();

    // Merge aggregates into `left_aggr`
    left_aggr.Combine(&temp_aggr);

    // release unneeded pointers
    /*
    left_meta2_table.reset();
    left_meta2_norm.reset();
    */
    // delete temp_aggr;

    /*
    std::cout << "left aggregates:" << std::endl;
    left_aggr.PrintState();
    left_aggr.PrintM1M2();
    */

    // >> Get data for metacluster 13
    // Prep the second statistics
    std::cout << "Calculating partial aggregates for metacluster 13" << std::endl;
    MeanAggr    right_aggr;
    std::string right_meta     { "E-GEOD-106540" };
    // std::string right_meta_key { "/" + right_meta };
    std::string right_meta_key { kconn.kinetic_domain->domain_key + "/" + right_meta };

    std::cout << "\t" << right_meta << std::endl;
    ARROW_ASSIGN_OR_RAISE(auto right_meta_table, kconn.MapPartitionData(right_meta_key, qdepth));
    ARROW_ASSIGN_OR_RAISE(auto right_meta_norm, CopyMatchedRows(gene_order, right_meta_table));

    // replace the old means and variances structures
    right_aggr.means     = std::move(right_meta_norm->column(1));
    right_aggr.variances = std::move(right_meta_norm->column(2));

    auto right_meta_kvmeta = right_meta_table->schema()->metadata()->Copy();
    ARROW_ASSIGN_OR_RAISE(right_aggr.count, Skytether::GetAggrCount(right_meta_kvmeta));
    // right_meta_kvmeta.reset();

    // release unneded pointers
    /*
    right_meta_table.reset();
    right_meta_norm.reset();
    */

    std::cout << "Metacluster 12 aggr dimensions: " << std::endl
              << "\trows: " << left_aggr.means->length()
              << std::endl
    ;

    std::cout << "Metacluster 13 aggr dimensions: " << std::endl
              << "\trows: " << right_aggr.means->length()
              << std::endl
    ;

    std::cout << "left aggregates:" << std::endl;
    left_aggr.PrintState();
    // left_aggr.PrintM1M2();

    std::cout << "right aggregates:" << std::endl;
    right_aggr.PrintState();
    // right_aggr.PrintM1M2();

    auto tstat = left_aggr.ComputeTStatWith(right_aggr);

    // release unneeded data
    // delete left_aggr;
    // delete right_aggr;

    ARROW_ASSIGN_OR_RAISE(auto table_buffer, WriteTableToBuffer(tstat));
    ARROW_RETURN_NOT_OK(kconn.PutKey(result_key, table_buffer));

    kconn.PrintStats();
    kconn.Disconnect();

    return Status::OK();
}

// ------------------------------
// Main Logic

int main(int argc, char **argv) {
    /*
    if (argc != 6) {
        printf("t-test <-o key-name> <-l key-name> <-r key-name>\n");
        return 1;
    }
    */

    string  domain_name, tablekey_one, tablekey_two, resultkey;
    string  host   { "10.23.89.4" };
    string  port   { "8123"       };
    string  passwd { "asdfasdf"   };
    uint8_t qdepth { 16           };

    auto run_status = Status::OK();

    char parsed_opt = (char) getopt(argc, argv, option_template);
    while (parsed_opt != (char) -1) {
        switch (parsed_opt) {
            case 'd': {
                domain_name = string { optarg };
                break;
            }

            case 'l': {
                tablekey_one = string { optarg };
                break;
            }

            case 'r': {
                tablekey_two = string { optarg };
                break;
            }

            case 'o': {
                resultkey = string { optarg };
                break;
            }

            case 'h': {
                host = string { optarg };
                break;
            }

            case 'q': {
                qdepth = (uint8_t) std::stoi(optarg);
                // std::cout << "AIO queue depth: " << std::to_string(qdepth) << std::endl;
                break;
            }

            default: {
                run_status = Status::Invalid("Invalid argument");
                break;
            }
        }

        parsed_opt = (char) getopt(argc, argv, option_template);
    }

    if (not run_status.ok()) { return 1; }

    KineticConn kconn { domain_name };
    kconn.Connect(host, port, passwd);
    if (not kconn.IsConnected()) {
        std::cerr << "Could not connect to kinetic" << std::endl;
        return 1;
    }
    else {
        std::cout << "Connected to host [" << host << "]" << std::endl;
    }

    run_status = RunMappedTTest(kconn, qdepth, resultkey);
    if (not run_status.ok()) {
        std::cout << "Error status:"              << std::endl
                  << "\t" << run_status.message() << std::endl
        ;

        return 1;
    }

    return 0;
}
