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

// brings in all Apache Arrow dependencies
#include "adapter_arrow.hpp"


// ------------------------------
// Macros and aliases

using std::unordered_map;
using arrow::util::string_view;

// ------------------------------
// Classes

struct MeanAggr {
    uint64_t                 count;
    shared_ptr<ChunkedArray> means;
    shared_ptr<ChunkedArray> variances;

    MeanAggr(): count(0), means(nullptr), variances(nullptr) {};

    Status PrintM1M2();
    Status PrintState();

    Status Initialize(shared_ptr<ChunkedArray> initial_vals);
    Status Combine(const MeanAggr *other_aggr);
    Status Accumulate( shared_ptr<Table> new_vals
                      ,int64_t           col_startndx = 1
                      ,int64_t           col_stopndx  = 0);

    shared_ptr<Table> TakeResult();
    shared_ptr<Table> ComputeTStatWith(const MeanAggr &other_aggr);
};
