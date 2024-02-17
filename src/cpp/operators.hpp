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
// Classes

struct Aggr {
    uint64_t                 count;
    shared_ptr<ChunkedArray> means;
    shared_ptr<ChunkedArray> variances;

    Aggr(): count(0), means(nullptr), variances(nullptr) {};

    shared_ptr<Table> TakeResult();
    Status            Initialize(shared_ptr<ChunkedArray> initial_vals);
    Status            Accumulate( shared_ptr<Table> new_vals
                                 ,int64_t           col_startndx=1
                                 ,int64_t           col_stopndx =0);
};
