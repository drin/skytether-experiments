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

#include "adapter_arrow.hpp"

// NOTE: For debugging
#include <ctime>
#include <iomanip>


// ------------------------------
// Macros and aliases

// >> Arrow: Numeric compute functions
using arrow::compute::Add;
using arrow::compute::Subtract;
using arrow::compute::Divide;
using arrow::compute::Multiply;
using arrow::compute::Power;
using arrow::compute::AbsoluteValue;

// >> Arrow: Other compute functions
using arrow::compute::Take;


// ------------------------------
// Functions

// for readability
using SharedChunkedArr = shared_ptr<ChunkedArray>;

SharedChunkedArr VecAdd(SharedChunkedArr left_op, SharedChunkedArr right_op);
SharedChunkedArr VecSub(SharedChunkedArr left_op, SharedChunkedArr right_op);
SharedChunkedArr VecDiv(SharedChunkedArr left_op, SharedChunkedArr right_op);
SharedChunkedArr VecDiv(SharedChunkedArr left_op, Datum            right_op);
SharedChunkedArr VecMul(SharedChunkedArr left_op, SharedChunkedArr right_op);
SharedChunkedArr VecPow(SharedChunkedArr left_op, Datum            right_op);
SharedChunkedArr VecAbs(SharedChunkedArr unary_op);

Result<shared_ptr<Table>>
CopyMatchedRows(shared_ptr<ChunkedArray> ordered_ids, shared_ptr<Table> src_table);
