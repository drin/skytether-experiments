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

#include "operators.hpp"
#include "util_arrow.hpp"


// ------------------------------
// Classes

// >> Aggr implementation
shared_ptr<Table>
Aggr::TakeResult() {
  auto result_schema = arrow::schema({
        arrow::field("mean"    , arrow::float64())
       ,arrow::field("variance", arrow::float64())
  });

  return Table::Make(result_schema, { this->means, this->variances });
}

Status
Aggr::Initialize(shared_ptr<ChunkedArray> initial_vals) {
  arrow::DoubleBuilder init_variance;

  // Initialize N empty variance values, where N is initial_vals->length
  ARROW_RETURN_NOT_OK(init_variance.AppendEmptyValues(initial_vals->length()));
  ARROW_ASSIGN_OR_RAISE(shared_ptr<Array> variance_arr, init_variance.Finish());

  // `variances` likely has different chunking from `initial_vals`
  variances   = std::make_shared<ChunkedArray>(variance_arr);
  means       = initial_vals;
  this->count = 1;

  return Status::OK();
}


/**
  * Per Wikipedipa; Moment2 (variance):
  *     fn(count, mean, M2, newValue):
  *         (count, mean, M2)  = existingAggregate
  *         count             += 1
  *
  *         delta              = newValue - mean
  *         mean              += delta / count
  *
  *         delta2             = newValue - mean
  *         M2                += delta * delta2
  *
  *         return (count, mean, M2)
 */
Status
Aggr::Accumulate(shared_ptr<Table> new_vals, int64_t col_startndx, int64_t col_stopndx) {
    shared_ptr<ChunkedArray> delta_mean;
    shared_ptr<ChunkedArray> delta_var;

    if (this->count == 0) {
        this->Initialize(new_vals->column(col_startndx));
        this->count   = 1;
        col_startndx += 1;
    }

    // if stop ndx is 0, then default to the last column
    if (col_stopndx == 0) { col_stopndx = new_vals->num_columns(); }

    for (; col_startndx < col_stopndx; ++col_startndx) {
        // use a result just to make call chains easier
        auto col_vals = new_vals->column(col_startndx);

        this->count += 1;
        delta_mean  = VecAbs(VecSub(col_vals, this->means));
        this->means = VecAdd(
             this->means
            ,VecDiv(delta_mean, Datum(this->count))
        );

        delta_var       = VecAbs(VecSub(col_vals, this->means));
        this->variances = VecAdd(
             this->variances
            ,VecMul(delta_var, delta_mean)
        );
    }

    return Status::OK();
}
