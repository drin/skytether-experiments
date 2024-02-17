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

// >> MeanAggr implementation

Status
MeanAggr::PrintM1M2() {
    auto final_vars = VecDiv(this->variances, Datum(this->count));

    std::cout << "samples aggregated: " << this->count << std::endl;

    std::cout << "___________________" << std::endl
              << "sample means:"       << std::endl
              << this->means->ToString()
              << std::endl
              << std::endl
    ;

    std::cout << "___________________" << std::endl
              << "sample variances:"   << std::endl
              << final_vars->ToString()
              << std::endl
              << std::endl
    ;

    return Status::OK();
}

Status
MeanAggr::PrintState() {
    auto current_state = this->TakeResult();

    std::cout << "samples aggregated: " << this->count << std::endl;
    std::cout << "current aggregates:" << std::endl
              << "+-----------------+" << std::endl
              << current_state->ToString()
              << "+-----------------+" << std::endl
              << std::endl
    ;

    return Status::OK();
}

Status
MeanAggr::Initialize(shared_ptr<ChunkedArray> initial_vals) {
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
 * Assumes rows correspond to each other
 * Per Wikipedia (parallel variance):
 *     n      = n_a + n_b
 *     delta  = avg_b - avg_a
 *     M2     = M2_a + M2_b + delta ** 2 * n_a * n_b / n
 *     var_ab = M2 / (n - 1)
 *
 *     return var_ab
 */
Status
MeanAggr::Combine(const MeanAggr *other_aggr) {
    // sum the sample sizes
    int64_t total_count = this->count + other_aggr->count;

    std::cout << "Dimensions of [A]:" << std::endl
              << "\tmeans: "     << this->means->length()     << std::endl
              << "\tvariances: " << this->variances->length() << std::endl
    ;

    std::cout << "Dimensions of [B]:" << std::endl
              << "\tmeans: "     << other_aggr->means->length()     << std::endl
              << "\tvariances: " << other_aggr->variances->length() << std::endl
    ;

    // >> update mean
    auto this_sum  = VecMul(this->means      , Datum(this->count      ));
    auto other_sum = VecMul(other_aggr->means, Datum(other_aggr->count));
    auto new_means = VecDiv(VecAdd(this_sum, other_sum), Datum(total_count));
    this->means    = new_means;

    // >> update M2
    // Recurrent term
    auto delta_mean    = VecSub(other_aggr->means, this->means);
    auto squared_delta = VecMul(delta_mean       , delta_mean );
    auto co_samplesize = (this->count * other_aggr->count) / total_count;
    auto co_variance   = VecMul(squared_delta, Datum(co_samplesize));

    // M2,a + M2,b + recurrent term
    auto combined_vars = VecAdd(this->variances, other_aggr->variances);
    auto new_variance  = VecAdd(combined_vars  , co_variance          );
    this->variances    = new_variance;

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
MeanAggr::Accumulate( shared_ptr<Table> new_vals
                     ,int64_t           col_startndx
                     ,int64_t           col_stopndx) {
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
        delta_mean  = VecSub(col_vals, this->means);
        this->means = VecAdd(
             this->means
            ,VecDiv(delta_mean, Datum(this->count))
        );

        delta_var       = VecSub(col_vals, this->means);
        this->variances = VecAdd(
             this->variances
            ,VecMul(delta_var, delta_mean)
        );
    }

    return Status::OK();
}

shared_ptr<Table>
MeanAggr::TakeResult() {
  auto result_schema = arrow::schema({
        arrow::field("mean"    , arrow::float64())
       ,arrow::field("variance", arrow::float64())
  });

  return Table::Make(result_schema, { this->means, this->variances });
}

shared_ptr<Table>
MeanAggr::ComputeTStatWith(const MeanAggr &other_aggr) {
    auto left_size  =  this->count;
    auto right_size = other_aggr.count;

    auto  left_vars = VecDiv(this->variances     , Datum(left_size) );
    auto right_vars = VecDiv(other_aggr.variances, Datum(right_size));

    // Subtract means; numerator
    auto ttest_num = VecSub(this->means, other_aggr.means);

    // square root of scaling factor (sample sizes); right side of denom
    double scale_factor = std::sqrt((1.0 / left_size) + (1.0 / right_size));

    // And now the "complex" variance calculations
    auto ttest_denom = VecAdd(
         VecMul(left_vars , Datum( left_size - 1))
        ,VecMul(right_vars, Datum(right_size - 1))
    );

    ttest_denom = VecDiv(ttest_denom, Datum((left_size + right_size - 2)));
    ttest_denom = VecPow(ttest_denom, Datum(0.5));
    ttest_denom = VecMul(ttest_denom, Datum(scale_factor));

    return Table::Make(
         arrow::schema({ arrow::field("t-statistic", arrow::float64()) })
        ,{ VecDiv(ttest_num, ttest_denom) }
    );
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
  MeanAggr partial_aggr;

  int64_t col_stopndx = src_table->num_columns();
  if (col_limit > 0 and col_startndx + col_limit < col_stopndx) {
    col_stopndx = col_startndx + col_limit;
  }

  auto aggr_tstart = std::chrono::steady_clock::now();
  partial_aggr.Accumulate(src_table, col_startndx, col_stopndx);
  auto aggr_tstop  = std::chrono::steady_clock::now();

  (*aggr_result) = partial_aggr.TakeResult();

  return std::chrono::duration_cast<std::chrono::milliseconds>(aggr_tstop - aggr_tstart);
}
