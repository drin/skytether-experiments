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
#include "adapter_arrow.hpp"


// ------------------------------
// Functions

shared_ptr<ChunkedArray>
VecAdd(shared_ptr<ChunkedArray> left_op, shared_ptr<ChunkedArray> right_op) {
    if (left_op == nullptr or right_op == nullptr) { return nullptr; }

    Result<Datum> op_result = Add(left_op, right_op);
    if (not op_result.ok()) { return nullptr; }

    return std::make_shared<ChunkedArray>(
        std::move(op_result.ValueOrDie()).chunks()
    );
}


shared_ptr<ChunkedArray>
VecSub(shared_ptr<ChunkedArray> left_op, shared_ptr<ChunkedArray> right_op) {
    if (left_op == nullptr or right_op == nullptr) { return nullptr; }

    Result<Datum> op_result = Subtract(left_op, right_op);
    if (not op_result.ok()) { return nullptr; }

    return std::make_shared<ChunkedArray>(
        std::move(op_result.ValueOrDie()).chunks()
    );
}


shared_ptr<ChunkedArray>
VecDiv(shared_ptr<ChunkedArray> left_op, shared_ptr<ChunkedArray> right_op) {
    if (left_op == nullptr or right_op == nullptr) { return nullptr; }

    Result<Datum> op_result = Divide(left_op, right_op);
    if (not op_result.ok()) { return nullptr; }

    return std::make_shared<ChunkedArray>(
        std::move(op_result.ValueOrDie()).chunks()
    );
}


shared_ptr<ChunkedArray>
VecDiv(shared_ptr<ChunkedArray> left_op, Datum right_op) {
    if (left_op == nullptr) { return nullptr; }

    Result<Datum> op_result = Divide(left_op, right_op);
    if (not op_result.ok()) { return nullptr; }

    return std::make_shared<ChunkedArray>(
        std::move(op_result.ValueOrDie()).chunks()
    );
}


shared_ptr<ChunkedArray>
VecMul(shared_ptr<ChunkedArray> left_op, shared_ptr<ChunkedArray> right_op) {
    if (left_op == nullptr or right_op == nullptr) { return nullptr; }

    Result<Datum> op_result = Multiply(left_op, right_op);
    if (not op_result.ok()) { return nullptr; }

    return std::make_shared<ChunkedArray>(
        std::move(op_result.ValueOrDie()).chunks()
    );
}


shared_ptr<ChunkedArray>
VecMul(shared_ptr<ChunkedArray> left_op, Datum right_op) {
    if (left_op == nullptr) { return nullptr; }

    Result<Datum> op_result = Multiply(left_op, right_op);
    if (not op_result.ok()) { return nullptr; }

    return std::make_shared<ChunkedArray>(
        std::move(op_result.ValueOrDie()).chunks()
    );
}


shared_ptr<ChunkedArray>
VecPow(shared_ptr<ChunkedArray> left_op, Datum right_op) {
    if (left_op == nullptr) { return nullptr; }

    Result<Datum> op_result = Power(left_op, right_op);
    if (not op_result.ok()) { return nullptr; }

    return std::make_shared<ChunkedArray>(
        std::move(op_result.ValueOrDie()).chunks()
    );
}


shared_ptr<ChunkedArray>
VecAbs(shared_ptr<ChunkedArray> unary_op) {
    if (unary_op == nullptr) { return nullptr; }

    Result<Datum> op_result = AbsoluteValue(unary_op);
    if (not op_result.ok()) { return nullptr; }

    return std::make_shared<ChunkedArray>(
        std::move(op_result.ValueOrDie()).chunks()
    );
}
