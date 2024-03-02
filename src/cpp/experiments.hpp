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
#pragma once

#include <string>
#include <iostream>
#include <vector>
#include <unordered_map>

#include <ctime>
#include <iomanip>

#include "adapter_arrow.hpp"
#include "util_arrow.hpp"
#include "operators.hpp"


// ------------------------------
// Type Aliases and Macros

// >> Core types
using std::shared_ptr;
using std::unique_ptr;

using std::string;
using std::string_view;
using std::vector;
using std::unordered_map;

using Int32Vec = std::vector<int32_t>;
using Int64Vec = std::vector<int64_t>;
using StrVec   = std::vector<std::string>;
