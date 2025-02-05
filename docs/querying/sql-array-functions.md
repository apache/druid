---
id: sql-array-functions
title: "SQL ARRAY functions"
sidebar_label: "Array functions"
---

<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->

<!--
  The format of the tables that describe the functions and operators
  should not be changed without updating the script create-sql-docs
  in web-console/script/create-sql-docs, because the script detects
  patterns in this markdown file and parse it to TypeScript file for web console
-->


:::info
 Apache Druid supports two query languages: Druid SQL and [native queries](querying.md).
 This document describes the SQL language.
:::

This page describes the operations you can perform on arrays using [Druid SQL](./sql.md). See [`ARRAY` data type documentation](./sql-data-types.md#arrays) for additional details. 

All array references in the array function documentation can refer to multi-value string columns or `ARRAY` literals.
These functions are largely identical to the [multi-value string functions](sql-multivalue-string-functions.md), but
use `ARRAY` types and behavior. Multi-value string `VARCHAR` columns can be converted to `VARCHAR ARRAY` to use with
these functions using `MV_TO_ARRAY`, and `ARRAY` types can be converted to multi-value string `VARCHAR` with
`ARRAY_TO_MV`.

The following table describes array functions. To learn more about array aggregation functions, see [SQL aggregation functions](./sql-aggregations.md).

|Function|Description|
|--------|-----|
|`ARRAY[expr1, expr2, ...]`|Constructs a SQL `ARRAY` literal from the provided expression arguments. All arguments must be of the same type.|
|`ARRAY_APPEND(arr, expr)`|Appends the expression to the array. The source array type determines the resulting array type.|
|`ARRAY_CONCAT(arr1, arr2)`|Concatenates two arrays. The type of `arr1` determines the resulting array type.|
|`ARRAY_CONTAINS(arr, expr)`|Checks if the array contains the specified expression. If the specified expression is a scalar value, returns true if the source array contains the value. If the specified expression is an array, returns true if the source array contains all elements of the expression.|
|`ARRAY_LENGTH(arr)`|Returns the length of the array.|
|`ARRAY_OFFSET(arr, long)`|Returns the array element at the specified zero-based index. Returns null if the index is out of bounds.|
|`ARRAY_OFFSET_OF(arr, expr)`|Returns the 0-based index of the first occurrence of `expr` in the array. If no matching elements exist in the array, returns `null`.|
|`ARRAY_ORDINAL(arr, long)`|Returns the array element at the specified one-based index. Returns null if the index is out of bounds.|
|`ARRAY_ORDINAL_OF(arr, expr)`|Returns the 1-based index of the first occurrence of `expr` in the array. If no matching elements exist in the array, returns `null`.|
|`ARRAY_OVERLAP(arr1, arr2)`|Returns true if two arrays have any elements in common. Treats `NULL` values as known elements.|
|`ARRAY_PREPEND(expr, arr)`|Prepends the expression to the array. The source array type determines the resulting array type.|
|`ARRAY_SLICE(arr, start, end)`|Returns a subset of the array from the zero-based index `start` (inclusive) to `end` (exclusive). Returns null if `start` is less than 0, greater than the length of the array, or greater than `end`.|
|`ARRAY_TO_MV(arr)`|Converts an array of any type into a [multi-value string](sql-data-types.md#multi-value-strings).|
|`ARRAY_TO_STRING(arr, delimiter)`|Joins all elements of the array into a string using the specified delimiter.|
|`SCALAR_IN_ARRAY(expr, arr)`|Checks if the scalar value is present in the array. Returns false if the value is non-null, or `UNKNOWN` if the value is `NULL`. Returns `UNKNOWN` if the array is `NULL`.|
|`STRING_TO_ARRAY(string, delimiter)`|Splits the string into an array of substrings using the specified delimiter. The delimiter must be a valid regular expression.|
