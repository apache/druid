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
|`ARRAY[expr1, expr2, ...]`|Constructs a SQL `ARRAY` literal from the expression arguments, using the type of the first argument as the output array type.|
|`ARRAY_LENGTH(arr)`|Returns length of the array expression.|
|`ARRAY_OFFSET(arr, long)`|Returns the array element at the 0-based index supplied, or null for an out of range index.|
|`ARRAY_ORDINAL(arr, long)`|Returns the array element at the 1-based index supplied, or null for an out of range index.|
|`ARRAY_CONTAINS(arr, expr)`|If `expr` is a scalar type, returns 1 if `arr` contains `expr`. If `expr` is an array, returns 1 if `arr` contains all elements of `expr`. Otherwise returns 0.|
|`ARRAY_OVERLAP(arr1, arr2)`|Returns 1 if `arr1` and `arr2` have any elements in common, else 0.|
|`ARRAY_OFFSET_OF(arr, expr)`|Returns the 0-based index of the first occurrence of `expr` in the array. If no matching elements exist in the array, returns `null` or `-1` if `druid.generic.useDefaultValueForNull=true` (deprecated legacy mode).|
|`ARRAY_ORDINAL_OF(arr, expr)`|Returns the 1-based index of the first occurrence of `expr` in the array. If no matching elements exist in the array, returns `null` or `-1` if `druid.generic.useDefaultValueForNull=true` (deprecated legacy mode).|
|`ARRAY_PREPEND(expr, arr)`|Adds `expr` to the beginning of `arr`, the resulting array type determined by the type of `arr`.|
|`ARRAY_APPEND(arr, expr)`|Appends `expr` to `arr`, the resulting array type determined by the type of `arr`.|
|`ARRAY_CONCAT(arr1, arr2)`|Concatenates `arr2` to `arr1`. The resulting array type is determined by the type of `arr1`.|
|`ARRAY_SLICE(arr, start, end)`|Returns the subarray of `arr` from the 0-based index `start` (inclusive) to `end` (exclusive). Returns `null`, if `start` is less than 0, greater than length of `arr`, or greater than `end`.|
|`ARRAY_TO_STRING(arr, str)`|Joins all elements of `arr` by the delimiter specified by `str`.|
|`STRING_TO_ARRAY(str1, str2)`|Splits `str1` into an array on the delimiter specified by `str2`, which is a regular expression.|
|`ARRAY_TO_MV(arr)`|Converts an `ARRAY` of any type into a multi-value string `VARCHAR`.|
