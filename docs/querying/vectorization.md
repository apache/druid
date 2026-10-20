---
id: vectorization
title: "Vectorization"
sidebar_label: "Vectorization"
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

The GroupBy, Timeseries, TimeBoundary, and Scan (in MSQ only) query types can run in _vectorized_ mode, which speeds up
query execution by processing batches of rows at a time. Not all queries can be vectorized. In particular, vectorization
currently has the following requirements:

- All query-level filters must either be able to run on bitmap indexes or must offer vectorized row-matchers. These
include `selector`, `bound`, `in`, `like`, `regex`, `search`, `and`, `or`, and `not`.
- All filters in filtered aggregators must offer vectorized row-matchers.
- All aggregators must offer vectorized implementations. These include `count`, `doubleSum`, `floatSum`, `longSum`. `longMin`,
 `longMax`, `doubleMin`, `doubleMax`, `floatMin`, `floatMax`, `longAny`, `doubleAny`, `floatAny`, `stringAny`,
 `hyperUnique`, `filtered`, `approxHistogram`, `approxHistogramFold`, and `fixedBucketsHistogram` (with numerical input). 
- All virtual columns must offer vectorized implementations. Currently for expression virtual columns, support for vectorization is decided on a per expression basis, depending on the type of input and the functions used by the expression. See the currently supported list in the [expression documentation](math-expr.md#vectorization-support).
- For GroupBy: All dimension specs must be "default" (no extraction functions or filtered dimension specs).
- For GroupBy: No multi-value dimensions.
- Only immutable segments (not real-time).
- Only [table datasources](datasource.md#table) (not joins, subqueries, lookups, or inline datasources).

Other query types (like TopN, Search, and native Scan) ignore the `vectorize` parameter, and will execute without
vectorization. These query types will ignore the `vectorize` parameter even if it is set to `"force"`.

## Query context parameters

The following query context parameters control vectorized execution:

<!-- BEGIN GENERATED QUERY CONTEXT PARAMETER TABLE: names=vectorize,vectorSize,vectorizeVirtualColumns -->

|Parameter|Description|Default|
|---|---|---|
|[`vectorize`](query-context-reference.md#vectorize)|Enables or disables vectorized query execution.|`true`|
|[`vectorizeVirtualColumns`](query-context-reference.md#vectorizevirtualcolumns)|Enables or disables vectorized query processing of queries with virtual columns, layered on top of `vectorize` (`vectorize` must also be set to true for a query to utilize vectorization).|`true`|
|[`vectorSize`](query-context-reference.md#vectorsize)|Sets the row batching size for a particular query.|`512`|

<!-- END GENERATED QUERY CONTEXT PARAMETER TABLE -->
