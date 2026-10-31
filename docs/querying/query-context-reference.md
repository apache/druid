---
id: query-context-reference
title: "Query context reference"
sidebar_label: "Query context reference"
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

The query context provides runtime configuration for individual queries in Apache&circledR; Druid. Each parameter in the query context controls a specific aspect of query behavior—from execution timeouts and resource limits to caching policies and processing strategies.

This reference documents every query context parameter, grouped by where it applies:

- **General parameters**: Apply to native queries and, unless noted, to SQL queries. Some also apply to other engines.
- **Join filter parameters**: Control filter rewrites and push-down for join queries.
- **Druid SQL parameters**: Apply to SQL queries only.
- **Query type parameters**: Apply to a specific native query type, such as TopN or GroupBy.
- **SQL-based ingestion parameters**: Apply to queries that run on the multi-stage query (MSQ) engine.
- **Dart parameters**: Apply to [Dart](dart.md) queries only.

Parameters are listed in alphabetical order within each group. Each parameter shows its type, default value,
the query interfaces (`JSON` for native queries, `SQL` for Druid SQL) and engines that honor it, and the Druid version
that introduced it.

To learn how to set the query context, see [Set query context](./query-context.md).
<a id="vectorization-parameters"></a>To learn which queries can run vectorized, see [Vectorization](vectorization.md).

## General parameters

The following parameters apply to native queries and, unless noted, to SQL queries through the native queries they plan into. Each parameter's **Query** and **Engine** rows show exactly where it applies.

<!-- BEGIN GENERATED QUERY CONTEXT PARAMETERS: * -->

### `brokerService`

| | |
|---|---|
|**Description**|Broker service to which this query should be routed. This parameter is honored only by a broker selector strategy of type *manual*. See [Router strategies](../design/router.md#router-strategies) for more details.|
|**Type**|String|
|**Default**|N/A|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Since**|0.22.0|

### `bySegment`

| | |
|---|---|
|**Description**|Native queries only. Return "by segment" results. Primarily used for debugging, setting it to `true` returns results associated with the data segment they came from.|
|**Type**|Boolean|
|**Default**|`false`|
|**Query**|JSON|
|**Engine**|Native|
|**Since**|0.1.0|

### `cloneQueryMode`

| | |
|---|---|
|**Description**|Indicates whether clone Historicals should be queried by brokers. Clone servers are created by the `cloneServers` Coordinator dynamic configuration. Possible values are `excludeClones`, `includeClones` and `preferClones`. `excludeClones` means that clone Historicals are not queried by the broker. `preferClones` indicates that when given a choice between the clone Historical and the original Historical which is being cloned, the broker chooses the clones. Historicals which are not involved in the cloning process will still be queried. `includeClones` means that broker queries any Historical without regarding clone status. This parameter only affects native queries. MSQ does not query Historicals directly.|
|**Type**|Enum (`preferClones`, `includeClones`, `excludeClones`)|
|**Default**|`excludeClones`|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Since**|34.0.0|

### `debug`

| | |
|---|---|
|**Description**|Flag indicating whether to enable debugging outputs for the query. When set to false, no additional logs will be produced (logs produced will be entirely dependent on your logging level). When set to true, the following addition logs will be produced:<br />- Log the stack trace of the exception (if any) produced by the query|
|**Type**|Boolean|
|**Default**|`false`|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Since**|0.22.0|

### `enableParallelMerge`

| | |
|---|---|
|**Description**|Enable parallel result merging on the Broker. Note that `druid.processing.merge.useParallelMergePool` must be enabled for this setting to be set to `true`. See [Broker configuration](../configuration/index.md#broker) for more details.|
|**Type**|Boolean|
|**Default**|`true`|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Since**|0.17.0|

### `finalize`

| | |
|---|---|
|**Description**|Flag indicating whether to "finalize" aggregation results. Primarily used for debugging. For instance, the `hyperUnique` aggregator returns the full HyperLogLog sketch instead of the estimated cardinality when this flag is set to `false`.|
|**Type**|Boolean|
|**Default**|N/A|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Since**|0.1.0|

### `forceProjections`

| | |
|---|---|
|**Description**|If `true`, the query engine must use a [projection](../querying/projections.md), and the query fails if no projection matches it. If `false`, Druid uses a matching projection when one exists and otherwise processes the query as usual.|
|**Type**|Boolean|
|**Default**|`false`|
|**Query**|JSON, SQL|
|**Engine**|Native, MSQ, Dart|
|**Since**|32.0.0|

### `lane`

| | |
|---|---|
|**Description**|Query lane, used to control usage limits on classes of queries. See [Broker configuration](../configuration/index.md#broker) for more details.|
|**Type**|String|
|**Default**|N/A|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Since**|0.18.0|

### `maxQueuedBytes`

| | |
|---|---|
|**Description**|Maximum number of bytes queued per query before exerting backpressure on the channel to the data server. Similar to `maxScatterGatherBytes`, except unlike that configuration, this one will trigger backpressure rather than query failure. Zero means disabled.|
|**Type**|Long|
|**Default**|`druid.broker.http.maxQueuedBytes`|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Since**|0.13.0-incubating|

### `maxScatterGatherBytes`

| | |
|---|---|
|**Description**|Maximum number of bytes gathered from data processes such as Historicals and realtime processes to execute a query. This parameter can be used to further reduce `maxScatterGatherBytes` limit at query time. See [Broker configuration](../configuration/index.md#broker) for more details.|
|**Type**|Long|
|**Default**|`druid.server.http.maxScatterGatherBytes`|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Since**|0.10.1|

### `maxSubqueryBytes`

| | |
|---|---|
|**Description**|Upper limit on the number of bytes a subquery can generate. Accepts `auto`, `disabled`, or a non-negative number of bytes. See [Broker configuration](../configuration/index.md#broker) and [subquery guardrails](../configuration/index.md#guardrails-for-materialization-of-subqueries) for more details.|
|**Type**|String|
|**Default**|`druid.server.http.maxSubqueryBytes`|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Since**|27.0.0|

### `maxSubqueryRows`

| | |
|---|---|
|**Description**|Upper limit on the number of rows a subquery can generate. See [Broker configuration](../configuration/index.md#broker) and [subquery guardrails](../configuration/index.md#guardrails-for-materialization-of-subqueries) for more details.|
|**Type**|Integer|
|**Default**|`druid.server.http.maxSubqueryRows`|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Since**|0.18.0|

### `noProjections`

| | |
|---|---|
|**Description**|If `true`, the query engine doesn't use any [projections](../querying/projections.md).|
|**Type**|Boolean|
|**Default**|`false`|
|**Query**|JSON, SQL|
|**Engine**|Native, MSQ, Dart|
|**Since**|32.0.0|

### `numRetriesOnMissingSegments`

| | |
|---|---|
|**Description**|Maximum number of times Druid retries a query for segments that a data server reports as missing. For native queries, this overrides `druid.broker.retryPolicy.numTries`.|
|**Type**|Integer|
|**Default**|`druid.broker.retryPolicy.numTries` for native queries, `3` for MSQ tasks|
|**Query**|JSON, SQL|
|**Engine**|Native, MSQ|
|**Since**|0.20.0|

### `parallelMergeInitialYieldRows`

| | |
|---|---|
|**Description**|Number of rows to yield per ForkJoinPool merge task for parallel result merging on the Broker, before forking off a new task to continue merging sequences. See [Broker configuration](../configuration/index.md#broker) for more details.|
|**Type**|Integer|
|**Default**|`druid.processing.merge.initialYieldNumRows`|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Since**|0.17.0|

### `parallelMergeParallelism`

| | |
|---|---|
|**Description**|Maximum number of parallel threads to use for parallel result merging on the Broker. See [Broker configuration](../configuration/index.md#broker) for more details.|
|**Type**|Integer|
|**Default**|`druid.processing.merge.parallelism`|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Since**|0.17.0|

### `parallelMergeSmallBatchRows`

| | |
|---|---|
|**Description**|Size of result batches to operate on in ForkJoinPool merge tasks for parallel result merging on the Broker. See [Broker configuration](../configuration/index.md#broker) for more details.|
|**Type**|Integer|
|**Default**|`druid.processing.merge.smallBatchNumRows`|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Since**|0.17.0|

### `perSegmentTimeout`

| | |
|---|---|
|**Description**|Per-segment processing timeout in millis, beyond which unfinished queries will be cancelled. Should be ≤ `timeout`. 0 `perSegmentTimeout` means `no per-segment timeout`. Generally, a standard default should be O(X seconds). A cluster-wide default value for this query context can be specified via `druid.query.default.context.perSegmentTimeout`.|
|**Type**|Long|
|**Default**|N/A|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Since**|35.0.0|

### `populateCache`

| | |
|---|---|
|**Description**|Flag indicating whether to save the results of the query to the query cache. Primarily used for debugging. When set to false, it disables saving the results of this query to the query cache. When set to true, Druid uses `druid.broker.cache.populateCache` or `druid.historical.cache.populateCache` to determine whether or not to save the results of this query to the query cache.|
|**Type**|Boolean|
|**Default**|`true`|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Since**|0.1.0|

### `populateResultLevelCache`

| | |
|---|---|
|**Description**|Flag indicating whether to save the results of the query to the result level cache. Primarily used for debugging. When set to false, it disables saving the results of this query to the query cache. When set to true, Druid uses `druid.broker.cache.populateResultLevelCache` to determine whether or not to save the results of this query to the result-level query cache.|
|**Type**|Boolean|
|**Default**|`true`|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Since**|0.13.0-incubating|

### `priority`

| | |
|---|---|
|**Description**|Query priority. Queries with higher priority get precedence for computational resources. A cluster-wide default value for this query context can be specified via `druid.query.default.context.priority`.|
|**Type**|Integer|
|**Default**|`0`|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Since**|0.5.0|

### `queryId`

| | |
|---|---|
|**Description**|Unique identifier given to this query. If a query ID is set or known, this can be used to cancel the query.|
|**Type**|String|
|**Default**|auto-generated|
|**Query**|JSON, SQL|
|**Engine**|Native, MSQ, Dart|
|**Since**|0.6.62|

### `realtimeSegmentsMode`

| | |
|---|---|
|**Description**|Controls whether realtime segments are queried. `include` queries all segments, including realtime. `exclude` skips realtime segments. `exclusive` queries only realtime segments.|
|**Type**|Enum (`include`, `exclusive`, `exclude`)|
|**Default**|`include`|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Since**|38.0.0|

### `realtimeSegmentsOnly`

| | |
|---|---|
|**Description**|**Deprecated.** Use `realtimeSegmentsMode=exclusive` instead. When set to `true`, this is equivalent to `realtimeSegmentsMode=exclusive`. When set to `false`, this is equivalent to `realtimeSegmentsMode=include`.|
|**Type**|Boolean|
|**Default**|`false`|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Since**|35.0.0|

### `returnPartialResults`

| | |
|---|---|
|**Description**|If true, the Broker returns partial results when segments are still missing after all retries. If false, the query fails instead.|
|**Type**|Boolean|
|**Default**|`false`|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Since**|0.20.0|

### `secondaryPartitionPruning`

| | |
|---|---|
|**Description**|Enable secondary partition pruning on the Broker. The Broker will always prune unnecessary segments from the input scan based on a filter on time intervals, but if the data is further partitioned with hash or range partitioning, this option will enable additional pruning based on a filter on secondary partition dimensions.|
|**Type**|Boolean|
|**Default**|`true`|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Since**|0.20.0|

### `serializeDateTimeAsLong`

| | |
|---|---|
|**Description**|If true, DateTime is serialized as long in the result returned by Broker and the data transportation between Broker and compute process|
|**Type**|Boolean|
|**Default**|`false`|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Since**|0.10.1|

### `serializeDateTimeAsLongInner`

| | |
|---|---|
|**Description**|If true, DateTime is serialized as long in the data transportation between Broker and compute process|
|**Type**|Boolean|
|**Default**|`false`|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Since**|0.10.1|

### `setProcessingThreadNames`

| | |
|---|---|
|**Description**|Flag indicating whether processing thread names will be set to `processing_<queryId>` while processing a query. Thread renaming aids in interpreting thread dumps, but has measurable thread renaming overhead when segment scans are very quick.|
|**Type**|Boolean|
|**Default**|`false`|
|**Query**|JSON|
|**Engine**|Native|
|**Since**|24.0.0|

### `timeout`

| | |
|---|---|
|**Description**|Query timeout in millis, beyond which unfinished queries will be cancelled. 0 timeout means `no timeout` (up to the server-side maximum query timeout, `druid.server.http.maxQueryTimeout`). To set the default timeout and maximum timeout, see [Broker configuration](../configuration/index.md#broker).|
|**Type**|Long|
|**Default**|`druid.server.http.defaultQueryTimeout`|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Since**|0.6.122|

### `uncoveredIntervalsLimit`

| | |
|---|---|
|**Description**|Maximum number of query intervals not covered by any segment that the Broker reports in the `uncoveredIntervals` entry of the response context. If more intervals are uncovered, the Broker also sets `uncoveredIntervalsOverflowed` to true. A value of 0 disables the check.|
|**Type**|Integer|
|**Default**|`0`|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Since**|0.9.0|

### `useCache`

| | |
|---|---|
|**Description**|Flag indicating whether to leverage the query cache for this query. When set to false, it disables reading from the query cache for this query. When set to true, Apache Druid uses `druid.broker.cache.useCache` or `druid.historical.cache.useCache` to determine whether or not to read from the query cache.|
|**Type**|Boolean|
|**Default**|`true`|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Since**|0.1.0|

### `useFilterCNF`

| | |
|---|---|
|**Description**|If true, Druid will attempt to convert the query filter to Conjunctive Normal Form (CNF). During query processing, columns can be pre-filtered by intersecting the bitmap indexes of all values that match the eligible filters, often greatly reducing the raw number of rows which need to be scanned. But this effect only happens for the top level filter, or individual clauses of a top level 'and' filter. As such, filters in CNF potentially have a higher chance to utilize a large amount of bitmap indexes on string columns during pre-filtering. However, this setting should be used with great caution, as it can sometimes have a negative effect on performance, and in some cases, the act of computing CNF of a filter can be expensive. We recommend hand tuning your filters to produce an optimal form if possible, or at least verifying through experimentation that using this parameter actually improves your query performance with no ill-effects.|
|**Type**|Boolean|
|**Default**|`false`|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Since**|0.19.0|

### `useNestedForUnknownTypeInSubquery`

| | |
|---|---|
|**Description**|If true, when the Broker materializes subquery results as frames to enforce `maxSubqueryBytes`, it serializes columns of unknown type as nested JSON instead of failing to materialize them.|
|**Type**|Boolean|
|**Default**|`druid.server.http.useNestedForUnknownTypeInSubquery`|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Since**|27.0.0|

### `useProjection`

| | |
|---|---|
|**Description**|Name of a [projection](../querying/projections.md) that the query engine must use. If the projection doesn't match the query, the query fails.|
|**Type**|String|
|**Default**|N/A|
|**Query**|JSON, SQL|
|**Engine**|Native, MSQ, Dart|
|**Since**|32.0.0|

### `useResultLevelCache`

| | |
|---|---|
|**Description**|Flag indicating whether to leverage the result level cache for this query. When set to false, it disables reading from the query cache for this query. When set to true, Druid uses `druid.broker.cache.useResultLevelCache` to determine whether or not to read from the result-level query cache.|
|**Type**|Boolean|
|**Default**|`true`|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Since**|0.13.0-incubating|

### `vectorize`

| | |
|---|---|
|**Description**|Enables or disables vectorized query execution. Possible values are `false` (disabled), `true` (enabled if possible, disabled otherwise, on a per-segment basis), and `force` (enabled, and query types that support vectorization will fail if they cannot be vectorized). The `"force"` setting is meant to aid in testing, and is not generally useful in production (since real-time segments can never be processed with vectorized execution, any queries on real-time data will fail). This will override `druid.query.default.context.vectorize` if it's set. See [Vectorization](../querying/vectorization.md) for the requirements a query must meet to run vectorized.|
|**Type**|Enum (`false`, `true`, `force`)|
|**Default**|`true`|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Since**|0.16.0-incubating|

### `vectorizeVirtualColumns`

| | |
|---|---|
|**Description**|Enables or disables vectorized query processing of queries with virtual columns, layered on top of `vectorize` (`vectorize` must also be set to true for a query to utilize vectorization). Possible values are `false` (disabled), `true` (enabled if possible, disabled otherwise, on a per-segment basis), and `force` (enabled, and groupBy or timeseries queries with virtual columns that cannot be vectorized will fail). The `"force"` setting is meant to aid in testing, and is not generally useful in production. This will override `druid.query.default.context.vectorizeVirtualColumns` if it's set.|
|**Type**|Enum (`false`, `true`, `force`)|
|**Default**|`true`|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Since**|0.20.0|

### `vectorSize`

| | |
|---|---|
|**Description**|Sets the row batching size for a particular query. This will override `druid.query.default.context.vectorSize` if it's set.|
|**Type**|Integer|
|**Default**|`512`|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Since**|0.16.0-incubating|

<!-- END GENERATED QUERY CONTEXT PARAMETERS -->

<a id="join-filter"></a>

## Join filter parameters

The following parameters control how Druid rewrites and pushes down filters in queries on [join datasources](datasource.md#join).

<!-- BEGIN GENERATED QUERY CONTEXT PARAMETERS: names=enableJoinFilterPushDown,enableJoinFilterRewrite,enableJoinFilterRewriteValueColumnFilters,enableRewriteJoinToFilter,joinFilterRewriteMaxSize -->

### `enableJoinFilterPushDown`

| | |
|---|---|
|**Description**|Controls whether a join query will attempt filter push down, which reduces the number of rows that have to be compared in a join operation.|
|**Type**|Boolean|
|**Default**|`true`|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Since**|0.18.0|

### `enableJoinFilterRewrite`

| | |
|---|---|
|**Description**|Controls whether filter clauses that reference non-base table columns will be rewritten into filters on base table columns.|
|**Type**|Boolean|
|**Default**|`true`|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Since**|0.18.0|

### `enableJoinFilterRewriteValueColumnFilters`

| | |
|---|---|
|**Description**|Controls whether Druid rewrites non-base table filters on non-key columns in the non-base table. Requires a scan of the non-base table.|
|**Type**|Boolean|
|**Default**|`false`|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Since**|0.18.0|

### `enableRewriteJoinToFilter`

| | |
|---|---|
|**Description**|Controls whether a join can be pushed partial or fully to the base table as a filter at runtime.|
|**Type**|Boolean|
|**Default**|`true`|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Since**|0.22.0|

### `joinFilterRewriteMaxSize`

| | |
|---|---|
|**Description**|The maximum size of the correlated value set used for filter rewrites. Set this limit to prevent excessive memory use.|
|**Type**|Long|
|**Default**|`10000`|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Since**|0.18.0|

<!-- END GENERATED QUERY CONTEXT PARAMETERS -->

## Druid SQL parameters

The following parameters apply to SQL queries only. For how to set them, see [SQL query context](sql-query-context.md).

<!-- BEGIN GENERATED QUERY CONTEXT PARAMETERS: queries=SQL engines~NATIVE queryTypes= -->

### `enableJoinLeftTableScanDirect`

| | |
|---|---|
|**Description**|This parameter applies to queries with joins. By default, when the left child is a simple scan with a filter, Druid runs the scan as a query, then joins it with the right child on the Broker. Setting this parameter to `true` overrides that behavior and pushes the join to the data servers instead. Even if a query doesn't explicitly include a join, this parameter may still apply since the SQL planner can translate the query into a join internally.|
|**Type**|Boolean|
|**Default**|`false`|
|**Query**|SQL|
|**Engine**|Native, MSQ, Dart|
|**Since**|0.22.0|

### `enableTimeBoundaryPlanning`

| | |
|---|---|
|**Description**|If `true`, Druid converts SQL queries to [time boundary queries](../querying/timeboundaryquery.md) wherever possible. Time boundary queries are very efficient for min-max calculation on the `__time` column in a datasource.|
|**Type**|Boolean|
|**Default**|`false`|
|**Query**|SQL|
|**Engine**|Native|
|**Since**|0.24.0|

### `engine`

| | |
|---|---|
|**Description**|SQL engine that runs the query through the `/druid/v2/sql` endpoint. Set to `msq-dart` to run the query with [Dart](../querying/dart.md). Druid ignores this parameter for JDBC connections.|
|**Type**|String|
|**Default**|`native`|
|**Query**|SQL|
|**Engine**|Native, Dart|
|**Since**|34.0.0|

### `inFunctionExprThreshold`

| | |
|---|---|
|**Description**|At or beyond this threshold number of values, SQL `IN` is eligible for execution using the native function `scalar_in_array` rather than an <code>&#124;&#124;</code> of `==`, even if the number of values is below `inFunctionThreshold`. This property only affects translation of SQL `IN` to a [native expression](../querying/math-expr.md). It doesn't affect translation of SQL `IN` to a [native filter](../querying/filters.md). This property is provided for backwards compatibility purposes, and may be removed in a future release.|
|**Type**|Integer|
|**Default**|`2`|
|**Query**|SQL|
|**Engine**|Native, MSQ, Dart|
|**Since**|31.0.0|

### `inFunctionThreshold`

| | |
|---|---|
|**Description**|At or beyond this threshold number of values, Druid converts SQL `IN` to [`SCALAR_IN_ARRAY`](../querying/sql-functions.md#scalar_in_array). A threshold of 0 forces this conversion in all cases. A threshold of `Integer.MAX_VALUE` disables this conversion. The converted function is eligible for fewer planning-time optimizations, which speeds up planning, but may prevent certain planning-time optimizations.|
|**Type**|Integer|
|**Default**|`100`|
|**Query**|SQL|
|**Engine**|Native, MSQ, Dart|
|**Since**|31.0.0|

### `inSubQueryThreshold`

| | |
|---|---|
|**Description**|At or beyond this threshold number of values, Druid converts SQL `IN` to `JOIN` on an inline table. The `inFunctionThreshold` setting takes priority over this one. A threshold of 0 forces usage of an inline table in all cases where the size of a SQL `IN` is larger than `inFunctionThreshold`. A threshold of `2147483647` disables the rewrite of SQL `IN` to `JOIN`.|
|**Type**|Integer|
|**Default**|`2147483647`|
|**Query**|SQL|
|**Engine**|Native, MSQ, Dart|
|**Since**|0.23.0|

### `maxNumericInFilters`

| | |
|---|---|
|**Description**|Max limit for the amount of numeric values that Druid can compare for a string type dimension when the entire SQL WHERE clause of a query translates only to an [OR](../querying/filters.md#or) of [bound filter](../querying/filters.md#bound-filter). By default, Druid doesn't restrict the amount of numeric bound filters on string columns, although this situation may block other queries from running. Set this parameter to a smaller value to prevent Druid from running queries that have prohibitively long segment processing times. The optimal limit requires some trial and error. We recommend starting with 100. Users who submit a query that exceeds the limit of `maxNumericInFilters` should rewrite their queries to use strings in the `WHERE` clause instead of numbers. For example, `WHERE someString IN (‘123’, ‘456’)`. This value can't exceed the set system configuration `druid.sql.planner.maxNumericInFilters`. If `druid.sql.planner.maxNumericInFilters` isn't set explicitly, Druid ignores this value.|
|**Type**|Integer|
|**Default**|`druid.sql.planner.maxNumericInFilters`|
|**Query**|SQL|
|**Engine**|Native, MSQ, Dart|
|**Since**|0.23.0|

### `maxPlanningTimeMs`

| | |
|---|---|
|**Description**|Maximum wall-clock time, in milliseconds, allowed for planning this query on the Broker. When planning exceeds this budget, planning is aborted and the query fails with an HTTP 504 `Query timeout` error. Use this to guard against pathological queries, such as one with a very large `IN` filter, whose planning time can spike. A non-positive value disables the timeout. To apply a cluster-wide default, set the `druid.query.default.context.maxPlanningTimeMs` runtime property (or the Broker's dynamic `queryContext` config); see [Overriding default query context values](../configuration/index.md#overriding-default-query-context-values).|
|**Type**|Long|
|**Default**|`0`|
|**Query**|SQL|
|**Engine**|Native, MSQ, Dart|
|**Since**|39.0.0|

### `sqlFinalizeOuterSketches`

| | |
|---|---|
|**Description**|If `false` (default behavior in Druid 25.0.0 and later), `DS_HLL`, `DS_THETA`, and `DS_QUANTILES_SKETCH` return sketches in query results. If `true` (default behavior in Druid 24.0.1 and earlier), Druid finalizes sketches from these functions when they appear in query results.<br /><br />This property is provided for backwards compatibility with behavior in Druid 24.0.1 and earlier. We don't recommend setting this parameter unless your application uses Druid 24.0.1 or earlier. Instead, use a function that doesn't return a sketch, such as `APPROX_COUNT_DISTINCT_DS_HLL`, `APPROX_COUNT_DISTINCT_DS_THETA`, `APPROX_QUANTILE_DS`, `DS_THETA_ESTIMATE`, or `DS_GET_QUANTILE`.|
|**Type**|Boolean|
|**Default**|`false`|
|**Query**|SQL|
|**Engine**|Native, MSQ, Dart|
|**Since**|25.0.0|

### `sqlPlannerBloat`

| | |
|---|---|
|**Description**|Calcite parameter which controls whether to merge two Project operators when inlining expressions causes complexity to increase. Implemented as a workaround to exception `There are not enough rules to produce a node with desired properties: convention=DRUID, sort=[]` thrown after rejecting the merge of two projects.|
|**Type**|Integer|
|**Default**|`1000`|
|**Query**|SQL|
|**Engine**|Native, MSQ, Dart|
|**Since**|31.0.1|

### `sqlPullUpLookup`

| | |
|---|---|
|**Description**|Whether to consider the [pull-up rewrite](../querying/lookups.md#pull-up) of the `LOOKUP` function during SQL planning.|
|**Type**|Boolean|
|**Default**|`true`|
|**Query**|SQL|
|**Engine**|Native, MSQ, Dart|
|**Since**|30.0.1|

### `sqlQueryId`

| | |
|---|---|
|**Description**|SQL query ID. For HTTP client, Druid returns it in the `X-Druid-SQL-Query-Id` header.<br/><br/>To specify a SQL query ID, use `sqlQueryId` instead of [`queryId`](../querying/query-context-reference.md). Setting `queryId` for a SQL request has no effect. All native queries underlying SQL use an auto-generated `queryId`.|
|**Type**|String|
|**Default**|auto-generated|
|**Query**|SQL|
|**Engine**|Native, MSQ, Dart|
|**Since**|0.14.0-incubating|

### `sqlReverseLookup`

| | |
|---|---|
|**Description**|Whether to consider the [reverse-lookup rewrite](../querying/lookups.md#reverse-lookup) of the `LOOKUP` function during SQL planning.<br /><br />Druid reverses calls to `LOOKUP` only when the number of matching keys is lower than both `inSubQueryThreshold` and `sqlReverseLookupThreshold`.|
|**Type**|Boolean|
|**Default**|`true`|
|**Query**|SQL|
|**Engine**|Native, MSQ, Dart|
|**Since**|30.0.1|

### `sqlReverseLookupThreshold`

| | |
|---|---|
|**Description**|Maximum size of `IN` filter to create when applying a [reverse-lookup rewrite](../querying/lookups.md#reverse-lookup). If a `LOOKUP` call matches more keys than the specified threshold, it remains unchanged.<br /><br />If `inSubQueryThreshold` is lower than `sqlReverseLookupThreshold`, Druid uses `inSubQueryThreshold` threshold instead.|
|**Type**|Integer|
|**Default**|`10000`|
|**Query**|SQL|
|**Engine**|Native, MSQ, Dart|
|**Since**|30.0.1|

### `sqlStringifyArrays`

| | |
|---|---|
|**Description**|If `true`, Druid serializes result columns with array values as JSON strings in the response instead of arrays.|
|**Type**|Boolean|
|**Default**|`true`, except for JDBC connections, where it's always `false`|
|**Query**|SQL|
|**Engine**|Native, MSQ, Dart|
|**Since**|0.22.0|

### `sqlTimeCondition`

| | |
|---|---|
|**Description**|Time condition to apply to all tables that the query reads, except global tables on the right side of a JOIN. Comma-separated string of ISO8601 intervals, such as `"2000-01-01/2000-02-01,2001-01-01/2001-02-01"`. An empty string is an error.<br /><br />Druid ANDs this condition with any other condition you provide on the `__time` column. Intervals without an explicit offset are in UTC; `sqlTimeZone` doesn't affect them.<br /><br />You can't use this parameter with `INSERT`, `REPLACE`, or [catalog DDL](../development/extensions-core/catalog.md#sql-ddl) statements.|
|**Type**|String|
|**Default**|none|
|**Query**|SQL|
|**Engine**|Native, MSQ, Dart|
|**Statement**|SELECT|
|**Since**|40.0.0|

### `sqlTimeZone`

| | |
|---|---|
|**Description**|Time zone for a connection. For example, `America/Los_Angeles` or an offset like `-08:00`. This parameter affects how time functions and timestamp literals behave.|
|**Type**|String|
|**Default**|`druid.sql.planner.sqlTimeZone`|
|**Query**|SQL|
|**Engine**|Native, MSQ, Dart|
|**Since**|0.10.0|

### `sqlUseBoundAndSelectors`

| | |
|---|---|
|**Description**|If `false` (default behavior in Druid 27.0.0 and later), the SQL planner uses [equality](../querying/filters.md#equality-filter), [null](../querying/filters.md#null-filter), and [range](../querying/filters.md#range-filter) filters instead of [selector](../querying/filters.md#selector-filter) and [bounds](../querying/filters.md#bound-filter). For filtering `ARRAY` typed values, `sqlUseBoundAndSelectors` must be `false`.|
|**Type**|Boolean|
|**Default**|`false`|
|**Query**|SQL|
|**Engine**|Native, MSQ, Dart|
|**Since**|30.0.1|

### `sqlUseExtractionFns`

| | |
|---|---|
|**Description**|If false, the SQL planner avoids using [`extractionFn`](../querying/dimensionspecs.md#extraction-functions) in favor of using other constructs such as [virtual columns](../querying/virtual-columns.md). This parameter is provided for compatibility with prior behavior, and may be removed in a future release.|
|**Type**|Boolean|
|**Default**|`false`|
|**Query**|SQL|
|**Engine**|Native, MSQ, Dart|
|**Since**|35.0.1|

### `useApproximateCountDistinct`

| | |
|---|---|
|**Description**|Whether to use an approximate cardinality algorithm for `COUNT(DISTINCT foo)`.|
|**Type**|Boolean|
|**Default**|`druid.sql.planner.useApproximateCountDistinct`|
|**Query**|SQL|
|**Engine**|Native, MSQ, Dart|
|**Since**|0.10.0|

### `useApproximateTopN`

| | |
|---|---|
|**Description**|If `true`, Druid converts SQL queries to approximate [TopN queries](../querying/topnquery.md) wherever possible. If `false`, Druid uses exact [GroupBy queries](../querying/groupbyquery.md) instead.|
|**Type**|Boolean|
|**Default**|`druid.sql.planner.useApproximateTopN`|
|**Query**|SQL|
|**Engine**|Native|
|**Since**|0.10.0|

### `useGroupingSetForExactDistinct`

| | |
|---|---|
|**Description**|Whether to use grouping sets to execute queries with multiple exact distinct aggregations.|
|**Type**|Boolean|
|**Default**|`druid.sql.planner.useGroupingSetForExactDistinct`|
|**Query**|SQL|
|**Engine**|Native|
|**Since**|0.22.0|

### `useLexicographicTopN`

| | |
|---|---|
|**Description**|If `true`, Druid can use [TopN queries](../querying/topnquery.md) with lexicographic dimension ordering. If `false`, Druid uses [GroupBy queries](../querying/groupbyquery.md) instead for lexicographic ordering. When both `useLexicographicTopN` and `useApproximateTopN` are `false`, TopN queries are never used.|
|**Type**|Boolean|
|**Default**|`druid.sql.planner.useLexicographicTopN`|
|**Query**|SQL|
|**Engine**|Native|
|**Since**|35.0.1|

### `useNativeQueryExplain`

| | |
|---|---|
|**Description**|If `true`, `EXPLAIN PLAN FOR` returns the explain plan as a JSON representation of equivalent native query, else it returns the original version of explain plan generated by Calcite.<br /><br />This property is provided for backwards compatibility. We don't recommend setting this parameter unless your application depends on the older behavior.|
|**Type**|Boolean|
|**Default**|`druid.sql.planner.useNativeQueryExplain`|
|**Query**|SQL|
|**Engine**|Native, MSQ, Dart|
|**Since**|0.23.0|

<!-- END GENERATED QUERY CONTEXT PARAMETERS -->

<a id="parameters-by-query-type"></a>
<a id="topn"></a>

## TopN parameters

The following parameters apply to [TopN queries](topnquery.md).

<!-- BEGIN GENERATED QUERY CONTEXT PARAMETERS: queryTypes=TOP_N -->

### `minTopNThreshold`

| | |
|---|---|
|**Description**|The top minTopNThreshold local results from each segment are returned for merging to determine the global topN.|
|**Type**|Integer|
|**Default**|`1000`|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Query type**|topN|
|**Since**|0.9.0|

<!-- END GENERATED QUERY CONTEXT PARAMETERS -->

<a id="timeseries"></a>

## Timeseries parameters

The following parameters apply to [Timeseries queries](timeseriesquery.md).

<!-- BEGIN GENERATED QUERY CONTEXT PARAMETERS: queryTypes=TIMESERIES -->

### `grandTotal`

| | |
|---|---|
|**Description**|If true, a timeseries query returns an additional row with the totals of all aggregations as its last row.|
|**Type**|Boolean|
|**Default**|`false`|
|**Query**|JSON|
|**Engine**|Native|
|**Query type**|timeseries|
|**Since**|0.13.0-incubating|

### `skipEmptyBuckets`

| | |
|---|---|
|**Description**|Disable timeseries zero-filling behavior, so only buckets with results will be returned.|
|**Type**|Boolean|
|**Default**|`false`|
|**Query**|JSON|
|**Engine**|Native|
|**Query type**|timeseries|
|**Since**|0.7.0|

<!-- END GENERATED QUERY CONTEXT PARAMETERS -->

## Scan parameters

The following parameters apply to [Scan queries](scan-query.md).

<!-- BEGIN GENERATED QUERY CONTEXT PARAMETERS: queryTypes=SCAN -->

### `maxRowsQueuedForOrdering`

| | |
|---|---|
|**Description**|The maximum number of rows returned when time ordering is used. Overrides the identically named config.|
|**Type**|Integer in [1, 2147483647]|
|**Default**|`druid.query.scan.maxRowsQueuedForOrdering`|
|**Query**|JSON|
|**Engine**|Native|
|**Query type**|scan|
|**Since**|0.15.0-incubating|

### `maxSegmentPartitionsOrderedInMemory`

| | |
|---|---|
|**Description**|The maximum number of segments scanned per historical when time ordering is used. Overrides the identically named config.|
|**Type**|Integer in [1, 2147483647]|
|**Default**|`druid.query.scan.maxSegmentPartitionsOrderedInMemory`|
|**Query**|JSON|
|**Engine**|Native|
|**Query type**|scan|
|**Since**|0.15.0-incubating|

<!-- END GENERATED QUERY CONTEXT PARAMETERS -->

## Search parameters

The following parameters apply to [Search queries](searchquery.md).

<!-- BEGIN GENERATED QUERY CONTEXT PARAMETERS: queryTypes=SEARCH -->

### `searchStrategy`

| | |
|---|---|
|**Description**|Overrides the value of `druid.query.search.searchStrategy` for this query.|
|**Type**|String|
|**Default**|`druid.query.search.searchStrategy`|
|**Query**|JSON|
|**Engine**|Native|
|**Query type**|search|
|**Since**|0.10.0|

<!-- END GENERATED QUERY CONTEXT PARAMETERS -->

<a id="groupby"></a>

## GroupBy parameters

The following parameters apply to [GroupBy queries](groupbyquery.md).

<!-- BEGIN GENERATED QUERY CONTEXT PARAMETERS: queryTypes=GROUP_BY -->

### `applyLimitPushDownToSegment`

| | |
|---|---|
|**Description**|If Broker pushes limit down to queryable nodes (historicals, peons) then limit results during segment scan. This context value can be used to override `druid.query.groupBy.applyLimitPushDownToSegment`.|
|**Type**|Boolean|
|**Default**|`druid.query.groupBy.applyLimitPushDownToSegment`|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Query type**|groupBy|
|**Since**|0.17.0|

### `bufferGrouperInitialBuckets`

| | |
|---|---|
|**Description**|Overrides the value of `druid.query.groupBy.bufferGrouperInitialBuckets` for this query.|
|**Type**|Integer|
|**Default**|`druid.query.groupBy.bufferGrouperInitialBuckets`|
|**Query**|JSON, SQL|
|**Engine**|Native, MSQ, Dart|
|**Query type**|groupBy|
|**Since**|0.9.2|

### `bufferGrouperMaxLoadFactor`

| | |
|---|---|
|**Description**|Overrides the value of `druid.query.groupBy.bufferGrouperMaxLoadFactor` for this query.|
|**Type**|Float|
|**Default**|`druid.query.groupBy.bufferGrouperMaxLoadFactor`|
|**Query**|JSON, SQL|
|**Engine**|Native, MSQ, Dart|
|**Query type**|groupBy|
|**Since**|0.9.2|

### `deferExpressionDimensions`

| | |
|---|---|
|**Description**|When an entry in `dimensions` references an `expression` virtual column, this property influences whether expression evaluation is deferred from cursor processing to the merge step. Options are:<ul><li>`fixedWidth`: Defer expressions with fixed-width inputs (numeric and dictionary-encoded string).</li><li>`fixedWidthNonNumeric`: Defer expressions with fixed-width inputs (numeric and dictionary-encoded string), unless the expression output and all inputs are numeric.</li><li>`singleString`: Defer string-typed expressions with a single dictionary-encoded string input.</li><li>`always`: Defer all expressions. May require building dictionaries for expression inputs.</li></ul><br />These properties only take effect when the `groupBy` query can be vectorized. Non-vectorized queries only defer string-typed expressions of single string inputs.|
|**Type**|Enum (`singleString`, `fixedWidth`, `fixedWidthNonNumeric`, `always`)|
|**Default**|`druid.query.groupBy.deferExpressionDimensions`|
|**Query**|JSON, SQL|
|**Engine**|Native, MSQ, Dart|
|**Query type**|groupBy|
|**Since**|31.0.0|

### `forceHashAggregation`

| | |
|---|---|
|**Description**|Overrides the value of `druid.query.groupBy.forceHashAggregation`.|
|**Type**|Boolean|
|**Default**|`druid.query.groupBy.forceHashAggregation`|
|**Query**|JSON, SQL|
|**Engine**|Native, MSQ, Dart|
|**Query type**|groupBy|
|**Since**|0.11.0|

### `forceLimitPushDown`

| | |
|---|---|
|**Description**|When all fields in the orderby are part of the grouping key, the Broker will push limit application down to the Historical processes. When the sorting order uses fields that are not in the grouping key, applying this optimization can result in approximate results with unknown accuracy, so this optimization is disabled by default in that case. Enabling this context flag turns on limit push down for limit/orderbys that contain non-grouping key columns.|
|**Type**|Boolean|
|**Default**|`druid.query.groupBy.forcePushDownLimit`|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Query type**|groupBy|
|**Since**|0.10.1|

### `groupByEnableMultiValueUnnesting`

| | |
|---|---|
|**Description**|Safety flag to enable/disable the implicit unnesting on multi value column's as part of the grouping key. If `true`, multi-value grouping keys are unnested. If `false`, Druid returns an error if a multi value column is found as part of the grouping key.|
|**Type**|Boolean|
|**Default**|`druid.query.groupBy.enableMultiValueUnnesting`|
|**Query**|JSON, SQL|
|**Engine**|Native, MSQ, Dart|
|**Query type**|groupBy|
|**Since**|0.23.0|

### `groupByIsSingleThreaded`

| | |
|---|---|
|**Description**|Overrides the value of `druid.query.groupBy.singleThreaded` for this query.|
|**Type**|Boolean|
|**Default**|`druid.query.groupBy.singleThreaded`|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Query type**|groupBy|
|**Since**|0.9.1|

### `intermediateCombineDegree`

| | |
|---|---|
|**Description**|Overrides the value of `druid.query.groupBy.intermediateCombineDegree`.|
|**Type**|Integer|
|**Default**|`druid.query.groupBy.intermediateCombineDegree`|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Query type**|groupBy|
|**Since**|0.12.0|

### `maxMergingDictionarySize`

| | |
|---|---|
|**Description**|Overrides the value of `druid.query.groupBy.maxMergingDictionarySize`.|
|**Type**|Number, or a [human-readable](../configuration/human-readable-byte.md) value such as `10K` or `256MiB`|
|**Default**|`druid.query.groupBy.maxMergingDictionarySize`|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Query type**|groupBy|
|**Since**|0.9.2|

### `maxOnDiskStorage`

| | |
|---|---|
|**Description**|Can be used to lower the value of `druid.query.groupBy.maxOnDiskStorage` for this query.|
|**Type**|Number, or a [human-readable](../configuration/human-readable-byte.md) value such as `10K` or `256MiB`|
|**Default**|`druid.query.groupBy.defaultOnDiskStorage`|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Query type**|groupBy|
|**Since**|0.9.2|

### `maxSelectorDictionarySize`

| | |
|---|---|
|**Description**|Overrides the value of `druid.query.groupBy.maxSelectorDictionarySize`.|
|**Type**|Number, or a [human-readable](../configuration/human-readable-byte.md) value such as `10K` or `256MiB`|
|**Default**|`druid.query.groupBy.maxSelectorDictionarySize`|
|**Query**|JSON, SQL|
|**Engine**|Native, MSQ, Dart|
|**Query type**|groupBy|
|**Since**|0.23.0|

### `maxSpillFileCount`

| | |
|---|---|
|**Description**|Can be used to override the value of `druid.query.groupBy.maxSpillFileCount` for this query.|
|**Type**|Integer|
|**Default**|`druid.query.groupBy.maxSpillFileCount`|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Query type**|groupBy|
|**Since**|37.0.0|

### `mergeThreadLocal`

| | |
|---|---|
|**Description**|Whether merge buffers should always be split into thread-local buffers. Setting this to `true` reduces thread contention, but uses memory less efficiently. This tradeoff is beneficial when memory is plentiful.|
|**Type**|Boolean|
|**Default**|`druid.query.groupBy.mergeThreadLocal`|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Query type**|groupBy|
|**Since**|24.0.0|

### `minSpillFileSize`

| | |
|---|---|
|**Description**|Can be used to override the value of `druid.query.groupBy.minSpillFileSize` for this query.|
|**Type**|Number, or a [human-readable](../configuration/human-readable-byte.md) value such as `10K` or `256MiB`|
|**Default**|`druid.query.groupBy.minSpillFileSize`|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Query type**|groupBy|
|**Since**|38.0.0|

### `numParallelCombineThreads`

| | |
|---|---|
|**Description**|Overrides the value of `druid.query.groupBy.numParallelCombineThreads`.|
|**Type**|Integer|
|**Default**|`druid.query.groupBy.numParallelCombineThreads`|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Query type**|groupBy|
|**Since**|0.12.0|

### `resultAsArray`

| | |
|---|---|
|**Description**|If true, groupBy results are returned as arrays instead of maps.|
|**Type**|Boolean|
|**Default**|`false`|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Query type**|groupBy|
|**Since**|0.17.0|

### `sortByDimsFirst`

| | |
|---|---|
|**Description**|Sort the results first by dimension values and then by timestamp.|
|**Type**|Boolean|
|**Default**|`false`|
|**Query**|JSON, SQL|
|**Engine**|Native|
|**Query type**|groupBy|
|**Since**|0.10.0|

<!-- END GENERATED QUERY CONTEXT PARAMETERS -->

## SQL-based ingestion parameters

The following parameters apply to [SQL-based ingestion](../multi-stage-query/index.md) and other queries that run on the multi-stage query engine, including [Dart](dart.md) queries where noted.

<!-- BEGIN GENERATED QUERY CONTEXT PARAMETERS: queryTypes= engines~MSQ engines!~NATIVE -->

### `arrayIngestMode`

| | |
|---|---|
|**Description**|Controls how ARRAY type values are stored in Druid segments. When set to `array` (the default, recommended for SQL compliance), Druid will store all ARRAY typed values in [ARRAY typed columns](../querying/arrays.md), and supports storing both VARCHAR and numeric typed arrays. When set to `mvd` (for backwards compatibility), Druid only supports VARCHAR typed arrays, and will store them as [multi-value string columns](../querying/multi-value-dimensions.md). See [`arrayIngestMode`] in the [Arrays](../querying/arrays.md) page for more details.|
|**Type**|String|
|**Default**|`array`|
|**Query**|SQL|
|**Engine**|MSQ|
|**Statement**|INSERT, REPLACE|
|**Since**|28.0.0|

### `backgroundFetchExternalFiles`

| | |
|---|---|
|**Description**|Controls how workers read cloud storage files (e.g. `s3`, `gs`, `azure`) referenced by [`EXTERN`](../multi-stage-query/reference.md#extern-function). When `true` (the default), each worker fetches its input files asynchronously when possible before reading them. This overlaps downloading with processing, which generally improves throughput when reading larger numbers of files. Otherwise, workers stream each file directly from cloud storage while processing it.<br /><br />This setting has no effect for input sources that use `prefix`, or input sources that request [system fields](../ingestion/input-sources.md).|
|**Type**|Boolean|
|**Default**|`true`|
|**Query**|SQL|
|**Engine**|MSQ|
|**Statement**|SELECT, INSERT, REPLACE|
|**Since**|38.0.0|

### `catalogValidationEnabled`

| | |
|---|---|
|**Description**|If false, Druid skips the catalog-specific validation of `INSERT` and `REPLACE` statements that write to tables defined in the Druid catalog.|
|**Type**|Boolean|
|**Default**|`true`|
|**Query**|SQL|
|**Engine**|MSQ|
|**Statement**|INSERT, REPLACE|
|**Since**|31.0.0|

### `durableShuffleStorage`

| | |
|---|---|
|**Description**|Whether to use durable storage for shuffle mesh. To use this feature, configure the durable storage at the server level using `druid.msq.intermediate.storage.enable=true`). If these properties are not configured, any query with the context variable `durableShuffleStorage=true` fails with a configuration error.|
|**Type**|Boolean|
|**Default**|`false`|
|**Query**|SQL|
|**Engine**|MSQ|
|**Statement**|SELECT, INSERT, REPLACE|
|**Since**|24.0.0|

### `executionMode`

| | |
|---|---|
|**Description**|Determines how the [SQL statements API](../api-reference/sql-api.md#query-from-deep-storage) fetches query results. Druid currently only supports `ASYNC`, which requires you to retrieve the results after the query completes.|
|**Type**|Enum (`SYNC`, `ASYNC`)|
|**Default**|N/A|
|**Query**|SQL|
|**Engine**|MSQ|
|**Since**|27.0.0|

### `failOnEmptyInsert`

| | |
|---|---|
|**Description**|When set to false (the default), an INSERT query generating no output rows will be no-op, and a REPLACE query generating no output rows will delete all data that matches the OVERWRITE clause. When set to true, an ingest query generating no output rows will throw an `InsertCannotBeEmpty` fault.|
|**Type**|Boolean|
|**Default**|`false`|
|**Query**|SQL|
|**Engine**|MSQ|
|**Statement**|INSERT, REPLACE|
|**Since**|29.0.0|

### `faultTolerance`

| | |
|---|---|
|**Description**|Whether to turn on fault tolerance mode or not. Failed workers are retried based on [Limits](../multi-stage-query/reference.md#limits). Cannot be used when `durableShuffleStorage` is explicitly set to false.|
|**Type**|Boolean|
|**Default**|`false`|
|**Query**|SQL|
|**Engine**|MSQ|
|**Statement**|SELECT, INSERT, REPLACE|
|**Since**|26.0.0|

### `finalizeAggregations`

| | |
|---|---|
|**Description**|Determines the type of aggregation to return. If true, Druid finalizes the results of complex aggregations that directly appear in query results. If false, Druid returns the aggregation's intermediate type rather than finalized type. This parameter is useful during ingestion, where it enables storing sketches directly in Druid tables. For more information about aggregations, see [SQL aggregation functions](../querying/sql-aggregations.md).|
|**Type**|Boolean|
|**Default**|`true`|
|**Query**|SQL|
|**Engine**|MSQ, Dart|
|**Statement**|SELECT, INSERT, REPLACE|
|**Since**|24.0.0|

### `forceSegmentSortByTime`

| | |
|---|---|
|**Description**|When set to `true` (the default), Druid prepends `__time` to [CLUSTERED BY](../multi-stage-query/reference.md#clustered-by) when determining the sort order for individual segments. Druid also requires that `segmentSortOrder`, if provided, starts with `__time`.<br /><br />When set to `false`, Druid uses the [CLUSTERED BY](../multi-stage-query/reference.md#clustered-by) alone to determine the sort order for individual segments, and does not require that `segmentSortOrder` begin with `__time`. Setting this parameter to `false` is an experimental feature; see [Sorting](../ingestion/partitioning.md#sorting) for details.|
|**Type**|Boolean|
|**Default**|`true`|
|**Query**|SQL|
|**Engine**|MSQ|
|**Statement**|INSERT, REPLACE|
|**Since**|31.0.0|

### `includeAllCounters`

| | |
|---|---|
|**Description**|Whether to include counters that were added in Druid 31 or later. This is a backwards compatibility option that must be set to `false` during a rolling update from versions prior to Druid 31.|
|**Type**|Boolean|
|**Default**|`true`|
|**Query**|SQL|
|**Engine**|MSQ|
|**Statement**|SELECT, INSERT, REPLACE|
|**Since**|31.0.0|

### `includeSegmentSource`

| | |
|---|---|
|**Description**|Controls the sources Druid queries for results in addition to the segments present on deep storage. Can be `NONE` or `REALTIME`. If set to `NONE`, only non-realtime (published and used) segments are downloaded from deep storage. If set to `REALTIME`, results are also included from realtime tasks. `REALTIME` cannot be used while writing data into the same datasource it is read from.|
|**Type**|String|
|**Default**|`NONE` for the MSQ task engine, `REALTIME` for Dart|
|**Query**|SQL|
|**Engine**|MSQ, Dart|
|**Statement**|SELECT, INSERT, REPLACE|
|**Since**|28.0.0|

### `indexSpec`

| | |
|---|---|
|**Description**|An [`indexSpec`](../ingestion/ingestion-spec.md#indexspec) to use when generating segments. May be a JSON string or object. See [Front coding](../ingestion/ingestion-spec.md#front-coding) for details on configuring an `indexSpec` with front coding.|
|**Type**|Object|
|**Default**|See [`indexSpec`](../ingestion/ingestion-spec.md#indexspec).|
|**Query**|SQL|
|**Engine**|MSQ|
|**Statement**|INSERT, REPLACE|
|**Since**|25.0.0|

### `maxConcurrentStages`

| | |
|---|---|
|**Description**|Number of stages that can run concurrently for a query. A higher number can potentially improve pipelining but results in less memory available for each stage.|
|**Type**|Integer|
|**Default**|`2` for Dart, `1` for the MSQ task engine|
|**Query**|SQL|
|**Engine**|MSQ, Dart|
|**Statement**|SELECT, INSERT, REPLACE|
|**Since**|31.0.0|

### `maxFrameSize`

| | |
|---|---|
|**Description**|Size of frames used for data transfer within the MSQ engine. You generally do not need to change this unless you have very large rows.|
|**Type**|Integer|
|**Default**|`1000000` (1 MB)|
|**Query**|SQL|
|**Engine**|MSQ, Dart|
|**Statement**|SELECT, INSERT, REPLACE|
|**Since**|35.0.0|

### `maxInputFilesPerWorker`

| | |
|---|---|
|**Description**|Maximum number of input files or segments per worker. If a single worker would need to read more than this number of files, the query fails with a `TooManyInputFiles` error. In this case, you should either increase this limit if your tasks have enough memory to handle more files, add more workers by increasing `maxNumTasks`, or split your query into smaller queries that process fewer files.|
|**Type**|Integer|
|**Default**|`10000`|
|**Query**|SQL|
|**Engine**|MSQ|
|**Statement**|SELECT, INSERT, REPLACE|
|**Since**|36.0.0|

### `maxNumSegments`

| | |
|---|---|
|**Description**|Maximum number of segments that an ingestion query can generate in a single time chunk. If the query would exceed this limit, it fails with a `TooManySegmentsInTimeChunk` error.|
|**Type**|Integer|
|**Default**|No limit|
|**Query**|SQL|
|**Engine**|MSQ|
|**Statement**|INSERT, REPLACE|
|**Since**|31.0.0|

### `maxNumTasks`

| | |
|---|---|
|**Description**|The maximum total number of tasks to launch, including the controller task. The lowest possible value for this setting is 2: one controller and one worker. All tasks must be able to launch simultaneously. If they cannot, the query returns a `TaskStartTimeout` error code after approximately 10 minutes.|
|**Type**|Integer|
|**Default**|`2`|
|**Query**|SQL|
|**Engine**|MSQ|
|**Statement**|SELECT, INSERT, REPLACE|
|**Since**|24.0.0|

### `maxParseExceptions`

| | |
|---|---|
|**Description**|Maximum number of parse exceptions that are ignored while executing the query before it stops with `TooManyWarningsFault`. To ignore all the parse exceptions, set the value to -1.|
|**Type**|Long|
|**Default**|`0`|
|**Query**|SQL|
|**Engine**|MSQ|
|**Statement**|SELECT, INSERT, REPLACE|
|**Since**|24.0.0|

### `maxPartitions`

| | |
|---|---|
|**Description**|Maximum number of output partitions for any single stage. For INSERT or REPLACE queries, this controls the maximum number of segments that can be generated. If the query would exceed this limit, it fails with a `TooManyPartitions` error. You can increase this limit if needed, break your query into smaller queries, or use a larger target segment size (via `rowsPerSegment`).|
|**Type**|Integer|
|**Default**|`25000`|
|**Query**|SQL|
|**Engine**|MSQ|
|**Statement**|SELECT, INSERT, REPLACE|
|**Since**|36.0.0|

### `maxRowsInMemory`

| | |
|---|---|
|**Description**|Maximum number of rows to store in memory at once before flushing to disk during the segment generation process. Ignored for non-INSERT queries. In most cases, use the default value. You may need to override the default if you run into one of the [known issues](../multi-stage-query/known-issues.md) around memory usage.|
|**Type**|Integer|
|**Default**|`100000`|
|**Query**|SQL|
|**Engine**|MSQ|
|**Statement**|INSERT, REPLACE|
|**Since**|36.0.0|

### `maxRowsMaterializedInWindow`

| | |
|---|---|
|**Description**|Maximum number of rows that window functions can materialize in a single window partition. If a partition exceeds this limit, the query fails with a `TooManyRowsInAWindow` error.|
|**Type**|Integer|
|**Default**|`100000`|
|**Query**|SQL|
|**Engine**|MSQ, Dart|
|**Statement**|SELECT, INSERT, REPLACE|
|**Since**|30.0.0|

### `maxThreads`

| | |
|---|---|
|**Description**|Maximum number of threads to use for processing. This only has an effect if it is greater than zero and less than the default thread count based on system configuration. Otherwise, it is ignored, and workers use the default thread count.|
|**Type**|Integer|
|**Default**|Not set (use default thread count)|
|**Query**|SQL|
|**Engine**|MSQ, Dart|
|**Statement**|SELECT, INSERT, REPLACE|
|**Since**|35.0.0|

### `removeNullBytes`

| | |
|---|---|
|**Description**|The MSQ engine can't process null bytes in strings and throws `InvalidNullByteFault` if it encounters them in the source data. If the parameter is set to true, the MSQ engine removes the null bytes in string fields when reading the data.|
|**Type**|Boolean|
|**Default**|`false`|
|**Query**|SQL|
|**Engine**|MSQ, Dart|
|**Statement**|SELECT, INSERT, REPLACE|
|**Since**|31.0.0|

### `rowsInMemory`

| | |
|---|---|
|**Description**|Alternate spelling of `maxRowsInMemory`. Ignored if `maxRowsInMemory` is set.|
|**Type**|Integer|
|**Default**|`100000`|
|**Query**|SQL|
|**Engine**|MSQ|
|**Statement**|INSERT, REPLACE|
|**Since**|24.0.0|

### `rowsPerPage`

| | |
|---|---|
|**Description**|The number of rows per page to target. The actual number of rows per page may be somewhat higher or lower than this number. In most cases, use the default.<br /> This property comes into effect only when `selectDestination` is set to `durableStorage`|
|**Type**|Integer|
|**Default**|`100000`|
|**Query**|SQL|
|**Engine**|MSQ|
|**Statement**|SELECT|
|**Since**|29.0.0|

### `rowsPerSegment`

| | |
|---|---|
|**Description**|The number of rows per segment to target. The actual number of rows per segment may be somewhat higher or lower than this number. In most cases, use the default. For general information about sizing rows per segment, see [Segment Size Optimization](../operations/segment-optimization.md).|
|**Type**|Integer|
|**Default**|`3000000`|
|**Query**|SQL|
|**Engine**|MSQ|
|**Statement**|INSERT, REPLACE|
|**Since**|24.0.0|

### `segmentSortOrder`

| | |
|---|---|
|**Description**|Normally, Druid sorts rows in individual segments using `__time` first, followed by the [CLUSTERED BY](../multi-stage-query/reference.md#clustered-by) clause. When you set `segmentSortOrder`, Druid uses the order from this context parameter instead. Provide the column list as comma-separated values or as a JSON array in string form.<br /><br />For example, consider an INSERT query that uses `CLUSTERED BY country` and has `segmentSortOrder` set to `__time,city,country`. Within each time chunk, Druid assigns rows to segments based on `country`, and then within each of those segments, Druid sorts those rows by `__time` first, then `city`, then `country`.|
|**Type**|String|
|**Default**|empty list|
|**Query**|SQL|
|**Engine**|MSQ|
|**Statement**|INSERT, REPLACE|
|**Since**|24.0.0|

### `selectDestination`

| | |
|---|---|
|**Description**|Controls where the final result of the select query is written. <br />Use `taskReport`(the default) to write select results to the task report. <b> This is not scalable since task reports size explodes for large results </b> <br/>Use `durableStorage` to write results to durable storage location. <b>For large results sets, its recommended to use `durableStorage` </b>. To configure durable storage see [`this`](../multi-stage-query/reference.md#durable-storage) section.|
|**Type**|String|
|**Default**|`taskReport`|
|**Query**|SQL|
|**Engine**|MSQ|
|**Statement**|SELECT|
|**Since**|27.0.0|

### `skipTypeVerification`

| | |
|---|---|
|**Description**|During query validation, Druid validates that [string arrays](../querying/arrays.md) and [multi-value dimensions](../querying/multi-value-dimensions.md) are not mixed in the same column. If you are intentionally migrating from one to the other, use this context parameter to disable type validation.<br /><br />Provide the column list as comma-separated values or as a JSON array in string form.|
|**Type**|String|
|**Default**|empty list|
|**Query**|SQL|
|**Engine**|MSQ|
|**Statement**|INSERT, REPLACE|
|**Since**|30.0.0|

### `sqlJoinAlgorithm`

| | |
|---|---|
|**Description**|Algorithm to use for `JOIN`: `broadcast` for broadcast hash join or `sortMerge` for sort-merge join. Affects all joins in the query. This is a hint to the MSQ engine, and the actual joins may proceed differently. The native engine only supports `broadcast`. See [Joins](../multi-stage-query/reference.md#joins) for more details.|
|**Type**|Enum (`broadcast`, `sortMerge`)|
|**Default**|`broadcast`|
|**Query**|SQL|
|**Engine**|MSQ, Dart|
|**Statement**|SELECT, INSERT, REPLACE|
|**Since**|26.0.0|

### `storeCompactionState`

| | |
|---|---|
|**Description**|When set to true, a REPLACE query stores as part of each segment's metadata a `lastCompactionState` field that captures the various specs used to create the segment. Future compaction jobs skip segments whose `lastCompactionState` matches the desired compaction state. Works the same as [`storeCompactionState`](../ingestion/tasks.md#context-parameters) task context flag.|
|**Type**|Boolean|
|**Default**|`false`|
|**Query**|SQL|
|**Engine**|MSQ|
|**Statement**|REPLACE|
|**Since**|30.0.0|

### `targetPartitionsPerWorker`

| | |
|---|---|
|**Description**|Number of partitions Druid generates for each worker. This number controls how much parallelism can be maintained throughout a query.|
|**Type**|Integer|
|**Default**|`1` for Dart, the number of processing threads for the MSQ task engine|
|**Query**|SQL|
|**Engine**|MSQ, Dart|
|**Statement**|SELECT, INSERT, REPLACE|
|**Since**|32.0.0|

### `taskAssignment`

| | |
|---|---|
|**Description**|Determines how many tasks to use. Possible values include: <ul><li>`max`: Uses as many tasks as possible, up to `maxNumTasks`.</li><li>`auto`: When file sizes can be determined through directory listing (for example: local files, S3, GCS, HDFS) uses as few tasks as possible without exceeding 512 MiB or 10,000 files per task, unless exceeding these limits is necessary to stay within `maxNumTasks`. When calculating the size of files, the weighted size is used, which considers the file format and compression format used if any. When file sizes cannot be determined through directory listing (for example: http), behaves the same as `max`.</li></ul>|
|**Type**|String|
|**Default**|`max`|
|**Query**|SQL|
|**Engine**|MSQ|
|**Statement**|SELECT, INSERT, REPLACE|
|**Since**|24.0.0|

### `taskLockType`

| | |
|---|---|
|**Description**|Type of time chunk lock that an INSERT or REPLACE query acquires: `EXCLUSIVE`, `SHARED`, `APPEND` or `REPLACE`. If not set, REPLACE queries use `EXCLUSIVE` and INSERT queries use `SHARED`. Ignored when `useConcurrentLocks` is true.|
|**Type**|String|
|**Default**|`EXCLUSIVE` for REPLACE, `SHARED` for INSERT|
|**Query**|SQL|
|**Engine**|MSQ|
|**Statement**|INSERT, REPLACE|
|**Since**|29.0.0|

### `useConcurrentLocks`

| | |
|---|---|
|**Description**|Whether an INSERT or REPLACE query uses concurrent locks: `APPEND` locks for INSERT and `REPLACE` locks for REPLACE. See [concurrent append and replace](../ingestion/concurrent-append-replace.md).|
|**Type**|Boolean|
|**Default**|`false`|
|**Query**|SQL|
|**Engine**|MSQ|
|**Statement**|INSERT, REPLACE|
|**Since**|30.0.0|

### `waitUntilSegmentsLoad`

| | |
|---|---|
|**Description**|If set, the ingest query waits for the generated segments to be loaded before exiting, else the ingest query exits without waiting. The task and live reports contain the information about the status of loading segments if this flag is set. This will ensure that any future queries made after the ingestion exits will include results from the ingestion. The drawback is that the controller task will stall till the segments are loaded.|
|**Type**|Boolean|
|**Default**|`false`|
|**Query**|SQL|
|**Engine**|MSQ|
|**Statement**|INSERT, REPLACE|
|**Since**|29.0.0|

<!-- END GENERATED QUERY CONTEXT PARAMETERS -->

## Dart parameters

The following parameters apply to [Dart](dart.md) queries only.

<!-- BEGIN GENERATED QUERY CONTEXT PARAMETERS: queryTypes= engines=DART -->

### `fullReport`

| | |
|---|---|
|**Description**|If true, a [Dart](../querying/dart.md) query returns a single `fullReport` column that contains the full query report, including the results, instead of the query results. The report size is limited by `druid.msq.dart.controller.maxQueryReportSize`.|
|**Type**|Boolean|
|**Default**|`false`|
|**Query**|SQL|
|**Engine**|Dart|
|**Since**|32.0.0|

### `maxNonLeafWorkers`

| | |
|---|---|
|**Description**|Number of workers to use for stages beyond the leaf stage.|
|**Type**|Integer|
|**Default**|1 (Scatter-gather style)|
|**Query**|SQL|
|**Engine**|Dart|
|**Statement**|SELECT|
|**Since**|32.0.0|

<!-- END GENERATED QUERY CONTEXT PARAMETERS -->

## Learn more

For more information, see the following topics:

- [Set query context](./query-context.md) to learn how to configure query context parameters.
- [SQL query context](sql-query-context.md) to learn how to set query context parameters in Druid SQL.
- [SQL-based ingestion reference](../multi-stage-query/reference.md#context-parameters) for SQL-based ingestion (MSQ).
