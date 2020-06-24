---
id: query-context
title: "Query context"
sidebar_label: "Context parameters"
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

## General parameters

The query context is used for various query configuration parameters. Query context parameters can be specified in
the following ways:

- For [Druid SQL](sql.md#client-apis), context parameters are provided either as a JSON object named `context` to the
HTTP POST API, or as properties to the JDBC connection.
- For [native queries](querying.md), context parameters are provided as a JSON object named `context`.

These parameters apply to all query types.

|property         |default                                 | description          |
|-----------------|----------------------------------------|----------------------|
|timeout          | `druid.server.http.defaultQueryTimeout`| Query timeout in millis, beyond which unfinished queries will be cancelled. 0 timeout means `no timeout`. To set the default timeout, see [Broker configuration](../configuration/index.html#broker) |
|priority         | `0`                                    | Query Priority. Queries with higher priority get precedence for computational resources.|
|lane             | `null`                                 | Query lane, used to control usage limits on classes of queries. See [Broker configuration](../configuration/index.html#broker) for more details.|
|queryId          | auto-generated                         | Unique identifier given to this query. If a query ID is set or known, this can be used to cancel the query |
|useCache         | `true`                                 | Flag indicating whether to leverage the query cache for this query. When set to false, it disables reading from the query cache for this query. When set to true, Apache Druid uses `druid.broker.cache.useCache` or `druid.historical.cache.useCache` to determine whether or not to read from the query cache |
|populateCache    | `true`                                 | Flag indicating whether to save the results of the query to the query cache. Primarily used for debugging. When set to false, it disables saving the results of this query to the query cache. When set to true, Druid uses `druid.broker.cache.populateCache` or `druid.historical.cache.populateCache` to determine whether or not to save the results of this query to the query cache |
|useResultLevelCache         | `true`                      | Flag indicating whether to leverage the result level cache for this query. When set to false, it disables reading from the query cache for this query. When set to true, Druid uses `druid.broker.cache.useResultLevelCache` to determine whether or not to read from the result-level query cache |
|populateResultLevelCache    | `true`                      | Flag indicating whether to save the results of the query to the result level cache. Primarily used for debugging. When set to false, it disables saving the results of this query to the query cache. When set to true, Druid uses `druid.broker.cache.populateResultLevelCache` to determine whether or not to save the results of this query to the result-level query cache |
|bySegment        | `false`                                | Return "by segment" results. Primarily used for debugging, setting it to `true` returns results associated with the data segment they came from |
|finalize         | `true`                                 | Flag indicating whether to "finalize" aggregation results. Primarily used for debugging. For instance, the `hyperUnique` aggregator will return the full HyperLogLog sketch instead of the estimated cardinality when this flag is set to `false` |
|maxScatterGatherBytes| `druid.server.http.maxScatterGatherBytes` | Maximum number of bytes gathered from data processes such as Historicals and realtime processes to execute a query. This parameter can be used to further reduce `maxScatterGatherBytes` limit at query time. See [Broker configuration](../configuration/index.html#broker) for more details.|
|maxQueuedBytes       | `druid.broker.http.maxQueuedBytes`        | Maximum number of bytes queued per query before exerting backpressure on the channel to the data server. Similar to `maxScatterGatherBytes`, except unlike that configuration, this one will trigger backpressure rather than query failure. Zero means disabled.|
|serializeDateTimeAsLong| `false`       | If true, DateTime is serialized as long in the result returned by Broker and the data transportation between Broker and compute process|
|serializeDateTimeAsLongInner| `false`  | If true, DateTime is serialized as long in the data transportation between Broker and compute process|
|enableParallelMerge|`true`|Enable parallel result merging on the Broker. Note that `druid.processing.merge.useParallelMergePool` must be enabled for this setting to be set to `true`. See [Broker configuration](../configuration/index.html#broker) for more details.|
|parallelMergeParallelism|`druid.processing.merge.pool.parallelism`|Maximum number of parallel threads to use for parallel result merging on the Broker. See [Broker configuration](../configuration/index.html#broker) for more details.|
|parallelMergeInitialYieldRows|`druid.processing.merge.task.initialYieldNumRows`|Number of rows to yield per ForkJoinPool merge task for parallel result merging on the Broker, before forking off a new task to continue merging sequences. See [Broker configuration](../configuration/index.html#broker) for more details.|
|parallelMergeSmallBatchRows|`druid.processing.merge.task.smallBatchNumRows`|Size of result batches to operate on in ForkJoinPool merge tasks for parallel result merging on the Broker. See [Broker configuration](../configuration/index.html#broker) for more details.|
|useFilterCNF|`false`| If true, Druid will attempt to convert the query filter to Conjunctive Normal Form (CNF). During query processing, columns can be pre-filtered by intersecting the bitmap indexes of all values that match the eligible filters, often greatly reducing the raw number of rows which need to be scanned. But this effect only happens for the top level filter, or individual clauses of a top level 'and' filter. As such, filters in CNF potentially have a higher chance to utilize a large amount of bitmap indexes on string columns during pre-filtering. However, this setting should be used with great caution, as it can sometimes have a negative effect on performance, and in some cases, the act of computing CNF of a filter can be expensive. We recommend hand tuning your filters to produce an optimal form if possible, or at least verifying through experimentation that using this parameter actually improves your query performance with no ill-effects.|

## Query-type-specific parameters

In addition, some query types offer context parameters specific to that query type.

### TopN

|property         |default              | description          |
|-----------------|---------------------|----------------------|
|minTopNThreshold | `1000`              | The top minTopNThreshold local results from each segment are returned for merging to determine the global topN. |

### Timeseries

|property         |default              | description          |
|-----------------|---------------------|----------------------|
|skipEmptyBuckets | `false`             | Disable timeseries zero-filling behavior, so only buckets with results will be returned. |

### GroupBy

See the list of [GroupBy query context](groupbyquery.md#advanced-configurations) parameters available on the groupBy
query page.

## Vectorization parameters

The GroupBy and Timeseries query types can run in _vectorized_ mode, which speeds up query execution by processing
batches of rows at a time. Not all queries can be vectorized. In particular, vectorization currently has the following
requirements:

- All query-level filters must either be able to run on bitmap indexes or must offer vectorized row-matchers. These
include "selector", "bound", "in", "like", "regex", "search", "and", "or", and "not".
- All filters in filtered aggregators must offer vectorized row-matchers.
- All aggregators must offer vectorized implementations. These include "count", "doubleSum", "floatSum", "longSum",
"hyperUnique", and "filtered".
- No virtual columns.
- For GroupBy: All dimension specs must be "default" (no extraction functions or filtered dimension specs).
- For GroupBy: No multi-value dimensions.
- For Timeseries: No "descending" order.
- Only immutable segments (not real-time).
- Only [table datasources](datasource.html#table) (not joins, subqueries, lookups, or inline datasources).

Other query types (like TopN, Scan, Select, and Search) ignore the "vectorize" parameter, and will execute without
vectorization. These query types will ignore the "vectorize" parameter even if it is set to `"force"`.

|property|default| description|
|--------|-------|------------|
|vectorize|`true`|Enables or disables vectorized query execution. Possible values are `false` (disabled), `true` (enabled if possible, disabled otherwise, on a per-segment basis), and `force` (enabled, and groupBy or timeseries queries that cannot be vectorized will fail). The `"force"` setting is meant to aid in testing, and is not generally useful in production (since real-time segments can never be processed with vectorized execution, any queries on real-time data will fail). This will override `druid.query.vectorize` if it's set.|
|vectorSize|`512`|Sets the row batching size for a particular query. This will override `druid.query.vectorSize` if it's set.|
