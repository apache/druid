---
id: dart
title: "SQL queries using the Dart engine"
sidebar_label: "Dart engine"
description: The Dart engine, a profile of MSQ, is an alternative to the native query engine that offers better parallelism and better performance for certain types of queries.
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ License); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ AS IS BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->

:::info[Experimental]

Dart is experimental. Use it in situations where it fits your use case better than the native query engine. But be aware that Dart has not received as much testing as the other query engines.

:::


Dart is a query engine that runs SELECT queries on Brokers and Historicals instead of on tasks.
It is a profile of MSQ in which Brokers act as controllers and the Historicals act as workers.

Use Dart as an alternative to the native query engine since it offers better parallelism, excelling at queries that involve:

- Large joins, which Dart performs using parallel sort-merges.
- High-cardinality exact groupBy operations.
- High-cardinality exact count distinct.

When processing these kinds of queries, Dart can parallelize through the entire query, leading to better performance.

By default, Dart queries include results from published segments and realtime tasks.

## Enable Dart

To enable Dart, add the following line to your `_common/common.runtime.properties` files:

```
druid.msq.dart.enabled = true
```

### Configure resource consumption

You can configure the Broker and the Historical to tune Dart's resource consumption. Since Brokers only act as controllers, they don't require substantial resources. Historicals, on the other hand, are processing the queries. More resources for Historicals can result in faster query processing.

For Brokers, you can set the following configs:

| Property name | Description | Default |
|---|---|---|
| `druid.msq.dart.controller.concurrentQueries` | Maximum number of query controllers that can run concurrently on that Broker. Druid queues additional controllers. Queries can get stuck waiting for each other if the total value on Brokers exceeds the setting on a single Historical (`druid.msq.dart.worker.concurrentQueries` ).| 1 |
| `druid.msq.dart.query.context.targetPartitionsPerWorker` |To parallelize queries as much as possible on each Historical, set this to the same value as `druid.processing.numThreads` on the Historicals. | 1 (Multithreading is turned off on Historicals) |


For Historicals, you can set the following configs:

| Property name | Description | Default Value |
|---|---|---|
| `druid.msq.dart.worker.concurrentQueries` | Maximum number of query workers that can run concurrently on a Historical. We recommend leaving this config at the default value. If need to change this value, set it to a value equal to or larger than `druid.msq.dart.controller.concurrentQueries` on your Brokers. If you don't, queries can get stuck waiting for each other. Don't set it to a value higher than the number of merge buffers. | Equal to the number of merge buffers |
| `druid.msq.dart.worker.heapFraction` | Maximum amount of heap available for use across all Dart queries as a decimal. | 0.35 (35% of heap) |
| `druid.msq.dart.worker.segmentLoadAheadCount` | Number of segments a worker will prefetch ahead of processing them. This lets tiers with different hardware tune prefetch independently. It acts as a worker-local default: a value supplied in the query context (`segmentLoadAheadCount`) always takes precedence when it is set. Non-positive values are treated as unset. | Unset (uses twice the number of processing threads) |


## Run a Dart query

Once enabled, you can use Dart in the Druid console or the SQL query API to issue queries.

### Druid console

In the **Query** view, select **Engine: SQL (Dart)** from the engine selector menu.

### API

Dart uses the SQL endpoint `/druid/v2/sql`. To use Dart, include the query context parameter `engine` and set it to `msq-dart`:

<Tabs>
<TabItem value="SET" label="SET" default>

As part of your query using `SET engine = 'msq-dart'`:

```json
{
"query":"SET \"engine\"='msq-dart';\nSELECT\n  user,\n  commentLength,\n  COUNT(*) AS \"COUNT\"\nFROM \"wikipedia\"\nGROUP BY 1, 2\nORDER BY 2 DESC"
}
```

</TabItem>

<TabItem value="context_block" label="Context block">

As part of a `context` block: 

```json
{
  "query": "SELECT\n  user,\n  commentLength,\n  COUNT(*) AS \"COUNT\"\nFROM \"wikipedia\"\nGROUP BY 1, 2\nORDER BY 2 DESC",
  "context": {
    "engine": "msq-dart"
  }
}
```


  </TabItem>
  </Tabs>

## Query context parameters

You can use any SQL query context parameters to control Dart's behavior unless otherwise stated. Additionally, Dart supports the following query context parameters, which don't apply to native queries. Most of them also apply to the MSQ task engine. See the [query context reference](query-context-reference.md) for details.

<!-- BEGIN GENERATED QUERY CONTEXT PARAMETER TABLE: engines~DART engines!~NATIVE -->

|Parameter|Statements|Description|Default|
|---|---|---|---|
|[`finalizeAggregations`](query-context-reference.md#finalizeaggregations)|SELECT, INSERT, REPLACE|Determines the type of aggregation to return.|`true`|
|[`fullReport`](query-context-reference.md#fullreport)||If true, a [Dart](../querying/dart.md) query returns a single `fullReport` column that contains the full query report, including the results, instead of the query results.|`false`|
|[`includeSegmentSource`](query-context-reference.md#includesegmentsource)|SELECT, INSERT, REPLACE|Controls the sources Druid queries for results in addition to the segments present on deep storage.|`NONE` for the MSQ task engine, `REALTIME` for Dart|
|[`maxConcurrentStages`](query-context-reference.md#maxconcurrentstages)|SELECT, INSERT, REPLACE|Number of stages that can run concurrently for a query.|`2` for Dart, `1` for the MSQ task engine|
|[`maxFrameSize`](query-context-reference.md#maxframesize)|SELECT, INSERT, REPLACE|Size of frames used for data transfer within the MSQ engine.|`1000000` (1 MB)|
|[`maxNonLeafWorkers`](query-context-reference.md#maxnonleafworkers)|SELECT|Number of workers to use for stages beyond the leaf stage.|1 (Scatter-gather style)|
|[`maxRowsMaterializedInWindow`](query-context-reference.md#maxrowsmaterializedinwindow)|SELECT, INSERT, REPLACE|Maximum number of rows that window functions can materialize in a single window partition.|`100000`|
|[`maxThreads`](query-context-reference.md#maxthreads)|SELECT, INSERT, REPLACE|Maximum number of threads to use for processing.|Not set (use default thread count)|
|[`removeNullBytes`](query-context-reference.md#removenullbytes)|SELECT, INSERT, REPLACE|The MSQ engine can't process null bytes in strings and throws `InvalidNullByteFault` if it encounters them in the source data.|`false`|
|[`sqlJoinAlgorithm`](query-context-reference.md#sqljoinalgorithm)|SELECT, INSERT, REPLACE|Algorithm to use for `JOIN`: `broadcast` for broadcast hash join or `sortMerge` for sort-merge join.|`broadcast`|
|[`targetPartitionsPerWorker`](query-context-reference.md#targetpartitionsperworker)|SELECT, INSERT, REPLACE|Number of partitions Druid generates for each worker.|`1` for Dart, the number of processing threads for the MSQ task engine|

<!-- END GENERATED QUERY CONTEXT PARAMETER TABLE -->


  ## Known issues and limitations

- Dart doesn't do the following:
  - Verify that `druid.msq.dart.controller.concurrentQueries` is set properly. If set too high, queries can get stuck on each other.
  - Use the query cache.
  - Perform query prioritization or laning.
- TopN queries are always exact. Approximate TopN queries (`useApproximateTopN`) aren't supported.
- Dart doesn't support the [Avatica JDBC driver](sql-jdbc-avatica.md). Druid ignores the `engine` context parameter when it's passed through an Avatica connection. Use the [Druid JDBC driver](sql-jdbc.md) instead.
- Realtime scans from the MSQ engine can't reliably read complex types. This can happen in situations such as if your data includes HLL Sketches for realtime data. Dart returns a `NullPointerException`. For more information, see [#18340](https://github.com/apache/druid/issues/18340).
- The `NilStageOutputReader` can sometimes lead to a `NoClassDefFoundError`. For more information, see [#18336](https://github.com/apache/druid/pull/18336).
- Broadcast joins with realtime data aren't supported. If the left table of a join has realtime data and you're doing a broadcast join, you must set `sqlJoinAlgorithm` to `sortMerge`.
