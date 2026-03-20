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

You can use any SQL query context parameters to control Dart's behavior unless otherwise stated. Additionally, the following table lists the supported MSQ engine query context parameters that Dart supports:

| Parameter | Description | Default value |
|---|---|---|
| `finalizeAggregations` | Determines the type of aggregation to return. If true, Druid finalizes the results of complex aggregations that directly appear in query results. If false, Druid returns the aggregation's intermediate type rather than finalized type. This parameter is useful during ingestion, where it enables storing sketches directly in Druid tables. For more information about aggregations, see [SQL aggregation functions](../querying/sql-aggregations.md). | `true` |
| `includeSegmentSource` |  Controls the sources Druid queries for results in addition to the segments present on deep storage. Can be `NONE` or `REALTIME`. If set to `NONE`, only non-realtime (published and used) segments are downloaded from deep storage. If set to `REALTIME`, results are also included from realtime tasks.|  `REALTIME` |
| `removeNullBytes` |The MSQ engine can't process null bytes in strings and throws `InvalidNullByteFault` if it encounters them in the source data. If the parameter is set to true, the MSQ engine removes the null bytes in string fields when reading the data. | `false` |
|`maxConcurrentStages`|Number of stages that can run concurrently for a query. A higher number can potentially improve pipelining but results in less memory available for each stage.|2|
|`maxNonLeafWorkers`|Number of workers to use for stages beyond the leaf stage.| 1 (Scatter-gather style)|
| `sqlJoinAlgorithm` | Algorithm to use for JOIN. Use `broadcast` (the default) for broadcast hash join or `sortMerge` for sort-merge join. Affects all JOIN operations in the query. This is a hint to the MSQ engine and the actual joins in the query may proceed in a different way than specified. See [Joins](../multi-stage-query/reference.md#joins) for more details. | `broadcast` |
|`targetPartitionsPerWorker`|Number of partitions Druid generates for each worker. This number controls how much parallelism can be maintained throughout a query.|1|


  ## Known issues and limitations

- Dart doesn't do the following:
  - Verify that `druid.msq.dart.controller.concurrentQueries` is set properly. If set too high, queries can get stuck on each other.
  - Use the query cache.
  - Perform query prioritization or laning.
- TopN queries are always exact. Approximate TopN queries (`useApproximateTopN`) aren't supported.
- Dart doesn't support JDBC connections. Druid ignores the `engine` context parameter when its passed through a JDBC connection.
- Realtime scans from the MSQ engine can't reliably read complex types. This can happen in situations such as if your data includes HLL Sketches for realtime data. Dart returns a `NullPointerException`. For more information, see [#18340](https://github.com/apache/druid/issues/18340).
- The `NilStageOutputReader` can sometimes lead to a `NoClassDefFoundError`. For more information, see [#18336](https://github.com/apache/druid/pull/18336).
- Broadcast joins with realtime data aren't supported. If the left table of a join has realtime data and you're doing a broadcast join, you must set `sqlJoinAlgorithm` to `sortMerge`.