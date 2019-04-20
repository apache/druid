---
layout: doc_page
title: "Query Context"
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

# Query Context

The query context is used for various query configuration parameters. The following parameters apply to all queries.

|property         |default                                 | description          |
|-----------------|----------------------------------------|----------------------|
|timeout          | `druid.server.http.defaultQueryTimeout`| Query timeout in millis, beyond which unfinished queries will be cancelled. 0 timeout means `no timeout`. To set the default timeout, see [Broker configuration](../configuration/index.html#broker) |
|priority         | `0`                                    | Query Priority. Queries with higher priority get precedence for computational resources.|
|queryId          | auto-generated                         | Unique identifier given to this query. If a query ID is set or known, this can be used to cancel the query |
|useCache         | `true`                                 | Flag indicating whether to leverage the query cache for this query. When set to false, it disables reading from the query cache for this query. When set to true, Apache Druid (incubating) uses druid.broker.cache.useCache or druid.historical.cache.useCache to determine whether or not to read from the query cache |
|populateCache    | `true`                                 | Flag indicating whether to save the results of the query to the query cache. Primarily used for debugging. When set to false, it disables saving the results of this query to the query cache. When set to true, Druid uses druid.broker.cache.populateCache or druid.historical.cache.populateCache to determine whether or not to save the results of this query to the query cache |
|useResultLevelCache         | `false`                                 | Flag indicating whether to leverage the result level cache for this query. When set to false, it disables reading from the query cache for this query. When set to true, Druid uses druid.broker.cache.useResultLevelCache to determine whether or not to read from the query cache |
|populateResultLevelCache    | `false`                                 | Flag indicating whether to save the results of the query to the result level cache. Primarily used for debugging. When set to false, it disables saving the results of this query to the query cache. When set to true, Druid uses druid.broker.cache.populateCache to determine whether or not to save the results of this query to the query cache |
|bySegment        | `false`                                | Return "by segment" results. Primarily used for debugging, setting it to `true` returns results associated with the data segment they came from |
|finalize         | `true`                                 | Flag indicating whether to "finalize" aggregation results. Primarily used for debugging. For instance, the `hyperUnique` aggregator will return the full HyperLogLog sketch instead of the estimated cardinality when this flag is set to `false` |
|chunkPeriod      | `P0D` (off)                            | At the Broker process level, long interval queries (of any type) may be broken into shorter interval queries to parallelize merging more than normal. Broken up queries will use a larger share of cluster resources, but, if you use groupBy "v1, it may be able to complete faster as a result. Use ISO 8601 periods. For example, if this property is set to `P1M` (one month), then a query covering a year would be broken into 12 smaller queries. The broker uses its query processing executor service to initiate processing for query chunks, so make sure "druid.processing.numThreads" is configured appropriately on the broker. [groupBy queries](groupbyquery.html) do not support chunkPeriod by default, although they do if using the legacy "v1" engine. This context is deprecated since it's only useful for groupBy "v1", and will be removed in the future releases.|
|maxScatterGatherBytes| `druid.server.http.maxScatterGatherBytes` | Maximum number of bytes gathered from data processes such as Historicals and realtime processes to execute a query. This parameter can be used to further reduce `maxScatterGatherBytes` limit at query time. See [Broker configuration](../configuration/index.html#broker) for more details.|
|maxQueuedBytes       | `druid.broker.http.maxQueuedBytes`        | Maximum number of bytes queued per query before exerting backpressure on the channel to the data server. Similar to `maxScatterGatherBytes`, except unlike that configuration, this one will trigger backpressure rather than query failure. Zero means disabled.|
|serializeDateTimeAsLong| `false`       | If true, DateTime is serialized as long in the result returned by Broker and the data transportation between Broker and compute process|
|serializeDateTimeAsLongInner| `false`  | If true, DateTime is serialized as long in the data transportation between Broker and compute process|

In addition, some query types offer context parameters specific to that query type.

### TopN queries

|property         |default              | description          |
|-----------------|---------------------|----------------------|
|minTopNThreshold | `1000`              | The top minTopNThreshold local results from each segment are returned for merging to determine the global topN. |

### Timeseries queries

|property         |default              | description          |
|-----------------|---------------------|----------------------|
|skipEmptyBuckets | `false`             | Disable timeseries zero-filling behavior, so only buckets with results will be returned. |

### GroupBy queries

See [GroupBy query context](groupbyquery.html#query-context).
