---
id: groupbyquery
title: "GroupBy queries"
sidebar_label: "GroupBy"
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

> Apache Druid supports two query languages: [Druid SQL](sql.md) and [native queries](querying.md).
> This document describes a query
> type in the native language. For information about when Druid SQL will use this query type, refer to the
> [SQL documentation](sql.md#query-types).

These types of Apache Druid queries take a groupBy query object and return an array of JSON objects where each object represents a
grouping asked for by the query.

> Note: If you are doing aggregations with time as your only grouping, or an ordered groupBy over a single dimension,
> consider [Timeseries](timeseriesquery.md) and [TopN](topnquery.md) queries as well as
> groupBy. Their performance may be better in some cases. See [Alternatives](#alternatives) below for more details.

An example groupBy query object is shown below:

``` json
{
  "queryType": "groupBy",
  "dataSource": "sample_datasource",
  "granularity": "day",
  "dimensions": ["country", "device"],
  "limitSpec": { "type": "default", "limit": 5000, "columns": ["country", "data_transfer"] },
  "filter": {
    "type": "and",
    "fields": [
      { "type": "selector", "dimension": "carrier", "value": "AT&T" },
      { "type": "or",
        "fields": [
          { "type": "selector", "dimension": "make", "value": "Apple" },
          { "type": "selector", "dimension": "make", "value": "Samsung" }
        ]
      }
    ]
  },
  "aggregations": [
    { "type": "longSum", "name": "total_usage", "fieldName": "user_count" },
    { "type": "doubleSum", "name": "data_transfer", "fieldName": "data_transfer" }
  ],
  "postAggregations": [
    { "type": "arithmetic",
      "name": "avg_usage",
      "fn": "/",
      "fields": [
        { "type": "fieldAccess", "fieldName": "data_transfer" },
        { "type": "fieldAccess", "fieldName": "total_usage" }
      ]
    }
  ],
  "intervals": [ "2012-01-01T00:00:00.000/2012-01-03T00:00:00.000" ],
  "having": {
    "type": "greaterThan",
    "aggregation": "total_usage",
    "value": 100
  }
}
```

Following are main parts to a groupBy query:

|property|description|required?|
|--------|-----------|---------|
|queryType|This String should always be "groupBy"; this is the first thing Druid looks at to figure out how to interpret the query|yes|
|dataSource|A String or Object defining the data source to query, very similar to a table in a relational database. See [DataSource](../querying/datasource.md) for more information.|yes|
|dimensions|A JSON list of dimensions to do the groupBy over; or see [DimensionSpec](../querying/dimensionspecs.md) for ways to extract dimensions. |yes|
|limitSpec|See [LimitSpec](../querying/limitspec.md).|no|
|having|See [Having](../querying/having.md).|no|
|granularity|Defines the granularity of the query. See [Granularities](../querying/granularities.md)|yes|
|filter|See [Filters](../querying/filters.md)|no|
|aggregations|See [Aggregations](../querying/aggregations.md)|no|
|postAggregations|See [Post Aggregations](../querying/post-aggregations.md)|no|
|intervals|A JSON Object representing ISO-8601 Intervals. This defines the time ranges to run the query over.|yes|
|subtotalsSpec| A JSON array of arrays to return additional result sets for groupings of subsets of top level `dimensions`. It is [described later](groupbyquery.html#more-on-subtotalsspec) in more detail.|no|
|context|An additional JSON Object which can be used to specify certain flags.|no|

To pull it all together, the above query would return *n\*m* data points, up to a maximum of 5000 points, where n is the cardinality of the `country` dimension, m is the cardinality of the `device` dimension, each day between 2012-01-01 and 2012-01-03, from the `sample_datasource` table. Each data point contains the (long) sum of `total_usage` if the value of the data point is greater than 100, the (double) sum of `data_transfer` and the (double) result of `total_usage` divided by `data_transfer` for the filter set for a particular grouping of `country` and `device`. The output looks like this:

```json
[
  {
    "version" : "v1",
    "timestamp" : "2012-01-01T00:00:00.000Z",
    "event" : {
      "country" : <some_dim_value_one>,
      "device" : <some_dim_value_two>,
      "total_usage" : <some_value_one>,
      "data_transfer" :<some_value_two>,
      "avg_usage" : <some_avg_usage_value>
    }
  },
  {
    "version" : "v1",
    "timestamp" : "2012-01-01T00:00:12.000Z",
    "event" : {
      "dim1" : <some_other_dim_value_one>,
      "dim2" : <some_other_dim_value_two>,
      "sample_name1" : <some_other_value_one>,
      "sample_name2" :<some_other_value_two>,
      "avg_usage" : <some_other_avg_usage_value>
    }
  },
...
]
```

## Behavior on multi-value dimensions

groupBy queries can group on multi-value dimensions. When grouping on a multi-value dimension, _all_ values
from matching rows will be used to generate one group per value. It's possible for a query to return more groups than
there are rows. For example, a groupBy on the dimension `tags` with filter `"t1" AND "t3"` would match only row1, and
generate a result with three groups: `t1`, `t2`, and `t3`. If you only need to include values that match
your filter, you can use a [filtered dimensionSpec](dimensionspecs.html#filtered-dimensionspecs). This can also
improve performance.

See [Multi-value dimensions](multi-value-dimensions.html) for more details.

## More on subtotalsSpec

The subtotals feature allows computation of multiple sub-groupings in a single query. To use this feature, add a "subtotalsSpec" to your query as a list of subgroup dimension sets. It should contain the `outputName` from dimensions in your `dimensions` attribute, in the same order as they appear in the `dimensions` attribute (although, of course, you may skip some). 

For example, consider a groupBy query like this one:

```json
{
"type": "groupBy",
 ...
 ...
"dimensions": [
  {
  "type" : "default",
  "dimension" : "d1col",
  "outputName": "D1"
  },
  {
  "type" : "extraction",
  "dimension" : "d2col",
  "outputName" :  "D2",
  "extractionFn" : extraction_func
  },
  {
  "type":"lookup",
  "dimension":"d3col",
  "outputName":"D3",
  "name":"my_lookup"
  }
],
...
...
"subtotalsSpec":[ ["D1", "D2", D3"], ["D1", "D3"], ["D3"]],
..

}
```

The result of the subtotalsSpec would be equivalent to concatenating the result of three groupBy queries, with the "dimensions" field being `["D1", "D2", D3"]`, `["D1", "D3"]` and `["D3"]`, given the `DimensionSpec` shown above.
The response for the query above would look something like: 

```json
[
  {
    "version" : "v1",
    "timestamp" : "t1",
    "event" : { "D1": "..", "D2": "..", "D3": ".." }
    }
  },
    {
    "version" : "v1",
    "timestamp" : "t2",
    "event" : { "D1": "..", "D2": "..", "D3": ".." }
    }
  },
  ...
  ...

   {
    "version" : "v1",
    "timestamp" : "t1",
    "event" : { "D1": "..", "D2": null, "D3": ".." }
    }
  },
    {
    "version" : "v1",
    "timestamp" : "t2",
    "event" : { "D1": "..", "D2": null, "D3": ".." }
    }
  },
  ...
  ...

  {
    "version" : "v1",
    "timestamp" : "t1",
    "event" : { "D1": null, "D2": null, "D3": ".." }
    }
  },
    {
    "version" : "v1",
    "timestamp" : "t2",
    "event" : { "D1": null, "D2": null, "D3": ".." }
    }
  },
...
]
```

> Notice that dimensions that are not included in an individual subtotalsSpec grouping are returned with a `null` value. This response format represents a behavior change as of Apache Druid 0.18.0. In release 0.17.0 and earlier, such dimensions were entirely excluded from the result.   


## Implementation details

### Strategies

GroupBy queries can be executed using two different strategies. The default strategy for a cluster is determined by the
"druid.query.groupBy.defaultStrategy" runtime property on the Broker. This can be overridden using "groupByStrategy" in
the query context. If neither the context field nor the property is set, the "v2" strategy will be used.

- "v2", the default, is designed to offer better performance and memory management. This strategy generates
per-segment results using a fully off-heap map. Data processes merge the per-segment results using a fully off-heap
concurrent facts map combined with an on-heap string dictionary. This may optionally involve spilling to disk. Data
processes return sorted results to the Broker, which merges result streams using an N-way merge. The broker materializes
the results if necessary (e.g. if the query sorts on columns other than its dimensions). Otherwise, it streams results
back as they are merged.

- "v1", a legacy engine, generates per-segment results on data processes (Historical, realtime, MiddleManager) using a map which
is partially on-heap (dimension keys and the map itself) and partially off-heap (the aggregated values). Data processes then
merge the per-segment results using Druid's indexing mechanism. This merging is multi-threaded by default, but can
optionally be single-threaded. The Broker merges the final result set using Druid's indexing mechanism again. The broker
merging is always single-threaded. Because the Broker merges results using the indexing mechanism, it must materialize
the full result set before returning any results. On both the data processes and the Broker, the merging index is fully
on-heap by default, but it can optionally store aggregated values off-heap.

### Differences between v1 and v2

Query API and results are compatible between the two engines; however, there are some differences from a cluster
configuration perspective:

- groupBy v1 controls resource usage using a row-based limit (maxResults) whereas groupBy v2 uses bytes-based limits.
In addition, groupBy v1 merges results on-heap, whereas groupBy v2 merges results off-heap. These factors mean that
memory tuning and resource limits behave differently between v1 and v2. In particular, due to this, some queries
that can complete successfully in one engine may exceed resource limits and fail with the other engine. See the
"Memory tuning and resource limits" section for more details.
- groupBy v1 imposes no limit on the number of concurrently running queries, whereas groupBy v2 controls memory usage
by using a finite-sized merge buffer pool. By default, the number of merge buffers is 1/4 the number of processing
threads. You can adjust this as necessary to balance concurrency and memory usage.
- groupBy v1 supports caching on either the Broker or Historical processes, whereas groupBy v2 only supports caching on
Historical processes.
- groupBy v2 supports both array-based aggregation and hash-based aggregation. The array-based aggregation is used only
when the grouping key is a single indexed string column. In array-based aggregation, the dictionary-encoded value is used
as the index, so the aggregated values in the array can be accessed directly without finding buckets based on hashing.

### Memory tuning and resource limits

When using groupBy v2, three parameters control resource usage and limits:

- `druid.processing.buffer.sizeBytes`: size of the off-heap hash table used for aggregation, per query, in bytes. At
most `druid.processing.numMergeBuffers` of these will be created at once, which also serves as an upper limit on the
number of concurrently running groupBy queries.
- `druid.query.groupBy.maxMergingDictionarySize`: size of the on-heap dictionary used when grouping on strings, per query,
in bytes. Note that this is based on a rough estimate of the dictionary size, not the actual size.
- `druid.query.groupBy.maxOnDiskStorage`: amount of space on disk used for aggregation, per query, in bytes. By default,
this is 0, which means aggregation will not use disk.

If `maxOnDiskStorage` is 0 (the default) then a query that exceeds either the on-heap dictionary limit, or the off-heap
aggregation table limit, will fail with a "Resource limit exceeded" error describing the limit that was exceeded.

If `maxOnDiskStorage` is greater than 0, queries that exceed the in-memory limits will start using disk for aggregation.
In this case, when either the on-heap dictionary or off-heap hash table fills up, partially aggregated records will be
sorted and flushed to disk. Then, both in-memory structures will be cleared out for further aggregation. Queries that
then go on to exceed `maxOnDiskStorage` will fail with a "Resource limit exceeded" error indicating that they ran out of
disk space.

With groupBy v2, cluster operators should make sure that the off-heap hash tables and on-heap merging dictionaries
will not exceed available memory for the maximum possible concurrent query load (given by
`druid.processing.numMergeBuffers`). See the [basic cluster tuning guide](../operations/basic-cluster-tuning.md) 
for more details about direct memory usage, organized by Druid process type.

Brokers do not need merge buffers for basic groupBy queries. Queries with subqueries (using a `query` dataSource) require one merge buffer if there is a single subquery, or two merge buffers if there is more than one layer of nested subqueries. Queries with [subtotals](groupbyquery.html#more-on-subtotalsspec) need one merge buffer. These can stack on top of each other: a groupBy query with multiple layers of nested subqueries, and that also uses subtotals, will need three merge buffers.

Historicals and ingestion tasks need one merge buffer for each groupBy query, unless [parallel combination](groupbyquery.html#parallel-combine) is enabled, in which case they need two merge buffers per query.

When using groupBy v1, all aggregation is done on-heap, and resource limits are done through the parameter
`druid.query.groupBy.maxResults`. This is a cap on the maximum number of results in a result set. Queries that exceed
this limit will fail with a "Resource limit exceeded" error indicating they exceeded their row limit. Cluster
operators should make sure that the on-heap aggregations will not exceed available JVM heap space for the expected
concurrent query load.

### Performance tuning for groupBy v2

#### Limit pushdown optimization

Druid pushes down the `limit` spec in groupBy queries to the segments on Historicals wherever possible to early prune unnecessary intermediate results and minimize the amount of data transferred to Brokers. By default, this technique is applied only when all fields in the `orderBy` spec is a subset of the grouping keys. This is because the `limitPushDown` doesn't guarantee the exact results if the `orderBy` spec includes any fields that are not in the grouping keys. However, you can enable this technique even in such cases if you can sacrifice some accuracy for fast query processing like in topN queries. See `forceLimitPushDown` in [advanced groupBy v2 configurations](#groupby-v2-configurations).


#### Optimizing hash table

The groupBy v2 engine uses an open addressing hash table for aggregation. The hash table is initialized with a given initial bucket number and gradually grows on buffer full. On hash collisions, the linear probing technique is used.

The default number of initial buckets is 1024 and the default max load factor of the hash table is 0.7. If you can see too many collisions in the hash table, you can adjust these numbers. See `bufferGrouperInitialBuckets` and `bufferGrouperMaxLoadFactor` in [Advanced groupBy v2 configurations](#groupby-v2-configurations).


#### Parallel combine

Once a Historical finishes aggregation using the hash table, it sorts the aggregated results and merges them before sending to the
Broker for N-way merge aggregation in the broker. By default, Historicals use all their available processing threads
(configured by `druid.processing.numThreads`) for aggregation, but use a single thread for sorting and merging
aggregates which is an http thread to send data to Brokers.

This is to prevent some heavy groupBy queries from blocking other queries. In Druid, the processing threads are shared
between all submitted queries and they are _not interruptible_. It means, if a heavy query takes all available
processing threads, all other queries might be blocked until the heavy query is finished. GroupBy queries usually take
longer time than timeseries or topN queries, they should release processing threads as soon as possible.

However, you might care about the performance of some really heavy groupBy queries. Usually, the performance bottleneck
of heavy groupBy queries is merging sorted aggregates. In such cases, you can use processing threads for it as well.
This is called _parallel combine_. To enable parallel combine, see `numParallelCombineThreads` in
[Advanced groupBy v2 configurations](#groupby-v2-configurations). Note that parallel combine can be enabled only when
data is actually spilled (see [Memory tuning and resource limits](#memory-tuning-and-resource-limits)).

Once parallel combine is enabled, the groupBy v2 engine can create a combining tree for merging sorted aggregates. Each
intermediate node of the tree is a thread merging aggregates from the child nodes. The leaf node threads read and merge
aggregates from hash tables including spilled ones. Usually, leaf processes are slower than intermediate nodes because they
need to read data from disk. As a result, less threads are used for intermediate nodes by default. You can change the
degree of intermediate nodes. See `intermediateCombineDegree` in [Advanced groupBy v2 configurations](#groupby-v2-configurations).

Please note that each Historical needs two merge buffers to process a groupBy v2 query with parallel combine: one for
computing intermediate aggregates from each segment and another for combining intermediate aggregates in parallel.


### Alternatives

There are some situations where other query types may be a better choice than groupBy.

- For queries with no "dimensions" (i.e. grouping by time only) the [Timeseries query](timeseriesquery.html) will
generally be faster than groupBy. The major differences are that it is implemented in a fully streaming manner (taking
advantage of the fact that segments are already sorted on time) and does not need to use a hash table for merging.

- For queries with a single "dimensions" element (i.e. grouping by one string dimension), the [TopN query](topnquery.html)
will sometimes be faster than groupBy. This is especially true if you are ordering by a metric and find approximate
results acceptable.

### Nested groupBys

Nested groupBys (dataSource of type "query") are performed differently for "v1" and "v2". The Broker first runs the
inner groupBy query in the usual way. "v1" strategy then materializes the inner query's results on-heap with Druid's
indexing mechanism, and runs the outer query on these materialized results. "v2" strategy runs the outer query on the
inner query's results stream with off-heap fact map and on-heap string dictionary that can spill to disk. Both
strategy perform the outer query on the Broker in a single-threaded fashion.

### Configurations

This section describes the configurations for groupBy queries. You can set the runtime properties in the `runtime.properties` file on Broker, Historical, and MiddleManager processes. You can set the query context parameters through the [query context](query-context.html).

#### Configurations for groupBy v2

Supported runtime properties:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.query.groupBy.maxMergingDictionarySize`|Maximum amount of heap space (approximately) to use for the string dictionary during merging. When the dictionary exceeds this size, a spill to disk will be triggered.|100000000|
|`druid.query.groupBy.maxOnDiskStorage`|Maximum amount of disk space to use, per-query, for spilling result sets to disk when either the merging buffer or the dictionary fills up. Queries that exceed this limit will fail. Set to zero to disable disk spilling.|0 (disabled)|

Supported query contexts:

|Key|Description|
|---|-----------|
|`maxMergingDictionarySize`|Can be used to lower the value of `druid.query.groupBy.maxMergingDictionarySize` for this query.|
|`maxOnDiskStorage`|Can be used to lower the value of `druid.query.groupBy.maxOnDiskStorage` for this query.|


### Advanced configurations

#### Common configurations for all groupBy strategies

Supported runtime properties:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.query.groupBy.defaultStrategy`|Default groupBy query strategy.|v2|
|`druid.query.groupBy.singleThreaded`|Merge results using a single thread.|false|

Supported query contexts:

|Key|Description|
|---|-----------|
|`groupByStrategy`|Overrides the value of `druid.query.groupBy.defaultStrategy` for this query.|
|`groupByIsSingleThreaded`|Overrides the value of `druid.query.groupBy.singleThreaded` for this query.|


#### GroupBy v2 configurations

Supported runtime properties:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.query.groupBy.bufferGrouperInitialBuckets`|Initial number of buckets in the off-heap hash table used for grouping results. Set to 0 to use a reasonable default (1024).|0|
|`druid.query.groupBy.bufferGrouperMaxLoadFactor`|Maximum load factor of the off-heap hash table used for grouping results. When the load factor exceeds this size, the table will be grown or spilled to disk. Set to 0 to use a reasonable default (0.7).|0|
|`druid.query.groupBy.forceHashAggregation`|Force to use hash-based aggregation.|false|
|`druid.query.groupBy.intermediateCombineDegree`|Number of intermediate nodes combined together in the combining tree. Higher degrees will need less threads which might be helpful to improve the query performance by reducing the overhead of too many threads if the server has sufficiently powerful cpu cores.|8|
|`druid.query.groupBy.numParallelCombineThreads`|Hint for the number of parallel combining threads. This should be larger than 1 to turn on the parallel combining feature. The actual number of threads used for parallel combining is min(`druid.query.groupBy.numParallelCombineThreads`, `druid.processing.numThreads`).|1 (disabled)|
|`druid.query.groupBy.applyLimitPushDownToSegment`|If Broker pushes limit down to queryable data server (historicals, peons) then limit results during segment scan. If typically there are a large number of segments taking part in a query on a data server, this setting may counterintuitively reduce performance if enabled.|false (disabled)|

Supported query contexts:

|Key|Description|Default|
|---|-----------|-------|
|`bufferGrouperInitialBuckets`|Overrides the value of `druid.query.groupBy.bufferGrouperInitialBuckets` for this query.|None|
|`bufferGrouperMaxLoadFactor`|Overrides the value of `druid.query.groupBy.bufferGrouperMaxLoadFactor` for this query.|None|
|`forceHashAggregation`|Overrides the value of `druid.query.groupBy.forceHashAggregation`|None|
|`intermediateCombineDegree`|Overrides the value of `druid.query.groupBy.intermediateCombineDegree`|None|
|`numParallelCombineThreads`|Overrides the value of `druid.query.groupBy.numParallelCombineThreads`|None|
|`sortByDimsFirst`|Sort the results first by dimension values and then by timestamp.|false|
|`forceLimitPushDown`|When all fields in the orderby are part of the grouping key, the Broker will push limit application down to the Historical processes. When the sorting order uses fields that are not in the grouping key, applying this optimization can result in approximate results with unknown accuracy, so this optimization is disabled by default in that case. Enabling this context flag turns on limit push down for limit/orderbys that contain non-grouping key columns.|false|
|`applyLimitPushDownToSegment`|If Broker pushes limit down to queryable nodes (historicals, peons) then limit results during segment scan. This context value can be used to override `druid.query.groupBy.applyLimitPushDownToSegment`.|true|


#### GroupBy v1 configurations

Supported runtime properties:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.query.groupBy.maxIntermediateRows`|Maximum number of intermediate rows for the per-segment grouping engine. This is a tuning parameter that does not impose a hard limit; rather, it potentially shifts merging work from the per-segment engine to the overall merging index. Queries that exceed this limit will not fail.|50000|
|`druid.query.groupBy.maxResults`|Maximum number of results. Queries that exceed this limit will fail.|500000|

Supported query contexts:

|Key|Description|Default|
|---|-----------|-------|
|`maxIntermediateRows`|Can be used to lower the value of `druid.query.groupBy.maxIntermediateRows` for this query.|None|
|`maxResults`|Can be used to lower the value of `druid.query.groupBy.maxResults` for this query.|None|
|`useOffheap`|Set to true to store aggregations off-heap when merging results.|false|

#### Array based result rows

Internally Druid always uses an array based representation of groupBy result rows, but by default this is translated
into a map based result format at the Broker. To reduce the overhead of this translation, results may also be returned
from the Broker directly in the array based format if `resultAsArray` is set to `true` on the query context.

Each row is positional, and has the following fields, in order:

* Timestamp (optional; only if granularity != ALL)
* Dimensions (in order)
* Aggregators (in order)
* Post-aggregators (optional; in order, if present)

This schema is not available on the response, so it must be computed from the issued query in order to properly read
the results.
