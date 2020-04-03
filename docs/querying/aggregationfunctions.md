---
id: aggregationfunctions
title: "Aggregation functions"
sidebar_label: "Aggregation functions"
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


## GroupBy queries

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

### Behavior on multi-value dimensions

groupBy queries can group on multi-value dimensions. When grouping on a multi-value dimension, _all_ values
from matching rows will be used to generate one group per value. It's possible for a query to return more groups than
there are rows. For example, a groupBy on the dimension `tags` with filter `"t1" AND "t3"` would match only row1, and
generate a result with three groups: `t1`, `t2`, and `t3`. If you only need to include values that match
your filter, you can use a [filtered dimensionSpec](dimensionspecs.html#filtered-dimensionspecs). This can also
improve performance.

See [Multi-value dimensions](multi-value-dimensions.html) for more details.

### More on subtotalsSpec

The subtotals feature allows computation of multiple sub-groupings in a single query. To use this feature, add a "subtotalsSpec" to your query, which should be a list of subgroup dimension sets. It should contain the "outputName" from dimensions in your "dimensions" attribute, in the same order as they appear in the "dimensions" attribute (although, of course, you may skip some). For example, consider a groupBy query like this one:

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

Response returned would be equivalent to concatenating result of 3 groupBy queries with "dimensions" field being ["D1", "D2", D3"], ["D1", "D3"] and ["D3"] with appropriate `DimensionSpec` json blob as used in above query.
Response for above query would look something like below...

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
    "event" : { "D1": "..", "D3": ".." }
    }
  },
    {
    "version" : "v1",
    "timestamp" : "t2",
    "event" : { "D1": "..", "D3": ".." }
    }
  },
  ...
  ...

  {
    "version" : "v1",
    "timestamp" : "t1",
    "event" : { "D3": ".." }
    }
  },
    {
    "version" : "v1",
    "timestamp" : "t2",
    "event" : { "D3": ".." }
    }
  },
...
]
```

### Implementation details

#### Strategies

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

#### Differences between v1 and v2

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

#### Memory tuning and resource limits

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

#### Performance tuning for groupBy v2

##### Limit pushdown optimization

Druid pushes down the `limit` spec in groupBy queries to the segments on Historicals wherever possible to early prune unnecessary intermediate results and minimize the amount of data transferred to Brokers. By default, this technique is applied only when all fields in the `orderBy` spec is a subset of the grouping keys. This is because the `limitPushDown` doesn't guarantee the exact results if the `orderBy` spec includes any fields that are not in the grouping keys. However, you can enable this technique even in such cases if you can sacrifice some accuracy for fast query processing like in topN queries. See `forceLimitPushDown` in [advanced groupBy v2 configurations](#groupby-v2-configurations).


##### Optimizing hash table

The groupBy v2 engine uses an open addressing hash table for aggregation. The hash table is initialized with a given initial bucket number and gradually grows on buffer full. On hash collisions, the linear probing technique is used.

The default number of initial buckets is 1024 and the default max load factor of the hash table is 0.7. If you can see too many collisions in the hash table, you can adjust these numbers. See `bufferGrouperInitialBuckets` and `bufferGrouperMaxLoadFactor` in [Advanced groupBy v2 configurations](#groupby-v2-configurations).


##### Parallel combine

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


#### Alternatives

There are some situations where other query types may be a better choice than groupBy.

- For queries with no "dimensions" (i.e. grouping by time only) the [Timeseries query](timeseriesquery.html) will
generally be faster than groupBy. The major differences are that it is implemented in a fully streaming manner (taking
advantage of the fact that segments are already sorted on time) and does not need to use a hash table for merging.

- For queries with a single "dimensions" element (i.e. grouping by one string dimension), the [TopN query](topnquery.html)
will sometimes be faster than groupBy. This is especially true if you are ordering by a metric and find approximate
results acceptable.

#### Nested groupBys

Nested groupBys (dataSource of type "query") are performed differently for "v1" and "v2". The Broker first runs the
inner groupBy query in the usual way. "v1" strategy then materializes the inner query's results on-heap with Druid's
indexing mechanism, and runs the outer query on these materialized results. "v2" strategy runs the outer query on the
inner query's results stream with off-heap fact map and on-heap string dictionary that can spill to disk. Both
strategy perform the outer query on the Broker in a single-threaded fashion.

#### Configurations

This section describes the configurations for groupBy queries. You can set the runtime properties in the `runtime.properties` file on Broker, Historical, and MiddleManager processes. You can set the query context parameters through the [query context](query-context.html).

##### Configurations for groupBy v2

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


#### Advanced configurations

##### Common configurations for all groupBy strategies

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


##### GroupBy v2 configurations

Supported runtime properties:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.query.groupBy.bufferGrouperInitialBuckets`|Initial number of buckets in the off-heap hash table used for grouping results. Set to 0 to use a reasonable default (1024).|0|
|`druid.query.groupBy.bufferGrouperMaxLoadFactor`|Maximum load factor of the off-heap hash table used for grouping results. When the load factor exceeds this size, the table will be grown or spilled to disk. Set to 0 to use a reasonable default (0.7).|0|
|`druid.query.groupBy.forceHashAggregation`|Force to use hash-based aggregation.|false|
|`druid.query.groupBy.intermediateCombineDegree`|Number of intermediate nodes combined together in the combining tree. Higher degrees will need less threads which might be helpful to improve the query performance by reducing the overhead of too many threads if the server has sufficiently powerful cpu cores.|8|
|`druid.query.groupBy.numParallelCombineThreads`|Hint for the number of parallel combining threads. This should be larger than 1 to turn on the parallel combining feature. The actual number of threads used for parallel combining is min(`druid.query.groupBy.numParallelCombineThreads`, `druid.processing.numThreads`).|1 (disabled)|
|`druid.query.groupBy.applyLimitPushDownToSegment`|If Broker pushes limit down to queryable nodes (historicals, peons) then limit results during segment scan.|true (enabled)|

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


##### GroupBy v1 configurations

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

##### Array based result rows

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



## Timeseries

These native query types take a timeseries query object and return an array of JSON objects where each object represents a value asked for by the timeseries query.

An example timeseries query object is shown below:

```json
{
  "queryType": "timeseries",
  "dataSource": "sample_datasource",
  "granularity": "day",
  "descending": "true",
  "filter": {
    "type": "and",
    "fields": [
      { "type": "selector", "dimension": "sample_dimension1", "value": "sample_value1" },
      { "type": "or",
        "fields": [
          { "type": "selector", "dimension": "sample_dimension2", "value": "sample_value2" },
          { "type": "selector", "dimension": "sample_dimension3", "value": "sample_value3" }
        ]
      }
    ]
  },
  "aggregations": [
    { "type": "longSum", "name": "sample_name1", "fieldName": "sample_fieldName1" },
    { "type": "doubleSum", "name": "sample_name2", "fieldName": "sample_fieldName2" }
  ],
  "postAggregations": [
    { "type": "arithmetic",
      "name": "sample_divide",
      "fn": "/",
      "fields": [
        { "type": "fieldAccess", "name": "postAgg__sample_name1", "fieldName": "sample_name1" },
        { "type": "fieldAccess", "name": "postAgg__sample_name2", "fieldName": "sample_name2" }
      ]
    }
  ],
  "intervals": [ "2012-01-01T00:00:00.000/2012-01-03T00:00:00.000" ]
}
```

There are 7 main parts to a timeseries query:

|property|description|required?|
|--------|-----------|---------|
|queryType|This String should always be "timeseries"; this is the first thing Apache Druid looks at to figure out how to interpret the query|yes|
|dataSource|A String or Object defining the data source to query, very similar to a table in a relational database. See [DataSource](../querying/datasource.md) for more information.|yes|
|descending|Whether to make descending ordered result. Default is `false`(ascending).|no|
|intervals|A JSON Object representing ISO-8601 Intervals. This defines the time ranges to run the query over.|yes|
|granularity|Defines the granularity to bucket query results. See [Granularities](../querying/granularities.md)|yes|
|filter|See [Filters](../querying/filters.md)|no|
|aggregations|See [Aggregations](../querying/aggregations.md)|no|
|postAggregations|See [Post Aggregations](../querying/post-aggregations.md)|no|
|limit|An integer that limits the number of results. The default is unlimited.|no|
|context|Can be used to modify query behavior, including [grand totals](#grand-totals) and [zero-filling](#zero-filling). See also [Context](../querying/query-context.md) for parameters that apply to all query types.|no|

To pull it all together, the above query would return 2 data points, one for each day between 2012-01-01 and 2012-01-03, from the "sample\_datasource" table. Each data point would be the (long) sum of sample\_fieldName1, the (double) sum of sample\_fieldName2 and the (double) result of sample\_fieldName1 divided by sample\_fieldName2 for the filter set. The output looks like this:

```json
[
  {
    "timestamp": "2012-01-01T00:00:00.000Z",
    "result": { "sample_name1": <some_value>, "sample_name2": <some_value>, "sample_divide": <some_value> }
  },
  {
    "timestamp": "2012-01-02T00:00:00.000Z",
    "result": { "sample_name1": <some_value>, "sample_name2": <some_value>, "sample_divide": <some_value> }
  }
]
```

### Grand totals

Druid can include an extra "grand totals" row as the last row of a timeseries result set. To enable this, add
`"grandTotal" : true` to your query context. For example:

```json
{
  "queryType": "timeseries",
  "dataSource": "sample_datasource",
  "intervals": [ "2012-01-01T00:00:00.000/2012-01-03T00:00:00.000" ],
  "granularity": "day",
  "aggregations": [
    { "type": "longSum", "name": "sample_name1", "fieldName": "sample_fieldName1" },
    { "type": "doubleSum", "name": "sample_name2", "fieldName": "sample_fieldName2" }
  ],
  "context": {
    "grandTotal": true
  }
}
```

The grand totals row will appear as the last row in the result array, and will have no timestamp. It will be the last
row even if the query is run in "descending" mode. Post-aggregations in the grand totals row will be computed based
upon the grand total aggregations.

### Zero-filling

Timeseries queries normally fill empty interior time buckets with zeroes. For example, if you issue a "day" granularity
timeseries query for the interval 2012-01-01/2012-01-04, and no data exists for 2012-01-02, you will receive:

```json
[
  {
    "timestamp": "2012-01-01T00:00:00.000Z",
    "result": { "sample_name1": <some_value> }
  },
  {
   "timestamp": "2012-01-02T00:00:00.000Z",
   "result": { "sample_name1": 0 }
  },
  {
    "timestamp": "2012-01-03T00:00:00.000Z",
    "result": { "sample_name1": <some_value> }
  }
]
```

Time buckets that lie completely outside the data interval are not zero-filled.

You can disable all zero-filling with the context flag "skipEmptyBuckets". In this mode, the data point for 2012-01-02
would be omitted from the results.

A query with this context flag set would look like:

```json
{
  "queryType": "timeseries",
  "dataSource": "sample_datasource",
  "granularity": "day",
  "aggregations": [
    { "type": "longSum", "name": "sample_name1", "fieldName": "sample_fieldName1" }
  ],
  "intervals": [ "2012-01-01T00:00:00.000/2012-01-04T00:00:00.000" ],
  "context" : {
    "skipEmptyBuckets": "true"
  }
}
```


## TopN queries

Apache Druid TopN queries return a sorted set of results for the values in a given dimension according to some criteria. Conceptually, they can be thought of as an approximate [GroupByQuery](../querying/groupbyquery.md) over a single dimension with an [Ordering](../querying/limitspec.md) spec. TopNs are much faster and resource efficient than GroupBys for this use case. These types of queries take a topN query object and return an array of JSON objects where each object represents a value asked for by the topN query.

TopNs are approximate in that each data process will rank their top K results and only return those top K results to the Broker. K, by default in Druid, is `max(1000, threshold)`. In practice, this means that if you ask for the top 1000 items ordered, the correctness of the first 900 items, approximately, will be 100%, and the ordering of the results after that is not guaranteed. TopNs can be made more accurate by increasing the threshold.

A topN query object looks like:

```json
{
  "queryType": "topN",
  "dataSource": "sample_data",
  "dimension": "sample_dim",
  "threshold": 5,
  "metric": "count",
  "granularity": "all",
  "filter": {
    "type": "and",
    "fields": [
      {
        "type": "selector",
        "dimension": "dim1",
        "value": "some_value"
      },
      {
        "type": "selector",
        "dimension": "dim2",
        "value": "some_other_val"
      }
    ]
  },
  "aggregations": [
    {
      "type": "longSum",
      "name": "count",
      "fieldName": "count"
    },
    {
      "type": "doubleSum",
      "name": "some_metric",
      "fieldName": "some_metric"
    }
  ],
  "postAggregations": [
    {
      "type": "arithmetic",
      "name": "average",
      "fn": "/",
      "fields": [
        {
          "type": "fieldAccess",
          "name": "some_metric",
          "fieldName": "some_metric"
        },
        {
          "type": "fieldAccess",
          "name": "count",
          "fieldName": "count"
        }
      ]
    }
  ],
  "intervals": [
    "2013-08-31T00:00:00.000/2013-09-03T00:00:00.000"
  ]
}
```

There are 11 parts to a topN query.

|property|description|required?|
|--------|-----------|---------|
|queryType|This String should always be "topN"; this is the first thing Druid looks at to figure out how to interpret the query|yes|
|dataSource|A String or Object defining the data source to query, very similar to a table in a relational database. See [DataSource](../querying/datasource.md) for more information.|yes|
|intervals|A JSON Object representing ISO-8601 Intervals. This defines the time ranges to run the query over.|yes|
|granularity|Defines the granularity to bucket query results. See [Granularities](../querying/granularities.md)|yes|
|filter|See [Filters](../querying/filters.md)|no|
|aggregations|See [Aggregations](../querying/aggregations.md)|for numeric metricSpec, aggregations or postAggregations should be specified. Otherwise no.|
|postAggregations|See [Post Aggregations](../querying/post-aggregations.md)|for numeric metricSpec, aggregations or postAggregations should be specified. Otherwise no.|
|dimension|A String or JSON object defining the dimension that you want the top taken for. For more info, see [DimensionSpecs](../querying/dimensionspecs.md)|yes|
|threshold|An integer defining the N in the topN (i.e. how many results you want in the top list)|yes|
|metric|A String or JSON object specifying the metric to sort by for the top list. For more info, see [TopNMetricSpec](../querying/topnmetricspec.md).|yes|
|context|See [Context](../querying/query-context.md)|no|

Please note the context JSON object is also available for topN queries and should be used with the same caution as the timeseries case.
The format of the results would look like so:

```json
[
  {
    "timestamp": "2013-08-31T00:00:00.000Z",
    "result": [
      {
        "dim1": "dim1_val",
        "count": 111,
        "some_metrics": 10669,
        "average": 96.11711711711712
      },
      {
        "dim1": "another_dim1_val",
        "count": 88,
        "some_metrics": 28344,
        "average": 322.09090909090907
      },
      {
        "dim1": "dim1_val3",
        "count": 70,
        "some_metrics": 871,
        "average": 12.442857142857143
      },
      {
        "dim1": "dim1_val4",
        "count": 62,
        "some_metrics": 815,
        "average": 13.14516129032258
      },
      {
        "dim1": "dim1_val5",
        "count": 60,
        "some_metrics": 2787,
        "average": 46.45
      }
    ]
  }
]
```

### Behavior on multi-value dimensions

topN queries can group on multi-value dimensions. When grouping on a multi-value dimension, _all_ values
from matching rows will be used to generate one group per value. It's possible for a query to return more groups than
there are rows. For example, a topN on the dimension `tags` with filter `"t1" AND "t3"` would match only row1, and
generate a result with three groups: `t1`, `t2`, and `t3`. If you only need to include values that match
your filter, you can use a [filtered dimensionSpec](dimensionspecs.html#filtered-dimensionspecs). This can also
improve performance.

See [Multi-value dimensions](multi-value-dimensions.html) for more details.

### Aliasing

The current TopN algorithm is an approximate algorithm. The top 1000 local results from each segment are returned for merging to determine the global topN. As such, the topN algorithm is approximate in both rank and results. Approximate results *ONLY APPLY WHEN THERE ARE MORE THAN 1000 DIM VALUES*. A topN over a dimension with fewer than 1000 unique dimension values can be considered accurate in rank and accurate in aggregates.

The threshold can be modified from it's default 1000 via the server parameter `druid.query.topN.minTopNThreshold` which need to restart servers to take effect or set `minTopNThreshold` in query context which take effect per query.

If you are wanting the top 100 of a high cardinality, uniformly distributed dimension ordered by some low-cardinality, uniformly distributed dimension, you are potentially going to get aggregates back that are missing data.

To put it another way, the best use cases for topN are when you can have confidence that the overall results are uniformly in the top. For example, if a particular site ID is in the top 10 for some metric for every hour of every day, then it will probably be accurate in the topN over multiple days. But if a site barely in the top 1000 for any given hour, but over the whole query granularity is in the top 500 (example: a site which gets highly uniform traffic co-mingling in the dataset with sites with highly periodic data), then a top500 query may not have that particular site a the exact rank, and may not be accurate for that particular site's aggregates.

Before continuing in this section, please consider if you really need exact results. Getting exact results is a very resource intensive process. For the vast majority of "useful" data results, an approximate topN algorithm supplies plenty of accuracy.

Users wishing to get an *exact rank and exact aggregates* topN over a dimension with greater than 1000 unique values should issue a groupBy query and sort the results themselves. This is very computationally expensive for high-cardinality dimensions.

Users who can tolerate *approximate rank* topN over a dimension with greater than 1000 unique values, but require *exact aggregates* can issue two queries. One to get the approximate topN dimension values, and another topN with dimension selection filters which only use the topN results of the first.

### Example First query

```json
{
    "aggregations": [
         {
             "fieldName": "L_QUANTITY_longSum",
             "name": "L_QUANTITY_",
             "type": "longSum"
         }
    ],
    "dataSource": "tpch_year",
    "dimension":"l_orderkey",
    "granularity": "all",
    "intervals": [
        "1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z"
    ],
    "metric": "L_QUANTITY_",
    "queryType": "topN",
    "threshold": 2
}
```

### Example second query

```json
{
    "aggregations": [
         {
             "fieldName": "L_TAX_doubleSum",
             "name": "L_TAX_",
             "type": "doubleSum"
         },
         {
             "fieldName": "L_DISCOUNT_doubleSum",
             "name": "L_DISCOUNT_",
             "type": "doubleSum"
         },
         {
             "fieldName": "L_EXTENDEDPRICE_doubleSum",
             "name": "L_EXTENDEDPRICE_",
             "type": "doubleSum"
         },
         {
             "fieldName": "L_QUANTITY_longSum",
             "name": "L_QUANTITY_",
             "type": "longSum"
         },
         {
             "name": "count",
             "type": "count"
         }
    ],
    "dataSource": "tpch_year",
    "dimension":"l_orderkey",
    "filter": {
        "fields": [
            {
                "dimension": "l_orderkey",
                "type": "selector",
                "value": "103136"
            },
            {
                "dimension": "l_orderkey",
                "type": "selector",
                "value": "1648672"
            }
        ],
        "type": "or"
    },
    "granularity": "all",
    "intervals": [
        "1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z"
    ],
    "metric": "L_QUANTITY_",
    "queryType": "topN",
    "threshold": 2
}
```
