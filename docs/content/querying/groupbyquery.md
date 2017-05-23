---
layout: doc_page
---
# groupBy Queries

These types of queries take a groupBy query object and return an array of JSON objects where each object represents a
grouping asked for by the query.

<div class="note info">
Note: If you are doing aggregations with time as your only grouping, or an ordered groupBy over a single dimension,
consider <a href="timeseriesquery.html">Timeseries</a> and <a href="topnquery.html">TopN</a> queries as well as
groupBy. Their performance may be better in some cases. See <a href="#alternatives">Alternatives</a> below for more details.
</div>

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

There are 11 main parts to a groupBy query:

|property|description|required?|
|--------|-----------|---------|
|queryType|This String should always be "groupBy"; this is the first thing Druid looks at to figure out how to interpret the query|yes|
|dataSource|A String or Object defining the data source to query, very similar to a table in a relational database. See [DataSource](../querying/datasource.html) for more information.|yes|
|dimensions|A JSON list of dimensions to do the groupBy over; or see [DimensionSpec](../querying/dimensionspecs.html) for ways to extract dimensions. |yes|
|limitSpec|See [LimitSpec](../querying/limitspec.html).|no|
|having|See [Having](../querying/having.html).|no|
|granularity|Defines the granularity of the query. See [Granularities](../querying/granularities.html)|yes|
|filter|See [Filters](../querying/filters.html)|no|
|aggregations|See [Aggregations](../querying/aggregations.html)|no|
|postAggregations|See [Post Aggregations](../querying/post-aggregations.html)|no|
|intervals|A JSON Object representing ISO-8601 Intervals. This defines the time ranges to run the query over.|yes|
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

### Implementation details

#### Strategies

GroupBy queries can be executed using two different strategies. The default strategy for a cluster is determined by the
"druid.query.groupBy.defaultStrategy" runtime property on the broker. This can be overridden using "groupByStrategy" in
the query context. If neither the context field nor the property is set, the "v2" strategy will be used.

- "v2", the default, is designed to offer better performance and memory management. This strategy generates
per-segment results using a fully off-heap map. Data nodes merge the per-segment results using a fully off-heap
concurrent facts map combined with an on-heap string dictionary. This may optionally involve spilling to disk. Data
nodes return sorted results to the broker, which merges result streams using an N-way merge. The broker materializes
the results if necessary (e.g. if the query sorts on columns other than its dimensions). Otherwise, it streams results
back as they are merged.

- "v1", a legacy engine, generates per-segment results on data nodes (historical, realtime, middleManager) using a map which
is partially on-heap (dimension keys and the map itself) and partially off-heap (the aggregated values). Data nodes then
merge the per-segment results using Druid's indexing mechanism. This merging is multi-threaded by default, but can
optionally be single-threaded. The broker merges the final result set using Druid's indexing mechanism again. The broker
merging is always single-threaded. Because the broker merges results using the indexing mechanism, it must materialize
the full result set before returning any results. On both the data nodes and the broker, the merging index is fully
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
- groupBy v1 supports caching on either the broker or historical nodes, whereas groupBy v2 only supports caching on
historical nodes.
- groupBy v1 supports using [chunkPeriod](query-context.html) to parallelize merging on the broker, whereas groupBy v2
ignores chunkPeriod.

#### Memory tuning and resource limits

When using groupBy v2, three parameters control resource usage and limits:

- druid.processing.buffer.sizeBytes: size of the off-heap hash table used for aggregation, per query, in bytes. At
most druid.processing.numMergeBuffers of these will be created at once, which also serves as an upper limit on the
number of concurrently running groupBy queries.
- druid.query.groupBy.maxMergingDictionarySize: size of the on-heap dictionary used when grouping on strings, per query,
in bytes. Note that this is based on a rough estimate of the dictionary size, not the actual size.
- druid.query.groupBy.maxOnDiskStorage: amount of space on disk used for aggregation, per query, in bytes. By default,
this is 0, which means aggregation will not use disk.

If maxOnDiskStorage is 0 (the default) then a query that exceeds either the on-heap dictionary limit, or the off-heap
aggregation table limit, will fail with a "Resource limit exceeded" error describing the limit that was exceeded.

If maxOnDiskStorage is greater than 0, queries that exceed the in-memory limits will start using disk for aggregation.
In this case, when either the on-heap dictionary or off-heap hash table fills up, partially aggregated records will be
sorted and flushed to disk. Then, both in-memory structures will be cleared out for further aggregation. Queries that
then go on to exceed maxOnDiskStorage will fail with a "Resource limit exceeded" error indicating that they ran out of
disk space.

With groupBy v2, cluster operators should make sure that the off-heap hash tables and on-heap merging dictionaries
will not exceed available memory for the maximum possible concurrent query load (given by
druid.processing.numMergeBuffers).

When using groupBy v1, all aggregation is done on-heap, and resource limits are done through the parameter
druid.query.groupBy.maxResults. This is a cap on the maximum number of results in a result set. Queries that exceed
this limit will fail with a "Resource limit exceeded" error indicating they exceeded their row limit. Cluster
operators should make sure that the on-heap aggregations will not exceed available JVM heap space for the expected
concurrent query load.

#### Alternatives

There are some situations where other query types may be a better choice than groupBy.

- For queries with no "dimensions" (i.e. grouping by time only) the [Timeseries query](timeseriesquery.html) will
generally be faster than groupBy. The major differences are that it is implemented in a fully streaming manner (taking
advantage of the fact that segments are already sorted on time) and does not need to use a hash table for merging.

- For queries with a single "dimensions" element (i.e. grouping by one string dimension), the [TopN query](topnquery.html)
will sometimes be faster than groupBy. This is especially true if you are ordering by a metric and find approximate
results acceptable.

#### Nested groupBys

Nested groupBys (dataSource of type "query") are performed differently for "v1" and "v2". The broker first runs the
inner groupBy query in the usual way. "v1" strategy then materializes the inner query's results on-heap with Druid's
indexing mechanism, and runs the outer query on these materialized results. "v2" strategy runs the outer query on the
inner query's results stream with off-heap fact map and on-heap string dictionary that can spill to disk. Both
strategy perform the outer query on the broker in a single-threaded fashion.

#### Server configuration

When using the "v2" strategy, the following runtime properties apply:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.query.groupBy.defaultStrategy`|Default groupBy query strategy.|v2|
|`druid.query.groupBy.bufferGrouperInitialBuckets`|Initial number of buckets in the off-heap hash table used for grouping results. Set to 0 to use a reasonable default.|0|
|`druid.query.groupBy.bufferGrouperMaxLoadFactor`|Maximum load factor of the off-heap hash table used for grouping results. When the load factor exceeds this size, the table will be grown or spilled to disk. Set to 0 to use a reasonable default.|0|
|`druid.query.groupBy.maxMergingDictionarySize`|Maximum amount of heap space (approximately) to use for the string dictionary during merging. When the dictionary exceeds this size, a spill to disk will be triggered.|100000000|
|`druid.query.groupBy.maxOnDiskStorage`|Maximum amount of disk space to use, per-query, for spilling result sets to disk when either the merging buffer or the dictionary fills up. Queries that exceed this limit will fail. Set to zero to disable disk spilling.|0 (disabled)|
|`druid.query.groupBy.singleThreaded`|Merge results using a single thread.|false|

This may require allocating more direct memory. The amount of direct memory needed by Druid is at least
`druid.processing.buffer.sizeBytes * (druid.processing.numMergeBuffers + druid.processing.numThreads + 1)`. You can
ensure at least this amount of direct memory is available by providing `-XX:MaxDirectMemorySize=<VALUE>` at the command
line.

When using the "v1" strategy, the following runtime properties apply:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.query.groupBy.defaultStrategy`|Default groupBy query strategy.|v2|
|`druid.query.groupBy.maxIntermediateRows`|Maximum number of intermediate rows for the per-segment grouping engine. This is a tuning parameter that does not impose a hard limit; rather, it potentially shifts merging work from the per-segment engine to the overall merging index. Queries that exceed this limit will not fail.|50000|
|`druid.query.groupBy.maxResults`|Maximum number of results. Queries that exceed this limit will fail.|500000|
|`druid.query.groupBy.singleThreaded`|Merge results using a single thread.|false|

#### Query context

When using the "v2" strategy, the following query context parameters apply:

|Property|Description|
|--------|-----------|
|`groupByStrategy`|Overrides the value of `druid.query.groupBy.defaultStrategy` for this query.|
|`groupByIsSingleThreaded`|Overrides the value of `druid.query.groupBy.singleThreaded` for this query.|
|`bufferGrouperInitialBuckets`|Overrides the value of `druid.query.groupBy.bufferGrouperInitialBuckets` for this query.|
|`bufferGrouperMaxLoadFactor`|Overrides the value of `druid.query.groupBy.bufferGrouperMaxLoadFactor` for this query.|
|`maxMergingDictionarySize`|Can be used to lower the value of `druid.query.groupBy.maxMergingDictionarySize` for this query.|
|`maxOnDiskStorage`|Can be used to lower the value of `druid.query.groupBy.maxOnDiskStorage` for this query.|
|`sortByDimsFirst`|Sort the results first by dimension values and then by timestamp.|
|`forcePushDownLimit`|When all fields in the orderby are part of the grouping key, the broker will push limit application down to the historical nodes. When the sorting order uses fields that are not in the grouping key, applying this optimization can result in approximate results with unknown accuracy, so this optimization is disabled by default in that case. Enabling this context flag turns on limit push down for limit/orderbys that contain non-grouping key columns.|

When using the "v1" strategy, the following query context parameters apply:

|Property|Description|
|--------|-----------|
|`groupByStrategy`|Overrides the value of `druid.query.groupBy.defaultStrategy` for this query.|
|`groupByIsSingleThreaded`|Overrides the value of `druid.query.groupBy.singleThreaded` for this query.|
|`maxIntermediateRows`|Can be used to lower the value of `druid.query.groupBy.maxIntermediateRows` for this query.|
|`maxResults`|Can be used to lower the value of `druid.query.groupBy.maxResults` for this query.|
|`useOffheap`|Set to true to store aggregations off-heap when merging results.|
