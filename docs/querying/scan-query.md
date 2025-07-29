---
id: scan-query
title: "Scan queries"
sidebar_label: "Scan"
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

:::info
 Apache Druid supports two query languages: [Druid SQL](sql.md) and [native queries](querying.md).
 This document describes a query
 type in the native language. For information about when Druid SQL will use this query type, refer to the
 [SQL documentation](sql-translation.md#query-types).
:::

The Scan query returns raw Apache Druid rows in streaming mode.  

In addition to straightforward usage where a Scan query is issued to the Broker, the Scan query can also be issued
directly to Historical processes or streaming ingestion tasks. This can be useful if you want to retrieve large
amounts of data in parallel.

An example Scan query object is shown below:

```json
 {
   "queryType": "scan",
   "dataSource": "wikipedia",
   "resultFormat": "list",
   "columns":[ "__time", "isRobot", "page","added", "isAnonymous", "user", "deleted" ],
   "intervals": [
     "2016-01-01/2017-01-02"
   ],
   "batchSize":20480,
   "limit":2
 }
```

The following are the main parameters for Scan queries:

|property|description|required?|
|--------|-----------|---------|
|queryType|This String should always be "scan"; this is the first thing Druid looks at to figure out how to interpret the query|yes|
|dataSource|A String or Object defining the data source to query, very similar to a table in a relational database. See [DataSource](../querying/datasource.md) for more information.|yes|
|intervals|A JSON Object representing ISO-8601 Intervals. This defines the time ranges to run the query over.|yes|
|resultFormat|How the results are represented: list, compactedList or valueVector. Currently only `list` and `compactedList` are supported. Default is `list`|no|
|filter|See [Filters](../querying/filters.md)|no|
|columns|A String array of dimensions and metrics to scan. If left empty, all dimensions and metrics are returned.|no|
|batchSize|The maximum number of rows buffered before being returned to the client. Default is `20480`|no|
|limit|How many rows to return. If not specified, all rows will be returned.|no|
|offset|Skip this many rows when returning results. Skipped rows will still need to be generated internally and then discarded, meaning that raising offsets to high values can cause queries to use additional resources.<br /><br />Together, "limit" and "offset" can be used to implement pagination. However, note that if the underlying datasource is modified in between page fetches in ways that affect overall query results, then the different pages will not necessarily align with each other.|no|
|order|The ordering of returned rows based on timestamp.  "ascending", "descending", and "none" (default) are supported.  Currently, "ascending" and "descending" are only supported for queries where the `__time` column is included in the `columns` field and the requirements outlined in the [time ordering](#time-ordering) section are met.|none|
|context|An additional JSON Object which can be used to specify certain flags (see the `query context properties` section below).|no|

## Example results

The format of the result when resultFormat equals `list`:

```json
 [ {
  "segmentId" : "wikipedia_2016-06-27T00:00:00.000Z_2016-06-28T00:00:00.000Z_2024-12-17T13:08:03.142Z",
  "columns" : [ "__time", "isRobot", "page","added", "isAnonymous", "user", "deleted" ],
  "events" : [ {
    "__time" : 1466985611080,
    "isRobot" : "true",
    "page" : "Salo Toraut",
    "added" : 31,
    "isAnonymous" : "false",
    "user" : "Lsjbot",
    "deleted" : 0
  }, {
    "__time" : 1466985634959,
    "isRobot" : "false",
    "page" : "Bailando 2015",
    "added" : 2,
    "isAnonymous" : "true",
    "user" : "181.230.118.178",
    "deleted" : 0
  } ],
  "rowSignature" : [ {
    "name" : "__time",
    "type" : "LONG"
  }, {
    "name" : "isRobot",
    "type" : "STRING"
  }, {
    "name" : "page",
    "type" : "STRING"
  }, {
    "name" : "added",
    "type" : "LONG"
  }, {
    "name" : "isAnonymous",
    "type" : "STRING"
  }, {
    "name" : "user",
    "type" : "STRING"
  }, {
    "name" : "deleted",
    "type" : "LONG"
  } ]
} ]
```

The format of the result when resultFormat equals `compactedList`:

```json
 [ {
  "segmentId" : "wikipedia_2016-06-27T00:00:00.000Z_2016-06-28T00:00:00.000Z_2024-12-17T13:08:03.142Z",
  "columns" : [ "__time", "isRobot", "isUnpatrolled", "page","added", "isNew", "delta", "isAnonymous", "user", "deleted", "namespace" ],
  "events" : [
    [ 1466985611080, "true", "Salo Toraut", 31, "false", "Lsjbot", 0 ],
    [ 1466985634959, "false", "Bailando 2015", 2, "true", "181.230.118.178", 0]
  ],
  "rowSignature" : [ {
    "name" : "__time",
    "type" : "LONG"
  }, {
    "name" : "isRobot",
    "type" : "STRING"
  }, {
    "name" : "page",
    "type" : "STRING"
  }, {
    "name" : "added",
    "type" : "LONG"
  }, {
    "name" : "isAnonymous",
    "type" : "STRING"
  }, {
    "name" : "user",
    "type" : "STRING"
  }, {
    "name" : "deleted",
    "type" : "LONG"
  } ]
} ]
```

## Time ordering

The Scan query currently supports ordering based on timestamp.  Note that using time ordering will yield results that
do not indicate which segment rows are from (`segmentId` will show up as `null`).  Furthermore, time ordering is only
supported where the result set limit is less than `druid.query.scan.maxRowsQueuedForOrdering` rows **or** all segments
scanned have fewer than `druid.query.scan.maxSegmentPartitionsOrderedInMemory` partitions.  Also, time ordering is not
supported for queries issued directly to historicals unless a list of segments is specified.  The reasoning behind
these limitations is that the implementation of time ordering uses two strategies that can consume too much heap memory
if left unbounded.  These strategies (listed below) are chosen on a per-Historical basis depending on query result set
limit and the number of segments being scanned.

1. Priority Queue: Each segment on a Historical is opened sequentially.  Every row is added to a bounded priority
queue which is ordered by timestamp.  For every row above the result set limit, the row with the earliest (if descending)
or latest (if ascending) timestamp will be dequeued.  After every row has been processed, the sorted contents of the
priority queue are streamed back to the Broker(s) in batches.  Attempting to load too many rows into memory runs the
risk of Historical nodes running out of memory.  The `druid.query.scan.maxRowsQueuedForOrdering` property protects
from this by limiting the number of rows in the query result set when time ordering is used.

2. N-Way Merge: For each segment, each partition is opened in parallel.  Since each partition's rows are already
time-ordered, an n-way merge can be performed on the results from each partition.  This approach doesn't persist the entire
result set in memory (like the Priority Queue) as it streams back batches as they are returned from the merge function.
However, attempting to query too many partition could also result in high memory usage due to the need to open
decompression and decoding buffers for each.  The `druid.query.scan.maxSegmentPartitionsOrderedInMemory` limit protects
from this by capping the number of partitions opened at any times when time ordering is used.

Both `druid.query.scan.maxRowsQueuedForOrdering` and `druid.query.scan.maxSegmentPartitionsOrderedInMemory` are
configurable and can be tuned based on hardware specs and number of dimensions being queried.  These config properties
can also be overridden using the `maxRowsQueuedForOrdering` and `maxSegmentPartitionsOrderedInMemory` properties in
the query context (see the Query Context Properties section).

## Configuration Properties

Configuration properties:

|property|description|values|default|
|--------|-----------|------|-------|
|druid.query.scan.maxRowsQueuedForOrdering|The maximum number of rows returned when time ordering is used|An integer in [1, 2147483647]|100000|
|druid.query.scan.maxSegmentPartitionsOrderedInMemory|The maximum number of segments scanned per historical when time ordering is used|An integer in [1, 2147483647]|50|


## Query context properties

|property|description|values|default|
|--------|-----------|------|-------|
|maxRowsQueuedForOrdering|The maximum number of rows returned when time ordering is used.  Overrides the identically named config.|An integer in [1, 2147483647]|`druid.query.scan.maxRowsQueuedForOrdering`|
|maxSegmentPartitionsOrderedInMemory|The maximum number of segments scanned per historical when time ordering is used.  Overrides the identically named config.|An integer in [1, 2147483647]|`druid.query.scan.maxSegmentPartitionsOrderedInMemory`|

Sample query context JSON object:

```json
{
  "maxRowsQueuedForOrdering": 100001,
  "maxSegmentPartitionsOrderedInMemory": 100
}
```

## Legacy mode

In older versions of Druid, the scan query supported a legacy mode designed for protocol compatibility with the former scan-query contrib extension from versions of Druid older than 0.11. This mode has been removed.
