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


The Scan query returns raw Apache Druid (incubating) rows in streaming mode.  The biggest difference between the Select query and the Scan
query is that the Scan query does not retain all the returned rows in memory before they are returned to the client.
The Select query _will_ retain the rows in memory, causing memory pressure if too many rows are returned.
The Scan query can return all the rows without issuing another pagination query.

In addition to straightforward usage where a Scan query is issued to the Broker, the Scan query can also be issued
directly to Historical processes or streaming ingestion tasks. This can be useful if you want to retrieve large
amounts of data in parallel.

An example Scan query object is shown below:

```json
 {
   "queryType": "scan",
   "dataSource": "wikipedia",
   "resultFormat": "list",
   "columns":[],
   "intervals": [
     "2013-01-01/2013-01-02"
   ],
   "batchSize":20480,
   "limit":3
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
|order|The ordering of returned rows based on timestamp.  "ascending", "descending", and "none" (default) are supported.  Currently, "ascending" and "descending" are only supported for queries where the `__time` column is included in the `columns` field and the requirements outlined in the [time ordering](#time-ordering) section are met.|none|
|legacy|Return results consistent with the legacy "scan-query" contrib extension. Defaults to the value set by `druid.query.scan.legacy`, which in turn defaults to false. See [Legacy mode](#legacy-mode) for details.|no|
|context|An additional JSON Object which can be used to specify certain flags (see the `query context properties` section below).|no|

## Example results

The format of the result when resultFormat equals `list`:

```json
 [{
    "segmentId" : "wikipedia_editstream_2012-12-29T00:00:00.000Z_2013-01-10T08:00:00.000Z_2013-01-10T08:13:47.830Z_v9",
    "columns" : [
      "timestamp",
      "robot",
      "namespace",
      "anonymous",
      "unpatrolled",
      "page",
      "language",
      "newpage",
      "user",
      "count",
      "added",
      "delta",
      "variation",
      "deleted"
    ],
    "events" : [ {
        "timestamp" : "2013-01-01T00:00:00.000Z",
        "robot" : "1",
        "namespace" : "article",
        "anonymous" : "0",
        "unpatrolled" : "0",
        "page" : "11._korpus_(NOVJ)",
        "language" : "sl",
        "newpage" : "0",
        "user" : "EmausBot",
        "count" : 1.0,
        "added" : 39.0,
        "delta" : 39.0,
        "variation" : 39.0,
        "deleted" : 0.0
    }, {
        "timestamp" : "2013-01-01T00:00:00.000Z",
        "robot" : "0",
        "namespace" : "article",
        "anonymous" : "0",
        "unpatrolled" : "0",
        "page" : "112_U.S._580",
        "language" : "en",
        "newpage" : "1",
        "user" : "MZMcBride",
        "count" : 1.0,
        "added" : 70.0,
        "delta" : 70.0,
        "variation" : 70.0,
        "deleted" : 0.0
    }, {
        "timestamp" : "2013-01-01T00:00:00.000Z",
        "robot" : "0",
        "namespace" : "article",
        "anonymous" : "0",
        "unpatrolled" : "0",
        "page" : "113_U.S._243",
        "language" : "en",
        "newpage" : "1",
        "user" : "MZMcBride",
        "count" : 1.0,
        "added" : 77.0,
        "delta" : 77.0,
        "variation" : 77.0,
        "deleted" : 0.0
    } ]
} ]
```

The format of the result when resultFormat equals `compactedList`:

```json
 [{
    "segmentId" : "wikipedia_editstream_2012-12-29T00:00:00.000Z_2013-01-10T08:00:00.000Z_2013-01-10T08:13:47.830Z_v9",
    "columns" : [
      "timestamp", "robot", "namespace", "anonymous", "unpatrolled", "page", "language", "newpage", "user", "count", "added", "delta", "variation", "deleted"
    ],
    "events" : [
     ["2013-01-01T00:00:00.000Z", "1", "article", "0", "0", "11._korpus_(NOVJ)", "sl", "0", "EmausBot", 1.0, 39.0, 39.0, 39.0, 0.0],
     ["2013-01-01T00:00:00.000Z", "0", "article", "0", "0", "112_U.S._580", "en", "1", "MZMcBride", 1.0, 70.0, 70.0, 70.0, 0.0],
     ["2013-01-01T00:00:00.000Z", "0", "article", "0", "0", "113_U.S._243", "en", "1", "MZMcBride", 1.0, 77.0, 77.0, 77.0, 0.0]
    ]
} ]
```

## Time ordering

The Scan query currently supports ordering based on timestamp for non-legacy queries.  Note that using time ordering
will yield results that do not indicate which segment rows are from (`segmentId` will show up as `null`).  Furthermore,
time ordering is only supported where the result set limit is less than `druid.query.scan.maxRowsQueuedForOrdering`
rows **or** all segments scanned have fewer than `druid.query.scan.maxSegmentPartitionsOrderedInMemory` partitions.  Also,
time ordering is not supported for queries issued directly to historicals unless a list of segments is specified.  The
reasoning behind these limitations is that the implementation of time ordering uses two strategies that can consume too
much heap memory if left unbounded.  These strategies (listed below) are chosen on a per-Historical basis depending on
query result set limit and the number of segments being scanned.

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

## Legacy mode

The Scan query supports a legacy mode designed for protocol compatibility with the former scan-query contrib extension.
In legacy mode you can expect the following behavior changes:

- The `__time` column is returned as `"timestamp"` rather than `"__time"`. This will take precedence over any other column
you may have that is named `"timestamp"`.
- The `__time` column is included in the list of columns even if you do not specifically ask for it.
- Timestamps are returned as ISO8601 time strings rather than integers (milliseconds since 1970-01-01 00:00:00 UTC).

Legacy mode can be triggered either by passing `"legacy" : true` in your query JSON, or by setting
`druid.query.scan.legacy = true` on your Druid processes. If you were previously using the scan-query contrib extension,
the best way to migrate is to activate legacy mode during a rolling upgrade, then switch it off after the upgrade
is complete.

## Configuration Properties

Configuration properties:

|property|description|values|default|
|--------|-----------|------|-------|
|druid.query.scan.maxRowsQueuedForOrdering|The maximum number of rows returned when time ordering is used|An integer in [1, 2147483647]|100000|
|druid.query.scan.maxSegmentPartitionsOrderedInMemory|The maximum number of segments scanned per historical when time ordering is used|An integer in [1, 2147483647]|50|
|druid.query.scan.legacy|Whether legacy mode should be turned on for Scan queries|true or false|false|


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
