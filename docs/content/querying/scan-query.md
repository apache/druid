---
layout: doc_page
title: "Scan query"
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

# Scan query

Scan query returns raw Druid rows in streaming mode.

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
   "limit":5
 }
```

There are several main parts to a scan query:

|property|description|required?|
|--------|-----------|---------|
|queryType|This String should always be "scan"; this is the first thing Druid looks at to figure out how to interpret the query|yes|
|dataSource|A String or Object defining the data source to query, very similar to a table in a relational database. See [DataSource](../querying/datasource.html) for more information.|yes|
|intervals|A JSON Object representing ISO-8601 Intervals. This defines the time ranges to run the query over.|yes|
|resultFormat|How result represented, list or compactedList or valueVector. Currently only `list` and `compactedList` are supported. Default is `list`|no|
|filter|See [Filters](../querying/filters.html)|no|
|columns|A String array of dimensions and metrics to scan. If left empty, all dimensions and metrics are returned.|no|
|batchSize|How many rows buffered before return to client. Default is `20480`|no|
|limit|How many rows to return. If not specified, all rows will be returned.|no|
|legacy|Return results consistent with the legacy "scan-query" contrib extension. Defaults to the value set by `druid.query.scan.legacy`, which in turn defaults to false. See [Legacy mode](#legacy-mode) for details.|no|
|context|An additional JSON Object which can be used to specify certain flags.|no|

## Example results

The format of the result when resultFormat equals to `list`:

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
    }, {
        "timestamp" : "2013-01-01T00:00:00.000Z",
        "robot" : "0",
        "namespace" : "article",
        "anonymous" : "0",
        "unpatrolled" : "0",
        "page" : "113_U.S._73",
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
        "page" : "113_U.S._756",
        "language" : "en",
        "newpage" : "1",
        "user" : "MZMcBride",
        "count" : 1.0,
        "added" : 68.0,
        "delta" : 68.0,
        "variation" : 68.0,
        "deleted" : 0.0
    } ]
} ]
```

The format of the result when resultFormat equals to `compactedList`:

```json
 [{
    "segmentId" : "wikipedia_editstream_2012-12-29T00:00:00.000Z_2013-01-10T08:00:00.000Z_2013-01-10T08:13:47.830Z_v9",
    "columns" : [
      "timestamp", "robot", "namespace", "anonymous", "unpatrolled", "page", "language", "newpage", "user", "count", "added", "delta", "variation", "deleted"
    ],
    "events" : [
     ["2013-01-01T00:00:00.000Z", "1", "article", "0", "0", "11._korpus_(NOVJ)", "sl", "0", "EmausBot", 1.0, 39.0, 39.0, 39.0, 0.0],
     ["2013-01-01T00:00:00.000Z", "0", "article", "0", "0", "112_U.S._580", "en", "1", "MZMcBride", 1.0, 70.0, 70.0, 70.0, 0.0],
     ["2013-01-01T00:00:00.000Z", "0", "article", "0", "0", "113_U.S._243", "en", "1", "MZMcBride", 1.0, 77.0, 77.0, 77.0, 0.0],
     ["2013-01-01T00:00:00.000Z", "0", "article", "0", "0", "113_U.S._73", "en", "1", "MZMcBride", 1.0, 70.0, 70.0, 70.0, 0.0],
     ["2013-01-01T00:00:00.000Z", "0", "article", "0", "0", "113_U.S._756", "en", "1", "MZMcBride", 1.0, 68.0, 68.0, 68.0, 0.0]
    ]
} ]
```

The biggest difference between select query and scan query is that, scan query doesn't retain all rows in memory before rows can be returned to client.  
It will cause memory pressure if too many rows required by select query.  
Scan query doesn't have this issue.  
Scan query can return all rows without issuing another pagination query, which is extremely useful when query against Historical or realtime process directly.

## Legacy mode

The Scan query supports a legacy mode designed for protocol compatibility with the former scan-query contrib extension.
In legacy mode you can expect the following behavior changes:

- The __time column is returned as "timestamp" rather than "__time". This will take precedence over any other column
you may have that is named "timestamp".
- The __time column is included in the list of columns even if you do not specifically ask for it.
- Timestamps are returned as ISO8601 time strings rather than integers (milliseconds since 1970-01-01 00:00:00 UTC).

Legacy mode can be triggered either by passing `"legacy" : true` in your query JSON, or by setting
`druid.query.scan.legacy = true` on your Druid processes. If you were previously using the scan-query contrib extension,
the best way to migrate is to activate legacy mode during a rolling upgrade, then switch it off after the upgrade
is complete.
