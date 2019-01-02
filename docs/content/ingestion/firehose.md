---
layout: doc_page
title: "Druid Firehoses"
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

# Druid Firehoses

Firehoses are used in [native batch ingestion tasks](../ingestion/native_tasks.html), stream push tasks automatically created by [Tranquility](../ingestion/stream-push.html), and the [stream-pull (deprecated)](../ingestion/stream-pull.html) ingestion model.

They are pluggable and thus the configuration schema can and will vary based on the `type` of the firehose.

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| type | String | Specifies the type of firehose. Each value will have its own configuration schema, firehoses packaged with Druid are described below. | yes |

## Additional Firehoses

There are several firehoses readily available in Druid, some are meant for examples, others can be used directly in a production environment.

For additional firehoses, please see our [extensions list](../development/extensions.html).

### LocalFirehose

This Firehose can be used to read the data from files on local disk.
It can be used for POCs to ingest data on disk.
This firehose is _splittable_ and can be used by [native parallel index tasks](./native_tasks.html#parallel-index-task).
Since each split represents a file in this firehose, each worker task of `index_parallel` will read a file.
A sample local firehose spec is shown below:

```json
{
    "type"    : "local",
    "filter"   : "*.csv",
    "baseDir"  : "/data/directory"
}
```

|property|description|required?|
|--------|-----------|---------|
|type|This should be "local".|yes|
|filter|A wildcard filter for files. See [here](http://commons.apache.org/proper/commons-io/apidocs/org/apache/commons/io/filefilter/WildcardFileFilter.html) for more information.|yes|
|baseDir|directory to search recursively for files to be ingested. |yes|

### HttpFirehose

This Firehose can be used to read the data from remote sites via HTTP.
This firehose is _splittable_ and can be used by [native parallel index tasks](./native_tasks.html#parallel-index-task).
Since each split represents a file in this firehose, each worker task of `index_parallel` will read a file.
A sample http firehose spec is shown below:

```json
{
    "type"    : "http",
    "uris"  : ["http://example.com/uri1", "http://example2.com/uri2"]
}
```

The below configurations can be optionally used for tuning the firehose performance.

|property|description|default|
|--------|-----------|-------|
|maxCacheCapacityBytes|Maximum size of the cache space in bytes. 0 means disabling cache. Cached files are not removed until the ingestion task completes.|1073741824|
|maxFetchCapacityBytes|Maximum size of the fetch space in bytes. 0 means disabling prefetch. Prefetched files are removed immediately once they are read.|1073741824|
|prefetchTriggerBytes|Threshold to trigger prefetching http objects.|maxFetchCapacityBytes / 2|
|fetchTimeout|Timeout for fetching a http object.|60000|
|maxFetchRetry|Maximum retry for fetching a http object.|3|

### IngestSegmentFirehose

This Firehose can be used to read the data from existing druid segments.
It can be used ingest existing druid segments using a new schema and change the name, dimensions, metrics, rollup, etc. of the segment.
A sample ingest firehose spec is shown below -

```json
{
    "type"    : "ingestSegment",
    "dataSource"   : "wikipedia",
    "interval" : "2013-01-01/2013-01-02"
}
```

|property|description|required?|
|--------|-----------|---------|
|type|This should be "ingestSegment".|yes|
|dataSource|A String defining the data source to fetch rows from, very similar to a table in a relational database|yes|
|interval|A String representing ISO-8601 Interval. This defines the time range to fetch the data over.|yes|
|dimensions|The list of dimensions to select. If left empty, no dimensions are returned. If left null or not defined, all dimensions are returned. |no|
|metrics|The list of metrics to select. If left empty, no metrics are returned. If left null or not defined, all metrics are selected.|no|
|filter| See [Filters](../querying/filters.html)|no|

#### SqlFirehose

SqlFirehoseFactory can be used to ingest events residing in RDBMS. The database connection information is provided as part of the ingestion spec. For each query, the results are fetched locally and indexed. If there are multiple queries from which data needs to be indexed, queries are prefetched in the background upto `maxFetchCapacityBytes` bytes.
An example is shown below:

```json
{
    "type" : "sql",
    "database": {
        "type": "mysql",
        "connectorConfig" : {
        "connectURI" : "jdbc:mysql://host:port/schema",
        "user" : "user",
        "password" : "password"
        }
     },
    "sqls" : ["SELECT * FROM table1", "SELECT * FROM table2"]
}
```


|property|description|default|required?|
|--------|-----------|-------|---------|
|type|This should be "sql".||Yes|
|database|Specifies the database connection details.`type` should specify the database type and `connectorConfig` should specify the database connection properties via `connectURI`, `user` and `password`||Yes|
|maxCacheCapacityBytes|Maximum size of the cache space in bytes. 0 means disabling cache. Cached files are not removed until the ingestion task completes.|1073741824|No|
|maxFetchCapacityBytes|Maximum size of the fetch space in bytes. 0 means disabling prefetch. Prefetched files are removed immediately once they are read.|1073741824|No|
|prefetchTriggerBytes|Threshold to trigger prefetching SQL result objects.|maxFetchCapacityBytes / 2|No|
|fetchTimeout|Timeout for fetching the result set.|60000|No|
|foldCase|Toggle case folding of database column names. This may be enabled in cases where the database returns case insensitive column names in query results.|false|No|
|sqls|List of SQL queries where each SQL query would retrieve the data to be indexed.||Yes|

### CombiningFirehose

This firehose can be used to combine and merge data from a list of different firehoses.
This can be used to merge data from more than one firehose.

```json
{
    "type"  :   "combining",
    "delegates" : [ { firehose1 }, { firehose2 }, ..... ]
}
```

|property|description|required?|
|--------|-----------|---------|
|type|This should be "combining"|yes|
|delegates|list of firehoses to combine data from|yes|


### Streaming Firehoses

The firehoses shown below should only be used with the [stream-pull (deprecated)](../ingestion/stream-pull.html) ingestion model, as they are not suitable for batch ingestion.

The EventReceiverFirehose is also used in tasks automatically generated by [Tranquility stream push](../ingestion/stream-push.html).

#### EventReceiverFirehose

EventReceiverFirehoseFactory can be used to ingest events using an http endpoint.

```json
{
  "type": "receiver",
  "serviceName": "eventReceiverServiceName",
  "bufferSize": 10000
}
```
When using this firehose, events can be sent by submitting a POST request to the http endpoint:

`http://<peonHost>:<port>/druid/worker/v1/chat/<eventReceiverServiceName>/push-events/`

|property|description|required?|
|--------|-----------|---------|
|type|This should be "receiver"|yes|
|serviceName|name used to announce the event receiver service endpoint|yes|
|bufferSize| size of buffer used by firehose to store events|no default(100000)|

Shut down time for EventReceiverFirehose can be specified by submitting a POST request to

`http://<peonHost>:<port>/druid/worker/v1/chat/<eventReceiverServiceName>/shutdown?shutoffTime=<shutoffTime>`

If shutOffTime is not specified, the firehose shuts off immediately.

#### TimedShutoffFirehose

This can be used to start a firehose that will shut down at a specified time.
An example is shown below:

```json
{
    "type"  :   "timed",
    "shutoffTime": "2015-08-25T01:26:05.119Z",
    "delegate": {
          "type": "receiver",
          "serviceName": "eventReceiverServiceName",
          "bufferSize": 100000
     }
}
```

|property|description|required?|
|--------|-----------|---------|
|type|This should be "timed"|yes|
|shutoffTime|time at which the firehose should shut down, in ISO8601 format|yes|
|delegate|firehose to use|yes|
