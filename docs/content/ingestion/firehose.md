---
layout: doc_page
title: "Apache Druid (incubating) Firehoses"
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

# Apache Druid (incubating) Firehoses

Firehoses are used in [native batch ingestion tasks](../ingestion/native_tasks.html) and stream push tasks automatically created by [Tranquility](../ingestion/stream-push.html).

They are pluggable, and thus the configuration schema can and will vary based on the `type` of the Firehose.

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| type | String | Specifies the type of Firehose. Each value will have its own configuration schema. Firehoses packaged with Druid are described below. | yes |

## Additional Firehoses

There are several Firehoses readily available in Druid. Some are meant for examples, and others can be used directly in a production environment.

For additional Firehoses, please see our [extensions list](../development/extensions.html).

### LocalFirehose

This Firehose can be used to read the data from files on local disk, and is mainly intended for proof-of-concept testing, and works with `string` typed parsers.
This Firehose is _splittable_ and can be used by [native parallel index tasks](./native_tasks.html#parallel-index-task).
Since each split represents a file in this Firehose, each worker task of `index_parallel` will read a file.
A sample local Firehose spec is shown below:

```json
{
    "type": "local",
    "filter" : "*.csv",
    "baseDir": "/data/directory"
}
```

|property|description|required?|
|--------|-----------|---------|
|type|This should be "local".|yes|
|filter|A wildcard filter for files. See [here](http://commons.apache.org/proper/commons-io/apidocs/org/apache/commons/io/filefilter/WildcardFileFilter.html) for more information.|yes|
|baseDir|directory to search recursively for files to be ingested. |yes|

### HttpFirehose

This Firehose can be used to read the data from remote sites via HTTP, and works with `string` typed parsers.
This Firehose is _splittable_ and can be used by [native parallel index tasks](./native_tasks.html#parallel-index-task).
Since each split represents a file in this Firehose, each worker task of `index_parallel` will read a file.
A sample HTTP Firehose spec is shown below:

```json
{
    "type": "http",
    "uris": ["http://example.com/uri1", "http://example2.com/uri2"]
}
```

The below configurations can be optionally used if the URIs specified in the spec require a Basic Authentication Header.
Omitting these fields from your spec will result in HTTP requests with no Basic Authentication Header.

|property|description|default|
|--------|-----------|-------|
|httpAuthenticationUsername|Username to use for authentication with specified URIs|None|
|httpAuthenticationPassword|PasswordProvider to use with specified URIs|None|

Example with authentication fields using the DefaultPassword provider (this requires the password to be in the ingestion spec):

```json
{
    "type": "http",
    "uris": ["http://example.com/uri1", "http://example2.com/uri2"],
    "httpAuthenticationUsername": "username",
    "httpAuthenticationPassword": "password123"
}
```

You can also use the other existing Druid PasswordProviders. Here is an example using the EnvironmentVariablePasswordProvider:

```json
{
    "type": "http",
    "uris": ["http://example.com/uri1", "http://example2.com/uri2"],
    "httpAuthenticationUsername": "username",
    "httpAuthenticationPassword": {
        "type": "environment",
        "variable": "HTTP_FIREHOSE_PW"
    }
}
```

The below configurations can optionally be used for tuning the Firehose performance.

|property|description|default|
|--------|-----------|-------|
|maxCacheCapacityBytes|Maximum size of the cache space in bytes. 0 means disabling cache. Cached files are not removed until the ingestion task completes.|1073741824|
|maxFetchCapacityBytes|Maximum size of the fetch space in bytes. 0 means disabling prefetch. Prefetched files are removed immediately once they are read.|1073741824|
|prefetchTriggerBytes|Threshold to trigger prefetching HTTP objects.|maxFetchCapacityBytes / 2|
|fetchTimeout|Timeout for fetching an HTTP object.|60000|
|maxFetchRetry|Maximum retries for fetching an HTTP object.|3|

### IngestSegmentFirehose

This Firehose can be used to read the data from existing druid segments, potentially using a new schema and changing the name, dimensions, metrics, rollup, etc. of the segment.
This Firehose is _splittable_ and can be used by [native parallel index tasks](./native_tasks.html#parallel-index-task).
This firehose is a bit strange, in that the `parser` is effectively ignored other than to collect the list of dimensions
and timestamp column to create a 'transform spec', so uses either `string` or `map` typed parsers. A sample ingest Firehose spec is shown below:

```json
{
    "type": "ingestSegment",
    "dataSource": "wikipedia",
    "interval": "2013-01-01/2013-01-02"
}
```

|property|description|required?|
|--------|-----------|---------|
|type|This should be "ingestSegment".|yes|
|dataSource|A String defining the data source to fetch rows from, very similar to a table in a relational database|yes|
|interval|A String representing the ISO-8601 interval. This defines the time range to fetch the data over.|yes|
|dimensions|The list of dimensions to select. If left empty, no dimensions are returned. If left null or not defined, all dimensions are returned. |no|
|metrics|The list of metrics to select. If left empty, no metrics are returned. If left null or not defined, all metrics are selected.|no|
|filter| See [Filters](../querying/filters.html)|no|
|maxInputSegmentBytesPerTask|When used with the native parallel index task, the maximum number of bytes of input segments to process in a single task. If a single segment is larger than this number, it will be processed by itself in a single task (input segments are never split across tasks). Defaults to 150MB.|no|

### SqlFirehose

This Firehose can be used to ingest events residing in an RDBMS. The database connection information is provided as part of the ingestion spec.
For each query, the results are fetched locally and indexed.
If there are multiple queries from which data needs to be indexed, queries are prefetched in the background, up to `maxFetchCapacityBytes` bytes.
This firehose works with `map` typed parsers. See the extension documentation for more detailed ingestion examples.

Requires one of the following extensions:
 * [MySQL Metadata Store](../development/extensions-core/mysql.html).
 * [PostgreSQL Metadata Store](../development/extensions-core/postgresql.html).


```json
{
    "type": "sql",
    "database": {
        "type": "mysql",
        "connectorConfig": {
            "connectURI": "jdbc:mysql://host:port/schema",
            "user": "user",
            "password": "password"
        }
     },
    "sqls": ["SELECT * FROM table1", "SELECT * FROM table2"]
}
```

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|This should be "sql".||Yes|
|database|Specifies the database connection details.||Yes|
|maxCacheCapacityBytes|Maximum size of the cache space in bytes. 0 means disabling cache. Cached files are not removed until the ingestion task completes.|1073741824|No|
|maxFetchCapacityBytes|Maximum size of the fetch space in bytes. 0 means disabling prefetch. Prefetched files are removed immediately once they are read.|1073741824|No|
|prefetchTriggerBytes|Threshold to trigger prefetching SQL result objects.|maxFetchCapacityBytes / 2|No|
|fetchTimeout|Timeout for fetching the result set.|60000|No|
|foldCase|Toggle case folding of database column names. This may be enabled in cases where the database returns case insensitive column names in query results.|false|No|
|sqls|List of SQL queries where each SQL query would retrieve the data to be indexed.||Yes|

#### Database

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|The type of database to query. Valid values are `mysql` and `postgresql`_||Yes|
|connectorConfig|Specify the database connection properties via `connectURI`, `user` and `password`||Yes|


### InlineFirehose

This Firehose can be used to read the data inlined in its own spec.
It can be used for demos or for quickly testing out parsing and schema, and works with `string` typed parsers.
A sample inline Firehose spec is shown below:

```json
{
    "type": "inline",
    "data": "0,values,formatted\n1,as,CSV"
}
```

|property|description|required?|
|--------|-----------|---------|
|type|This should be "inline".|yes|
|data|Inlined data to ingest.|yes|

### CombiningFirehose

This Firehose can be used to combine and merge data from a list of different Firehoses.

```json
{
    "type": "combining",
    "delegates": [ { firehose1 }, { firehose2 }, ... ]
}
```

|property|description|required?|
|--------|-----------|---------|
|type|This should be "combining"|yes|
|delegates|List of Firehoses to combine data from|yes|


### Streaming Firehoses

The EventReceiverFirehose is used in tasks automatically generated by
[Tranquility stream push](../ingestion/stream-push.html). These Firehoses are not suitable for batch ingestion.

#### EventReceiverFirehose

This Firehose can be used to ingest events using an HTTP endpoint, and works with `string` typed parsers.

```json
{
  "type": "receiver",
  "serviceName": "eventReceiverServiceName",
  "bufferSize": 10000
}
```
When using this Firehose, events can be sent by submitting a POST request to the HTTP endpoint:

`http://<peonHost>:<port>/druid/worker/v1/chat/<eventReceiverServiceName>/push-events/`

|property|description|required?|
|--------|-----------|---------|
|type|This should be "receiver"|yes|
|serviceName|Name used to announce the event receiver service endpoint|yes|
|maxIdleTime|A Firehose is automatically shut down after not receiving any events for this period of time, in milliseconds. If not specified, a Firehose is never shut down due to being idle. Zero and negative values have the same effect.|no|
|bufferSize|Size of buffer used by Firehose to store events|no, default is 100000|

Shut down time for EventReceiverFirehose can be specified by submitting a POST request to

`http://<peonHost>:<port>/druid/worker/v1/chat/<eventReceiverServiceName>/shutdown?shutoffTime=<shutoffTime>`

If shutOffTime is not specified, the Firehose shuts off immediately.

#### TimedShutoffFirehose

This can be used to start a Firehose that will shut down at a specified time.
An example is shown below:

```json
{
    "type":  "timed",
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
|shutoffTime|Time at which the Firehose should shut down, in ISO8601 format|yes|
|delegate|Firehose to use|yes|
