---
id: native-batch-firehose
title: "Native batch ingestion with firehose"
sidebar_label: "Firehose (deprecated)"
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


Firehoses are deprecated in 0.17.0. It's highly recommended to use the [Native batch ingestion input sources](./native-batch-input-source.md) instead.

There are several firehoses readily available in Druid, some are meant for examples, others can be used directly in a production environment.

## StaticS3Firehose

> You need to include the [`druid-s3-extensions`](../development/extensions-core/s3.md) as an extension to use the StaticS3Firehose.

This firehose ingests events from a predefined list of S3 objects.
This firehose is _splittable_ and can be used by the [Parallel task](./native-batch.md).
Since each split represents an object in this firehose, each worker task of `index_parallel` will read an object.

Sample spec:

```json
"firehose" : {
    "type" : "static-s3",
    "uris": ["s3://foo/bar/file.gz", "s3://bar/foo/file2.gz"]
}
```

This firehose provides caching and prefetching features. In the Simple task, a firehose can be read twice if intervals or
shardSpecs are not specified, and, in this case, caching can be useful. Prefetching is preferred when direct scan of objects is slow.
Note that prefetching or caching isn't that useful in the Parallel task.

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|This should be `static-s3`.|None|yes|
|uris|JSON array of URIs where s3 files to be ingested are located.|None|`uris` or `prefixes` must be set|
|prefixes|JSON array of URI prefixes for the locations of s3 files to be ingested.|None|`uris` or `prefixes` must be set|
|maxCacheCapacityBytes|Maximum size of the cache space in bytes. 0 means disabling cache. Cached files are not removed until the ingestion task completes.|1073741824|no|
|maxFetchCapacityBytes|Maximum size of the fetch space in bytes. 0 means disabling prefetch. Prefetched files are removed immediately once they are read.|1073741824|no|
|prefetchTriggerBytes|Threshold to trigger prefetching s3 objects.|maxFetchCapacityBytes / 2|no|
|fetchTimeout|Timeout for fetching an s3 object.|60000|no|
|maxFetchRetry|Maximum retry for fetching an s3 object.|3|no|

## StaticGoogleBlobStoreFirehose

> You need to include the [`druid-google-extensions`](../development/extensions-core/google.md) as an extension to use the StaticGoogleBlobStoreFirehose.

This firehose ingests events, similar to the StaticS3Firehose, but from an Google Cloud Store.

As with the S3 blobstore, it is assumed to be gzipped if the extension ends in .gz

This firehose is _splittable_ and can be used by the [Parallel task](./native-batch.md).
Since each split represents an object in this firehose, each worker task of `index_parallel` will read an object.

Sample spec:

```json
"firehose" : {
    "type" : "static-google-blobstore",
    "blobs": [
        {
          "bucket": "foo",
          "path": "/path/to/your/file.json"
        },
        {
          "bucket": "bar",
          "path": "/another/path.json"
        }
    ]
}
```

This firehose provides caching and prefetching features. In the Simple task, a firehose can be read twice if intervals or
shardSpecs are not specified, and, in this case, caching can be useful. Prefetching is preferred when direct scan of objects is slow.
Note that prefetching or caching isn't that useful in the Parallel task.

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|This should be `static-google-blobstore`.|None|yes|
|blobs|JSON array of Google Blobs.|None|yes|
|maxCacheCapacityBytes|Maximum size of the cache space in bytes. 0 means disabling cache. Cached files are not removed until the ingestion task completes.|1073741824|no|
|maxFetchCapacityBytes|Maximum size of the fetch space in bytes. 0 means disabling prefetch. Prefetched files are removed immediately once they are read.|1073741824|no|
|prefetchTriggerBytes|Threshold to trigger prefetching Google Blobs.|maxFetchCapacityBytes / 2|no|
|fetchTimeout|Timeout for fetching a Google Blob.|60000|no|
|maxFetchRetry|Maximum retry for fetching a Google Blob.|3|no|

Google Blobs:

|property|description|default|required?|
|--------|-----------|-------|---------|
|bucket|Name of the Google Cloud bucket|None|yes|
|path|The path where data is located.|None|yes|

## HDFSFirehose

> You need to include the [`druid-hdfs-storage`](../development/extensions-core/hdfs.md) as an extension to use the HDFSFirehose.

This firehose ingests events from a predefined list of files from the HDFS storage.
This firehose is _splittable_ and can be used by the [Parallel task](./native-batch.md).
Since each split represents an HDFS file, each worker task of `index_parallel` will read files.

Sample spec:

```json
"firehose" : {
    "type" : "hdfs",
    "paths": "/foo/bar,/foo/baz"
}
```

This firehose provides caching and prefetching features. During native batch indexing, a firehose can be read twice if
`intervals` are not specified, and, in this case, caching can be useful. Prefetching is preferred when direct scanning
of files is slow.
Note that prefetching or caching isn't that useful in the Parallel task.

|Property|Description|Default|
|--------|-----------|-------|
|type|This should be `hdfs`.|none (required)|
|paths|HDFS paths. Can be either a JSON array or comma-separated string of paths. Wildcards like `*` are supported in these paths.|none (required)|
|maxCacheCapacityBytes|Maximum size of the cache space in bytes. 0 means disabling cache. Cached files are not removed until the ingestion task completes.|1073741824|
|maxFetchCapacityBytes|Maximum size of the fetch space in bytes. 0 means disabling prefetch. Prefetched files are removed immediately once they are read.|1073741824|
|prefetchTriggerBytes|Threshold to trigger prefetching files.|maxFetchCapacityBytes / 2|
|fetchTimeout|Timeout for fetching each file.|60000|
|maxFetchRetry|Maximum number of retries for fetching each file.|3|

You can also ingest from other storage using the HDFS firehose if the HDFS client supports that storage.
However, if you want to ingest from cloud storage, consider using the service-specific input source for your data storage.
If you want to use a non-hdfs protocol with the HDFS firehose, you need to include the protocol you want
in `druid.ingestion.hdfs.allowedProtocols`. See [HDFS firehose security configuration](../configuration/index.md#hdfs-input-source) for more details.

## LocalFirehose

This Firehose can be used to read the data from files on local disk, and is mainly intended for proof-of-concept testing, and works with `string` typed parsers.
This Firehose is _splittable_ and can be used by [native parallel index tasks](native-batch.md).
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
|filter|A wildcard filter for files. See [here](http://commons.apache.org/proper/commons-io/apidocs/org/apache/commons/io/filefilter/WildcardFileFilter) for more information.|yes|
|baseDir|directory to search recursively for files to be ingested. |yes|

<a name="http-firehose"></a>

## HttpFirehose

This Firehose can be used to read the data from remote sites via HTTP, and works with `string` typed parsers.
This Firehose is _splittable_ and can be used by [native parallel index tasks](native-batch.md).
Since each split represents a file in this Firehose, each worker task of `index_parallel` will read a file.
A sample HTTP Firehose spec is shown below:

```json
{
    "type": "http",
    "uris": ["http://example.com/uri1", "http://example2.com/uri2"]
}
```

You can only use protocols listed in the `druid.ingestion.http.allowedProtocols` property as HTTP firehose input sources.
The `http` and `https` protocols are allowed by default. See [HTTP firehose security configuration](../configuration/index.md#http-input-source) for more details.

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
Note that prefetching or caching isn't that useful in the Parallel task.

|property|description|default|
|--------|-----------|-------|
|maxCacheCapacityBytes|Maximum size of the cache space in bytes. 0 means disabling cache. Cached files are not removed until the ingestion task completes.|1073741824|
|maxFetchCapacityBytes|Maximum size of the fetch space in bytes. 0 means disabling prefetch. Prefetched files are removed immediately once they are read.|1073741824|
|prefetchTriggerBytes|Threshold to trigger prefetching HTTP objects.|maxFetchCapacityBytes / 2|
|fetchTimeout|Timeout for fetching an HTTP object.|60000|
|maxFetchRetry|Maximum retries for fetching an HTTP object.|3|

<a name="segment-firehose"></a>

## IngestSegmentFirehose

This Firehose can be used to read the data from existing druid segments, potentially using a new schema and changing the name, dimensions, metrics, rollup, etc. of the segment.
This Firehose is _splittable_ and can be used by [native parallel index tasks](native-batch.md).
This firehose will accept any type of parser, but will only utilize the list of dimensions and the timestamp specification.
 A sample ingest Firehose spec is shown below:

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
|filter| See [Filters](../querying/filters.md)|no|
|maxInputSegmentBytesPerTask|Deprecated. Use [Segments Split Hint Spec](./native-batch.md#segments-split-hint-spec) instead. When used with the native parallel index task, the maximum number of bytes of input segments to process in a single task. If a single segment is larger than this number, it will be processed by itself in a single task (input segments are never split across tasks). Defaults to 150MB.|no|

<a name="sql-firehose"></a>

## SqlFirehose

This Firehose can be used to ingest events residing in an RDBMS. The database connection information is provided as part of the ingestion spec.
For each query, the results are fetched locally and indexed.
If there are multiple queries from which data needs to be indexed, queries are prefetched in the background, up to `maxFetchCapacityBytes` bytes.
This Firehose is _splittable_ and can be used by [native parallel index tasks](native-batch.md).
This firehose will accept any type of parser, but will only utilize the list of dimensions and the timestamp specification. See the extension documentation for more detailed ingestion examples.

Requires one of the following extensions:
 * [MySQL Metadata Store](../development/extensions-core/mysql.md).
 * [PostgreSQL Metadata Store](../development/extensions-core/postgresql.md).


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
|database|Specifies the database connection details. The database type corresponds to the extension that supplies the `connectorConfig` support. The specified extension must be loaded into Druid:<br/><br/><ul><li>[mysql-metadata-storage](../development/extensions-core/mysql.md) for `mysql`</li><li> [postgresql-metadata-storage](../development/extensions-core/postgresql.md) extension for `postgresql`.</li></ul><br/><br/>You can selectively allow JDBC properties in `connectURI`. See [JDBC connections security config](../configuration/index.md#jdbc-connections-to-external-databases) for more details.||Yes|
|maxCacheCapacityBytes|Maximum size of the cache space in bytes. 0 means disabling cache. Cached files are not removed until the ingestion task completes.|1073741824|No|
|maxFetchCapacityBytes|Maximum size of the fetch space in bytes. 0 means disabling prefetch. Prefetched files are removed immediately once they are read.|1073741824|No|
|prefetchTriggerBytes|Threshold to trigger prefetching SQL result objects.|maxFetchCapacityBytes / 2|No|
|fetchTimeout|Timeout for fetching the result set.|60000|No|
|foldCase|Toggle case folding of database column names. This may be enabled in cases where the database returns case insensitive column names in query results.|false|No|
|sqls|List of SQL queries where each SQL query would retrieve the data to be indexed.||Yes|

### Database

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|The type of database to query. Valid values are `mysql` and `postgresql`_||Yes|
|connectorConfig|Specify the database connection properties via `connectURI`, `user` and `password`||Yes|

## InlineFirehose

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

## CombiningFirehose

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