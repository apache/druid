---
id: lookups-cached-global
title: "Globally cached lookups"
---

import {DRUIDVERSION} from "@site/static/js/versions.js"

<!-- vale off -->

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

Globally cached lookups all draw from the same cache pool, allowing each Druid service to have a fixed cache pool that can be used by cached lookups.

To use this Apache Druid extension, [include](../configuration/extensions.md#loading-extensions) `druid-lookups-cached-global` in the extensions load list.

## Configuration

Use globally cached lookup in the following scenarios:
- The lookup is too large to pass at query time.
- You want the lookup data to reside in a Druid data source handled by Druid and the lookup is small enough to reasonably reside in memory--between tens and tens of thousand of entries per lookup.

:::info
 Druid no longer supports static lookup configuration. You can configure lookups through
 [dynamic configuration](./lookups.md#configuration).
:::

Specify globally cached lookups as part of the [cluster wide config for lookups](./lookups.md) as a type of `cachedNamespace`. For example:

 ```json
 {
    "type": "cachedNamespace",
    "extractionNamespace": {
       "type": "uri",
       "uri": "file:/tmp/prefix/",
       "namespaceParseSpec": {
         "format": "csv",
         "columns": [
             "[\"key\"",
             "\"value\"]"
      ]
       },
       "pollPeriod": "PT5M"
     },
     "firstCacheTimeout": 0
 }
 ```

 ```json
{
    "type": "cachedNamespace",
    "extractionNamespace": {
       "type": "jdbc",
       "connectorConfig": {
         "connectURI": "jdbc:mysql:\/\/localhost:3306\/druid",
         "user": "druid",
         "password": "diurd"
       },
       "table": "lookupTable",
       "keyColumn": "mykeyColumn",
       "valueColumn": "myValueColumn",
       "filter" : "myFilterSQL (Where clause statement  e.g LOOKUPTYPE=1)",
       "tsColumn": "timeColumn"
    },
    "firstCacheTimeout": 120000,
    "injective":true
}
 ```

The parameters are as follows:

|Property|Description|Required|Default|
|--------|-----------|--------|-------|
|`extractionNamespace`|Specifies how to populate the local cache. See the [example configuration](#example-configuration) below for more detail.|Yes|-|
|`firstCacheTimeout`|How long to wait (in ms) for the first run of the cache to populate.|No|`0` (do not wait)|
|`injective`|If the underlying map is [injective](./lookups.md#query-rewrites) (keys and values are unique) then set this to `true` for optimizations to occur internally.|No|`false`|

If `firstCacheTimeout` is set to a non-zero value, it should be less than `druid.manager.lookups.hostUpdateTimeout`. If `firstCacheTimeout` is NOT set, then management is essentially asynchronous and does not know if a lookup succeeded or failed in starting. In such a case logs from the processes using lookups should be monitored for repeated failures.

To enable complete functionality of globally cached lookups, load the `druid-lookups-cached-global` extension on the Broker, Peon, and Historical processes.

## Example configuration

In the case where only one [tier](./lookups.md#dynamic-configuration) exists (`realtime_customer2`) with one `cachedNamespace` lookup called `country_code`, the resulting configuration JSON looks similar to the following:

```json
{
  "realtime_customer2": {
    "country_code": {
      "version": "v0",
      "lookupExtractorFactory": {
        "type": "cachedNamespace",
        "extractionNamespace": {
          "type": "jdbc",
          "connectorConfig": {
            "connectURI": "jdbc:mysql:\/\/localhost:3306\/druid",
            "user": "druid",
            "password": "diurd"
          },
          "table": "lookupValues",
          "keyColumn": "value_id",
          "valueColumn": "value_text",
          "filter": "value_type='country'",
          "tsColumn": "timeColumn"
        },
        "firstCacheTimeout": 120000,
        "injective": true
      }
    }
  }
}
```

Where the Coordinator endpoint `/druid/coordinator/v1/lookups/realtime_customer2/country_code` should return

```json
{
  "version": "v0",
  "lookupExtractorFactory": {
    "type": "cachedNamespace",
    "extractionNamespace": {
      "type": "jdbc",
      "connectorConfig": {
        "connectURI": "jdbc:mysql://localhost:3306/druid",
        "user": "druid",
        "password": "diurd"
      },
      "table": "lookupValues",
      "keyColumn": "value_id",
      "valueColumn": "value_text",
      "filter": "value_type='country'",
      "tsColumn": "timeColumn"
    },
    "firstCacheTimeout": 120000,
    "injective": true
  }
}
```

## Cache settings

Lookups are cached locally on Historical processes. The following are settings used by the processes which service queries when
setting namespaces (Broker, Peon, Historical)

|Property|Description|Default|
|--------|-----------|-------|
|`druid.lookup.namespace.cache.type`|Specifies the type of caching for the namespaces to use. May be one of [`offHeap`, `onHeap`]. `offHeap` uses a temporary file for off-heap storage of the namespace (memory mapped files). `onHeap` stores all cache on the heap in standard java map types.|`onHeap`|
|`druid.lookup.namespace.numExtractionThreads`|The number of threads in the thread pool dedicated for lookup extraction and updates. If you have a lot of lookups and they take long time to extract, increase this number to avoid timeouts.|2|
|`druid.lookup.namespace.numBufferedEntries`|If using off-heap caching, the number of records to store on an on-heap buffer.|100,000|

Druid populates the cache in different ways depending on the following settings. In general, most namespaces employ
a `pollPeriod` at the end of which they poll the remote resource of interest for updates.

`onHeap` uses `ConcurrentMap`s in the java heap which affects garbage collection and heap sizing.
`offHeap` uses an on-heap buffer and MapDB using memory-mapped files in the java temporary directory.
Therefore, if the total number of entries in the `cachedNamespace` exceeds the buffer's configured capacity, Druid maintains the extra entries in memory as page cache, and paged in and out according to general operating system tunings.
We strongly recommended that you set `druid.lookup.namespace.numBufferedEntries` when using `offHeap`. Select a value between 10% and 50% of the number of entries in the lookup.

## Supported lookups

For information on additional lookups, refer to the [extensions list](../configuration/extensions.md).

### URI lookup

You can specify remapping values for each globally cached lookup with a JSON object, for example:

```json
{
  "type":"uri",
  "uri": "s3://bucket/some/key/prefix/renames-0003.gz",
  "namespaceParseSpec":{
    "format":"csv",
    "columns":[
        "[\"key\"",
        "\"value\"]"
      ]
  },
  "pollPeriod":"PT5M"
}
```

```json
{
  "type":"uri",
  "uriPrefix": "s3://bucket/some/key/prefix/",
  "fileRegex":"renames-[0-9]*\\.gz",
  "namespaceParseSpec":{
    "format":"csv",
    "columns":[
        "[\"key\"",
        "\"value\"]"
      ]
  },
  "pollPeriod":"PT5M",
  "maxHeapPercentage": 10
}
```

|Property|Description|Required|Default|
|--------|-----------|--------|-------|
|`pollPeriod`|Time period between polling for updates, in ms.|No|0 (only once)|
|`uri`|URI for the lookup file. Can be a file, HDFS, S3 or GCS path.|You must define either `uri` or `uriPrefix`.*|None|
|`uriPrefix`|A URI prefix that specifies a directory or other searchable resource where lookup files are located.|You must define either `uri` or `uriPrefix`.*|None|
|`fileRegex`|Optional regex for matching the file name under `uriPrefix`. Only used if `uriPrefix` is defined.|No|`".*"`|
|`namespaceParseSpec`|How to interpret the data at the URI.|Yes||
|`maxHeapPercentage`|The maximum percentage of heap size for the lookup to consume. If the lookup grows beyond this size, Druid logs warning messages in the respective service logs.|No|10% of JVM heap size|

*Define `uri` or `uriPrefix` as either a local file system (`file://`), HDFS (`hdfs://`), S3 (`s3://`) or GCS (`gs://`) location. Druid doesn't support HTTP location.

The `pollPeriod` value specifies the period in ISO 8601 format between checks for replacement data for the lookup. If the source of the lookup provides a timestamp, Druid only updates the lookup when the lookup has changed since the prior `pollPeriod` timestamp. A value of 0, an absent parameter, or `null` all direct Druid to populate the lookup once without attempting to check again for new data. 

Whenever a poll occurs, the updating system looks for a file with the most recent timestamp and assumes that is the one with the most recent data set, using it to replace the local cache of the lookup data.

The `namespaceParseSpec` can be one of a number of values. The examples below rename `foo` to `bar`, `baz` to `bat`, and `buck` to `truck`. All `parseSpec` types assume each input is delimited by a new line.

Druid only uses ONE file which matches the search. For most implementations, the discriminator for choosing the URIs is whichever one reports the most recent timestamp in its modification time.

Druid supports `csv`, `tsv`, `customJSON`, and `simpleJSON` `parseSpec` types:

#### csv lookupParseSpec

|Parameter|Description|Required|Default|
|---------|-----------|--------|-------|
|`columns`|The list of columns in the csv file.|No if `hasHeaderRow` is defined.|`null`|
|`keyColumn`|The name of the column containing the key.|No|The first column.|
|`valueColumn`|The name of the column containing the value.|No|The second column.|
|`hasHeaderRow`|A flag to indicate that column information can be extracted from the input files' header row.|No|`false`|
|`skipHeaderRows`|Number of header rows to skip.|No|`0`|

If both `skipHeaderRows` and `hasHeaderRow` options are defined, Druid first applies `skipHeaderRows`. For example, if you set
`skipHeaderRows` to 2 and `hasHeaderRow` to `true`, Druid skips the first two lines and then extracts column information
from the third line.

Example input:

```
bar,something,foo
bat,something2,baz
truck,something3,buck
```

Example `namespaceParseSpec`:

```json
"namespaceParseSpec": {
  "format": "csv",
  "columns": ["value","somethingElse","key"],
  "keyColumn": "key",
  "valueColumn": "value"
}
```

#### tsv lookupParseSpec

|Parameter|Description|Required|Default|
|---------|-----------|--------|-------|
|`columns`|The list of columns in the TSV file.|Yes|`null`|
|`keyColumn`|The name of the column containing the key|No|The first column.|
|`valueColumn`|The name of the column containing the value|No|The second column.|
|`delimiter`|The delimiter in the file.|no|tab (`\t`)|
|`listDelimiter`|The list delimiter in the file.|No| (`\u0001`)|
|`hasHeaderRow`|A flag to indicate that column information can be extracted from the input files' header row.|No|`false`|
|`skipHeaderRows`|Number of header rows to skip.|No|`0`|

If both `skipHeaderRows` and `hasHeaderRow` options are defined, Druid first applies `skipHeaderRows`. For example, if you set
`skipHeaderRows` to `2` and `hasHeaderRow` to `true`, Druid skips the first two lines and then extract column information
from the third line.

Example input:

```
bar|something,1|foo
bat|something,2|baz
truck|something,3|buck
```

Example `namespaceParseSpec`:

```json
"namespaceParseSpec": {
  "format": "tsv",
  "columns": ["value","somethingElse","key"],
  "keyColumn": "key",
  "valueColumn": "value",
  "delimiter": "|"
}
```

#### customJson lookupParseSpec

|Parameter|Description|Required|Default|
|---------|-----------|--------|-------|
|`keyFieldName`|The field name of the key.|Yes|`null`|
|`valueFieldName`|The field name of the value.|Yes|`null`|

Example input:

```json
{"key": "foo", "value": "bar", "somethingElse" : "something"}
{"key": "baz", "value": "bat", "somethingElse" : "something"}
{"key": "buck", "somethingElse": "something", "value": "truck"}
```

Example `namespaceParseSpec`:

```json
"namespaceParseSpec": {
  "format": "customJson",
  "keyFieldName": "key",
  "valueFieldName": "value"
}
```

With `customJson` parsing, if the value field for a particular row is missing or null Druid skips that line and doesn't include it in the lookup.

#### simpleJson lookupParseSpec

The `simpleJson` `lookupParseSpec` doesn't take any parameters. It's simply a line-delimited JSON file where the field is the key, and the field's value is the value.

Example input:

```json
{"foo": "bar"}
{"baz": "bat"}
{"buck": "truck"}
```

Example `namespaceParseSpec`:

```json
"namespaceParseSpec":{
  "format": "simpleJson"
}
```

### JDBC lookup

The JDBC lookups polls a database to populate its local cache. If the `tsColumn` is defined it must be able to accept comparisons in the format `'2015-01-01 00:00:00'`. 

For example, the following must be valid SQL for the table:  
`SELECT * FROM some_lookup_table WHERE timestamp_column >  '2015-01-01 00:00:00'`.  

If `tsColumn` is defined, the caching service attempts to only poll values that were written *after* the last sync.
If `tsColumn` is not defined, Druid pulls the entire table every time.

|Parameter|Description|Required|Default|
|---------|-----------|--------|-------|
|`connectorConfig`|The connector configuration to use. You can set `connectURI`, `user` and `password`. You can selectively allow JDBC properties in `connectURI`. See [JDBC connections security config](../configuration/index.md#jdbc-connections-to-external-databases) for more details.|Yes||
|`table`|The table containing the key value pairs.|Yes||
|`keyColumn`|The column in `table` containing the keys.|Yes||
|`valueColumn`|The column in `table` containing the values.|Yes||
|`filter`|The filter to use when selecting lookups.|No|No Filter.|
|`tsColumn`| The column in `table` containing the update date/time for the key.|No|Not used.|
|`pollPeriod`|How often to poll the database.|No|`0` (once).|
|`jitterSeconds`|Maximum jitter to add (in seconds) as delay. The actual value used is a random selection from `0` to `jitterSeconds`). Druid uses this to distribute database load more evenly.|No|`0`|
|`loadTimeoutSeconds`|Maximum amount of time (in seconds) to query and populate lookup values&mdash;this is helpful in lookup updates. On lookup update, Druid waits a maximum of `loadTimeoutSeconds` for new lookups to appear and continues serving from the old lookup until the new lookup successfully loads.|No|`0`|
|`maxHeapPercentage`|Mximum percentage of heap size for the lookup to consume. If the lookup grows beyond this size, Druid logs warning messages in the respective service logs.|No|10% of JVM heap size.|

```json
{
  "type":"jdbc",
  "connectorConfig":{
    "connectURI":"jdbc:mysql://localhost:3306/druid",
    "user":"druid",
    "password":"diurd"
  },
  "table":"some_lookup_table",
  "keyColumn":"the_old_dim_value",
  "valueColumn":"the_new_dim_value",
  "tsColumn":"timestamp_column",
  "pollPeriod":600000,
  "jitterSeconds": 120,
  "maxHeapPercentage": 10
}
```

:::info
 If you use JDBC, you must add your database's client JAR files to the extension's directory.
 For Postgres, the connector JAR is already included.
 See the MySQL extension documentation for instructions to obtain [MySQL](../development/extensions-core/mysql.md#installing-the-mysql-connector-library) or [MariaDB](../development/extensions-core/mysql.md#alternative-installing-the-mariadb-connector-library) connector libraries.
 The connector JAR should reside in the classpath of Druid's main class loader.
 To add the connector JAR to the classpath, copy the downloaded file to `lib/` under the distribution root directory. Alternatively, create a symbolic link to the connector in the `lib` directory.
:::

## Introspection points

Globally cached lookups have introspection points at `/keys` and `/values` which return a complete set of the keys and values (respectively) in the lookup. Introspection to `/` returns the entire map. Introspection to `/version` returns the version indicator for the lookup.


<!-- vale on -->