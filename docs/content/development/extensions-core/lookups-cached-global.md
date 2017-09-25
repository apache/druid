---
layout: doc_page
---

# Globally Cached Lookups

<div class="note caution">
Lookups are an <a href="../experimental.html">experimental</a> feature.
</div>

Make sure to [include](../../operations/including-extensions.html) `druid-lookups-cached-global` as an extension.

## Configuration
<div class="note caution">
Static configuration is no longer supported. Lookups can be configured through
<a href="../../querying/lookups.html#configuration">dynamic configuration</a>.
</div>

Globally cached lookups are appropriate for lookups which are not possible to pass at query time due to their size,
or are not desired to be passed at query time because the data is to reside in and be handled by the Druid servers,
and are small enough to reasonably populate on a node. This usually means tens to tens of thousands of entries per lookup.

Globally cached lookups all draw from the same cache pool, allowing each node to have a fixed cache pool that can be used by cached lookups.

Globally cached lookups can be specified as part of the [cluster wide config for lookups](../../querying/lookups.html) as a type of `cachedNamespace`

 ```json
 {
    "type": "cachedNamespace",
    "extractionNamespace": {
       "type": "uri",
       "uri": "file:/tmp/prefix/",
       "namespaceParseSpec": {
         "format": "csv",
         "columns": [
           "key",
           "value"
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
         "createTables": true,
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

The parameters are as follows

|Property|Description|Required|Default|
|--------|-----------|--------|-------|
|`extractionNamespace`|Specifies how to populate the local cache. See below|Yes|-|
|`firstCacheTimeout`|How long to wait (in ms) for the first run of the cache to populate. 0 indicates to not wait|No|`0` (do not wait)|
|`injective`|If the underlying map is injective (keys and values are unique) then optimizations can occur internally by setting this to `true`|No|`false`|

If `firstCacheTimeout` is set to a non-zero value, it should be less than `druid.manager.lookups.hostUpdateTimeout`. If `firstCacheTimeout` is NOT set, then management is essentially asynchronous and does not know if a lookup succeeded or failed in starting. In such a case logs from the lookup nodes should be monitored for repeated failures.

Proper functionality of globally cached lookups requires the following extension to be loaded on the broker, peon, and historical nodes:
`druid-lookups-cached-global`

## Example configuration

In a simple case where only one [tier](../../querying/lookups.html#dynamic-configuration) exists (`realtime_customer2`) with one `cachedNamespace` lookup called `country_code`, the resulting configuration json looks similar to the following:

```json
{
  "realtime_customer2": {
    "country_code": {
      "type": "cachedNamespace",
      "extractionNamespace": {
         "type": "jdbc",
         "connectorConfig": {
           "createTables": true,
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
      "injective":true
    }
  }
}
```

Where the coordinator endpoint `/druid/coordinator/v1/lookups/realtime_customer2/country_code` should return

```json
{
  "type": "cachedNamespace",
  "extractionNamespace": {
    "type": "jdbc",
    "connectorConfig": {
      "createTables": true,
      "connectURI": "jdbc:mysql:\/\/localhost:3306\/druid",
      "user": "druid",
      "password": "diurd"
    },
    "table": "lookupTable",
    "keyColumn": "country_id",
    "valueColumn": "country_name",
    "tsColumn": "timeColumn"
  },
  "firstCacheTimeout": 120000,
  "injective":true
}
```

## Cache Settings

Lookups are cached locally on historical nodes. The following are settings used by the nodes which service queries when 
setting namespaces (broker, peon, historical)

|Property|Description|Default|
|--------|-----------|-------|
|`druid.lookup.namespace.cache.type`|Specifies the type of caching to be used by the namespaces. May be one of [`offHeap`, `onHeap`]. `offHeap` uses a temporary file for off-heap storage of the namespace (memory mapped files). `onHeap` stores all cache on the heap in standard java map types.|`onHeap`|
|`druid.lookup.namespace.numExtractionThreads`|The number of threads in the thread pool dedicated for lookup extraction and updates. This number may need to be scaled up, if you have a lot of lookups and they take long time to extract, to avoid timeouts.|2|

The cache is populated in different ways depending on the settings below. In general, most namespaces employ 
a `pollPeriod` at the end of which time they poll the remote resource of interest for updates.

`onHeap` uses `ConcurrentMap`s in the java heap, and thus affects garbage collection and heap sizing.
`offHeap` uses a 10MB on-heap buffer and MapDB using memory-mapped files in the java temporary directory.
So if total `cachedNamespace` lookup size is in excess of 10MB, the extra will be kept in memory as page cache, and paged in and out by general OS tunings.

# Supported Lookups

For additional lookups, please see our [extensions list](../extensions.html).

## URI lookup

The remapping values for each globally cached lookup can be specified by a json object as per the following examples:

```json
{
  "type":"uri",
  "uri": "s3://bucket/some/key/prefix/renames-0003.gz",
  "namespaceParseSpec":{
    "format":"csv",
    "columns":["key","value"]
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
    "columns":["key","value"]
  },
  "pollPeriod":"PT5M"
}
```

|Property|Description|Required|Default|
|--------|-----------|--------|-------|
|`pollPeriod`|Period between polling for updates|No|0 (only once)|
|`uri`|URI for the file of interest|No|Use `uriPrefix`|
|`uriPrefix`|A URI which specifies a directory (or other searchable resource) in which to search for files|No|Use `uri`|
|`fileRegex`|Optional regex for matching the file name under `uriPrefix`. Only used if `uriPrefix` is used|No|`".*"`|
|`namespaceParseSpec`|How to interpret the data at the URI|Yes||

One of either `uri` xor `uriPrefix` must be specified.

The `pollPeriod` value specifies the period in ISO 8601 format between checks for replacement data for the lookup. If the source of the lookup is capable of providing a timestamp, the lookup will only be updated if it has changed since the prior tick of `pollPeriod`. A value of 0, an absent parameter, or `null` all mean populate once and do not attempt to look for new data later. Whenever an poll occurs, the updating system will look for a file with the most recent timestamp and assume that one with the most recent data set, replacing the local cache of the lookup data.

The `namespaceParseSpec` can be one of a number of values. Each of the examples below would rename foo to bar, baz to bat, and buck to truck. All parseSpec types assumes each input is delimited by a new line. See below for the types of parseSpec supported.

Only ONE file which matches the search will be used. For most implementations, the discriminator for choosing the URIs is by whichever one reports the most recent timestamp for its modification time.

### csv lookupParseSpec
|Parameter|Description|Required|Default|
|---------|-----------|--------|-------|
|`columns`|The list of columns in the csv file|no if `hasHeaderRow` is set|`null`|
|`keyColumn`|The name of the column containing the key|no|The first column|
|`valueColumn`|The name of the column containing the value|no|The second column|
|`hasHeaderRow`|A flag to indicate that column information can be extracted from the input files' header row|no|false|
|`skipHeaderRows`|Number of header rows to be skipped|no|0|

If both `skipHeaderRows` and `hasHeaderRow` options are set, `skipHeaderRows` is first applied. For example, if you set
`skipHeaderRows` to 2 and `hasHeaderRow` to true, Druid will skip the first two lines and then extract column information
from the third line.

*example input*

```
bar,something,foo
bat,something2,baz
truck,something3,buck
```

*example namespaceParseSpec*

```json
"namespaceParseSpec": {
  "format": "csv",
  "columns": ["value","somethingElse","key"],
  "keyColumn": "key",
  "valueColumn": "value"
}
```

### tsv lookupParseSpec
|Parameter|Description|Required|Default|
|---------|-----------|--------|-------|
|`columns`|The list of columns in the tsv file|yes|`null`|
|`keyColumn`|The name of the column containing the key|no|The first column|
|`valueColumn`|The name of the column containing the value|no|The second column|
|`delimiter`|The delimiter in the file|no|tab (`\t`)|
|`listDelimiter`|The list delimiter in the file|no| (`\u0001`)|
|`hasHeaderRow`|A flag to indicate that column information can be extracted from the input files' header row|no|false|
|`skipHeaderRows`|Number of header rows to be skipped|no|0|

If both `skipHeaderRows` and `hasHeaderRow` options are set, `skipHeaderRows` is first applied. For example, if you set
`skipHeaderRows` to 2 and `hasHeaderRow` to true, Druid will skip the first two lines and then extract column information
from the third line.

*example input*

```
bar|something,1|foo
bat|something,2|baz
truck|something,3|buck
```

*example namespaceParseSpec*

```json
"namespaceParseSpec": {
  "format": "tsv",
  "columns": ["value","somethingElse","key"],
  "keyColumn": "key",
  "valueColumn": "value",
  "delimiter": "|"
}
```

### customJson lookupParseSpec

|Parameter|Description|Required|Default|
|---------|-----------|--------|-------|
|`keyFieldName`|The field name of the key|yes|null|
|`valueFieldName`|The field name of the value|yes|null|

*example input*

```json
{"key": "foo", "value": "bar", "somethingElse" : "something"}
{"key": "baz", "value": "bat", "somethingElse" : "something"}
{"key": "buck", "somethingElse": "something", "value": "truck"}
```

*example namespaceParseSpec*

```json
"namespaceParseSpec": {
  "format": "customJson",
  "keyFieldName": "key",
  "valueFieldName": "value"
}
```

With customJson parsing, if the value field for a particular row is missing or null then that line will be skipped, and
will not be included in the lookup.

### simpleJson lookupParseSpec
The `simpleJson` lookupParseSpec does not take any parameters. It is simply a line delimited json file where the field is the key, and the field's value is the value.

*example input*
 
```json
{"foo": "bar"}
{"baz": "bat"}
{"buck": "truck"}
```

*example namespaceParseSpec*

```json
"namespaceParseSpec":{
  "format": "simpleJson"
}
```

## JDBC lookup

The JDBC lookups will poll a database to populate its local cache. If the `tsColumn` is set it must be able to accept comparisons in the format `'2015-01-01 00:00:00'`. For example, the following must be valid sql for the table `SELECT * FROM some_lookup_table WHERE timestamp_column >  '2015-01-01 00:00:00'`. If `tsColumn` is set, the caching service will attempt to only poll values that were written *after* the last sync. If `tsColumn` is not set, the entire table is pulled every time.

|Parameter|Description|Required|Default|
|---------|-----------|--------|-------|
|`namespace`|The namespace to define|Yes||
|`connectorConfig`|The connector config to use|Yes||
|`table`|The table which contains the key value pairs|Yes||
|`keyColumn`|The column in `table` which contains the keys|Yes||
|`valueColumn`|The column in `table` which contains the values|Yes||
|`filter`|The filter to use when selecting lookups, this is used to create a where clause on lookup population|No|No Filter|
|`tsColumn`| The column in `table` which contains when the key was updated|No|Not used|
|`pollPeriod`|How often to poll the DB|No|0 (only once)|

```json
{
  "type":"jdbc",
  "namespace":"some_lookup",
  "connectorConfig":{
    "createTables":true,
    "connectURI":"jdbc:mysql://localhost:3306/druid",
    "user":"druid",
    "password":"diurd"
  },
  "table":"some_lookup_table",
  "keyColumn":"the_old_dim_value",
  "valueColumn":"the_new_dim_value",
  "tsColumn":"timestamp_column",
  "pollPeriod":600000
}
```

# Introspection

Globally cached lookups have introspection points at `/keys` and `/values` which return a complete set of the keys and values (respectively) in the lookup. Introspection to `/` returns the entire map. Introspection to `/version` returns the version indicator for the lookup.
