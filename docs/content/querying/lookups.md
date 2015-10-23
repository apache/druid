---
layout: doc_page
---
# Lookups

Lookups are a concept in Druid where dimension values are (optionally) replaced with a new value. See [dimension specs](../querying/dimensionspecs.html) for more information. For the purpose of these documents, a "key" refers to a dimension value to match, and a "value" refers to its replacement. So if you wanted to rename `appid-12345` to `Super Mega Awesome App` then the key would be `appid-12345` and the value would be `Super Mega Awesome App`. 

It is worth noting that lookups support use cases where keys map to unique values (injective) as per a country code and a country name, and also supports use cases where multiple IDs map to the same value as per multiple app-ids belonging to a single account manager.

Lookups do not have history. They always use the current data. This means that if the chief account manager for a particular app-id changes, and you issue a query with a lookup to store the app-id to account manager relationship, it will return the current account manager for that app-id REGARDLESS of the time range over which you query.

If you require data timerange sensitive lookups, such a use case is not currently supported dynamically at query time, and such data belongs in the raw denormalized data for use in Druid.

Very small lookups (count of keys on the order of a few dozen to a few hundred) can be passed at query time as a map lookup as per [dimension specs](../querying/dimensionspecs.html).

Namespaced lookups are appropriate for lookups which are not possible to pass at query time due to their size, or are not desired to be passed at query time because the data is to reside in and be handled by the Druid servers. Namespaced lookups can be specified as part of the runtime properties file. The property is a list of the namespaces described as per the sections on this page.

 ```json
 druid.query.extraction.namespace.lookups=\
   [{ "type":"uri", "namespace":"some_uri_lookup","uri": "file:/tmp/prefix/",\
   "namespaceParseSpec":\
     {"format":"csv","columns":["key","value"]},\
   "pollPeriod":"PT5M"},\
   { "type":"jdbc", "namespace":"some_jdbc_lookup",\
   "connectorConfig":{"createTables":true,"connectURI":"jdbc:mysql://localhost:3306/druid","user":"druid","password":"diurd"},\
   "table": "lookupTable", "keyColumn": "mykeyColumn", "valueColumn": "MyValueColumn", "tsColumn": "timeColumn"}]
 ```

Proper funcitonality of Namespaced lookups requires the following extension to be loaded on the broker, peon, and historical nodes:
`io.druid.extensions:druid-namespace-lookup`

## Cache Settings
The following are settings used by the nodes which service queries when setting namespaces (broker, peon, historical)

|Property|Description|Default|
|--------|-----------|-------|
|`druid.query.extraction.namespace.cache.type`|Specifies the type of caching to be used by the namespaces. May be one of [`offHeap`, `onHeap`]. `offHeap` uses a temporary file for off-heap storage of the namespace (memory mapped files). `onHeap` stores all cache on the heap in standard java map types.|`onHeap`|

The cache is populated in different ways depending on the settings below. In general, most namespaces employ a `pollPeriod` at the end of which time they poll the remote resource of interest for updates. The notable exception being the kafka namespace lookup as defined below.

## URI namespace update
The remapping values for each namespaced lookup can be specified by json as per

```json
{
  "type":"uri",
  "namespace":"some_lookup",
  "uri": "s3://bucket/some/key/prefix/",
  "namespaceParseSpec":{
    "format":"csv",
    "columns":["key","value"]
  },
  "pollPeriod":"PT5M",
  "versionRegex": "renames-[0-9]*\\.gz"
}
```

|Property|Description|Required|Default|
|--------|-----------|--------|-------|
|`namespace`|The namespace to define|Yes||
|`pollPeriod`|Period between polling for updates|No|0 (only once)|
|`versionRegex`|Regex to help find newer versions of the namespace data|Yes||
|`namespaceParseSpec`|How to interpret the data at the URI|Yes||

The `pollPeriod` value specifies the period in ISO 8601 format between checks for updates. If the source of the lookup is capable of providing a timestamp, the lookup will only be updated if it has changed since the prior tick of `pollPeriod`. A value of 0, an absent parameter, or `null` all mean populate once and do not attempt to update. Whenever an update occurs, the updating system will look for a file with the most recent timestamp and assume that one with the most recent data.

The `versionRegex` value specifies a regex to use to determine if a filename in the parent path of the uri should be considered when trying to find the latest version. Omitting this setting or setting it equal to `null` will match to all files it can find (equivalent to using `".*"`). The search occurs in the most significant "directory" of the uri.

The `namespaceParseSpec` can be one of a number of values. Each of the examples below would rename foo to bar, baz to bat, and buck to truck. All parseSpec types assumes each input is delimited by a new line. See below for the types of parseSpec supported.


### csv lookupParseSpec

|Parameter|Description|Required|Default|
|---------|-----------|--------|-------|
|`columns`|The list of columns in the csv file|yes|`null`|
|`keyColumn`|The name of the column containing the key|no|The first column|
|`valueColumn`|The name of the column containing the value|no|The second column|

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
|`columns`|The list of columns in the csv file|yes|`null`|
|`keyColumn`|The name of the column containing the key|no|The first column|
|`valueColumn`|The name of the column containing the value|no|The second column|
|`delimiter`|The delimiter in the file|no|tab (`\t`)|


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
  "type": "simpleJson"
}
```

## JDBC namespaced lookup

The JDBC lookups will poll a database to populate its local cache. If the `tsColumn` is set it must be able to accept comparisons in the format `'2015-01-01 00:00:00'`. For example, the following must be valid sql for the table `SELECT * FROM some_lookup_table WHERE timestamp_column >  '2015-01-01 00:00:00'`. If `tsColumn` is set, the caching service will attempt to only poll values that were written *after* the last sync. If `tsColumn` is not set, the entire table is pulled every time.

|Parameter|Description|Required|Default|
|---------|-----------|--------|-------|
|`namespace`|The namespace to define|Yes||
|`connectorConfig`|The connector config to use|Yes||
|`table`|The table which contains the key value pairs|Yes||
|`keyColumn`|The column in `table` which contains the keys|Yes||
|`valueColumn`|The column in `table` which contains the values|Yes||
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

# Kafka namespaced lookup
If you need updates to populate as promptly as possible, it is possible to plug into a kafka topic whose key is the old value and message is the desired new value (both in UTF-8). This requires the following extension: "io.druid.extensions:kafka-extraction-namespace"

```json
{
  "type":"kafka",
  "namespace":"testTopic",
  "kafkaTopic":"testTopic"
}
```

|Parameter|Description|Required|Default|
|---------|-----------|--------|-------|
|`namespace`|The namespace to define|Yes||
|`kafkaTopic`|The kafka topic to read the data from|Yes||


## Kafka renames
The extension `kafka-extraction-namespace` enables reading from a kafka feed which has name/key pairs to allow renaming of dimension values. An example use case would be to rename an ID to a human readable format.

Currently the historical node caches the key/value pairs from the kafka feed in an ephemeral memory mapped DB via MapDB.

## Configuration
The following options are used to define the behavior and should be included wherever the extension is included (all query servicing nodes):

|Property|Description|Default|
|--------|-----------|-------|
|`druid.query.rename.kafka.properties`|A json map of kafka consumer properties. See below for special properties.|See below|

The following are the handling for kafka consumer properties in `druid.query.rename.kafka.properties`

|Property|Description|Default|
|--------|-----------|-------|
|`zookeeper.connect`|Zookeeper connection string|`localhost:2181/kafka`|
|`group.id`|Group ID, auto-assigned for publish-subscribe model and cannot be overridden|`UUID.randomUUID().toString()`|
|`auto.offset.reset`|Setting to get the entire kafka rename stream. Cannot be overridden|`smallest`|

## Testing the kafka rename functionality
To test this setup, you can send key/value pairs to a kafka stream via the following producer console:

`./bin/kafka-console-producer.sh --property parse.key=true --property key.separator="->" --broker-list localhost:9092 --topic testTopic`

Renames can then be published as `OLD_VAL->NEW_VAL` followed by newline (enter or return)
