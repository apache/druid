---
layout: doc_page
---
# Lookups

<div class="note caution">
Lookups are an <a href="../development/experimental.html">experimental</a> feature.
</div>

Lookups are a concept in Druid where dimension values are (optionally) replaced with new values. 
See [dimension specs](../querying/dimensionspecs.html) for more information. For the purpose of these documents, 
a "key" refers to a dimension value to match, and a "value" refers to its replacement. 
So if you wanted to rename `appid-12345` to `Super Mega Awesome App` then the key would be `appid-12345` and the value 
would be `Super Mega Awesome App`. 

It is worth noting that lookups support use cases where keys map to unique values (injective) such as a country code and 
a country name, and also supports use cases where multiple IDs map to the same value, e.g. multiple app-ids belonging to 
a single account manager.

Lookups do not have history. They always use the current data. This means that if the chief account manager for a 
particular app-id changes, and you issue a query with a lookup to store the app-id to account manager relationship, 
it will return the current account manager for that app-id REGARDLESS of the time range over which you query.

If you require data time range sensitive lookups, such a use case is not currently supported dynamically at query time, 
and such data belongs in the raw denormalized data for use in Druid.

Very small lookups (count of keys on the order of a few dozen to a few hundred) can be passed at query time as a "map" 
lookup as per [dimension specs](../querying/dimensionspecs.html).

## Configuration

|Property|Description|Default|
|--------|-----------|-------|
|`druid.lookup.snapshotWorkingDir`| Working path used to store snapshot of current lookup configuration, leaving this property null will disable snapshot/bootstrap utility|null|

Namespaced lookups are appropriate for lookups which are not possible to pass at query time due to their size, 
or are not desired to be passed at query time because the data is to reside in and be handled by the Druid servers. 
Namespaced lookups can be specified as part of the runtime properties file. The property is a list of the namespaces 
described as per the sections on this page. For example:

 ```json
 druid.query.extraction.namespace.lookups=
   [
     {
       "type": "uri",
       "namespace": "some_uri_lookup",
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
     {
       "type": "jdbc",
       "namespace": "some_jdbc_lookup",
       "connectorConfig": {
         "createTables": true,
         "connectURI": "jdbc:mysql:\/\/localhost:3306\/druid",
         "user": "druid",
         "password": "diurd"
       },
       "table": "lookupTable",
       "keyColumn": "mykeyColumn",
       "valueColumn": "MyValueColumn",
       "tsColumn": "timeColumn"
     }
   ]
 ```

Proper functionality of Namespaced lookups requires the following extension to be loaded on the broker, peon, and historical nodes: 
`druid-namespace-lookup`

## Cache Settings

Lookups are cached locally on historical nodes. The following are settings used by the nodes which service queries when 
setting namespaces (broker, peon, historical)

|Property|Description|Default|
|--------|-----------|-------|
|`druid.query.extraction.namespace.cache.type`|Specifies the type of caching to be used by the namespaces. May be one of [`offHeap`, `onHeap`]. `offHeap` uses a temporary file for off-heap storage of the namespace (memory mapped files). `onHeap` stores all cache on the heap in standard java map types.|`onHeap`|

The cache is populated in different ways depending on the settings below. In general, most namespaces employ 
a `pollPeriod` at the end of which time they poll the remote resource of interest for updates. A notable exception 
is the Kafka namespace lookup, defined below.

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
  "format": "simpleJson"
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

## Testing the Kafka rename functionality

To test this setup, you can send key/value pairs to a kafka stream via the following producer console:

```
./bin/kafka-console-producer.sh --property parse.key=true --property key.separator="->" --broker-list localhost:9092 --topic testTopic
```

Renames can then be published as `OLD_VAL->NEW_VAL` followed by newline (enter or return)

Dynamic configuration (EXPERIMENTAL)
------------------------------------

The following documents the behavior of the cluster-wide config which is accessible through the coordinator.
The configuration is propagated through the concept of "tier" of servers.
A "tier" is defined as a group of services which should receive a set of lookups.
For example, you might have all historicals be part of `__default`, and Peons be part of individual tiers for the datasources they are tasked with.
The tiers for lookups are completely independent of historical tiers.

These configs are accessed using JSON through the following URI template

```
http://<COORDINATOR_IP>:<PORT>/druid/coordinator/v1/lookups/{tier}/{id}
```

All URIs below are assumed to have `http://<COORDINATOR_IP>:<PORT>` prepended.

If you have NEVER configured lookups before, you MUST post an empty json object `{}` to `/druid/coordinator/v1/lookups` to initialize the configuration.

These endpoints will return one of the following results:

* 404 if the resource is not found
* 400 if there is a problem in the formatting of the request
* 202 if the request was accepted asynchronously (`POST` and `DELETE`)
* 200 if the request succeeded (`GET` only)

## Configuration propagation behavior
The configuration is propagated to the query serving nodes (broker / router / peon / historical) by the coordinator.
The query serving nodes have an internal API for managing `POST`/`GET`/`DELETE` of lookups.
The coordinator periodically checks the dynamic configuration for changes and, when it detects a change it does the following:

1. Post all lookups for a tier to all Druid nodes within that tier.
2. Delete lookups from a tier which were dropped between the prior configuration values and this one.

If there is no configuration change, the coordinator checks for any nodes which might be new since the last time it propagated lookups and adds all lookups for that node (assuming that node's tier has lookups).
If there are errors while trying to add or update configuration on a node, that node is temporarily skipped until the next management period. The next management period the update will attempt to be propagated again.
If there is an error while trying to delete a lookup from a node (or if a node is down when the coordinator is propagating the config), the delete is not attempted again. In such a case it is possible that a node has lookups that are no longer managed by the coordinator.

## Bulk update
Lookups can be updated in bulk by posting a JSON object to `/druid/coordinator/v1/lookups`. The format of the json object is as follows:


```json
{
    "tierName": {
        "lookupExtractorFactoryName": {
          "someExtractorField": "someExtractorValue"
        }
    }
}
```

So a config might look something like:
```json
{
    "__default": {
        "country_code": {
          "type": "simple_json",
          "uri": "http://some.host.com/codes.json"
        },
        "site_id": {
            "type": "confidential_jdbc",
            "auth": "/etc/jdbc.internal",
            "table": "sites",
            "key": "site_id",
            "value": "site_name"
        },
        "site_id_customer1": {
            "type": "confidential_jdbc",
            "auth": "/etc/jdbc.customer1",
            "table": "sites",
            "key": "site_id",
            "value": "site_name"
        },
        "site_id_customer2": {
            "type": "confidential_jdbc",
            "auth": "/etc/jdbc.customer2",
            "table": "sites",
            "key": "site_id",
            "value": "site_name"
        }
    },
    "realtime_customer1": {
        "country_code": {
          "type": "simple_json",
          "uri": "http://some.host.com/codes.json"
        },
        "site_id_customer1": {
            "type": "confidential_jdbc",
            "auth": "/etc/jdbc.customer1",
            "table": "sites",
            "key": "site_id",
            "value": "site_name"
        }
    },
    "realtime_customer2": {
        "country_code": {
          "type": "simple_json",
          "uri": "http://some.host.com/codes.json"
        },
        "site_id_customer2": {
            "type": "confidential_jdbc",
            "auth": "/etc/jdbc.customer2",
            "table": "sites",
            "key": "site_id",
            "value": "site_name"
        }
    }
}
```

All entries in the map will UPDATE existing entries. No entries will be deleted.

## Update Lookup
A `POST` to a particular lookup extractor factory via `/druid/coordinator/v1/lookups/{tier}/{id}` will update that specific extractor factory.

For example, a post to `/druid/coordinator/v1/lookups/realtime_customer1/site_id_customer1` might contain the following:

```json
{
    "type": "confidential_jdbc",
    "auth": "/etc/jdbc.customer1",
    "table": "sites_updated",
    "key": "site_id",
    "value": "site_name"
}
```

This will replace the `site_id_customer1` lookup in the `realtime_customer1` with the definition above.

## Get Lookup
A `GET` to a particular lookup extractor factory is accomplished via `/druid/coordinator/v1/lookups/{tier}/{id}`

Using the prior example, a `GET` to `/druid/coordinator/v1/lookups/realtime_customer2/site_id_customer2` should return

```json
{
    "type": "confidential_jdbc",
    "auth": "/etc/jdbc.customer2",
    "table": "sites",
    "key": "site_id",
    "value": "site_name"
}
```

## Delete Lookup
A `DELETE` to `/druid/coordinator/v1/lookups/{tier}/{id}` will remove that lookup from the cluster.

## List tier names
A `GET` to `/druid/coordinator/v1/lookups` will return a list of known tier names in the dynamic configuration.
To discover a list of tiers currently active in the cluster **instead of** ones known in the dynamic configuration, the parameter `discover=true` can be added as per `/druid/coordinator/v1/lookups?discover=true`.

## List lookup names
A `GET` to `/druid/coordinator/v1/lookups/{tier}` will return a list of known lookup names for that tier.

# Internal API

The Peon, Router, Broker, and Historical nodes all have the ability to consume lookup configuration.
There is an internal API these nodes use to list/load/drop their lookups starting at `/druid/listen/v1/lookups`.
These follow the same convention for return values as the cluster wide dynamic configuration.
Usage of these endpoints is quite advanced and not recommended for most users.
The endpoints are as follows:

## Get Lookups

A `GET` to the node at `/druid/listen/v1/lookups` will return a json map of all the lookups currently active on the node.
The return value will be a json map of the lookups to their extractor factories.

```json

{
    "some_lookup_name": {
        "type": "simple_json",
        "uri": "http://some.host.com/codes.json"
    }
}

```

## Get Lookup

A `GET` to the node at `/druid/listen/v1/lookups/some_lookup_name` will return the LookupExtractorFactory for the lookup identified by `some_lookup_name`.
The return value will be the json representation of the factory.

```json
{
    "type": "simple_json",
    "uri": "http://some.host.com/codes.json"
}
```

## Bulk Add or Update Lookups

A `POST` to the node at `/druid/listen/v1/lookups` of a JSON map of lookup names to LookupExtractorFactory will cause the service to add or update its lookups.
The return value will be a JSON map in the following format:

```json
{
    "status": "accepted",
    "failedUpdates": {}
}

```

If a lookup cannot be started, or is left in an undefined state, the lookup in error will be returned in the `failedUpdates` field as per:

```json
{
    "status": "accepted",
    "failedUpdates": {
        "country_code": {
            "type": "simple_json",
            "uri": "http://some.host.com/codes.json"
        }
    }
}

```

The `failedUpdates` field of the return value should be checked if a user is wanting to assure that every update succeeded.

## Add or Update Lookup

A `POST` to the node at `/druid/listen/v1/lookups/some_lookup_name` will behave very similarly to a bulk update.

If `some_lookup_name` is desired to have the LookupExtractorFactory definition of 

```json
{
    "type": "simple_json",
    "uri": "http://some.host.com/codes.json"
}
```

Then a post to `/druid/listen/v1/lookups/some_lookup_name` will behave the same as a `POST` to `/druid/listen/v1/lookups` of

```json

{
    "some_lookup_name": {
        "type": "simple_json",
        "uri": "http://some.host.com/codes.json"
    }
}

```

## Remove a Lookup
A `DELETE` to `/druid/listen/v1/lookups/some_lookup_name` will remove that lookup from the node. Success will reflect the ID.

# Configuration
See the [coordinator configuration guilde](../configuration/coordinator.html) for coordinator configuration

To configure a Broker / Router / Historical / Peon to announce itself as part of a lookup tier, use the `druid.zk.paths.lookupTier` property.

|Property | Description | Default |
|---------|-------------|---------|
|`druid.lookup.tierName`| The tier for **lookups** for this node. This is independent of other tiers.|`__default`|
