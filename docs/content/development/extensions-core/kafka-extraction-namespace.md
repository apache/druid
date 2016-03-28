---
layout: doc_page
---

# Kafka Namespaced Lookup

<div class="note caution">
Lookups are an <a href="../development/experimental.html">experimental</a> feature.
</div>

Make sure to [include](../../operations/including-extensions.html) `druid-namespace-lookup` and `druid-kafka-extraction-namespace` as an extension.

Note that this lookup does not employ a `pollPeriod`.

If you need updates to populate as promptly as possible, it is possible to plug into a kafka topic whose key is the old value and message is the desired new value (both in UTF-8).

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
