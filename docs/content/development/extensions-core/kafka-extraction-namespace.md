---
layout: doc_page
---

# Kafka Lookups

<div class="note caution">
Lookups are an <a href="../experimental.html">experimental</a> feature.
</div>

Make sure to [include](../../operations/including-extensions.html) `druid-namespace-lookup` and `druid-kafka-extraction-namespace` as an extension.

If you need updates to populate as promptly as possible, it is possible to plug into a kafka topic whose key is the old value and message is the desired new value (both in UTF-8) as a LookupExtractorFactory.

```json
{
  "type":"kafka",
  "kafkaTopic":"testTopic",
  "kafkaProperties":{"zookeeper.connect","somehost:2181/kafka"}
}
```

|Parameter|Description|Required|Default|
|---------|-----------|--------|-------|
|`kafkaTopic`|The kafka topic to read the data from|Yes||
|`kafkaProperties`|Kafka consumer properties. At least"zookeeper.connect" must be specified. Only the zookeeper connector is supported|Yes||

The extension `kafka-extraction-namespace` enables reading from a kafka feed which has name/key pairs to allow renaming of dimension values. An example use case would be to rename an ID to a human readable format.

The consumer properties `group.id` and `auto.offset.reset` CANNOT be set in `kafkaProperties` as they are set by the extension as `UUID.randomUUID().toString()` and `smallest` respectively.

See [lookups](../../querying/lookups.html) for how to configure and use lookups.

## Testing the Kafka rename functionality

To test this setup, you can send key/value pairs to a kafka stream via the following producer console:

```
./bin/kafka-console-producer.sh --property parse.key=true --property key.separator="->" --broker-list localhost:9092 --topic testTopic
```

Renames can then be published as `OLD_VAL->NEW_VAL` followed by newline (enter or return)
