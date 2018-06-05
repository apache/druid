---
layout: doc_page
---

# Kafka Lookups

<div class="note caution">
Lookups are an <a href="../experimental.html">experimental</a> feature.
</div>

Make sure to [include](../../operations/including-extensions.html) `druid-lookups-cached-global` and `druid-kafka-extraction-namespace` as an extension.

If you need updates to populate as promptly as possible, it is possible to plug into a kafka topic whose key is the old value and message is the desired new value (both in UTF-8) as a LookupExtractorFactory.

```json
{
  "type":"kafka",
  "kafkaTopic":"testTopic",
  "kafkaProperties":{"zookeeper.connect":"somehost:2181/kafka"}
}
```

|Parameter|Description|Required|Default|
|---------|-----------|--------|-------|
|`kafkaTopic`|The kafka topic to read the data from|Yes||
|`kafkaProperties`|Kafka consumer properties. At least"zookeeper.connect" must be specified. Only the zookeeper connector is supported|Yes||
|`connectTimeout`|How long to wait for an initial connection|No|`0` (do not wait)|
|`isOneToOne`|The map is a one-to-one (see [Lookup DimensionSpecs](../../querying/dimensionspecs.html))|No|`false`|

The extension `kafka-extraction-namespace` enables reading from a kafka feed which has name/key pairs to allow renaming of dimension values. An example use case would be to rename an ID to a human readable format.

The consumer properties `group.id` and `auto.offset.reset` CANNOT be set in `kafkaProperties` as they are set by the extension as `UUID.randomUUID().toString()` and `smallest` respectively.

See [lookups](../../querying/lookups.html) for how to configure and use lookups.

# Limitations

Currently the Kafka lookup extractor feeds the entire kafka stream into a local cache. If you are using OnHeap caching, this can easily clobber your java heap if the kafka stream spews a lot of unique keys.
OffHeap caching should alleviate these concerns, but there is still a limit to the quantity of data that can be stored.
There is currently no eviction policy.

## Testing the Kafka rename functionality

To test this setup, you can send key/value pairs to a kafka stream via the following producer console:

```
./bin/kafka-console-producer.sh --property parse.key=true --property key.separator="->" --broker-list localhost:9092 --topic testTopic
```

Renames can then be published as `OLD_VAL->NEW_VAL` followed by newline (enter or return)
