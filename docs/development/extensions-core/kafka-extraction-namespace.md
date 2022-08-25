---
id: kafka-extraction-namespace
title: "Apache Kafka Lookups"
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

To use this Apache Druid extension, [include](../../development/extensions.md#loading-extensions) `druid-lookups-cached-global` and `druid-kafka-extraction-namespace` in the extensions load list.

If you need updates to populate as promptly as possible, it is possible to plug into a Kafka topic whose key is the old value and message is the desired new value (both in UTF-8) as a LookupExtractorFactory.

```json
{
  "type":"kafka",
  "kafkaTopic":"testTopic",
  "kafkaProperties":{
    "bootstrap.servers":"kafka.service:9092"
  }
}
```

| Parameter         | Description                                                                             | Required | Default           |
|-------------------|-----------------------------------------------------------------------------------------|----------|-------------------|
| `kafkaTopic`      | The Kafka topic to read the data from                                                   | Yes      ||
| `kafkaProperties` | Kafka consumer properties (`bootstrap.servers` must be specified)                       | Yes      ||
| `connectTimeout`  | How long to wait for an initial connection                                              | No       | `0` (do not wait) |
| `isOneToOne`      | The map is a one-to-one (see [Lookup DimensionSpecs](../../querying/dimensionspecs.md)) | No       | `false`           |

The extension `kafka-extraction-namespace` enables reading from an [Apache Kafka](https://kafka.apache.org/) topic which has name/key pairs to allow renaming of dimension values. An example use case would be to rename an ID to a human-readable format.

## How it Works

The extractor works by consuming the configured Kafka topic from the beginning, and appending every record to an internal map. The key of the Kafka record is used as they key of the map, and the payload of the record is used as the value. At query time, a lookup can be used to transform the key into the associated value. See [lookups](../../querying/lookups.md) for how to configure and use lookups in a query. Keys and values are both stored as strings by the lookup extractor.

The extractor remains subscribed to the topic, so new records are added to the lookup map as they appear. This allows for lookup values to be updated in near-realtime. If two records are added to the topic with the same key, the record with the larger offset will replace the previous record in the lookup map. A record with a `null` payload will be treated as a tombstone record, and the associated key will be removed from the lookup map.

The extractor treats the input topic much like a [KTable](https://kafka.apache.org/23/javadoc/org/apache/kafka/streams/kstream/KTable.html). As such, it is best to create your Kafka topic using a [log compaction](https://kafka.apache.org/documentation/#compaction) strategy, so that the most-recent version of a key is always preserved in Kafka. Without properly configuring retention and log compaction, older keys that are automatically removed from Kafka will not be available and will be lost when Druid services are restarted.

### Example

Consider a `country_codes` topic is being consumed, and the following records are added to the topic in the following order:

| Offset | Key | Payload     |
|--------|-----|-------------|
| 1      | NZ  | Nu Zeelund  |
| 2      | AU  | Australia   |
| 3      | NZ  | New Zealand |
| 4      | AU  | `null`      |
| 5      | NZ  | Aotearoa    |
| 6      | CZ  | Czechia     |

This input topic would be consumed from the beginning, and result in a lookup namespace containing the following mappings (notice that the entry for _Australia_ was added and then deleted):

| Key | Value     |
|-----|-----------|
| NZ  | Aotearoa  |
| CZ  | Czechia   |

Now when a query uses this extraction namespace, the country codes can be mapped to the full country name at query time.

## Tombstones and Deleting Records

The Kafka lookup extractor treats `null` Kafka messages as tombstones. This means that a record on the input topic with a `null` message payload on Kafka will remove the associated key from the lookup map, effectively deleting it.

## Limitations

The consumer properties `group.id`, `auto.offset.reset` and `enable.auto.commit` cannot be set in `kafkaProperties` as they are set by the extension as `UUID.randomUUID().toString()`, `earliest` and `false` respectively. This is because the entire topic must be consumed by the Druid service from the very beginning so that a complete map of lookup values can be built. Setting any of these consumer properties will cause the extractor to not start.

Currently, the Kafka lookup extractor feeds the entire Kafka topic into a local cache. If you are using on-heap caching, this can easily clobber your java heap if the Kafka stream spews a lot of unique keys. Off-heap caching should alleviate these concerns, but there is still a limit to the quantity of data that can be stored.  There is currently no eviction policy.

## Testing the Kafka rename functionality

To test this setup, you can send key/value pairs to a Kafka stream via the following producer console:

```
./bin/kafka-console-producer.sh --property parse.key=true --property key.separator="->" --broker-list localhost:9092 --topic testTopic
```

Renames can then be published as `OLD_VAL->NEW_VAL` followed by newline (enter or return)
