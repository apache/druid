---
layout: doc_page
---

# Kafka Firehose

Make sure to [include](../../operations/including-extensions.html) `druid-kafka` as an extension.

This firehose acts as a Kafka 1.0.x consumer and ingests data from Kafka.

**Note:** Add **kafka-clients-1.0.0.jar** to lib directory of druid to use this firehose.

Sample spec:

```json
"firehose": {
  "type": "kafka-1.0",
  "consumerProps": {
    "bootstrap.servers": "localhost:9092",
    "group.id": "druid-example",
    "fetch.max.bytes" : "1048586",
    "auto.offset.reset": "latest",
    "enable.auto.commit": "false"
  },
  "feed": "wikipedia",
  "rowDelimiter": "\n",
  "messageEncoding": "UTF-8"
}
```

|Property|Description|Required|Default|
|--------|-----------|--------|-------|
|type|This should be "kafka-1.0"|yes||
|consumerProps|The full list of consumer configs can be [here](https://kafka.apache.org/10/documentation/#newconsumerconfigs).|yes||
|feed|Kafka maintains feeds of messages in categories called topics. This is the topic name.|yes||
|rowDelimiter|Kafka maintains feeds of messages in categories called topics. This is the topic name.|no|`null`, which means one meassge is one row|
|messageEncoding|The Kafka message encoding. Only available when `rowDelimiter` is set.|no|"UTF-8"|
