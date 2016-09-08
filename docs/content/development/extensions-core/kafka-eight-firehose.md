---
layout: doc_page
---

# Kafka Eight Firehose

Make sure to [include](../../operations/including-extensions.html) `druid-kafka-eight` as an extension.

This firehose acts as a Kafka 0.8.x consumer and ingests data from Kafka.

Sample spec:

```json
"firehose": {
  "type": "kafka-0.8",
  "consumerProps": {
    "zookeeper.connect": "localhost:2181",
    "zookeeper.connection.timeout.ms" : "15000",
    "zookeeper.session.timeout.ms" : "15000",
    "zookeeper.sync.time.ms" : "5000",
    "group.id": "druid-example",
    "fetch.message.max.bytes" : "1048586",
    "auto.offset.reset": "largest",
    "auto.commit.enable": "false"
  },
  "feed": "wikipedia"
}
```

|property|description|required?|
|--------|-----------|---------|
|type|This should be "kafka-0.8"|yes|
|consumerProps|The full list of consumer configs can be [here](https://kafka.apache.org/08/configuration.html).|yes|
|feed|Kafka maintains feeds of messages in categories called topics. This is the topic name.|yes|
