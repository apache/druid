---
layout: doc_page
---
# KafkaSimpleConsumerFirehose
This firehose acts as a Kafka simple consumer and ingests data from Kafka, currently still in experimental section.
The configuration for KafkaSimpleConsumerFirehose is similar to the KafkaFirehose [Kafka firehose example](realtime-ingestion.html#realtime-specfile), except `firehose` should be replaced with `firehoseV2` like this:
```json
"firehoseV2": {
"type" : "kafka-0.8-v2",
"brokerList" :  ["localhost:4443"],
"queueBufferLength":10001,
"resetBehavior":"latest",
"partitionIdList" : ["0"],
"clientId" : "localclient",
"feed": "wikipedia"
}
```

|property|description|required?|
|--------|-----------|---------|
|type|kafka-0.8-v2|yes|
|brokerList|list of the kafka brokers|yes|
|queueBufferLength|the buffer length for kafka message queue|no default(20000)|
|resetBehavior|in case of kafkaOffsetOutOfRange error happens, consumer should starts from the earliest or latest message available|no default(earliest)|
|partitionIdList|list of kafka partition ids|yes|
|clientId|the clientId for kafka SimpleConsumer|yes|
|feed|kafka topic|yes|

For using this firehose at scale and possibly in production, it is recommended to set replication factor to at least three, which means at least three Kafka brokers in the `brokerList`. For a 1*10^4 events per second topic, keeping one partition can work properly, but more partition could be added if higher throughput is required.

