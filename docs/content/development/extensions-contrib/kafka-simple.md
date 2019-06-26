---
layout: doc_page
title: "Kafka Simple Consumer"
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

# Kafka Simple Consumer

To use this Apache Druid (incubating) extension, make sure to [include](../../operations/including-extensions.html) `druid-kafka-eight-simpleConsumer` extension.

## Firehose

This is an experimental firehose to ingest data from Apache Kafka using the Kafka simple consumer api. Currently, this firehose would only work inside standalone realtime processes.
The configuration for KafkaSimpleConsumerFirehose is similar to the Kafka Eight Firehose , except `firehose` should be replaced with `firehoseV2` like this:

```json
"firehoseV2": {
  "type" : "kafka-0.8-v2",
  "brokerList" :  ["localhost:4443"],
  "queueBufferLength":10001,
  "resetOffsetToEarliest":"true",
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
|resetOffsetToEarliest|in case of kafkaOffsetOutOfRange error happens, consumer should starts from the earliest or latest message available|true|
|partitionIdList|list of kafka partition ids|yes|
|clientId|the clientId for kafka SimpleConsumer|yes|
|feed|kafka topic|yes|

For using this firehose at scale and possibly in production, it is recommended to set replication factor to at least three, which means at least three Kafka brokers in the `brokerList`. For a 1*10^4 events per second kafka topic, keeping one partition can work properly, but more partitions could be added if higher throughput is required.
