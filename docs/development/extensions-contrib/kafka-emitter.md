---
id: kafka-emitter
title: "Kafka Emitter"
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


To use this Apache Druid extension, [include](../../configuration/extensions.md#loading-extensions) `kafka-emitter` in the extensions load list.

## Introduction

This extension emits Druid metrics to [Apache Kafka](https://kafka.apache.org) directly with JSON format.<br />
Currently, Kafka has not only their nice ecosystem but also consumer API readily available.
So, If you currently use Kafka, It's easy to integrate various tool or UI
to monitor the status of your Druid cluster with this extension.

## Configuration

All the configuration parameters for the Kafka emitter are under `druid.emitter.kafka`.

| Property                                           | Description                                                                                                                               | Required | Default               |
|----------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------|-----------|-----------------------|
| `druid.emitter.kafka.bootstrap.servers`            | Comma-separated Kafka broker. (`[hostname:port],[hostname:port]...`)                                                                      | yes       | none                  |
| `druid.emitter.kafka.event.types`                  | Comma-separated event types. <br/>Supported types are `alerts`, `metrics`, `requests`, and `segment_metadata`.                            | no        | `["metrics", "alerts"]` |
| `druid.emitter.kafka.metric.topic`                 | Kafka topic name for emitter's target to emit service metrics. If `event.types` contains `metrics`, this field cannot be empty.           | no        | none                  |
| `druid.emitter.kafka.alert.topic`                  | Kafka topic name for emitter's target to emit alerts. If `event.types` contains `alerts`, this field cannot empty.                        | no        | none                  |
| `druid.emitter.kafka.request.topic`                | Kafka topic name for emitter's target to emit request logs. If `event.types` contains `requests`, this field cannot be empty.             | no        | none                  |
| `druid.emitter.kafka.segmentMetadata.topic`        | Kafka topic name for emitter's target to emit segment metadata. If `event.types` contains `segment_metadata`, this field cannot be empty. | no        | none                  |
| `druid.emitter.kafka.producer.config`              | JSON configuration to set additional properties to Kafka producer.                                                                        | no        | none                  |
| `druid.emitter.kafka.clusterName`                  | Optional value to specify the name of your Druid cluster. It can help make groups in your monitoring environment.                         | no        | none                  |
| `druid.emitter.kafka.extra.dimensions` | Optional JSON configuration to specify a map of extra string dimensions for the events emitted. These can help make groups in your monitoring environment. | no | none |

### Example

```
druid.emitter.kafka.bootstrap.servers=hostname1:9092,hostname2:9092
druid.emitter.kafka.event.types=["metrics", alerts", "requests", "segment_metadata"]
druid.emitter.kafka.metric.topic=druid-metric
druid.emitter.kafka.alert.topic=druid-alert
druid.emitter.kafka.request.topic=druid-request-logs
druid.emitter.kafka.segmentMetadata.topic=druid-segment-metadata 
druid.emitter.kafka.producer.config={"max.block.ms":10000}
druid.emitter.kafka.extra.dimensions={"region":"us-east-1","environment":"preProd"}
```

