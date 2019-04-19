---
layout: doc_page
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

# Kafka Emitter

To use this Apache Druid (incubating) extension, make sure to [include](../../operations/including-extensions.html) `kafka-emitter` extension.

## Introduction

This extension emits Druid metrics to [Apache Kafka](https://kafka.apache.org) directly with JSON format.<br>
Currently, Kafka has not only their nice ecosystem but also consumer API readily available. 
So, If you currently use Kafka, It's easy to integrate various tool or UI 
to monitor the status of your Druid cluster with this extension.

## Configuration

All the configuration parameters for the Kafka emitter are under `druid.emitter.kafka`.

|property|description|required?|default|
|--------|-----------|---------|-------|
|`druid.emitter.kafka.bootstrap.servers`|Comma-separated Kafka broker. (`[hostname:port],[hostname:port]...`)|yes|none|
|`druid.emitter.kafka.metric.topic`|Kafka topic name for emitter's target to emit service metric.|yes|none|
|`druid.emitter.kafka.alert.topic`|Kafka topic name for emitter's target to emit alert.|yes|none|
|`druid.emitter.kafka.producer.config`|JSON formatted configuration which user want to set additional properties to Kafka producer.|no|none|
|`druid.emitter.kafka.clusterName`|Optional value to specify name of your druid cluster. It can help make groups in your monitoring environment. |no|none|

### Example

```
druid.emitter.kafka.bootstrap.servers=hostname1:9092,hostname2:9092
druid.emitter.kafka.metric.topic=druid-metric
druid.emitter.kafka.alert.topic=druid-alert
druid.emitter.kafka.producer.config={"max.block.ms":10000}
```
