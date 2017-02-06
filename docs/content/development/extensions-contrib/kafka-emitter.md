---
layout: doc_page
---

# Kafka Emitter

To use this extension, make sure to [include](../../operations/including-extensions.html) `kafka-emitter` extension.

## Introduction

This extension emits druid metrics to a Kafka(https://kafka.apache.org) directly with JSON format.<br>
Currently, Kafka has not only their nice ecosystem but also consumer API readily available. 
So, If you currently use kafka, It's easy to integrate various tool or UI 
to monitor the status of your druid cluster with this extension.

## Configuration

All the configuration parameters for the Kafka emitter are under `druid.emitter.kafka`.

|property|description|required?|default|
|--------|-----------|---------|-------|
|`druid.emitter.kafka.bootstrap.servers`|Comma-separated Kafka broker. (`[hostname:port],[hostname:port]...`)|yes|none|
|`druid.emitter.kafka.metric.topic`|Kafka topic name for emitter's target to emit service metric.|yes|none|
|`druid.emitter.kafka.alert.topic`|Kafka topic name for emitter's target to emit alert.|yes|none|
|`druid.emitter.kafka.clusterName`|Optional value to specify name of your druid cluster. It can help make groups in your monitoring environment. |no|none|  
