---
id: kafka-share-group-ingestion
title: "Kafka share group ingestion"
sidebar_label: "Kafka share group ingestion"
description: "At-least-once ingestion from Apache Kafka using share groups (KIP-932)."
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

:::info
Kafka share group ingestion is experimental. It requires Apache Kafka 4.2 or higher with share groups enabled on the broker.
:::

Kafka share groups (KIP-932) let several Druid tasks consume records from the same partition. You can run more tasks than partitions. Kafka assigns individual records, and records from one partition can arrive out of order.

## Task spec

Submit an `index_kafka_share_group` task to the Overlord. To share the workload, submit multiple tasks with the same topic, group ID, datasource, and ingestion spec.

```json
{
  "type": "index_kafka_share_group",
  "dataSchema": {
    "dataSource": "my_datasource",
    "timestampSpec": {
      "column": "__time",
      "format": "auto"
    },
    "dimensionsSpec": {
      "useSchemaDiscovery": true
    },
    "granularitySpec": {
      "segmentGranularity": "DAY",
      "queryGranularity": "NONE"
    }
  },
  "ioConfig": {
    "type": "kafka_share_group",
    "topic": "my_topic",
    "groupId": "druid-share-group",
    "consumerProperties": {
      "bootstrap.servers": "kafka-broker:9092",
      "max.poll.records": 500
    },
    "inputFormat": {
      "type": "json"
    }
  },
  "tuningConfig": {
    "type": "KafkaTuningConfig"
  }
}
```

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `topic` | String | Yes | -- | Kafka topic to consume. |
| `groupId` | String | Yes | -- | Kafka share group identifier. |
| `consumerProperties` | Map | Yes | -- | Kafka connection and consumer settings, including `bootstrap.servers`. |
| `inputFormat` | Object | Yes | -- | [Input format](./data-formats.md) used to parse the record. |
| `pollTimeout` | Long | No | 100 | Maximum wait for a Kafka poll in milliseconds. |

Druid uses explicit acknowledgements and sets `share.acquire.mode=record_limit` so each poll is bounded by `max.poll.records`. Configure the starting position with Kafka's `share.auto.offset.reset` group setting. Druid ignores consumer settings that share groups do not support, including `auto.offset.reset`, `enable.auto.commit`, and `partition.assignment.strategy`.

Share group tasks use time-chunk `APPEND` locks. Submit and restart tasks through the Overlord task API.

Set `tuningConfig.streamingPartitionsSpec.partitionDimensions` to record distinct string dimension values in each published segment. The Broker uses those values to skip segments that cannot match a query filter. See [Streaming partitions spec](./kafka-ingestion.md#streaming-partitions-spec) for tuning options.

## Delivery semantics

A task parses one poll, pushes its segments to deep storage, and commits segment metadata before acknowledging the records. Records that yield zero rows under the configured parsing and filtering rules can be acknowledged without publishing a segment.

Kafka can redeliver unacknowledged records after a task fails. If a task publishes successfully and then fails before Kafka confirms the acknowledgement, a replacement task can publish the same records again. Share group ingestion provides at-least-once delivery while the records remain retryable in Kafka. Topic retention and the Kafka share group's delivery attempt limit apply.

Tune `max.poll.records` so parsing, segment publication, and acknowledgement complete within both the broker's `group.share.record.lock.duration.ms` (30 seconds by default) and the consumer's `max.poll.interval.ms`. The `ingest/kafka/shareGroup/pollToAck/time` metric reports this elapsed time in milliseconds for successful batches. Smaller polls can produce more, smaller segments.

`max.poll.records` counts Kafka records. Each record can produce multiple Druid rows. Omit `maxRowsPerSegment` and `maxTotalRows` from the task's tuning config. Share group tasks publish based on poll boundaries and reject custom row limits.

When eight published batches are waiting for Historical servers to load their segments, the task pauses before polling again. It resumes when there is room for another batch. A positive `handoffConditionTimeout` logs a warning after that many milliseconds at the limit. Set it to `0` to wait without a timeout warning. A stop request ends the wait.

Once Kafka exhausts the delivery attempts for a record, the share group stops retrying it. Kafka 4.4 and later can send exhausted records to a [share group dead-letter queue](https://kafka.apache.org/44/operations/basic-kafka-operations/#configuring-dead-letter-queues-dlq-on-share-groups) for replay. Enable copying the original record when you need its payload for replay.
