---
id: kafka-share-group-ingestion
title: "Kafka share group ingestion"
sidebar_label: "Kafka share group ingestion"
description: "Queue-semantics ingestion from Apache Kafka using share groups (KIP-932) and a durable Druid inbox."
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
Requires Apache Kafka 4.2 or higher with share groups (KIP-932) enabled on the broker.
:::

## Overview

Kafka share groups (KIP-932) use broker-managed, per-record acquisition locks and explicit acknowledgement. Consumers are not limited by exclusive partition assignment and can scale beyond the topic partition count. Delivery order within a partition is not guaranteed.

Druid's `ShareGroupIndexTask` stores acquired records in a durable inbox before acknowledging them to Kafka. Druid then claims inbox batches and atomically publishes their segments with inbox completion, so task retries do not reopen a publish-before-acknowledge duplicate window.

## When to use share group ingestion

| Scenario | Consumer group | Share group |
|----------|---------------|-------------|
| Workers needed exceed partition count | Extra workers may be idle | Records may be shared across more workers |
| Elastic scaling | Partition reassignment | Broker-managed record sharing |
| Per-message processing time varies | Head-of-line blocking | Independent processing |
| Ordered processing required per partition | Yes | No (delivery order not guaranteed) |

Choose share groups when throughput and elastic scaling matter more than strict per-partition ordering.

## Task spec

Submit a `ShareGroupIndexTask` to the Overlord. There are no start/end offsets -- the broker tracks them.

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
      "bootstrap.servers": "kafka-broker:9092"
    },
    "inputFormat": {
      "type": "json"
    },
    "inboxId": "my-datasource-v1",
    "inboxStorage": {
      "type": "local",
      "basePath": "/tmp/druid-share-inbox"
    },
    "pollTimeout": 2000
  },
  "tuningConfig": {
    "type": "KafkaTuningConfig",
    "maxRowsPerSegment": 5000000
  }
}
```

## IO configuration

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `topic` | String | Yes | -- | Kafka topic to consume from. |
| `groupId` | String | Yes | -- | Share group identifier. Multiple tasks with the same `groupId` share the workload. |
| `consumerProperties` | Map | Yes | -- | Kafka consumer properties. Must include `bootstrap.servers`. See [Consumer property restrictions](#consumer-property-restrictions). |
| `inputFormat` | Object | Yes | -- | Input format for parsing records (json, csv, avro, etc.). |
| `inboxId` | String | Yes | -- | Stable inbox generation shared by tasks processing the same datasource and ingestion spec. |
| `inboxStorage` | Object | Yes | -- | Storage connector for immutable raw batches. Use storage accessible to every indexing worker. |
| `pollTimeout` | Long | No | 100 | Poll timeout in milliseconds. |
| `maxStagingRecords` | Integer | No | 10000 | Maximum acquired records retained while batches are staged. |
| `maxStagingBytes` | Long | No | 268435456 | Maximum estimated bytes retained while batches are staged. |
| `maxConcurrentUploads` | Integer | No | 2 | Maximum concurrent inbox uploads per task. |
| `renewalFraction` | Double | No | 0.5 | Fraction of the Kafka acquisition-lock duration at which staging records are renewed. |
| `maxProcessingManifests` | Integer | No | 16 | Maximum manifests claimed for one processing batch. |
| `maxProcessingRecords` | Integer | No | 100000 | Maximum records claimed for one processing batch. |
| `maxProcessingBytes` | Long | No | 268435456 | Maximum raw bytes claimed for one processing batch. |
| `claimDurationMillis` | Long | No | 300000 | Duration of a Druid inbox-processing claim. |
| `claimRenewalPeriodMillis` | Long | No | 60000 | Interval for renewing an active processing claim. |
| `inboxPollPeriodMillis` | Long | No | 1000 | Wait before polling again when no inbox work is available. |

The local connector in the example is suitable only for local or single-worker testing. Use a shared connector such as S3, Azure, or Google Cloud Storage when tasks can run on different workers.

### Consumer property restrictions

Share consumers (KIP-932) reject some keys that are valid for regular consumer groups. Druid silently strips the keys below from `consumerProperties` (with a `WARN` log per stripped key) before constructing the `KafkaShareConsumer`:

| Stripped key | Why |
|--------------|-----|
| `auto.offset.reset` | Initial position is broker-controlled for share groups. |
| `enable.auto.commit` | Share consumers use explicit acknowledgements that Druid flushes synchronously. |
| `group.instance.id` | Share groups do not support static membership. |
| `isolation.level` | Controlled by share-group broker behavior rather than this consumer property. |
| `partition.assignment.strategy` | Broker controls per-record delivery for share groups. |
| `interceptor.classes` | Not supported for share consumers. |
| `session.timeout.ms` | Not configurable on `KafkaShareConsumer`. |
| `heartbeat.interval.ms` | Not configurable on `KafkaShareConsumer`. |
| `group.protocol` | Always `SHARE` for share consumers. |
| `group.remote.assignor` | Not applicable to share groups. |

`share.acknowledgement.mode=explicit` is set automatically and must not be overridden.

### Tuning configuration

`tuningConfig` accepts the standard `KafkaTuningConfig` fields used by the appenderator. Share-group tasks do not use offset checkpoints or sequence rollover.

## How it works

1. The task polls records from `KafkaShareConsumer` with per-record acquisition locks.
2. It writes an immutable compressed raw batch to `inboxStorage` and verifies its checksum.
3. It records the batch manifest and per-record receipts in Druid metadata.
4. Only durably recorded offsets are acknowledged with `ACCEPT`; failed or unstaged records are released for redelivery.
5. The task independently claims staged manifests with a renewable, epoch-fenced lease.
6. It restores and parses the verified raw records, builds segments, and publishes them.
7. Segment publication and inbox completion are committed in one Druid metadata transaction.

## Safety invariants

1. **Durable before ACK:** Kafka records are accepted only after both the raw object and Druid receipt are durable.
2. **Replay safe:** receipts deduplicate Kafka redelivery by cluster, topic, partition, offset, inbox generation, and ingestion-spec fingerprint.
3. **Fenced processing:** expired claims can be recovered, while stale processors cannot complete them.
4. **Atomic completion:** segment publication and inbox completion succeed or fail in the same metadata transaction.

## Graceful stop

When the Overlord asks a task to stop, the acquisition loop wakes the consumer and releases records that have not reached durable staging. Durably staged records remain in the inbox and can be claimed after another task starts. An active processing claim is invalidated and becomes recoverable after its lease expires.

## Acquisition lock duration

The broker controls the lock via `group.share.record.lock.duration.ms`. The runner logs the effective value once after the first poll:

```
Effective broker acquisition lock timeout for share-group[my-group]: 30000 ms
```

The acquisition loop renews records while bounded background workers upload them. Renewal is scheduled at the earlier of `acquisitionLockTimeout * renewalFraction` and half of `max.poll.interval.ms`. If upload or receipt registration fails, the records are released. Segment construction occurs after Kafka acknowledgement and is protected by the renewable Druid metadata claim instead of the Kafka acquisition lock.

## Scaling

Tasks with the same `groupId` share the workload automatically; you can run more tasks than partitions:

```
Topic: 4 partitions
Tasks with same groupId: 20
Result: Kafka can distribute records across more workers than the partition count
```

Adding or removing tasks does not require Druid to calculate partition assignments.

## Delivery semantics

Kafka may redeliver records when acknowledgements fail or acquisition locks expire. Druid suppresses these retries using durable record receipts. After staging, Druid owns recovery through the inbox; segment publication and inbox completion are atomic. This guarantee is scoped to a stable Kafka cluster and topic identity, `inboxId`, and ingestion-spec fingerprint.

## Metrics

Share-group tasks emit the standard realtime ingestion metrics, including `ingest/events/processed`, `ingest/events/unparseable`, and `ingest/persists/count`.

## Limitations (current release)

- No supervisor integration; tasks are submitted manually via the Overlord API. A `KafkaShareGroupSupervisor` is planned as a future enhancement.
- Delivery order within a partition is not guaranteed.
- Completed inbox metadata and raw objects require an external retention and cleanup policy.
- Offset reset, offset checkpoints, and sequence rollover do not apply to share groups.
