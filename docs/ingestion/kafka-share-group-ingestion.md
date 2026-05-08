---
id: kafka-share-group-ingestion
title: "Kafka share group ingestion"
sidebar_label: "Kafka share group ingestion"
description: "Queue-semantics ingestion from Apache Kafka using share groups (KIP-932). Scale consumers beyond partition count with at-least-once delivery."
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
Requires Apache Kafka 4.0 or higher with share groups (KIP-932) enabled on the broker.
:::

## Overview

Kafka share groups (KIP-932) let multiple consumers read from the same partition concurrently. The broker manages per-record acquisition locks and explicit acknowledgement, so consumer count is not capped by partition count, joining or leaving consumers does not pause the group, and a slow record does not block its partition.

Druid's `ShareGroupIndexTask` consumes from a share group and publishes segments with at-least-once delivery: records are acknowledged only after their segments are atomically registered in the metadata store.

## When to use share group ingestion

| Scenario | Consumer group | Share group |
|----------|---------------|-------------|
| Workers needed exceed partition count | Idle workers | All workers active |
| Elastic scaling (auto-scale events) | Rebalancing pause (30-60s) | Zero pause |
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
    "topic": "my_topic",
    "groupId": "druid-share-group",
    "consumerProperties": {
      "bootstrap.servers": "kafka-broker:9092"
    },
    "inputFormat": {
      "type": "json"
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
| `pollTimeout` | Long | No | 2000 | Poll timeout in milliseconds. |

### Consumer property restrictions

Share consumers (KIP-932) reject some keys that are valid for regular consumer groups. Druid silently strips the keys below from `consumerProperties` (with a `WARN` log per stripped key) before constructing the `KafkaShareConsumer`:

| Stripped key | Why |
|--------------|-----|
| `auto.offset.reset` | Initial position is broker-controlled for share groups. |
| `enable.auto.commit` | Share consumers always require explicit `acknowledge()` + `commitSync()`. |
| `group.instance.id` | Share groups do not support static membership. |
| `isolation.level` | Always read-committed for share groups. |
| `partition.assignment.strategy` | Broker controls per-record delivery for share groups. |
| `interceptor.classes` | Not supported for share consumers. |
| `session.timeout.ms` | Share groups have no consumer-group session model. |
| `heartbeat.interval.ms` | Share groups have no heartbeat. |
| `group.protocol` | Always `SHARE` for share consumers. |
| `group.remote.assignor` | Not applicable to share groups. |

`share.acknowledgement.mode=explicit` is set automatically and must not be overridden.

### Tuning configuration

`tuningConfig` accepts the standard `KafkaTuningConfig` fields. Phase 1 honors:

- `maxRowsInMemory` / `maxBytesInMemory`: triggers a mid-batch persist when the appenderator signals `isPersistRequired`.
- `maxRowsPerSegment`: when reached during a batch the runner logs the event; over-threshold segments are pushed at the end-of-batch publish boundary.

Mid-batch checkpoint and sequence rollover are deferred to Phase 2.

## How it works

1. The task subscribes to the topic with a `KafkaShareConsumer` using the configured `groupId`.
2. The broker delivers batches of records with per-record acquisition locks.
3. Each polled record is parsed by `StreamChunkReader` (the same multi-row parser as `KafkaIndexTask`); a record may produce zero, one, or many `InputRow`s. All resulting rows are added to the appenderator before the record is acknowledged.
4. Parse failures go through `ParseExceptionHandler` (so `maxParseExceptions` is honored). Bytes/processed/unparseable counters are incremented exactly once per row.
5. Segments persist mid-batch on memory pressure and unconditionally at end-of-batch, then publish atomically via `SegmentTransactionalAppendAction`.
6. After a successful publish, every offset in the batch is acknowledged with `ACCEPT` and a `commitSync()` flushes acknowledgements to the broker.
7. On task failure or graceful stop before publish, unacknowledged records are redelivered by the broker after the acquisition lock expires.

## Safety invariants

1. **ACK after publish:** `ACCEPT` is sent only after the segment is registered in the metadata store. No data loss on task failure.
2. **Multi-row safe:** every row produced from a record is added to the appenderator before that record is acknowledged.
3. **Resource safe:** `Appenderator` and `KafkaShareConsumer` are released on every exit path.
4. **Terminal state:** every polled record reaches exactly one terminal state -- `ACCEPT`, `RELEASE`, or broker redelivery after lock expiry.

## Graceful stop

When the Overlord asks a task to stop, the runner calls `KafkaShareConsumer.wakeup()`. The in-flight `poll()` throws `WakeupException`; the runner exits the loop after committing any in-flight batch. Records polled but not yet published remain unacknowledged and are redelivered by the broker after the acquisition lock expires.

## Acquisition lock duration

The broker controls the lock via `group.share.record.lock.duration.ms`. The runner logs the effective value once after the first poll:

```
Effective broker acquisition lock timeout for share-group[my-group]: 30000 ms
```

In Phase 1 a single thread does both poll and publish. If a batch exceeds the lock duration, in-flight records may be redelivered (duplicates). Tune `pollTimeout`, `maxRowsInMemory`, and `maxRowsPerSegment` so each cycle stays well under the lock window. Background renewal via `RENEW` is a Phase 2 enhancement.

## Scaling

Tasks with the same `groupId` share the workload automatically; you can run more tasks than partitions:

```
Topic: 4 partitions
Tasks with same groupId: 20
Result: All 20 tasks actively consuming (broker distributes records)
```

Adding or removing tasks does not trigger a rebalancing pause.

## Delivery semantics

At-least-once. On task failure, records between the last committed acknowledgement and the failure point are redelivered, which may produce duplicates across restarts. A deduplication cache is planned.

## Metrics

In addition to the standard ingestion metrics (`ingest/events/processed`, `ingest/events/unparseable`, `ingest/persists/count`, etc.), share-group ingestion emits:

| Metric | Description |
|--------|-------------|
| `ingest/shareGroup/commitFailures` | Per-batch count of partitions whose `commitSync()` failed. A non-zero value means the affected records will be redelivered; alert on sustained non-zero values. |

## Limitations (current release)

- Single-threaded ingestion per task. Two-thread architecture with background `RENEW` is a Phase 2 enhancement.
- No supervisor integration; tasks are submitted manually via the Overlord API. A `KafkaShareGroupSupervisor` is planned for Phase 2.
- No deduplication cache (at-least-once).
- Delivery order within a partition is not guaranteed.
- Mid-batch checkpoint / sequence rollover is not supported. If a batch grossly exceeds `maxRowsPerSegment` the runner still publishes correctly (multiple segments per batch), but the threshold is only checked at end-of-batch boundaries.

## Demo: end-to-end validation with Druid UI

### Prerequisites

- Java 17
- Kafka 4.2.0 (with share groups enabled)
- Druid 31.0.0 release (downloaded)

### Step 1: Start Kafka with share groups

```bash
cd kafka_2.13-4.2.0

KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/server.properties

echo "group.share.enable=true" >> config/server.properties
echo "group.share.record.lock.duration.ms=30000" >> config/server.properties

bin/kafka-server-start.sh config/server.properties
```

### Step 2: Create topic and produce messages

```bash
cd kafka_2.13-4.2.0

bin/kafka-topics.sh --create --topic druid-share-test --partitions 4 --bootstrap-server localhost:9092

bin/kafka-console-producer.sh --topic druid-share-test --bootstrap-server localhost:9092
```

Paste these JSON records:

```json
{"__time":"2025-06-01T00:00:00.000Z","item":"widget_a","value":100,"category":"electronics"}
{"__time":"2025-06-01T01:00:00.000Z","item":"widget_b","value":250,"category":"clothing"}
{"__time":"2025-06-01T02:00:00.000Z","item":"widget_c","value":50,"category":"electronics"}
{"__time":"2025-06-01T03:00:00.000Z","item":"widget_d","value":175,"category":"food"}
{"__time":"2025-06-01T04:00:00.000Z","item":"widget_e","value":320,"category":"electronics"}
```

### Step 3: Build the extension and set up Druid

```bash
cd /path/to/druid
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn package \
  -pl extensions-core/kafka-indexing-service -am \
  -Pskip-static-checks -Dmaven.test.skip=true -T1C -q

cd ~/Downloads
curl -O https://dlcdn.apache.org/druid/31.0.0/apache-druid-31.0.0-bin.tar.gz
tar -xzf apache-druid-31.0.0-bin.tar.gz
cd apache-druid-31.0.0

rm extensions/druid-kafka-indexing-service/*.jar
cp /path/to/druid/extensions-core/kafka-indexing-service/target/druid-kafka-indexing-service-*.jar \
   extensions/druid-kafka-indexing-service/
cp ~/.m2/repository/org/apache/kafka/kafka-clients/4.2.0/kafka-clients-4.2.0.jar \
   extensions/druid-kafka-indexing-service/

bin/start-druid
```

### Step 4: Submit task via Druid console

Open `http://localhost:8888`, go to the **Ingestion** tab, click **Submit JSON task**, and paste:

```json
{
  "type": "index_kafka_share_group",
  "dataSchema": {
    "dataSource": "share_group_demo",
    "timestampSpec": {"column": "__time", "format": "auto"},
    "dimensionsSpec": {"useSchemaDiscovery": true},
    "granularitySpec": {"segmentGranularity": "DAY", "queryGranularity": "NONE"}
  },
  "ioConfig": {
    "type": "kafka_share_group",
    "topic": "druid-share-test",
    "groupId": "druid-demo-share-group",
    "consumerProperties": {"bootstrap.servers": "localhost:9092"},
    "inputFormat": {"type": "json"},
    "pollTimeout": 2000
  },
  "tuningConfig": {"type": "KafkaTuningConfig"}
}
```

### Step 5: Query data

Go to the **Query** tab and run:

```sql
SELECT COUNT(*) AS total_rows FROM share_group_demo;
SELECT category, COUNT(*) AS cnt, SUM(value) AS total FROM share_group_demo GROUP BY category;
```

## Running tests

Unit tests:

```bash
mvn test -pl extensions-core/kafka-indexing-service \
  -Dtest="org.apache.druid.indexing.kafka.ShareGroupIndexTaskIOConfigTest,\
org.apache.druid.indexing.kafka.KafkaShareGroupRecordSupplierTest,\
org.apache.druid.indexing.kafka.ShareGroupIndexTaskTest,\
org.apache.druid.indexing.kafka.ShareGroupIndexTaskRunnerTest,\
org.apache.druid.indexing.kafka.ShareGroupConsumerPropertiesTest" \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -Pskip-static-checks -Dweb.console.skip=true -T1C
```

E2E test (requires Docker; Testcontainers starts an `apache/kafka:4.1.1` broker with `group.share.enable=true`):

```bash
mvn test -pl embedded-tests -am \
  -Dtest="org.apache.druid.testing.embedded.indexing.EmbeddedShareGroupIngestionTest" \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -Pskip-static-checks -Dweb.console.skip=true -T1C
```
