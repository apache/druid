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
Requires Apache Kafka 4.0 or higher. Share groups (KIP-932) must be enabled on the broker.
:::

## Overview

Standard Kafka consumer groups bind each partition to exactly one consumer. This creates a hard scaling ceiling: you cannot have more consumers than partitions. Rebalancing when consumers join or leave pauses all consumers in the group. A single slow message blocks all subsequent messages in its partition.

Kafka share groups (KIP-932) eliminate these constraints. The broker manages record delivery across consumers with per-record acquisition locks and explicit acknowledgement. Multiple consumers can read from the same partition concurrently. There is no rebalancing pause. Slow records do not block other records.

Druid's share group ingestion uses `ShareGroupIndexTask` to consume from Kafka share groups and publish segments with at-least-once delivery guarantees. Records are acknowledged only after segments are atomically registered in the metadata store.

## When to use share group ingestion

| Scenario | Consumer group | Share group |
|----------|---------------|-------------|
| Workers needed exceed partition count | Idle workers | All workers active |
| Elastic scaling (auto-scale events) | Rebalancing pause (30-60s) | Zero pause |
| Per-message processing time varies | Head-of-line blocking | Independent processing |
| Ordered processing required per partition | Yes | No (delivery order not guaranteed) |

Use share group ingestion when throughput and elastic scaling matter more than strict per-partition ordering.

## Task spec

Submit a `ShareGroupIndexTask` to the Overlord. Unlike standard Kafka ingestion, there are no start/end offsets -- the broker manages offset tracking.

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
| `consumerProperties` | Map | Yes | -- | Kafka consumer properties. Must include `bootstrap.servers`. |
| `inputFormat` | Object | Yes | -- | Input format for parsing records (json, csv, avro, etc.). |
| `pollTimeout` | Long | No | 2000 | Poll timeout in milliseconds. |

## How it works

1. The task subscribes to the topic using a `KafkaShareConsumer` with the configured `groupId`.
2. The broker delivers batches of records with acquisition locks.
3. The task parses records, adds rows to an appenderator, and persists segments.
4. Segments are published atomically to the metadata store.
5. After successful publish, the task acknowledges all records in the batch with `ACCEPT`.
6. The task calls `commitSync()` to commit acknowledgements to the broker.
7. On task failure, unacknowledged records are redelivered by the broker to another consumer in the share group.

## Safety invariants

1. Records are acknowledged with `ACCEPT` only after the segment containing them is atomically registered in the metadata store. No data loss on task failure.
2. Every polled record reaches exactly one terminal state: `ACCEPT` (processed), `RELEASE` (redelivered), or task crash (broker redelivers after lock timeout).

## Scaling

Multiple tasks with the same `groupId` distribute the workload automatically. Unlike consumer groups, you can run more tasks than partitions:

```
Topic: 4 partitions
Tasks with same groupId: 20
Result: All 20 tasks actively consuming (broker distributes records)
```

Adding or removing tasks does not trigger a rebalancing pause. New tasks begin consuming immediately.

## Delivery semantics

Share group ingestion provides **at-least-once** delivery. On task failure, records between the last committed acknowledgement and the failure point are redelivered. Duplicate records may be ingested across task restarts. A deduplication cache is planned for a future release.

## Limitations (current release)

- Single-threaded ingestion per task. Two-thread architecture with background lock renewal is planned.
- No supervisor integration. Tasks must be submitted manually via the Overlord API.
- No deduplication cache. Redelivered records after task failure may produce duplicates.
- Delivery order within a partition is not guaranteed.

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

# Enable share groups
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
# Build the kafka-indexing-service extension
cd /path/to/druid
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn package \
  -pl extensions-core/kafka-indexing-service -am \
  -Pskip-static-checks -Dmaven.test.skip=true -T1C -q

# Download and extract Druid release
cd ~/Downloads
curl -O https://dlcdn.apache.org/druid/31.0.0/apache-druid-31.0.0-bin.tar.gz
tar -xzf apache-druid-31.0.0-bin.tar.gz
cd apache-druid-31.0.0

# Replace the kafka extension with our build
rm extensions/druid-kafka-indexing-service/*.jar
cp /path/to/druid/extensions-core/kafka-indexing-service/target/druid-kafka-indexing-service-*.jar \
   extensions/druid-kafka-indexing-service/
cp ~/.m2/repository/org/apache/kafka/kafka-clients/4.2.0/kafka-clients-4.2.0.jar \
   extensions/druid-kafka-indexing-service/

# Start Druid
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
  -Dtest="org.apache.druid.indexing.kafka.ShareGroupIndexTaskIOConfigTest,org.apache.druid.indexing.kafka.KafkaShareGroupRecordSupplierTest,org.apache.druid.indexing.kafka.ShareGroupIndexTaskTest" \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -Pskip-static-checks -Dweb.console.skip=true -T1C
```

E2E test (requires Docker running -- Testcontainers starts a Kafka 4.1.1 broker with share groups enabled automatically):

```bash
mvn test -pl extensions-core/kafka-indexing-service \
  -Dtest="org.apache.druid.indexing.kafka.simulate.EmbeddedShareGroupIngestionTest" \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -Pskip-static-checks -Dweb.console.skip=true -T1C
```

The E2E test uses `ShareGroupKafkaResource` which starts an `apache/kafka:4.1.1` container with `group.share.enable=true`. No manual Kafka setup is needed.
