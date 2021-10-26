---
id: index
title: "Ingestion"
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

Loading data in Druid is called _ingestion_ or _indexing_. When you ingest data into Druid, Druid reads the data from your source system and stores it in data files called _segments_. In general, segment files contain a few million rows.

For most ingestion methods, the Druid [MiddleManager](../design/middlemanager.md) processes or the [Indexer](../design/indexer.md) processes load your source data. One exception is
Hadoop-based ingestion, which uses a Hadoop MapReduce job on YARN MiddleManager or Indexer processes to start and monitor Hadoop jobs. 

During ingestion Druid creates segments and stores them in [deep storage](../dependencies/deep-storage.md). Historical nodes load the segments into memory to respond to queries. For streaming ingestion, the Middle Managers and indexers can respond to queries in real-time with arriving data. See the [Storage design](../design/architecture.md#storage-design) section of the Druid design documentation for more information.

This topic introduces streaming and batch ingestion methods. The following topics describe ingestion concepts and information that apply to all [ingestion methods](#ingestion-methods):
- [Druid data model](./data-model.md) introduces concepts of datasources, primary timestamp, dimensions, and metrics.
- [Data rollup](./rollup.md) describes rollup as a concept and provides suggestions to maximize the benefits of rollup.
- [Partitioning](./partitioning.md) describes time chunk and secondary partitioning in Druid.
- [Ingestion spec reference](./ingestion-spec.md) provides a reference for the configuration options in the ingestion spec.

For additional information about concepts and configurations that are unique to each ingestion method, see the topic for the ingestion method.

## Ingestion methods

The tables below list Druid's most common data ingestion methods, along with comparisons to help you choose
the best one for your situation. Each ingestion method supports its own set of source systems to pull from. For details
about how each method works, as well as configuration properties specific to that method, check out its documentation
page.

### Streaming

The most recommended, and most popular, method of streaming ingestion is the
[Kafka indexing service](../development/extensions-core/kafka-ingestion.md) that reads directly from Kafka. Alternatively, the Kinesis
indexing service works with Amazon Kinesis Data Streams.

Streaming ingestion uses an ongoing process called a supervisor that reads from the data stream to ingest data into Druid.

This table compares the options:

| **Method** | [Kafka](../development/extensions-core/kafka-ingestion.md) | [Kinesis](../development/extensions-core/kinesis-ingestion.md) |
|---|-----|--------------|
| **Supervisor type** | `kafka` | `kinesis`|
| **How it works** | Druid reads directly from Apache Kafka. | Druid reads directly from Amazon Kinesis.|
| **Can ingest late data?** | Yes | Yes |
| **Exactly-once guarantees?** | Yes | Yes |

### Batch

When doing batch loads from files, you should use one-time [tasks](tasks.md), and you have three options: `index_parallel` (native batch; parallel), `index_hadoop` (Hadoop-based),
or `index` (native batch; single-task).

In general, we recommend native batch whenever it meets your needs, since the setup is simpler (it does not depend on
an external Hadoop cluster). However, there are still scenarios where Hadoop-based batch ingestion might be a better choice,
for example when you already have a running Hadoop cluster and want to
use the cluster resource of the existing cluster for batch ingestion.

This table compares the three available options:

| **Method** | [Native batch (parallel)](native-batch.md#parallel-task) | [Hadoop-based](hadoop.md) | [Native batch (simple)](native-batch.md#simple-task) |
|---|-----|--------------|------------|
| **Task type** | `index_parallel` | `index_hadoop` | `index`  |
| **Parallel?** | Yes, if `inputFormat` is splittable and `maxNumConcurrentSubTasks` > 1 in `tuningConfig`. See [data format documentation](./data-formats.md) for details. | Yes, always. | No. Each task is single-threaded. |
| **Can append or overwrite?** | Yes, both. | Overwrite only. | Yes, both. |
| **External dependencies** | None. | Hadoop cluster (Druid submits Map/Reduce jobs). | None. |
| **Input locations** | Any [`inputSource`](./native-batch.md#input-sources). | Any Hadoop FileSystem or Druid datasource. | Any [`inputSource`](./native-batch.md#input-sources). |
| **File formats** | Any [`inputFormat`](./data-formats.md#input-format). | Any Hadoop InputFormat. | Any [`inputFormat`](./data-formats.md#input-format). |
| **[Rollup modes](./rollup.md)** | Perfect if `forceGuaranteedRollup` = true in the [`tuningConfig`](native-batch.md#tuningconfig).  | Always perfect. | Perfect if `forceGuaranteedRollup` = true in the [`tuningConfig`](native-batch.md#tuningconfig). |
| **Partitioning options** | Dynamic, hash-based, and range-based partitioning methods are available. See [partitionsSpec](./native-batch.md#partitionsspec) for details.| Hash-based or range-based partitioning via [`partitionsSpec`](hadoop.md#partitionsspec). | Dynamic and hash-based partitioning methods are available. See [partitionsSpec](./native-batch.md#partitionsspec-1) for details. |

