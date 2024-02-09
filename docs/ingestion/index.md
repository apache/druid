---
id: index
title: Ingestion overview
sidebar_label: Overview
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

Loading data in Druid is called _ingestion_ or _indexing_. When you ingest data into Druid, Druid reads the data from
your source system and stores it in data files called [_segments_](../design/segments.md).
In general, segment files contain a few million rows each.

For most ingestion methods, the Druid [MiddleManager](../design/middlemanager.md) processes or the
[Indexer](../design/indexer.md) processes load your source data. The sole exception is Hadoop-based ingestion, which
uses a Hadoop MapReduce job on YARN.

During ingestion, Druid creates segments and stores them in [deep storage](../design/deep-storage.md). Historical nodes load the segments into memory to respond to queries. For streaming ingestion, the Middle Managers and indexers can respond to queries in real-time with arriving data. For more information, see [Storage overview](../design/storage.md).

This topic introduces streaming and batch ingestion methods. The following topics describe ingestion concepts and information that apply to all [ingestion methods](#ingestion-methods):

- [Druid schema model](./schema-model.md) introduces concepts of datasources, primary timestamp, dimensions, and metrics.
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

There are two available options for streaming ingestion. Streaming ingestion is controlled by a continuously-running
supervisor.

| **Method** | [Kafka](../ingestion/kafka-ingestion.md) | [Kinesis](../ingestion/kinesis-ingestion.md) |
|---|-----|--------------|
| **Supervisor type** | `kafka` | `kinesis`|
| **How it works** | Druid reads directly from Apache Kafka. | Druid reads directly from Amazon Kinesis.|
| **Can ingest late data?** | Yes. | Yes. |
| **Exactly-once guarantees?** | Yes. | Yes. |

### Batch

There are three available options for batch ingestion. Batch ingestion jobs are associated with a controller task that
runs for the duration of the job.

| **Method** | [Native batch](./native-batch.md) | [SQL](../multi-stage-query/index.md) | [Hadoop-based](hadoop.md) |
|---|-----|--------------|------------|
| **Controller task type** | `index_parallel` | `query_controller` | `index_hadoop` |
| **How you submit it** | Send an `index_parallel` spec to the [Tasks API](../api-reference/tasks-api.md). | Send an [INSERT](../multi-stage-query/concepts.md#insert) or [REPLACE](../multi-stage-query/concepts.md#replace) statement to the [SQL task API](../api-reference/sql-ingestion-api.md#submit-a-query). | Send an `index_hadoop` spec to the [Tasks API](../api-reference/tasks-api.md). |
| **Parallelism** | Using subtasks, if [`maxNumConcurrentSubTasks`](native-batch.md#tuningconfig) is greater than 1. | Using `query_worker` subtasks. | Using YARN. |
| **Fault tolerance** | Workers automatically relaunched upon failure. Controller task failure leads to job failure. | Controller or worker task failure leads to job failure. | YARN containers automatically relaunched upon failure. Controller task failure leads to job failure. |
| **Can append?** | Yes. | Yes (INSERT). | No. |
| **Can overwrite?** | Yes. | Yes (REPLACE). | Yes. |
| **External dependencies** | None. | None. | Hadoop cluster. |
| **Input sources** | Any [`inputSource`](./input-sources.md). | Any [`inputSource`](./input-sources.md) (using [EXTERN](../multi-stage-query/concepts.md#extern)) or Druid datasource (using FROM). | Any Hadoop FileSystem or Druid datasource. |
| **Input formats** | Any [`inputFormat`](./data-formats.md#input-format). | Any [`inputFormat`](./data-formats.md#input-format). | Any Hadoop InputFormat. |
| **Secondary partitioning options** | Dynamic, hash-based, and range-based partitioning methods are available. See [partitionsSpec](./native-batch.md#partitionsspec) for details.| Range partitioning ([CLUSTERED BY](../multi-stage-query/concepts.md#clustering)). |  Hash-based or range-based partitioning via [`partitionsSpec`](hadoop.md#partitionsspec). |
| **[Rollup modes](./rollup.md#perfect-rollup-vs-best-effort-rollup)** | Perfect if `forceGuaranteedRollup` = true in the [`tuningConfig`](native-batch.md#tuningconfig).  | Always perfect. | Always perfect. |
