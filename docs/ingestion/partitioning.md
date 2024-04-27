---
id: partitioning
title: Partitioning
sidebar_label: Partitioning
description: Describes time chunk and secondary partitioning in Druid. Provides guidance to choose a secondary partition dimension.
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

You can use segment partitioning and sorting within your Druid datasources to reduce the size of your data and increase performance.

One way to partition is to load data into separate datasources. This is a perfectly viable approach that works very well when the number of datasources does not lead to excessive per-datasource overheads.

This topic describes how to set up partitions within a single datasource. It does not cover how to use multiple datasources. See [Multitenancy considerations](../querying/multitenancy.md) for more details on splitting data into separate datasources and potential operational considerations.

## Time chunk partitioning

Druid always partitions datasources by time into _time chunks_. Each time chunk contains one or more segments. This partitioning happens for all ingestion methods based on the `segmentGranularity` parameter in your ingestion spec `dataSchema` object.

Partitioning by time is important for two reasons:

1. Queries that filter by `__time` (SQL) or `intervals` (native) are able to use time partitioning to prune the set of segments to consider.
2. Certain data management operations, such as overwriting and compacting existing data, acquire exclusive write locks on time partitions.
3. Each segment file is wholly contained within a time partition. Too-fine-grained partitioning may cause a large number
   of small segments, which leads to poor performance.

The most common choices to balance these considerations are `hour` and `day`. For streaming ingestion, `hour` is especially
common, because it allows compaction to follow ingestion with less of a time delay.

## Secondary partitioning

Druid can partition segments within a particular time chunk further depending upon options that vary based on the ingestion type you have chosen. In general, secondary partitioning on a particular dimension improves locality. This means that rows with the same value for that dimension are stored together, decreasing access time.

To achieve the best performance and smallest overall footprint, partition your data on a "natural"
dimension that you often use as a filter when possible. Such partitioning often improves compression and query performance. For example, some cases have yielded threefold storage size decreases.

## Partitioning and sorting

Partitioning and sorting work well together. If you do have a "natural" partitioning dimension, consider placing it first in the `dimensions` list of your `dimensionsSpec`. This way Druid sorts rows within each segment by that column. This sorting configuration frequently improves compression more than using partitioning alone.

Note that Druid always sorts rows within a segment by timestamp first, even before the first dimension listed in your `dimensionsSpec`. This sorting can preclude the efficacy of dimension sorting. To work around this limitation if necessary, set your `queryGranularity` equal to `segmentGranularity` in your [`granularitySpec`](./ingestion-spec.md#granularityspec). Druid will set all timestamps within the segment to the same value, letting you identify a [secondary timestamp](schema-design.md#secondary-timestamps) as the "real" timestamp.

## How to configure partitioning

Not all ingestion methods support an explicit partitioning configuration, and not all have equivalent levels of flexibility. If you are doing initial ingestion through a less-flexible method like
Kafka), you can use [reindexing](../data-management/update.md#reindex) or [compaction](../data-management/compaction.md) to repartition your data after initial ingestion. This is a powerful technique you can use to optimally partition any data older than a certain time threshold while you continuously add new data from a stream.

The following table shows how each ingestion method handles partitioning:

|Method|How it works|
|------|------------|
|[Native batch](native-batch.md)|Configured using [`partitionsSpec`](native-batch.md#partitionsspec) inside the `tuningConfig`.|
|[SQL](../multi-stage-query/index.md)|Configured using [`PARTITIONED BY`](../multi-stage-query/concepts.md#partitioning) and [`CLUSTERED BY`](../multi-stage-query/concepts.md#clustering).|
|[Hadoop](hadoop.md)|Configured using [`partitionsSpec`](hadoop.md#partitionsspec) inside the `tuningConfig`.|
|[Kafka indexing service](../ingestion/kafka-ingestion.md)|Kafka topic partitioning defines how Druid partitions the datasource. You can also [reindex](../data-management/update.md#reindex) or [compact](../data-management/compaction.md) to repartition after initial ingestion.|
|[Kinesis indexing service](../ingestion/kinesis-ingestion.md)|Kinesis stream sharding defines how Druid partitions the datasource. You can also [reindex](../data-management/update.md#reindex) or [compact](../data-management/compaction.md) to repartition after initial ingestion.|

## Learn more

See the following topics for more information:

* [`partitionsSpec`](native-batch.md#partitionsspec) for more detail on partitioning with Native Batch ingestion.
* [Reindexing](../data-management/update.md#reindex) and [Compaction](../data-management/compaction.md) for information on how to repartition existing data in Druid.
