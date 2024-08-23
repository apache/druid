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

The following table describes how to configure time chunk partitioning.

|Method|Configuration|
|------|------------|
|[SQL](../multi-stage-query/index.md)|[`PARTITIONED BY`](../multi-stage-query/concepts.md#partitioning)|
|[Kafka](../ingestion/kafka-ingestion.md) or [Kinesis](../ingestion/kinesis-ingestion.md)|`segmentGranularity` inside the [`granularitySpec`](ingestion-spec.md#granularityspec)|
|[Native batch](native-batch.md) or [Hadoop](hadoop.md)|`segmentGranularity` inside the [`granularitySpec`](ingestion-spec.md#granularityspec)|

## Secondary partitioning

Druid further partitions each time chunk into immutable segments. Secondary partitioning on a particular dimension improves locality. This means that rows with the same value for that dimension are stored together, decreasing access time.

To achieve the best performance and smallest overall footprint, partition your data on a "natural" dimension that
you often use as a filter, or that achieves some alignment within your data. Such partitioning can improve compression
and query performance by significant multiples.

The following table describes how to configure secondary partitioning.

|Method|Configuration|
|------|------------|
|[SQL](../multi-stage-query/index.md)|[`CLUSTERED BY`](../multi-stage-query/concepts.md#clustering)|
|[Kafka](../ingestion/kafka-ingestion.md) or [Kinesis](../ingestion/kinesis-ingestion.md)|Upstream partitioning defines how Druid partitions the datasource. You can also alter clustering using [`REPLACE`](../multi-stage-query/concepts.md#replace) (with `CLUSTERED BY`) or [compaction](../data-management/compaction.md) after initial ingestion.|
|[Native batch](native-batch.md) or [Hadoop](hadoop.md)|[`partitionsSpec`](native-batch.md#partitionsspec) inside the `tuningConfig`|

## Sorting

Each segment is internally sorted to promote compression and locality.

Partitioning and sorting work well together. If you do have a "natural" partitioning dimension, consider placing it
first in your sort order as well. This way, Druid sorts rows within each segment by that column. This sorting configuration
frequently improves compression and performance more than using partitioning alone.

The following table describes how to configure sorting.

|Method|Configuration|
|------|------------|
|[SQL](../multi-stage-query/index.md)|Uses order of fields in [`CLUSTERED BY`](../multi-stage-query/concepts.md#clustering) or [`segmentSortOrder`](../multi-stage-query/reference.md#context) in the query context|
|[Kafka](../ingestion/kafka-ingestion.md) or [Kinesis](../ingestion/kinesis-ingestion.md)|Uses order of fields in [`dimensionsSpec`](ingestion-spec.md#granularityspec)|
|[Native batch](native-batch.md) or [Hadoop](hadoop.md)|Uses order of fields in [`dimensionsSpec`](ingestion-spec.md#granularityspec)|

:::info
Druid implicitly sorts rows within a segment by `__time` first before any `dimensions` or `CLUSTERED BY` fields, unless
you set `forceSegmentSortByTime` to `false` in your
[query context](../multi-stage-query/reference.md#context-parameters) (for SQL) or in your
[`dimensionsSpec`](ingestion-spec.md#dimensionsspec) (for other ingestion forms).

Setting `forceSegmentSortByTime` to `false` is an experimental feature. Segments created with sort orders that
do not start with `__time` can only be read by Druid 31 or later. Additionally, at this time, certain queries are not
supported on such segments, including:

- Native queries with `granularity` other than `all`.
- Native `scan` query with ascending or descending time order.
- SQL queries that plan into an unsupported native query.
:::

## Learn more

See the following topics for more information:

* [`partitionsSpec`](native-batch.md#partitionsspec) for more detail on partitioning with Native Batch ingestion.
* [Reindexing](../data-management/update.md#reindex) and [Compaction](../data-management/compaction.md) for information on how to repartition existing data in Druid.
