---
id: compaction
title: "Compaction"
description: "Defines compaction and automatic compaction (auto-compaction or autocompaction) for segment optimization. Use cases and strategies for compaction. Describes compaction task configuration."
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
  
Query performance in Apache Druid depends on optimally sized segments. Compaction is one strategy you can use to optimize segment size for your Druid database. Compaction tasks read an existing set of segments for a given time interval and combine the data into a new "compacted" set of segments. In some cases the compacted segments are larger, but there are fewer of them. In other cases the compacted segments may be smaller. Compaction tends to increase performance because optimized segments require less per-segment processing and less memory overhead for ingestion and for querying paths.

## Compaction guidelines

There are several cases to consider compaction for segment optimization:

- With streaming ingestion, data can arrive out of chronological order creating many small segments.
- If you append data using `appendToExisting` for [native batch](../ingestion/native-batch.md) ingestion creating suboptimal segments.
- When you use `index_parallel` for parallel batch indexing and the parallel ingestion tasks create many small segments.
- When a misconfigured ingestion task creates oversized segments.

By default, compaction does not modify the underlying data of the segments. However, there are cases when you may want to modify data during compaction to improve query performance:

- If, after ingestion, you realize that data for the time interval is sparse, you can use compaction to increase the segment granularity.
- If you don't need fine-grained granularity for older data, you can use compaction to change older segments to a coarser query granularity. For example, from `minute` to `hour` or `hour` to `day`. This reduces the storage space required for older data.
- You can change the dimension order to improve sorting and reduce segment size.
- You can remove unused columns in compaction or implement an aggregation metric for older data.
- You can change segment rollup from dynamic partitioning with best-effort rollup to hash or range partitioning with perfect rollup. For more information on rollup, see [perfect vs best-effort rollup](../ingestion/rollup.md#perfect-rollup-vs-best-effort-rollup).

Compaction does not improve performance in all situations. For example, if you rewrite your data with each ingestion task, you don't need to use compaction. See [Segment optimization](../operations/segment-optimization.md) for additional guidance to determine if compaction will help in your environment.

## Ways to run compaction

Automatic compaction, also called auto-compaction, works in most use cases and should be your first option. 

The Coordinator uses its [segment search policy](../design/coordinator.md#segment-search-policy-in-automatic-compaction) to periodically identify segments for compaction starting from newest to oldest. When the Coordinator discovers segments that have not been compacted or segments that were compacted with a different or changed spec, it submits compaction tasks for the time interval covering those segments.

To learn more, see [Automatic compaction](../data-management/automatic-compaction.md).

In cases where you require more control over compaction, you can manually submit compaction tasks. For example:

- Automatic compaction is running into the limit of task slots available to it, so tasks are waiting for previous automatic compaction tasks to complete. Manual compaction can use all available task slots, therefore you can complete compaction more quickly by submitting more concurrent tasks for more intervals.
- You want to force compaction for a specific time range or you want to compact data out of chronological order.

See [Setting up a manual compaction task](./manual-compaction.md#setting-up-manual-compaction) for more about manual compaction tasks.

## Data handling with compaction

During compaction, Druid overwrites the original set of segments with the compacted set. Druid also locks the segments for the time interval being compacted to ensure data consistency. By default, compaction tasks do not modify the underlying data. You can configure the compaction task to change the query granularity or add or remove dimensions in the compaction task. This means that the only changes to query results should be the result of intentional, not automatic, changes.

You can set `dropExisting` in `ioConfig` to "true" in the compaction task to configure Druid to replace all existing segments fully contained by the interval. See the suggestion for reindexing with finer granularity under [Implementation considerations](../ingestion/native-batch.md#implementation-considerations) for an example.
:::info
 WARNING: `dropExisting` in `ioConfig` is a beta feature.
:::

If an ingestion task needs to write data to a segment for a time interval locked for compaction, by default the ingestion task supersedes the compaction task and the compaction task fails without finishing. For manual compaction tasks, you can adjust the input spec interval to avoid conflicts between ingestion and compaction. For automatic compaction, you can set the `skipOffsetFromLatest` key to adjust the auto-compaction starting point from the current time to reduce the chance of conflicts between ingestion and compaction.
Another option is to set the compaction task to higher priority than the ingestion task.
For more information, see [Avoid conflicts with ingestion](../data-management/automatic-compaction.md#avoid-conflicts-with-ingestion).

### Segment granularity handling

Unless you modify the segment granularity in [`granularitySpec`](manual-compaction.md#compaction-granularity-spec), Druid attempts to retain the granularity for the compacted segments. When segments have different segment granularities with no overlap in interval Druid creates a separate compaction task for each to retain the segment granularity in the compacted segment.

If segments have different segment granularities before compaction but there is some overlap in interval, Druid attempts find start and end of the overlapping interval and uses the closest segment granularity level for the compacted segment.

For example consider two overlapping segments: segment "A" for the interval 01/01/2021-01/02/2021 with day granularity and segment "B" for the interval 01/01/2021-02/01/2021. Druid attempts to combine and compact the overlapped segments. In this example, the earliest start time for the two segments is 01/01/2020 and the latest end time of the two segments is 02/01/2020. Druid compacts the segments together even though they have different segment granularity. Druid uses month segment granularity for the newly compacted segment even though segment A's original segment granularity was DAY.

### Query granularity handling

Unless you modify the query granularity in the [`granularitySpec`](manual-compaction.md#compaction-granularity-spec), Druid retains the query granularity for the compacted segments. If segments have different query granularities before compaction, Druid chooses the finest level of granularity for the resulting compacted segment. For example if a compaction task combines two segments, one with day query granularity and one with minute query granularity, the resulting segment uses minute query granularity.

:::info
 In Apache Druid 0.21.0 and prior, Druid sets the granularity for compacted segments to the default granularity of `NONE` regardless of the query granularity of the original segments.
:::

If you configure query granularity in compaction to go from a finer granularity like month to a coarser query granularity like year, then Druid overshadows the original segment with coarser granularity. Because the new segments have a coarser granularity, running a kill task to remove the overshadowed segments for those intervals will cause you to permanently lose the finer granularity data.

### Dimension handling

Apache Druid supports schema changes. Therefore, dimensions can be different across segments even if they are a part of the same datasource. See [Segments with different schemas](../design/segments.md#segments-with-different-schemas). If the input segments have different dimensions, the resulting compacted segment includes all dimensions of the input segments.

Even when the input segments have the same set of dimensions, the dimension order or the data type of dimensions can be different. The dimensions of recent segments precede that of old segments in terms of data types and the ordering because more recent segments are more likely to have the preferred order and data types.

If you want to control dimension ordering or ensure specific values for dimension types, you can configure a custom `dimensionsSpec` in the compaction task spec.

### Rollup

Druid only rolls up the output segment when `rollup` is set for all input segments.
See [Roll-up](../ingestion/rollup.md) for more details.
You can check that your segments are rolled up or not by using [Segment Metadata Queries](../querying/segmentmetadataquery.md#analysistypes).

## Learn more

See the following topics for more information:
- [Segment optimization](../operations/segment-optimization.md) for guidance to determine if compaction will help in your case.
- [Manual compaction](./manual-compaction.md) for how to run a one-time compaction task.
- [Automatic compaction](automatic-compaction.md) for how to enable and configure automatic compaction.

