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

## Compaction strategies
There are several cases to consider compaction for segment optimization:
- With streaming ingestion, data can arrive out of chronological order creating lots of small segments.
- If you append data using `appendToExisting` for [native batch](native-batch.md) ingestion creating suboptimal segments.
- When you use `index_parallel` for parallel batch indexing and the parallel ingestion tasks create many small segments.
- When a misconfigured ingestion task creates oversized segments.

By default, compaction does not modify the underlying data of the segments. However, there are cases when you may want to modify data during compaction to improve query performance:
- If, after ingestion, you realize that data for the time interval is sparse, you can use compaction to increase the segment granularity.
- Over time you don't need fine-grained granularity for older data so you want use compaction to change older segments to a coarser query granularity. This reduces the storage space required for older data. For example from `minute` to `hour`, or `hour` to `day`. You cannot go from coarser granularity to finer granularity.
- You can change the dimension order to improve sorting and reduce segment size.
- You can remove unused columns in compaction or implement an aggregation metric for older data.
- You can change segment rollup from dynamic partitioning with best-effort rollup to hash or range partitioning with perfect rollup. For more information on rollup, see [perfect vs best-effort rollup](index.md#perfect-rollup-vs-best-effort-rollup).

Compaction does not improve performance in all situations. For example, if you rewrite your data with each ingestion task, you don't need to use compaction. See [Segment optimization](../operations/segment-optimization.md) for additional guidance to determine if compaction will help in your environment.

## Types of compaction
You can configure the Druid Coordinator to perform automatic compaction, also called auto-compaction, for a datasource. Using a segment search policy, the coordinator periodically identifies segments for compaction starting with the newest to oldest. When it discovers segments that have not been compacted or segments that were compacted with a different or changed spec, it submits compaction task for those segments and only those segments.

Automatic compaction works in most use cases and should be your first option. To learn more about automatic compaction, see [Compacting Segments](../design/coordinator.md#compacting-segments).

In cases where you require more control over compaction, you can manually submit compaction tasks. For example:
- Automatic compaction is running into the limit of task slots available to it, so tasks are waiting for previous automatic compaction tasks to complete. Manual compaction can use all available task slots, therefore you can complete compaction more quickly by submitting more concurrent tasks for more intervals.
- You want to force compaction for a specific time range or you want to compact data out of chronological order.

See [Setting up a manual compaction task](#setting-up-manual-compaction) for more about manual compaction tasks.

## Data handling with compaction
During compaction, Druid overwrites the original set of segments with the compacted set. Druid also locks the segments for the time interval being compacted to ensure data consistency. By default, compaction tasks do not modify the underlying data. You can configure the compaction task to change the query granularity or add or remove dimensions in the compaction task. This means that the only changes to query results should be the result of intentional, not automatic, changes.

For compaction tasks, `dropExisting` in `ioConfig` can be set to "true" for Druid to drop (mark unused) all existing segments fully contained by the interval of the compaction task. For an example of why this is important, see the suggestion for reindexing with finer granularity under [Implementation considerations](native-batch.md#implementation-considerations). WARNING: this functionality is still in beta and can result in temporary data unavailability for data within the compaction task interval.

If an ingestion task needs to write data to a segment for a time interval locked for compaction, by default the ingestion task supersedes the compaction task and the compaction task fails without finishing. For manual compaction tasks you can adjust the input spec interval to avoid conflicts between ingestion and compaction. For automatic compaction, you can set the `skipOffsetFromLatest` key to adjustment the auto compaction starting point from the current time to reduce the chance of conflicts between ingestion and compaction. See [Compaction dynamic configuration](../configuration/index.md#compaction-dynamic-configuration) for more information. Another option is to set the compaction task to higher priority than the ingestion task.

### Segment granularity handling

Unless you modify the segment granularity in the [granularity spec](#compaction-granularity-spec), Druid attempts to retain the granularity for the compacted segments. When segments have different segment granularities with no overlap in interval Druid creates a separate compaction task for each to retain the segment granularity in the compacted segment.

If segments have different segment granularities before compaction but there is some overlap in interval, Druid attempts find start and end of the overlapping interval and uses the closest segment granularity level for the compacted segment. For example consider two overlapping segments: segment "A" for the interval 01/01/2021-01/02/2021 with day granularity and segment "B" for the interval 01/01/2021-02/01/2021. Druid attempts to combine and compacted the overlapped segments. In this example, the earliest start time for the two segments above is 01/01/2020 and the latest end time of the two segments above is 02/01/2020. Druid compacts the segments together even though they have different segment granularity. Druid uses month segment granularity for the newly compacted segment even though segment A's original segment granularity was DAY.

### Query granularity handling

Unless you modify the query granularity in the [granularity spec](#compaction-granularity-spec), Druid retains the query granularity for the compacted segments. If segments have different query granularities before compaction, Druid chooses the finest level of granularity for the resulting compacted segment. For example if a compaction task combines two segments, one with day query granularity and one with minute query granularity, the resulting segment uses minute query granularity.

> In Apache Druid 0.21.0 and prior, Druid sets the granularity for compacted segments to the default granularity of `NONE` regardless of the query granularity of the original segments.

If you configure query granularity in compaction to go from a finer granularity like month to a coarser query granularity like year, then Druid overshadows the original segment with coarser granularity. Because the new segments have a coarser granularity, running a kill task to remove the overshadowed segments for those intervals will cause you to permanently lose the finer granularity data.

### Dimension handling
Apache Druid supports schema changes. Therefore, dimensions can be different across segments even if they are a part of the same data source. See [Different schemas among segments](../design/segments.md#different-schemas-among-segments). If the input segments have different dimensions, the resulting compacted segment include all dimensions of the input segments. 

Even when the input segments have the same set of dimensions, the dimension order or the data type of dimensions can be different. The dimensions of recent segments precede that of old segments in terms of data types and the ordering because more recent segments are more likely to have the preferred order and data types.

If you want to control dimension ordering or ensure specific values for dimension types, you can configure a custom `dimensionsSpec` in the compaction task spec.

### Rollup
Druid only rolls up the output segment when `rollup` is set for all input segments.
See [Roll-up](../ingestion/index.md#rollup) for more details.
You can check that your segments are rolled up or not by using [Segment Metadata Queries](../querying/segmentmetadataquery.md#analysistypes).

## Setting up manual compaction

To perform a manual compaction, you submit a compaction task. Compaction tasks merge all segments for the defined interval according to the following syntax:

```json
{
    "type": "compact",
    "id": <task_id>,
    "dataSource": <task_datasource>,
    "ioConfig": <IO config>,
    "dimensionsSpec" <custom dimensionsSpec>,
    "metricsSpec" <custom metricsSpec>,
    "tuningConfig" <parallel indexing task tuningConfig>,
    "granularitySpec" <compaction task granularitySpec>,
    "context": <task context>
}
```

|Field|Description|Required|
|-----|-----------|--------|
|`type`|Task type. Should be `compact`|Yes|
|`id`|Task id|No|
|`dataSource`|Data source name to compact|Yes|
|`ioConfig`|I/O configuration for compaction task. See [Compaction I/O configuration](#compaction-io-configuration) for details.|Yes|
|`dimensionsSpec`|Custom dimensions spec. The compaction task uses the specified dimensions spec if it exists instead of generating one.|No|
|`metricsSpec`|Custom metrics spec. The compaction task uses the specified metrics spec rather than generating one.|No|
|`segmentGranularity`|When set, the compaction task changes the segment granularity for the given interval.  Deprecated. Use `granularitySpec`. |No.|
|`tuningConfig`|[Parallel indexing task tuningConfig](native-batch.md#tuningconfig). Note that your tuning config cannot contain a non-zero value for `awaitSegmentAvailabilityTimeoutMillis` because it is not supported by compaction tasks at this time.|No|
|`context`|[Task context](./tasks.md#context)|No|
|`granularitySpec`|Custom `granularitySpec` to describe the `segmentGranularity` and `queryGranularity` for the compacted segments. See [Compaction granularitySpec](#compaction-granularity-spec).|No|

> Note: Use `granularitySpec` over `segmentGranularity` and only set one of these values. If you specify different values for these in the same compaction spec, the task fails.

To control the number of result segments per time chunk, you can set [maxRowsPerSegment](../configuration/index.md#compaction-dynamic-configuration) or [numShards](../ingestion/native-batch.md#tuningconfig).

> You can run multiple compaction tasks in parallel. For example, if you want to compact the data for a year, you are not limited to running a single task for the entire year. You can run 12 compaction tasks with month-long intervals.

A compaction task internally generates an `index` task spec for performing compaction work with some fixed parameters. For example, its `inputSource` is always the [DruidInputSource](native-batch.md#druid-input-source), and `dimensionsSpec` and `metricsSpec` include all dimensions and metrics of the input segments by default.

Compaction tasks would exit without doing anything and issue a failure status code:
- if the interval you specify has no data segments loaded<br>
OR
- if the interval you specify is empty.

Note that the metadata between input segments and the resulting compacted segments may differ if the metadata among the input segments differs as well. If all input segments have the same metadata, however, the resulting output segment will have the same metadata as all input segments.


### Example compaction task
The following JSON illustrates a compaction task to compact _all segments_ within the interval `2017-01-01/2018-01-01` and create new segments:

```json
{
  "type" : "compact",
  "dataSource" : "wikipedia",
  "ioConfig" : {
    "type": "compact",
    "inputSpec": {
      "type": "interval",
      "interval": "2020-01-01/2021-01-01",
    }
  },
  "granularitySpec": {
      "segmentGranularity":"day",
      "queryGranularity":"hour"
    }
}
```

This task doesn't specify a `granularitySpec` so Druid retains the original segment granularity unchanged when compaction is complete.

### Compaction I/O configuration

The compaction `ioConfig` requires specifying `inputSpec` as follows:

|Field|Description|Default|Required?|
|-----|-----------|-------|--------|
|`type`|Task type. Should be `compact`|none|Yes|
|`inputSpec`|Input specification|none|Yes|
|`dropExisting`|If `true`, then the compaction task drops (mark unused) all existing segments fully contained by either the `interval` in the `interval` type `inputSpec` or the umbrella interval of the `segments` in the `segment` type `inputSpec` when the task publishes new compacted segments. If compaction fails, Druid does not drop or mark unused any segments. WARNING: this functionality is still in beta and can result in temporary data unavailability for data within the compaction task interval.|false|no|


There are two supported `inputSpec`s for now.

The interval `inputSpec` is:

|Field|Description|Required|
|-----|-----------|--------|
|`type`|Task type. Should be `interval`|Yes|
|`interval`|Interval to compact|Yes|

The segments `inputSpec` is:

|Field|Description|Required|
|-----|-----------|--------|
|`type`|Task type. Should be `segments`|Yes|
|`segments`|A list of segment IDs|Yes|

### Compaction granularity spec

You can optionally use the `granularitySpec` object to configure the segment granularity and the query granularity of the compacted segments. Their syntax is as follows:
```json
    "type": "compact",
    "id": <task_id>,
    "dataSource": <task_datasource>,
    ...
    ,
    "granularitySpec": {
      "segmentGranularity": <time_period>,
      "queryGranularity": <time_period>
    }
    ...
```

`granularitySpec` takes the following keys:

|Field|Description|Required|
|-----|-----------|--------|
|`segmentGranularity`|Time chunking period for the segment granularity. Defaults to 'null', which preserves the original segment granularity. Accepts all [Query granularity](../querying/granularities.md) values.|No|
|`queryGranularity`|Time chunking period for the query granularity. Defaults to 'null', which preserves the original query granularity. Accepts all [Query granularity](../querying/granularities.md) values. Not supported for automatic compaction.|No|

For example, to set the segment granularity to "day" and the query granularity to "hour":
```json
{
  "type" : "compact",
  "dataSource" : "wikipedia",
  "ioConfig" : {
    "type": "compact",
    "inputSpec": {
      "type": "interval",
      "interval": "2017-01-01/2018-01-01",
    },
    "granularitySpec": {
      "segmentGranularity":"day",
      "queryGranularity":"hour"
    }
  }
}
```

## Learn more
See the following topics for more information:
- [Segment optimization](../operations/segment-optimization.md) for guidance to determine if compaction will help in your case.
- [Compacting Segments](../design/coordinator.md#compacting-segments) for more on automatic compaction.
- See [Compaction Configuration API](../operations/api-reference.md#compaction-configuration)
and [Compaction Configuration](../configuration/index.md#compaction-dynamic-configuration) for automatic compaction configuration information.
