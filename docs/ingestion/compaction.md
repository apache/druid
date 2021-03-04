---
id: compaction
title: "Compaction"
description: "Defines compaction and automatic compaction (auto-compaction or autocompaction) as a strategy for segment optimization. Use cases for compaction. Describes compaction task configuration."
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

Compaction in Apache Druid is a strategy to optimize segment size. Compaction tasks read an existing set of segments for a given time range and combine the data into a new "compacted" set of segments. The compacted segments are generally larger, but there are fewer of them. Compaction can sometimes increase performance because it reduces the number of segments and, consequently, the per-segment processing and the memory overhead required for ingestion and for querying paths.

As a strategy, compaction is effective when you have data arriving out of chronological order resulting in lots of small segments. For example if you are appending data using `appendToExisting` for [native batch](./native_batch.md) ingestion. Conversely, if you are rewriting your data with each ingestion task, you don't need to use compaction. See [Segment optimization](../operations/segment-optimization.md) for guidance to determine if compaction will help in your case.

## Types of segment compaction
You can configure the Druid Coordinator to perform automatic compaction, also called auto-compaction, for a datasource. Using a segment search policy, the coordinator periodically identifies segments for compaction starting with the newest to oldest. When segments can benefit from compaction, the coordinator automatically submits a compaction task. 

Automatic compaction works in most use cases and should be your first option. To learn more about automatic compaction, see [Compacting Segments](../design/coordinator.md#compacting-segments).

In cases where you require more control over compaction, you can manually submit compaction tasks. For example:
- Automatic compaction is too slow.
- You want to force compaction for a specific time range.
- Compacting recent data before older data suboptimal is suboptimal in your environment.

See [Setting up a manual compaction task](#setting-up-manual-compaction) more about manual compaction tasks.


## Data handling with compaction
During compaction, Druid overwrites the original set of segments with the compacted set without modifying the data. During compaction Druid locks the segments for the time interval being compacted to ensure data consistency.

If an ingestion task needs to write data to a segment for a time interval locked for compaction, the ingestion task supersedes the compaction task and the compaction task fails without finishing. For manual compaction tasks you can adjust the input spec interval to avoid conflicts between ingestion and compaction. For automatic compaction, you can set the `skipOffsetFromLatest` key to adjustment the auto compaction starting point from the current time to reduce the chance of conflicts between ingestion and compaction. See [Compaction dynamic configuration](../configuration/index.md#compaction-dynamic-configuration) for more information.

### Segment granularity handling

Unless you modify the segment granularity in the [granularity spec](#compaction-granularity-spec), Druid attempts to retain the granularity for the compacted segments. When segments have different segment granularities with no overlap in interval Druid creates a separate compaction task for each to retain the segment granularity in the compacted segment. If segments have different segment granularities before compaction but there is some overlap in interval, Druid attempts find start and end of the overlapping interval and uses the closest segment granularity level for the compacted segment.

### Query granularity handling

Unless you modify the query granularity in the [granularity spec](#compaction-granularity-spec), Druid retains the query granularity for the compacted segments. If segments have different query granularities before compaction, Druid chooses the finest level of granularity for the resulting compacted segment. For example if a compaction task combines two segments, one with day query granularity and one with minute query granularity, the resulting segment uses minute query granularity.

> In Apache Druid 0.21.0 and prior, Druid sets the granularity for compacted segments to the default granularity of `NONE` regardless of the query granularity of the original segments.

If you configure query granularity in compaction to go from a finer granularity like month to a coarser query granularity like year, then Druid overshadows the original segment with finer granularity. Because the new segments have a coarser granularity, running a kill task to remove the overshadowed segments for those intervals will cause you to permanently lose the finer granularity data.


### Dimension handling
Apache Druid supports schema changes. Therefore, dimensions can be different across segments even if they are a part of the same data source. See [Different schemas among segments](../design/segments.md#different-schemas-among-segments). If the input segments have different dimensions, the resulting compacted segment includes all dimensions of the input segments. 

Even when the input segments have the same set of dimensions, the dimension order or the data type of dimensions can be different. For example, the data type of some dimensions can be changed from `string` to primitive types, or the order of dimensions can be changed for better locality. In this case, the dimensions of recent segments precede that of old segments in terms of data types and the ordering because more recent segments are more likely to have the preferred order and data types.

If you want to use your own ordering and types, you can specify a custom `dimensionsSpec` in the compaction task spec.

### Rollup
Druid only rolls up the output segment when `rollup` is set for all input segments. See [Roll-up](../ingestion/index.md#rollup) for more details.
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
|`tuningConfig`|[Parallel indexing task tuningConfig](./native-batch.md#tuningconfig)|No|
|`context`|[Task context](./tasks.md#context)|No|
|`granularitySpec`|Custom `granularitySpec` to describe the `segmentGranularity` and `queryGranularity` for the compacted segments. See [Compaction granularitySpec](#compaction-granularity-spec).|No|

To control the number of result segments per time chunk, you can set [maxRowsPerSegment](../configuration/index.md#compaction-dynamic-configuration) or [numShards](../ingestion/native-batch.md#tuningconfig).

> You can run multiple compaction tasks at the same time. For example, you can run 12 compaction tasks per month instead of running a single task for the entire year.

A compaction task internally generates an `index` task spec for performing compaction work with some fixed parameters. For example, its `inputSource` is always the [DruidInputSource](native-batch.md#druid-input-source), and `dimensionsSpec` and `metricsSpec`include all dimensions and metrics of the input segments by default.

Compaction tasks exit without doing anything and issue a failure status code:
- if the interval you specify has no data segments loaded<br>
OR
- if the interval you specify is empty.

The output segment can have different metadata from the input segments unless all input segments have the same metadata.


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
      "interval": "2017-01-01/2018-01-01",
    }
  }
}
```

This task doesn't specify a `granularitySpec` so Druid retains the original segment granularity unchanged when compaction is complete.

### Compaction I/O configuration

The compaction `ioConfig` requires specifying `inputSpec` as follows:

|Field|Description|Required|
|-----|-----------|--------|
|`type`|Task type. Should be `compact`|Yes|
|`inputSpec`|Input specification|Yes|

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

You can optionally use the `granularitySpec` object to configure the segment granularity and the query granularity of the compacted segments. They syntax is as follows:
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
|`segmentGranularity`|Time chunking period for the segment granularity. Defaults to 'null'. Accepts all [Query granularities](../querying/granularities.md).|No|
|`queryGranularity`|Time chunking period for the query granularity. Defaults to 'null'. Accepts all [Query granularities](../querying/granularities.md). Not supported for automatic compaction.|No|

For example, to set the set the segment granularity to "day" and the query granularity to "hour":
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
