---
id: manual-compaction
title: "Manual compaction"
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

In Apache Druid, compaction is a special type of ingestion task that reads data from a Druid datasource and writes it back into the same datasource. A common use case for this is to [optimally size segments](../operations/segment-optimization.md) after ingestion to improve query performance.

You can perform manual compaction where you submit a one-time compaction task for a specific interval. Generally, you don't need to do this if you use [automatic compaction](./automatic-compaction.md), which is recommended for most workloads.

## Setting up manual compaction

 Compaction tasks merge all segments for the defined interval according to the following syntax:

```json
{
    "type": "compact",
    "id": <task_id>,
    "dataSource": <task_datasource>,
    "ioConfig": <IO config>,
    "dimensionsSpec": <custom dimensionsSpec>,
    "transformSpec": <custom transformSpec>,
    "metricsSpec": <custom metricsSpec>,
    "tuningConfig": <parallel indexing task tuningConfig>,
    "granularitySpec": <compaction task granularitySpec>,
    "context": <task context>
}
```

|Field|Description|Required|
|-----|-----------|--------|
|`type`|Task type. Set the value to `compact`.|Yes|
|`id`|Task ID|No|
|`dataSource`|Data source name to compact|Yes|
|`ioConfig`|I/O configuration for compaction task. See [Compaction I/O configuration](#compaction-io-configuration) for details.|Yes|
|`dimensionsSpec`|When set, the compaction task uses the specified `dimensionsSpec` rather than generating one from existing segments. See [Compaction dimensionsSpec](#compaction-dimensions-spec) for details.|No|
|`transformSpec`|When set, the compaction task uses the specified `transformSpec` rather than using `null`. See [Compaction transformSpec](#compaction-transform-spec) for details.|No|
|`metricsSpec`|When set, the compaction task uses the specified `metricsSpec` rather than generating one from existing segments.|No|
|`segmentGranularity`|Deprecated. Use `granularitySpec`.|No|
|`tuningConfig`|[Tuning configuration](../ingestion/native-batch.md#tuningconfig) for parallel indexing. `awaitSegmentAvailabilityTimeoutMillis` value is not supported for compaction tasks. Leave this parameter at the default value, 0.|No|
|`granularitySpec`|When set, the compaction task uses the specified `granularitySpec` rather than generating one from existing segments. See [Compaction `granularitySpec`](#compaction-granularity-spec) for details.|No|
|`context`|[Task context](../ingestion/tasks.md#context)|No|

:::info
 Note: Use `granularitySpec` over `segmentGranularity` and only set one of these values. If you specify different values for these in the same compaction spec, the task fails.
:::

To control the number of result segments per time chunk, you can set [`maxRowsPerSegment`](../ingestion/native-batch.md#partitionsspec) or [`numShards`](../ingestion/native-batch.md#tuningconfig).

:::info
 You can run multiple compaction tasks in parallel. For example, if you want to compact the data for a year, you are not limited to running a single task for the entire year. You can run 12 compaction tasks with month-long intervals.
:::

A compaction task internally generates an `index` or `index_parallel` task spec for performing compaction work with some fixed parameters. For example, its `inputSource` is always the [`druid` input source](../ingestion/input-sources.md), and `dimensionsSpec` and `metricsSpec` include all dimensions and metrics of the input segments by default.

Compaction tasks typically fetch all [relevant segments](#compaction-io-configuration) prior to launching any subtasks, _unless_ the following properties are all set to non-null values. It is strongly recommended to set them to non-null values to maximize performance and minimize disk usage of the `compact` task:

- [`granularitySpec`](#compaction-granularity-spec), with non-null values for each of `segmentGranularity`, `queryGranularity`, and `rollup`
- [`dimensionsSpec`](#compaction-dimensions-spec)
- `metricsSpec`

Compaction tasks exit without doing anything and issue a failure status code in either of the following cases:

- If the interval you specify has no data segments loaded.
- If the interval you specify is empty.

Note that the metadata between input segments and the resulting compacted segments may differ if the metadata among the input segments differs as well. If all input segments have the same metadata, however, the resulting output segment will have the same metadata as all input segments.


## Manual compaction task example

The following JSON illustrates a compaction task to compact _all segments_ within the interval `2020-01-01/2021-01-01` and create new segments:

```json
{
  "type": "compact",
  "dataSource": "wikipedia",
  "ioConfig": {
    "type": "compact",
    "inputSpec": {
      "type": "interval",
      "interval": "2020-01-01/2021-01-01"
    }
  },
  "granularitySpec": {
    "segmentGranularity": "day",
    "queryGranularity": "hour"
  }
}
```

`granularitySpec` is an optional field.
If you don't specify `granularitySpec`, Druid retains the original segment and query granularities when compaction is complete.

## Compaction I/O configuration

The compaction `ioConfig` requires specifying `inputSpec` as follows:

|Field|Description|Default|Required|
|-----|-----------|-------|--------|
|`type`|Task type. Set the value to `compact`.|none|Yes|
|`inputSpec`|Specification of the target [interval](#interval-inputspec) or [segments](#segments-inputspec).|none|Yes|
|`dropExisting`|If `true`, the task replaces all existing segments fully contained by either of the following:<br />- the `interval` in the `interval` type `inputSpec`.<br />- the umbrella interval of the `segments` in the `segment` type `inputSpec`.<br />If compaction fails, Druid does not change any of the existing segments.<br />**WARNING**: `dropExisting` in `ioConfig` is a beta feature. |false|No|
|`allowNonAlignedInterval`|If `true`, the task allows an explicit [`segmentGranularity`](#compaction-granularity-spec) that is not aligned with the provided [interval](#interval-inputspec) or [segments](#segments-inputspec). This parameter is only used if [`segmentGranularity`](#compaction-granularity-spec) is explicitly provided.<br /><br />This parameter is provided for backwards compatibility. In most scenarios it should not be set, as it can lead to data being accidentally overshadowed. This parameter may be removed in a future release.|false|No|

The compaction task has two kinds of `inputSpec`:

### Interval `inputSpec`

|Field|Description|Required|
|-----|-----------|--------|
|`type`|Task type. Set the value to `interval`.|Yes|
|`interval`|Interval to compact.|Yes|

### Segments `inputSpec`

|Field|Description|Required|
|-----|-----------|--------|
|`type`|Task type. Set the value to `segments`.|Yes|
|`segments`|A list of segment IDs.|Yes|

## Compaction dimensions spec

|Field|Description|Required|
|-----|-----------|--------|
|`dimensions`| A list of dimension names or objects. Cannot have the same column in both `dimensions` and `dimensionExclusions`. Defaults to `null`, which preserves the original dimensions.|No|
|`dimensionExclusions`| The names of dimensions to exclude from compaction. Only names are supported here, not objects. This list is only used if the dimensions list is null or empty; otherwise it is ignored. Defaults to `[]`.|No|

## Compaction transform spec

|Field|Description|Required|
|-----|-----------|--------|
|`filter`| The `filter` conditionally filters input rows during compaction. Only rows that pass the filter will be included in the compacted segments. Any of Druid's standard [query filters](../querying/filters.md) can be used. Defaults to 'null', which will not filter any row. |No|

## Compaction granularity spec

|Field|Description|Required|
|-----|-----------|--------|
|`segmentGranularity`|Time chunking period for the segment granularity. Defaults to 'null', which preserves the original segment granularity. Accepts all [Query granularity](../querying/granularities.md) values.|No|
|`queryGranularity`|The resolution of timestamp storage within each segment. Defaults to 'null', which preserves the original query granularity. Accepts all [Query granularity](../querying/granularities.md) values.|No|
|`rollup`|Enables compaction-time rollup. To preserve the original setting, keep the default value. To enable compaction-time rollup, set the value to `true`. Once the data is rolled up, you can no longer recover individual records.|No|

## Learn more

See the following topics for more information:
* [Compaction](compaction.md) for an overview of compaction and how to set up manual compaction in Druid.
* [Segment optimization](../operations/segment-optimization.md) for guidance on evaluating and optimizing Druid segment size.
* [Coordinator process](../design/coordinator.md#automatic-compaction) for details on how the Coordinator plans compaction tasks.

