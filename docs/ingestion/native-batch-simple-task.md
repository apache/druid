---
id: native-batch-simple-task
title: "JSON-based batch simple task indexing"
sidebar_label: "JSON-based batch (simple)"
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
 This page describes native batch ingestion using [ingestion specs](ingestion-spec.md). Refer to the [ingestion
 methods](../ingestion/index.md#batch) table to determine which ingestion method is right for you.
:::

The simple task ([task type](tasks.md) `index`) executes single-threaded as a single task within the indexing service. For parallel, scalable options consider using [`index_parallel` tasks](./native-batch.md) or [SQL-based batch ingestion](../multi-stage-query/index.md).

## Simple task example

A sample task is shown below:

```json
{
  "type" : "index",
  "spec" : {
    "dataSchema" : {
      "dataSource" : "wikipedia",
      "timestampSpec" : {
        "column" : "timestamp",
        "format" : "auto"
      },
      "dimensionsSpec" : {
        "dimensions": ["country", "page","language","user","unpatrolled","newPage","robot","anonymous","namespace","continent","region","city"],
        "dimensionExclusions" : []
      },
      "metricsSpec" : [
        {
          "type" : "count",
          "name" : "count"
        },
        {
          "type" : "doubleSum",
          "name" : "added",
          "fieldName" : "added"
        },
        {
          "type" : "doubleSum",
          "name" : "deleted",
          "fieldName" : "deleted"
        },
        {
          "type" : "doubleSum",
          "name" : "delta",
          "fieldName" : "delta"
        }
      ],
      "granularitySpec" : {
        "type" : "uniform",
        "segmentGranularity" : "DAY",
        "queryGranularity" : "NONE",
        "intervals" : [ "2013-08-31/2013-09-01" ]
      }
    },
    "ioConfig" : {
      "type" : "index",
      "inputSource" : {
        "type" : "local",
        "baseDir" : "examples/indexing/",
        "filter" : "wikipedia_data.json"
       },
       "inputFormat": {
         "type": "json"
       }
    },
    "tuningConfig" : {
      "type" : "index",
      "partitionsSpec": {
        "type": "hashed",
        "partitionDimensions": ["country"],
        "targetRowsPerSegment": 5000000
      }
    }
  }
}
```

## Simple task configuration

|property|description|required?|
|--------|-----------|---------|
|type|The task type, this should always be `index`.|yes|
|id|The task ID. If this is not explicitly specified, Druid generates the task ID using task type, data source name, interval, and date-time stamp. |no|
|spec|The ingestion spec including the [data schema](#dataschema), [IO config](#ioconfig), and [tuning config](#tuningconfig).|yes|
|context|Context to specify various task configuration parameters. See [Task context parameters](tasks.md#context-parameters) for more details.|no|

### `dataSchema`

This field is required.

See the [`dataSchema`](./ingestion-spec.md#dataschema) section of the ingestion docs for details.

If you do not specify `intervals` explicitly in your dataSchema's granularitySpec, the Local Index Task will do an extra
pass over the data to determine the range to lock when it starts up.  If you specify `intervals` explicitly, any rows
outside the specified intervals will be thrown away. We recommend setting `intervals` explicitly if you know the time
range of the data because it allows the task to skip the extra pass, and so that you don't accidentally replace data outside
that range if there's some stray data with unexpected timestamps.

### `ioConfig`

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|The task type, this should always be "index".|none|yes|
|inputFormat|[`inputFormat`](./data-formats.md#input-format) to specify how to parse input data.|none|yes|
|appendToExisting|Creates segments as additional shards of the latest version, effectively appending to the segment set instead of replacing it. This means that you can append new segments to any datasource regardless of its original partitioning scheme. You must use the `dynamic` partitioning type for the appended segments. If you specify a different partitioning type, the task fails with an error.|false|no|
|dropExisting|If this setting is `false` then ingestion proceeds as usual. Set this to `true` and `appendToExisting` to `false` to enforce true "replace" functionality as described next. If `true` and `appendToExisting` is `false` and the `granularitySpec` contains at least one`interval`, then the ingestion task will create regular segments for time chunk intervals with input data and `tombstones` for all other time chunks with no data. The task will publish the data segments and the tombstone segments together when the it publishes new segments. The net effect of the data segments and the tombstones is to completely adhere to a "replace" semantics where the  input data contained in the `granularitySpec` intervals replaces all existing data in the intervals even for time chunks that would be empty in the case that no input data was associated with them. In the extreme case when the input data set that falls in the `granularitySpec` intervals is empty all existing data in the interval will be replaced with an empty data set (i.e. with nothing -- all existing data will be covered by `tombstones`). If ingestion fails, no segments and tombstones will be published. The following two combinations are not supported and will make the ingestion fail with an error: `dropExisting` is `true` and `interval` is not specified in `granularitySpec` or  `appendToExisting` is true and `dropExisting` is `true`. WARNING: this functionality is still in beta and even though we are not aware of any bugs, use with caution.|false|no|

### `tuningConfig`

The tuningConfig is optional and default parameters will be used if no tuningConfig is specified. See below for more details.

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|The task type, this should always be "index".|none|yes|
|maxRowsInMemory|Used in determining when intermediate persists to disk should occur. Normally user does not need to set this, but depending on the nature of data, if rows are short in terms of bytes, user may not want to store a million rows in memory and this value should be set.|1000000|no|
|maxBytesInMemory|Used in determining when intermediate persists to disk should occur. Normally this is computed internally and user does not need to set it. This value represents number of bytes to aggregate in heap memory before persisting. This is based on a rough estimate of memory usage and not actual usage. The maximum heap memory usage for indexing is maxBytesInMemory * (2 + maxPendingPersists). Note that `maxBytesInMemory` also includes heap usage of artifacts created from intermediary persists. This means that after every persist, the amount of `maxBytesInMemory` until next persist will decreases, and task will fail when the sum of bytes of all intermediary persisted artifacts exceeds `maxBytesInMemory`.|1/6 of max JVM memory|no|
|maxTotalRows|Deprecated. Use `partitionsSpec` instead. Total number of rows in segments waiting for being pushed. Used in determining when intermediate pushing should occur.|20000000|no|
|numShards|Deprecated. Use `partitionsSpec` instead. Directly specify the number of shards to create. If this is specified and `intervals` is specified in the `granularitySpec`, the index task can skip the determine intervals/partitions pass through the data.|null|no|
|partitionDimensions|Deprecated. Use `partitionsSpec` instead. The dimensions to partition on. Leave blank to select all dimensions. Only used with `forceGuaranteedRollup` = true, will be ignored otherwise.|null|no|
|partitionsSpec|Defines how to partition data in each timeChunk, see [PartitionsSpec](#partitionsspec)|`dynamic` if `forceGuaranteedRollup` = false, `hashed` if `forceGuaranteedRollup` = true|no|
|indexSpec|Defines segment storage format options to be used at indexing time, see [IndexSpec](ingestion-spec.md#indexspec)|null|no|
|indexSpecForIntermediatePersists|Defines segment storage format options to be used at indexing time for intermediate persisted temporary segments. This can be used to disable dimension/metric compression on intermediate segments to reduce memory required for final merging. However, disabling compression on intermediate segments might increase page cache use while they are used before getting merged into final segment published, see [IndexSpec](ingestion-spec.md#indexspec) for possible values.|same as indexSpec|no|
|maxPendingPersists|Maximum number of persists that can be pending but not started. If this limit would be exceeded by a new intermediate persist, ingestion will block until the currently-running persist finishes. Maximum heap memory usage for indexing scales with maxRowsInMemory * (2 + maxPendingPersists).|0 (meaning one persist can be running concurrently with ingestion, and none can be queued up)|no|
|forceGuaranteedRollup|Forces guaranteeing the [perfect rollup](rollup.md). The perfect rollup optimizes the total size of generated segments and querying time while indexing time will be increased. If this is set to true, the index task will read the entire input data twice: one for finding the optimal number of partitions per time chunk and one for generating segments. Note that the result segments would be hash-partitioned. This flag cannot be used with `appendToExisting` of IOConfig. For more details, see the below __Segment pushing modes__ section.|false|no|
|reportParseExceptions|DEPRECATED. If true, exceptions encountered during parsing will be thrown and will halt ingestion; if false, unparseable rows and fields will be skipped. Setting `reportParseExceptions` to true will override existing configurations for `maxParseExceptions` and `maxSavedParseExceptions`, setting `maxParseExceptions` to 0 and limiting `maxSavedParseExceptions` to no more than 1.|false|no|
|pushTimeout|Milliseconds to wait for pushing segments. It must be >= 0, where 0 means to wait forever.|0|no|
|segmentWriteOutMediumFactory|Segment write-out medium to use when creating segments. See [SegmentWriteOutMediumFactory](native-batch.md#segmentwriteoutmediumfactory).|Not specified, the value from `druid.peon.defaultSegmentWriteOutMediumFactory.type` is used|no|
|logParseExceptions|If true, log an error message when a parsing exception occurs, containing information about the row where the error occurred.|false|no|
|maxParseExceptions|The maximum number of parse exceptions that can occur before the task halts ingestion and fails. Overridden if `reportParseExceptions` is set.|unlimited|no|
|maxSavedParseExceptions|When a parse exception occurs, Druid can keep track of the most recent parse exceptions. "maxSavedParseExceptions" limits how many exception instances will be saved. These saved exceptions will be made available after the task finishes in the [task completion report](tasks.md#task-reports). Overridden if `reportParseExceptions` is set.|0|no|

### `partitionsSpec`

PartitionsSpec is to describe the secondary partitioning method.
You should use different partitionsSpec depending on the [rollup mode](rollup.md) you want.
For perfect rollup, you should use `hashed`.

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|This should always be `hashed`|none|yes|
|maxRowsPerSegment|Used in sharding. Determines how many rows are in each segment.|5000000|no|
|numShards|Directly specify the number of shards to create. If this is specified and `intervals` is specified in the `granularitySpec`, the index task can skip the determine intervals/partitions pass through the data. `numShards` cannot be specified if `maxRowsPerSegment` is set.|null|no|
|partitionDimensions|The dimensions to partition on. Leave blank to select all dimensions.|null|no|
|partitionFunction|A function to compute hash of partition dimensions. See [Hash partition function](./native-batch.md#hash-partition-function)|`murmur3_32_abs`|no|

For best-effort rollup, you should use `dynamic`.

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|This should always be `dynamic`|none|yes|
|maxRowsPerSegment|Used in sharding. Determines how many rows are in each segment.|5000000|no|
|maxTotalRows|Total number of rows in segments waiting for being pushed.|20000000|no|

## Segment pushing modes

While ingesting data using the simple task indexing, Druid creates segments from the input data and pushes them. For segment pushing,
the simple task index supports the following segment pushing modes based upon your type of [rollup](./rollup.md):

- Bulk pushing mode: Used for perfect rollup. Druid pushes every segment at the very end of the index task. Until then, Druid stores created segments in memory and local storage of the service running the index task. This mode can cause problems if you have limited storage capacity, and is not recommended to use in production.
To enable bulk pushing mode, set `forceGuaranteedRollup` in your TuningConfig. You can not use bulk pushing with `appendToExisting` in your IOConfig.
- Incremental pushing mode: Used for best-effort rollup. Druid pushes segments are incrementally during the course of the indexing task. The index task collects data and stores created segments in the memory and disks of the services running the task until the total number of collected rows exceeds `maxTotalRows`. At that point the index task immediately pushes all segments created up until that moment, cleans up pushed segments, and continues to ingest the remaining data.
