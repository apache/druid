---
id: native-batch
title: JSON-based batch
sidebar_label: JSON-based batch
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
 This page describes JSON-based batch ingestion using [ingestion specs](ingestion-spec.md). For SQL-based batch ingestion using the [`druid-multi-stage-query`](../multi-stage-query/index.md) extension, see [SQL-based ingestion](../multi-stage-query/index.md). Refer to the [ingestion methods](../ingestion/index.md#batch) table to determine which ingestion method is right for you.
:::

Apache Druid supports the following types of JSON-based batch indexing tasks:
- Parallel task indexing (`index_parallel`) that can run multiple indexing tasks concurrently. Parallel task works well for production ingestion tasks.
- Simple task indexing (`index`) that run a single indexing task at a time. Simple task indexing is suitable for development and test environments.

This topic covers the configuration for `index_parallel` ingestion specs.

For related information on batch indexing, see:
- [Batch ingestion method comparison table](./index.md#batch) for a comparison of batch ingestion methods.
- [Tutorial: Loading a file](../tutorials/tutorial-batch.md) for a tutorial on JSON-based batch ingestion.
- [Input sources](./input-sources.md) for possible input sources.
- [Source input formats](./data-formats.md#input-format) for possible input formats.

## Submit an indexing task

To run either kind of JSON-based batch indexing task, you can:

- Use the **Load Data** UI in the web console to define and submit an ingestion spec.
- Define an ingestion spec in JSON based upon the [examples](#parallel-indexing-example) and reference topics for batch indexing. Then POST the ingestion spec to the [Tasks API endpoint](../api-reference/tasks-api.md), `/druid/indexer/v1/task`, the Overlord service. Alternatively, you can use the indexing script included with Druid at `bin/post-index-task`.

## Parallel task indexing

The parallel task type `index_parallel` is a task for multi-threaded batch indexing. Parallel task indexing only relies on Druid resources. It doesn't depend on other external systems like Hadoop.

The `index_parallel` task is a supervisor task that orchestrates
the whole indexing process. The supervisor task splits the input data and creates worker tasks to process the individual portions of data.

Druid issues the worker tasks to the Overlord. The Overlord schedules and runs the workers on MiddleManagers or Indexers. After a worker task successfully processes the assigned input portion, it reports the resulting segment list to the Supervisor task.

The Supervisor task periodically checks the status of worker tasks. If a task fails, the Supervisor retries the task until the number of retries reaches the configured limit. If all worker tasks succeed, it publishes the reported segments at once and finalizes ingestion.

The detailed behavior of the parallel task is different depending on the `partitionsSpec`. See [`partitionsSpec`](#partitionsspec) for more details.

Parallel tasks require the following:

- A splittable [`inputSource`](#splittable-input-sources) in the `ioConfig`. For a list of supported splittable input formats, see [Splittable input sources](#splittable-input-sources).
- The `maxNumConcurrentSubTasks` greater than 1 in the `tuningConfig`. Otherwise tasks run sequentially. The `index_parallel` task reads each input file one by one and creates segments by itself.

### Supported compression formats

JSON-based batch ingestion supports the following compression formats:

- `bz2`
- `gz`
- `xz`
- `zip`
- `sz` (Snappy)
- `zst` (ZSTD)

### Implementation considerations

This section covers implementation details to consider when you implement parallel task ingestion.

#### Volume control for worker tasks

You can control the amount of input data each worker task processes using different configurations depending on the phase in parallel ingestion.
See [`partitionsSpec`](#partitionsspec) for details about how partitioning affects data volume for tasks.

For the tasks that read data from the `inputSource`, you can set the [Split hint spec](#split-hint-spec) in the `tuningConfig`.
For the task that merge shuffled segments, you can set the `totalNumMergeTasks` in the `tuningConfig`.

#### Number of running tasks

The `maxNumConcurrentSubTasks` in the `tuningConfig` determines the number of concurrent worker tasks that run in parallel. The Supervisor task checks the number of current running worker tasks and creates more if it's smaller than `maxNumConcurrentSubTasks` regardless of the number of available task slots. This may affect to other ingestion performance. See [Capacity planning](#capacity-planning) section for more details.

#### Replacing or appending data

By default, JSON-based batch ingestion replaces all data in the intervals in your `granularitySpec` for any segment that it writes to. If you want to add to the segment instead, set the `appendToExisting` flag in the `ioConfig`. JSON-based batch ingestion only replaces data in segments where it actively adds data. If there are segments in the intervals for your `granularitySpec` that don't have data from a task, they remain unchanged. If any existing segments partially overlap with the intervals in the `granularitySpec`, the portion of those segments outside the interval for the new spec remain visible.

You can also perform concurrent append and replace tasks. For more information, see [Concurrent append and replace](./concurrent-append-replace.md)


#### Fully replacing existing segments using tombstones

:::info
This feature is still experimental.
:::

You can set `dropExisting` flag in the `ioConfig` to true if you want the ingestion task to replace all existing segments that start and end within the intervals for your `granularitySpec`. This applies whether or not the new data covers all existing segments. `dropExisting` only applies when `appendToExisting` is false and the `granularitySpec` contains an `interval`.

The following examples demonstrate when to set the `dropExisting` property to true in the `ioConfig`:

Consider an existing segment with an interval of 2020-01-01 to 2021-01-01 and `YEAR` `segmentGranularity`. You want to overwrite the whole interval of 2020-01-01 to 2021-01-01 with new data using the finer segmentGranularity of `MONTH`. If the replacement data does not have a record within every months from 2020-01-01 to 2021-01-01 Druid cannot drop the original `YEAR` segment even if it does include all the replacement data. Set `dropExisting` to true in this case to replace the original segment at `YEAR` `segmentGranularity` since you no longer need it.

Imagine you want to re-ingest or overwrite a datasource and the new data does not contain some time intervals that exist in the datasource. For example, a datasource contains the following data at `MONTH` `segmentGranularity`:

- **January**: 1 record  
- **February**: 10 records  
- **March**: 10 records  

You want to re-ingest and overwrite with new data as follows:

- **January**: 0 records  
- **February**: 10 records  
- **March**: 9 records  

Unless you set `dropExisting` to true, the result after ingestion with overwrite using the same `MONTH` `segmentGranularity` would be:

* **January**: 1 record  
* **February**: 10 records  
* **March**: 9 records

This may not be what it is expected since the new data has 0 records for January. Set `dropExisting` to true to replace the unneeded January segment with a tombstone.
   
## Parallel indexing example

The following example illustrates the configuration for a parallel indexing task.

<details>
<summary>Click to view the example</summary>

```json
{
  "type": "index_parallel",
  "spec": {
    "dataSchema": {
      "dataSource": "wikipedia_parallel_index_test",
      "timestampSpec": {
        "column": "timestamp"
      },
      "dimensionsSpec": {
        "dimensions": [
          "country",
          "page",
          "language",
          "user",
          "unpatrolled",
          "newPage",
          "robot",
          "anonymous",
          "namespace",
          "continent",
          "region",
          "city"
        ]
      },
      "metricsSpec": [
        {
          "type": "count",
          "name": "count"
        },
        {
          "type": "doubleSum",
          "name": "added",
          "fieldName": "added"
        },
        {
          "type": "doubleSum",
          "name": "deleted",
          "fieldName": "deleted"
        },
        {
          "type": "doubleSum",
          "name": "delta",
          "fieldName": "delta"
        }
      ],
      "granularitySpec": {
        "segmentGranularity": "DAY",
        "queryGranularity": "second",
        "intervals": [
          "2013-08-31/2013-09-02"
        ]
      }
    },
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "local",
        "baseDir": "examples/indexing/",
        "filter": "wikipedia_index_data*"
      },
      "inputFormat": {
        "type": "json"
      }
    },
    "tuningConfig": {
      "type": "index_parallel",
      "partitionsSpec": {
        "type": "single_dim",
        "partitionDimension": "country",
        "targetRowsPerSegment": 5000000
      },
      "maxNumConcurrentSubTasks": 2
    }
  }
}
```
</details>

## Parallel indexing configuration

The following table defines the primary sections of the input spec:

|Property|Description|Required|
|--------|-----------|--------|
|`type`|The task type. For parallel task indexing, set the value to `index_parallel`.|yes|
|`id`|The task ID. If omitted, Druid generates the task ID using the task type, data source name, interval, and date-time stamp.|no|
|`spec`|The ingestion spec that defines the [data schema](#dataschema), [IO config](#ioconfig), and [tuning config](#tuningconfig).|yes|
|`context`|Context to specify various task configuration parameters. See [Task context parameters](../ingestion/tasks.md#context-parameters) for more details.|no|

### `dataSchema`

This field is required. In general, it defines the way that Druid stores your data: the primary timestamp column, the dimensions, metrics, and any transformations. For an overview, see [Ingestion Spec DataSchema](../ingestion/ingestion-spec.md#dataschema).

When defining the `granularitySpec` for index parallel, consider the defining `intervals` explicitly if you know the time range of the data. This way locking failure happens faster and Druid won't accidentally replace data outside the interval range some rows contain unexpected timestamps. The reasoning is as follows:

- If you explicitly define `intervals`, JSON-based batch ingestion locks all intervals specified when it starts up. Problems with locking become evident quickly when multiple ingestion or indexing tasks try to obtain a lock on the same interval. For example, if a Kafka ingestion task tries to obtain a lock on a locked interval causing the ingestion task fail. Furthermore, if there are rows outside the specified intervals, Druid drops them, avoiding conflict with unexpected intervals.
- If you don't define `intervals`, JSON-based batch ingestion locks each interval when the interval is discovered. In this case, if the task overlaps with a higher-priority task, issues with conflicting locks occur later in the ingestion process. If the source data includes rows with unexpected timestamps, they may caused unexpected locking of intervals.

### `ioConfig`

The following table lists the properties of a `ioConfig` object:

|Property|Description|Default|Required|
|--------|-----------|-------|---------|
|`type`|The task type. Set to the value to `index_parallel`.|none|yes|
|`inputFormat`|[`inputFormat`](./data-formats.md#input-format) to specify how to parse input data.|none|yes|
|`appendToExisting`|Creates segments as additional shards of the latest version, effectively appending to the segment set instead of replacing it. This means that you can append new segments to any datasource regardless of its original partitioning scheme. You must use the `dynamic` partitioning type for the appended segments. If you specify a different partitioning type, the task fails with an error.|false|no|
|`dropExisting`|If `true` and `appendToExisting` is `false` and the `granularitySpec` contains an`interval`, then the ingestion task replaces all existing segments fully contained by the specified `interval` when the task publishes new segments. If ingestion fails, Druid doesn't change any existing segments. In the case of misconfiguration where either `appendToExisting` is `true` or `interval` isn't specified in `granularitySpec`, Druid doesn't replace any segments even if `dropExisting` is `true`. WARNING: this feature is still experimental.|false|no|

### `tuningConfig`

The `tuningConfig` is optional. Druid uses default parameters if `tuningConfig` is not specified.

The following table lists the properties of a `tuningConfig` object:

|Property|Description|Default|Required|
|--------|-----------|-------|---------|
|`type`|The task type. Set the value to`index_parallel`.|none|yes|
|`maxRowsInMemory`|Determines when Druid should perform intermediate persists to disk. Normally you don't need to set this. Depending on the nature of your data, if rows are short in terms of bytes. For example, you may not want to store a million rows in memory. In this case, set this value.|1000000|no|
|`maxBytesInMemory`|Use to determine when Druid should perform intermediate persists to disk. Normally Druid computes this internally and you don't need to set it. This value represents number of bytes to aggregate in heap memory before persisting. This is based on a rough estimate of memory usage and not actual usage. The maximum heap memory usage for indexing is `maxBytesInMemory * (2 + maxPendingPersists)`. Note that `maxBytesInMemory` also includes heap usage of artifacts created from intermediary persists. This means that after every persist, the amount of `maxBytesInMemory` until next persist will decrease. Tasks fail when the sum of bytes of all intermediary persisted artifacts exceeds `maxBytesInMemory`.|1/6 of max JVM memory|no|
|`maxColumnsToMerge`|Limit of the number of segments to merge in a single phase when merging segments for publishing. This limit affects the total number of columns present in a set of segments to merge. If the limit is exceeded, segment merging occurs in multiple phases. Druid merges at least 2 segments per phase, regardless of this setting.|-1 (unlimited)|no|
|`maxTotalRows`|Deprecated. Use `partitionsSpec` instead. Total number of rows in segments waiting to be pushed. Used to determine when intermediate pushing should occur.|20000000|no|
|`numShards`|Deprecated. Use `partitionsSpec` instead. Directly specify the number of shards to create when using a `hashed` `partitionsSpec`. If this is specified and `intervals` is specified in the `granularitySpec`, the index task can skip the determine intervals/partitions pass through the data.|null|no|
|`splitHintSpec`|Hint to control the amount of data that each first phase task reads. Druid may ignore the hint depending on the implementation of the input source. See [Split hint spec](#split-hint-spec) for more details.|size-based split hint spec|no|
|`partitionsSpec`|Defines how to partition data in each timeChunk, see [PartitionsSpec](#partitionsspec).|`dynamic` if `forceGuaranteedRollup` = false, `hashed` or `single_dim` if `forceGuaranteedRollup` = true|no|
|`indexSpec`|Defines segment storage format options to be used at indexing time, see [IndexSpec](ingestion-spec.md#indexspec).|null|no|
|`indexSpecForIntermediatePersists`|Defines segment storage format options to use at indexing time for intermediate persisted temporary segments. You can use this configuration to disable dimension/metric compression on intermediate segments to reduce memory required for final merging. However, if you disable compression on intermediate segments, page cache use my increase while intermediate segments are used before Druid merges them to the final published segment published. See [IndexSpec](./ingestion-spec.md#indexspec) for possible values.|same as `indexSpec`|no|
|`maxPendingPersists`|Maximum number of pending persists that remain not started. If a new intermediate persist exceeds this limit, ingestion blocks until the currently-running persist finishes. Maximum heap memory usage for indexing scales with `maxRowsInMemory * (2 + maxPendingPersists)`.|0 (meaning one persist can be running concurrently with ingestion, and none can be queued up)|no|
|`forceGuaranteedRollup`|Forces [perfect rollup](rollup.md). The perfect rollup optimizes the total size of generated segments and querying time but increases indexing time. If true, specify `intervals` in the `granularitySpec` and use either `hashed` or `single_dim` for the `partitionsSpec`. You cannot use this flag in conjunction with `appendToExisting` of `IOConfig`. For more details, see [Segment pushing modes](#segment-pushing-modes).|false|no|
|`reportParseExceptions`|If true, Druid throws exceptions encountered during parsing and halts ingestion. If false, Druid skips unparseable rows and fields.|false|no|
|`pushTimeout`|Milliseconds to wait to push segments. Must be >= 0, where 0 means to wait forever.|0|no|
|`segmentWriteOutMediumFactory`|Segment write-out medium to use when creating segments. See [SegmentWriteOutMediumFactory](#segmentwriteoutmediumfactory).|If not specified, uses the value from `druid.peon.defaultSegmentWriteOutMediumFactory.type` |no|
|`maxNumConcurrentSubTasks`|Maximum number of worker tasks that can be run in parallel at the same time. The supervisor task spawns worker tasks up to `maxNumConcurrentSubTasks` regardless of the current available task slots. If this value is 1, the supervisor task processes data ingestion on its own instead of spawning worker tasks. If this value is set to too large, the supervisor may create too many worker tasks that block other ingestion tasks. See [Capacity planning](#capacity-planning) for more details.|1|no|
|`maxRetry`|Maximum number of retries on task failures.|3|no|
|`maxNumSegmentsToMerge`|Max limit for the number of segments that a single task can merge at the same time in the second phase. Used only when `forceGuaranteedRollup` is true.|100|no|
|`totalNumMergeTasks`|Total number of tasks that merge segments in the merge phase when `partitionsSpec` is set to `hashed` or `single_dim`.|10|no|
|`taskStatusCheckPeriodMs`|Polling period in milliseconds to check running task statuses.|1000|no|
|`chatHandlerTimeout`|Timeout for reporting the pushed segments in worker tasks.|PT10S|no|
|`chatHandlerNumRetries`|Retries for reporting the pushed segments in worker tasks.|5|no|
|`awaitSegmentAvailabilityTimeoutMillis`|Milliseconds to wait for the newly indexed segments to become available for query after ingestion completes. If `<= 0`, no wait occurs. If `> 0`, the task waits for the Coordinator to indicate that the new segments are available for querying. If the timeout expires, the task exits as successful, but the segments are not confirmed as available for query.|Long|no (default = 0)|

### Split Hint Spec

The split hint spec is used to help the supervisor task divide input sources. Each worker task processes a single input division. You can control the amount of data each worker task reads during the first phase.

#### Size-based Split Hint Spec

The size-based split hint spec affects all splittable input sources except for the HTTP input source and SQL input source.

|Property|Description|Default|Required|
|--------|-----------|-------|---------|
|`type`|Set the value to `maxSize`.|none|yes|
|`maxSplitSize`|Maximum number of bytes of input files to process in a single subtask. If a single file is larger than the limit, Druid processes the file alone in a single subtask. Druid does not split files across tasks. One subtask will not process more files than `maxNumFiles` even when their total size is smaller than `maxSplitSize`. [Human-readable format](../configuration/human-readable-byte.md) is supported.|1GiB|no|
|`maxNumFiles`|Maximum number of input files to process in a single subtask. This limit avoids task failures when the ingestion spec is too long. There are two known limits on the max size of serialized ingestion spec: the max ZNode size in ZooKeeper (`jute.maxbuffer`) and the max packet size in MySQL (`max_allowed_packet`). These limits can cause ingestion tasks fail if the serialized ingestion spec size hits one of them. One subtask will not process more data than `maxSplitSize` even when the total number of files is smaller than `maxNumFiles`.|1000|no|

#### Segments Split Hint Spec

The segments split hint spec is used only for [`DruidInputSource`](./input-sources.md).

|Property|Description|Default|Required|
|--------|-----------|-------|---------|
|`type`|Set the value to `segments`.|none|yes|
|`maxInputSegmentBytesPerTask`|Maximum number of bytes of input segments to process in a single subtask. If a single segment is larger than this number, Druid processes the segment alone in a single subtask. Druid never splits input segments across tasks. A single subtask will not process more segments than `maxNumSegments` even when their total size is smaller than `maxInputSegmentBytesPerTask`. [Human-readable format](../configuration/human-readable-byte.md) is supported.|1GiB|no|
|`maxNumSegments`|Maximum number of input segments to process in a single subtask. This limit avoids failures due to the the ingestion spec being too long. There are two known limits on the max size of serialized ingestion spec: the max ZNode size in ZooKeeper (`jute.maxbuffer`) and the max packet size in MySQL (`max_allowed_packet`). These limits can make ingestion tasks fail when the serialized ingestion spec size hits one of them. A single subtask will not process more data than `maxInputSegmentBytesPerTask` even when the total number of segments is smaller than `maxNumSegments`.|1000|no|

### `partitionsSpec`

The primary partition for Druid is time. You can define a secondary partitioning method in the partitions spec. Use the `partitionsSpec` type that applies for your [rollup](rollup.md) method. 

For perfect rollup, you can use:

- `hashed` partitioning based on the hash value of specified dimensions for each row
- `single_dim` based on ranges of values for a single dimension
- `range` based on ranges of values of multiple dimensions.

For best-effort rollup, use `dynamic`.

For an overview, see [Partitioning](./partitioning.md).

The `partitionsSpec` types have different characteristics.

| PartitionsSpec | Ingestion speed | Partitioning method | Supported rollup mode | Secondary partition pruning at query time |
|----------------|-----------------|---------------------|-----------------------|-------------------------------|
| `dynamic` | Fastest  | [Dynamic partitioning](#dynamic-partitioning) based on the number of rows in a segment. | Best-effort rollup | N/A |
| `hashed`  | Moderate | Multiple dimension [hash-based partitioning](#hash-based-partitioning) may reduce both your datasource size and query latency by improving data locality. See [Partitioning](./partitioning.md) for more details. | Perfect rollup | The broker can use the partition information to prune segments early to speed up queries. Since the broker knows how to hash `partitionDimensions` values to locate a segment, given a query including a filter on all the `partitionDimensions`, the broker can pick up only the segments holding the rows satisfying the filter on `partitionDimensions` for query processing.<br/><br/>Note that `partitionDimensions` must be set at ingestion time to enable secondary partition pruning at query time.|
| `single_dim` | Slower | Single dimension [range partitioning](#single-dimension-range-partitioning) may reduce your datasource size and query latency by improving data locality. See [Partitioning](./partitioning.md) for more details. | Perfect rollup | The broker can use the partition information to prune segments early to speed up queries. Since the broker knows the range of `partitionDimension` values in each segment, given a query including a filter on the `partitionDimension`, the broker can pick up only the segments holding the rows satisfying the filter on `partitionDimension` for query processing. |
| `range` | Slowest | Multiple dimension [range partitioning](#multi-dimension-range-partitioning) may reduce your datasource size and query latency by improving data locality. See [Partitioning](./partitioning.md) for more details. | Perfect rollup | The broker can use the partition information to prune segments early to speed up queries. Since the broker knows the range of `partitionDimensions` values within each segment, given a query including a filter on the first of the `partitionDimensions`, the broker can pick up only the segments holding the rows satisfying the filter on the first partition dimension for query processing. |

#### Dynamic partitioning

|Property|Description|Default|Required|
|--------|-----------|-------|---------|
|`type`|Set the value to `dynamic`.|none|yes|
|`maxRowsPerSegment`|Used in sharding. Determines how many rows are in each segment.|5000000|no|
|`maxTotalRows`|Total number of rows across all segments waiting for being pushed. Used in determining when intermediate segment push should occur.|20000000|no|

With the dynamic partitioning, the parallel index task runs in a single phase spawning multiple worker tasks (type `single_phase_sub_task`), each of which creates segments.

How the worker task creates segments:

- Whenever the number of rows in the current segment exceeds
  `maxRowsPerSegment`.
- When the total number of rows in all segments across all time chunks reaches to `maxTotalRows`. At this point the task pushes all segments created so far to the deep storage and creates new ones.

#### Hash-based partitioning

|Property|Description|Default|Required|
|--------|-----------|-------|---------|
|`type`|Set the value to `hashed`.|none|yes|
|`numShards`|Directly specify the number of shards to create. If this is specified and `intervals` is specified in the `granularitySpec`, the index task can skip the determine intervals/partitions pass through the data. This property and `targetRowsPerSegment` cannot both be set.|none|no|
|`targetRowsPerSegment`|A target row count for each partition. If `numShards` is left unspecified, the Parallel task will determine a partition count automatically such that each partition has a row count close to the target, assuming evenly distributed keys in the input data. A target per-segment row count of 5 million is used if both `numShards` and `targetRowsPerSegment` are null. |null (or 5,000,000 if both `numShards` and `targetRowsPerSegment` are null)|no|
|`partitionDimensions`|The dimensions to partition on. Leave blank to select all dimensions.|null|no|
|`partitionFunction`|A function to compute hash of partition dimensions. See [Hash partition function](#hash-partition-function)|`murmur3_32_abs`|no|

The Parallel task with hash-based partitioning is similar to [MapReduce](https://en.wikipedia.org/wiki/MapReduce).
The task runs in up to three phases: `partial dimension cardinality`, `partial segment generation` and `partial segment merge`.

The `partial dimension cardinality` phase is an optional phase that only runs if `numShards` is not specified.
The Parallel task splits the input data and assigns them to worker tasks based on the split hint spec.
Each worker task (type `partial_dimension_cardinality`) gathers estimates of partitioning dimensions cardinality for
each time chunk. The Parallel task will aggregate these estimates from the worker tasks and determine the highest
cardinality across all of the time chunks in the input data, dividing this cardinality by `targetRowsPerSegment` to
automatically determine `numShards`.

In the `partial segment generation` phase, just like the Map phase in MapReduce,
the Parallel task splits the input data based on the split hint spec
and assigns each split to a worker task. Each worker task (type `partial_index_generate`) reads the assigned split, and partitions rows by the time chunk from `segmentGranularity` (primary partition key) in the `granularitySpec`
and then by the hash value of `partitionDimensions` (secondary partition key) in the `partitionsSpec`.
The partitioned data is stored in local storage of 
the [middleManager](../design/middlemanager.md) or the [indexer](../design/indexer.md).

The `partial segment merge` phase is similar to the Reduce phase in MapReduce.
The Parallel task spawns a new set of worker tasks (type `partial_index_generic_merge`) to merge the partitioned data created in the previous phase. Here, the partitioned data is shuffled based on
the time chunk and the hash value of `partitionDimensions` to be merged; each worker task reads the data falling in the same time chunk and the same hash value from multiple MiddleManager/Indexer processes and merges them to create the final segments. Finally, they push the final segments to the deep storage at once.

##### Hash partition function

In hash partitioning, the partition function is used to compute hash of partition dimensions. The partition dimension values are first serialized into a byte array as a whole, and then the partition function is applied to compute hash of the byte array. Druid currently supports only one partition function.

|name|description|
|----|-----------|
|`murmur3_32_abs`|Applies an absolute value function to the result of [`murmur3_32`](https://guava.dev/releases/16.0/api/docs/com/google/common/hash/Hashing.html#murmur3_32()).|

#### Single-dimension range partitioning

:::info
 Single dimension range partitioning is not supported in the sequential mode of the `index_parallel` task type.
:::

Range partitioning has [several benefits](#benefits-of-range-partitioning) related to storage footprint and query
performance.

The Parallel task will use one subtask when you set `maxNumConcurrentSubTasks` to 1.

When you use this technique to partition your data, segment sizes may be unequally distributed if the data in your `partitionDimension` is also unequally distributed.  Therefore, to avoid imbalance in data layout, review the distribution of values in your source data before deciding on a partitioning strategy.

Range partitioning is not possible on multi-value dimensions. If the provided
`partitionDimension` is multi-value, your ingestion job will report an error.

|Property|Description|Default|Required|
|--------|-----------|-------|---------|
|`type`|Set the value to `single_dim`.|none|yes|
|`partitionDimension`|The dimension to partition on. Only rows with a single dimension value are allowed.|none|yes|
|`targetRowsPerSegment`|Target number of rows to include in a partition, should be a number that targets segments of 500MB\~1GB.|none|either this or `maxRowsPerSegment`|
|`maxRowsPerSegment`|Soft max for the number of rows to include in a partition.|none|either this or `targetRowsPerSegment`|
|`assumeGrouped`|Assume that input data has already been grouped on time and dimensions. Ingestion will run faster, but may choose sub-optimal partitions if this assumption is violated.|false|no|

With `single-dim` partitioning, the Parallel task runs in 3 phases,
i.e., `partial dimension distribution`, `partial segment generation`, and `partial segment merge`.
The first phase is to collect some statistics to find
the best partitioning and the other 2 phases are to create partial segments
and to merge them, respectively, as in hash-based partitioning.

In the `partial dimension distribution` phase, the Parallel task splits the input data and
assigns them to worker tasks based on the split hint spec. Each worker task (type `partial_dimension_distribution`) reads
the assigned split and builds a histogram for `partitionDimension`.
The Parallel task collects those histograms from worker tasks and finds
the best range partitioning based on `partitionDimension` to evenly
distribute rows across partitions. Note that either `targetRowsPerSegment`
or `maxRowsPerSegment` will be used to find the best partitioning.

In the `partial segment generation` phase, the Parallel task spawns new worker tasks (type `partial_range_index_generate`)
to create partitioned data. Each worker task reads a split created as in the previous phase,
partitions rows by the time chunk from the `segmentGranularity` (primary partition key) in the `granularitySpec`
and then by the range partitioning found in the previous phase.
The partitioned data is stored in local storage of
the [middleManager](../design/middlemanager.md) or the [indexer](../design/indexer.md).

In the `partial segment merge` phase, the parallel index task spawns a new set of worker tasks (type `partial_index_generic_merge`) to merge the partitioned
data created in the previous phase. Here, the partitioned data is shuffled based on
the time chunk and the value of `partitionDimension`; each worker task reads the segments
falling in the same partition of the same range from multiple MiddleManager/Indexer processes and merges
them to create the final segments. Finally, they push the final segments to the deep storage.

:::info
 Because the task with single-dimension range partitioning makes two passes over the input
 in `partial dimension distribution` and `partial segment generation` phases,
 the task may fail if the input changes in between the two passes.
:::

#### Multi-dimension range partitioning

:::info
 Multi-dimension range partitioning is not supported in the sequential mode of the `index_parallel` task type.
:::

Range partitioning has [several benefits](#benefits-of-range-partitioning) related to storage footprint and query
performance. Multi-dimension range partitioning improves over single-dimension range partitioning by allowing
Druid to distribute segment sizes more evenly, and to prune on more dimensions.

Range partitioning is not possible on multi-value dimensions. If one of the provided
`partitionDimensions` is multi-value, your ingestion job will report an error.

|Property|Description|Default|Required|
|--------|-----------|-------|---------|
|`type`|Set the value to `range`.|none|yes|
|`partitionDimensions`|An array of dimensions to partition on. Order the dimensions from most frequently queried to least frequently queried. For best results, limit your number of dimensions to between three and five dimensions.|none|yes|
|`targetRowsPerSegment`|Target number of rows to include in a partition, should be a number that targets segments of 500MB\~1GB.|none|either this or `maxRowsPerSegment`|
|maxRowsPerSegment|Soft max for the number of rows to include in a partition.|none|either this or `targetRowsPerSegment`|
|`assumeGrouped`|Assume that input data has already been grouped on time and dimensions. Ingestion will run faster, but may choose sub-optimal partitions if this assumption is violated.|false|no|

#### Benefits of range partitioning

Range partitioning, either `single_dim` or `range`, has several benefits:

1. Lower storage footprint due to combining similar data into the same segments, which improves compressibility.
2. Better query performance due to Broker-level segment pruning, which removes segments from
   consideration when they cannot possibly contain data matching the query filter.

For Broker-level segment pruning to be effective, you must include partition dimensions in the `WHERE` clause. Each
partition dimension can participate in pruning if the prior partition dimensions (those to its left) are also
participating, and if the query uses filters that support pruning.

Filters that support pruning include:

- Equality on string literals, like `x = 'foo'` and `x IN ('foo', 'bar')` where `x` is a string.
- Comparison between string columns and string literals, like `x < 'foo'` or other comparisons
  involving `<`, `>`, `<=`, or `>=`.

For example, if you configure the following `range` partitioning during ingestion:

```json
"partitionsSpec": {
  "type": "range",
  "partitionDimensions": ["countryName", "cityName"],
  "targetRowsPerSegment": 5000000
}
```

Then, filters like `WHERE countryName = 'United States'` or `WHERE countryName = 'United States' AND cityName = 'New York'`
can make use of pruning. However, `WHERE cityName = 'New York'` cannot make use of pruning, because countryName is not
involved. The clause `WHERE cityName LIKE 'New%'` cannot make use of pruning either, because LIKE filters do not
support pruning.

## HTTP status endpoints

The Supervisor task provides some HTTP endpoints to get running status.

`http://{PEON_IP}:{PEON_PORT}/druid/worker/v1/chat/{SUPERVISOR_TASK_ID}/mode`  
Returns `parallel` if the indexing task is running in parallel. Otherwise, it returns `sequential`.

`http://{PEON_IP}:{PEON_PORT}/druid/worker/v1/chat/{SUPERVISOR_TASK_ID}/phase`  
Returns the name of the current phase if the task running in the parallel mode.

`http://{PEON_IP}:{PEON_PORT}/druid/worker/v1/chat/{SUPERVISOR_TASK_ID}/progress`  
Returns the estimated progress of the current phase if the supervisor task is running in the parallel mode.

An example of the result is

```json
{
  "running":10,
  "succeeded":0,
  "failed":0,
  "complete":0,
  "total":10,
  "estimatedExpectedSucceeded":10
}
```

`http://{PEON_IP}:{PEON_PORT}/druid/worker/v1/chat/{SUPERVISOR_TASK_ID}/subtasks/running`  
Returns the task IDs of running worker tasks, or an empty list if the supervisor task is running in the sequential mode.

`http://{PEON_IP}:{PEON_PORT}/druid/worker/v1/chat/{SUPERVISOR_TASK_ID}/subtaskspecs`  
Returns all worker task specs, or an empty list if the supervisor task is running in the sequential mode.

`http://{PEON_IP}:{PEON_PORT}/druid/worker/v1/chat/{SUPERVISOR_TASK_ID}/subtaskspecs/running`  
Returns running worker task specs, or an empty list if the supervisor task is running in the sequential mode.

`http://{PEON_IP}:{PEON_PORT}/druid/worker/v1/chat/{SUPERVISOR_TASK_ID}/subtaskspecs/complete`  
Returns complete worker task specs, or an empty list if the supervisor task is running in the sequential mode.

`http://{PEON_IP}:{PEON_PORT}/druid/worker/v1/chat/{SUPERVISOR_TASK_ID}/subtaskspec/{SUB_TASK_SPEC_ID}`  
Returns the worker task spec of the given id, or HTTP 404 Not Found error if the supervisor task is running in the sequential mode.

`http://{PEON_IP}:{PEON_PORT}/druid/worker/v1/chat/{SUPERVISOR_TASK_ID}/subtaskspec/{SUB_TASK_SPEC_ID}/state`  
Returns the state of the worker task spec of the given id, or HTTP 404 Not Found error if the supervisor task is running in the sequential mode.
The returned result contains the worker task spec, a current task status if exists, and task attempt history.

<details>
<summary>Click to view the response</summary>

```json
{
  "spec": {
    "id": "index_parallel_lineitem_2018-04-20T22:12:43.610Z_2",
    "groupId": "index_parallel_lineitem_2018-04-20T22:12:43.610Z",
    "supervisorTaskId": "index_parallel_lineitem_2018-04-20T22:12:43.610Z",
    "context": null,
    "inputSplit": {
      "split": "/path/to/data/lineitem.tbl.5"
    },
    "ingestionSpec": {
      "dataSchema": {
        "dataSource": "lineitem",
        "timestampSpec": {
          "column": "l_shipdate",
          "format": "yyyy-MM-dd"
        },
        "dimensionsSpec": {
          "dimensions": [
            "l_orderkey",
            "l_partkey",
            "l_suppkey",
            "l_linenumber",
            "l_returnflag",
            "l_linestatus",
            "l_shipdate",
            "l_commitdate",
            "l_receiptdate",
            "l_shipinstruct",
            "l_shipmode",
            "l_comment"
          ]
        },
        "metricsSpec": [
          {
            "type": "count",
            "name": "count"
          },
          {
            "type": "longSum",
            "name": "l_quantity",
            "fieldName": "l_quantity",
            "expression": null
          },
          {
            "type": "doubleSum",
            "name": "l_extendedprice",
            "fieldName": "l_extendedprice",
            "expression": null
          },
          {
            "type": "doubleSum",
            "name": "l_discount",
            "fieldName": "l_discount",
            "expression": null
          },
          {
            "type": "doubleSum",
            "name": "l_tax",
            "fieldName": "l_tax",
            "expression": null
          }
        ],
        "granularitySpec": {
          "type": "uniform",
          "segmentGranularity": "YEAR",
          "queryGranularity": {
            "type": "none"
          },
          "rollup": true,
          "intervals": [
            "1980-01-01T00:00:00.000Z/2020-01-01T00:00:00.000Z"
          ]
        },
        "transformSpec": {
          "filter": null,
          "transforms": []
        }
      },
      "ioConfig": {
        "type": "index_parallel",
        "inputSource": {
          "type": "local",
          "baseDir": "/path/to/data/",
          "filter": "lineitem.tbl.5"
        },
        "inputFormat": {
          "type": "tsv",
          "delimiter": "|",
          "columns": [
            "l_orderkey",
            "l_partkey",
            "l_suppkey",
            "l_linenumber",
            "l_quantity",
            "l_extendedprice",
            "l_discount",
            "l_tax",
            "l_returnflag",
            "l_linestatus",
            "l_shipdate",
            "l_commitdate",
            "l_receiptdate",
            "l_shipinstruct",
            "l_shipmode",
            "l_comment"
          ]
        },
        "appendToExisting": false,
        "dropExisting": false
      },
      "tuningConfig": {
        "type": "index_parallel",
        "partitionsSpec": {
          "type": "dynamic"
        },
        "maxRowsInMemory": 1000000,
        "maxTotalRows": 20000000,
        "numShards": null,
        "indexSpec": {
          "bitmap": {
            "type": "roaring"
          },
          "dimensionCompression": "lz4",
          "metricCompression": "lz4",
          "longEncoding": "longs"
        },
        "indexSpecForIntermediatePersists": {
          "bitmap": {
            "type": "roaring"
          },
          "dimensionCompression": "lz4",
          "metricCompression": "lz4",
          "longEncoding": "longs"
        },
        "maxPendingPersists": 0,
        "reportParseExceptions": false,
        "pushTimeout": 0,
        "segmentWriteOutMediumFactory": null,
        "maxNumConcurrentSubTasks": 4,
        "maxRetry": 3,
        "taskStatusCheckPeriodMs": 1000,
        "chatHandlerTimeout": "PT10S",
        "chatHandlerNumRetries": 5,
        "logParseExceptions": false,
        "maxParseExceptions": 2147483647,
        "maxSavedParseExceptions": 0,
        "forceGuaranteedRollup": false
      }
    }
  },
  "currentStatus": {
    "id": "index_sub_lineitem_2018-04-20T22:16:29.922Z",
    "type": "index_sub",
    "createdTime": "2018-04-20T22:16:29.925Z",
    "queueInsertionTime": "2018-04-20T22:16:29.929Z",
    "statusCode": "RUNNING",
    "duration": -1,
    "location": {
      "host": null,
      "port": -1,
      "tlsPort": -1
    },
    "dataSource": "lineitem",
    "errorMsg": null
  },
  "taskHistory": []
}
```
</details>

`http://{PEON_IP}:{PEON_PORT}/druid/worker/v1/chat/{SUPERVISOR_TASK_ID}/subtaskspec/{SUB_TASK_SPEC_ID}/history`  
Returns the task attempt history of the worker task spec of the given id, or HTTP 404 Not Found error if the supervisor task is running in the sequential mode.

## Segment pushing modes
While ingesting data using the parallel task indexing, Druid creates segments from the input data and pushes them. For segment pushing,
the parallel task index supports the following segment pushing modes based upon your type of [rollup](./rollup.md):

- Bulk pushing mode: Used for perfect rollup. Druid pushes every segment at the very end of the index task. Until then, Druid stores created segments in memory and local storage of the service running the index task. To enable bulk pushing mode, set `forceGuaranteedRollup` to `true` in your tuning config. You cannot use bulk pushing with `appendToExisting` in your IOConfig.
- Incremental pushing mode: Used for best-effort rollup. Druid pushes segments are incrementally during the course of the indexing task. The index task collects data and stores created segments in the memory and disks of the services running the task until the total number of collected rows exceeds `maxTotalRows`. At that point the index task immediately pushes all segments created up until that moment, cleans up pushed segments, and continues to ingest the remaining data.

## Capacity planning

The Supervisor task can create up to `maxNumConcurrentSubTasks` worker tasks no matter how many task slots are currently available.
As a result, total number of tasks which can be run at the same time is `(maxNumConcurrentSubTasks + 1)` (including the Supervisor task).
Please note that this can be even larger than total number of task slots (sum of the capacity of all workers).
If `maxNumConcurrentSubTasks` is larger than `n (available task slots)`, then
`maxNumConcurrentSubTasks` tasks are created by the supervisor task, but only `n` tasks would be started.
Others will wait in the pending state until any running task is finished.

If you are using the Parallel Index Task with stream ingestion together,
we would recommend to limit the max capacity for batch ingestion to prevent
stream ingestion from being blocked by batch ingestion. Suppose you have
`t` Parallel Index Tasks to run at the same time, but want to limit
the max number of tasks for batch ingestion to `b`. Then, (sum of `maxNumConcurrentSubTasks`
of all Parallel Index Tasks + `t` (for supervisor tasks)) must be smaller than `b`.

If you have some tasks of a higher priority than others, you may set their
`maxNumConcurrentSubTasks` to a higher value than lower priority tasks.
This may help the higher priority tasks to finish earlier than lower priority tasks
by assigning more task slots to them.

## Splittable input sources
Use the `inputSource` object to define the location where your index can read data. Only the native parallel task and simple task support the input source.

For details on available input sources see:
- [S3 input source](./input-sources.md#s3-input-source) (`s3`) reads data from AWS S3 storage.
- [Google Cloud Storage input source](./input-sources.md#google-cloud-storage-input-source) (`gs`) reads data from Google Cloud Storage.
- [Azure input source](./input-sources.md#azure-input-source) (`azure`) reads data from Azure Blob Storage and Azure Data Lake.
- [HDFS input source](./input-sources.md#hdfs-input-source) (`hdfs`) reads data from HDFS storage.
- [HTTP input Source](./input-sources.md#http-input-source) (`http`) reads data from HTTP servers.
- [Inline input Source](./input-sources.md#inline-input-source) reads data you paste into the web console.
- [Local input Source](./input-sources.md#local-input-source) (`local`) reads data from local storage.
- [Druid input Source](./input-sources.md#druid-input-source) (`druid`) reads data from a Druid datasource.
- [SQL input Source](./input-sources.md#sql-input-source) (`sql`) reads data from a RDBMS source.

For information on how to combine input sources, see [Combining input source](./input-sources.md#combining-input-source).

### `segmentWriteOutMediumFactory`

|Property|Type|Description|Required|
|-----|----|-----------|--------|
|`type`|String|See [Additional Peon Configuration: SegmentWriteOutMediumFactory](../configuration/index.md#segmentwriteoutmediumfactory) for explanation and available options.|yes|
