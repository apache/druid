---
id: native-batch
title: "Native batch ingestion"
sidebar_label: "Native batch"
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


Apache Druid currently has two types of native batch indexing tasks, `index_parallel` which can run
multiple tasks in parallel, and `index` which will run a single indexing task. Please refer to our
[Hadoop-based vs. native batch comparison table](index.md#batch) for comparisons between Hadoop-based, native batch
(simple), and native batch (parallel) ingestion.

To run either kind of native batch indexing task, write an ingestion spec as specified below. Then POST it to the
[`/druid/indexer/v1/task`](../operations/api-reference.md#tasks) endpoint on the Overlord, or use the
`bin/post-index-task` script included with Druid.

## Tutorial

This page contains reference documentation for native batch ingestion.
For a walk-through instead, check out the [Loading a file](../tutorials/tutorial-batch.md) tutorial, which
demonstrates the "simple" (single-task) mode.

## Parallel task

The Parallel task (type `index_parallel`) is a task for parallel batch indexing. This task only uses Druid's resource and
doesn't depend on other external systems like Hadoop. The `index_parallel` task is a supervisor task that orchestrates
the whole indexing process. The supervisor task splits the input data and creates worker tasks to process those splits.
The created worker tasks are issued to the Overlord so that they can be scheduled and run on MiddleManagers or Indexers.
Once a worker task successfully processes the assigned input split, it reports the generated segment list to the supervisor task.
The supervisor task periodically checks the status of worker tasks. If one of them fails, it retries the failed task
until the number of retries reaches the configured limit. If all worker tasks succeed, it publishes the reported segments at once and finalizes ingestion.

The detailed behavior of the Parallel task is different depending on the [`partitionsSpec`](#partitionsspec).
See each `partitionsSpec` for more details.

To use this task, the [`inputSource`](#input-sources) in the `ioConfig` should be _splittable_ and `maxNumConcurrentSubTasks` should be set to larger than 1 in the `tuningConfig`.
Otherwise, this task runs sequentially; the `index_parallel` task reads each input file one by one and creates segments by itself.
The supported splittable input formats for now are:

- [`s3`](#s3-input-source) reads data from AWS S3 storage.
- [`gs`](#google-cloud-storage-input-source) reads data from Google Cloud Storage.
- [`azure`](#azure-input-source) reads data from Azure Blob Storage.
- [`hdfs`](#hdfs-input-source) reads data from HDFS storage.
- [`http`](#http-input-source) reads data from HTTP servers.
- [`local`](#local-input-source) reads data from local storage.
- [`druid`](#druid-input-source) reads data from a Druid datasource.
- [`sql`](#sql-input-source) reads data from a RDBMS source.

Some other cloud storage types are supported with the legacy [`firehose`](#firehoses-deprecated).
The below `firehose` types are also splittable. Note that only text formats are supported
with the `firehose`.

### Compression formats supported
The supported compression formats for native batch ingestion are `bz2`, `gz`, `xz`, `zip`, `sz` (Snappy), and `zst` (ZSTD).

- [`static-cloudfiles`](../development/extensions-contrib/cloudfiles.md#firehose)

You may want to consider the below things:

- You may want to control the amount of input data each worker task processes. This can be
  controlled using different configurations depending on the phase in parallel ingestion (see [`partitionsSpec`](#partitionsspec) for more details).
  For the tasks that read data from the `inputSource`, you can set the [Split hint spec](#split-hint-spec) in the `tuningConfig`.
  For the tasks that merge shuffled segments, you can set the `totalNumMergeTasks` in the `tuningConfig`.
- The number of concurrent worker tasks in parallel ingestion is determined by `maxNumConcurrentSubTasks` in the `tuningConfig`.
  The supervisor task checks the number of current running worker tasks and creates more if it's smaller than `maxNumConcurrentSubTasks`
  no matter how many task slots are currently available.
  This may affect to other ingestion performance. See the below [Capacity Planning](#capacity-planning) section for more details.
- By default, batch ingestion replaces all data (in your `granularitySpec`'s intervals) in any segment that it writes to.
  If you'd like to add to the segment instead, set the `appendToExisting` flag in the `ioConfig`. Note that it only replaces
  data in segments where it actively adds data: if there are segments in your `granularitySpec`'s intervals that have
  no data written by this task, they will be left alone. If any existing segments partially overlap with the
  `granularitySpec`'s intervals, the portion of those segments outside the new segments' intervals will still be visible.

### Task syntax

A sample task is shown below:

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
          "page",
          "language",
          "user",
          "unpatrolled",
          "newPage",
          "robot",
          "anonymous",
          "namespace",
          "continent",
          "country",
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
          "intervals" : [ "2013-08-31/2013-09-02" ]
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
        "maxNumConcurrentSubTasks": 2
    }
  }
}
```

|property|description|required?|
|--------|-----------|---------|
|type|The task type, this should always be `index_parallel`.|yes|
|id|The task ID. If this is not explicitly specified, Druid generates the task ID using task type, data source name, interval, and date-time stamp. |no|
|spec|The ingestion spec including the data schema, IOConfig, and TuningConfig. See below for more details. |yes|
|context|Context containing various task configuration parameters. See below for more details.|no|

### `dataSchema`

This field is required.

See [Ingestion Spec DataSchema](../ingestion/index.md#dataschema)

If you specify `intervals` explicitly in your dataSchema's `granularitySpec`, batch ingestion will lock the full intervals
specified when it starts up, and you will learn quickly if the specified interval overlaps with locks held by other
tasks (e.g., Kafka ingestion). Otherwise, batch ingestion will lock each interval as it is discovered, so you may only
learn that the task overlaps with a higher-priority task later in ingestion.  If you specify `intervals` explicitly, any
rows outside the specified intervals will be thrown away. We recommend setting `intervals` explicitly if you know the
time range of the data so that locking failure happens faster, and so that you don't accidentally replace data outside
that range if there's some stray data with unexpected timestamps.

### `ioConfig`

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|The task type, this should always be `index_parallel`.|none|yes|
|inputFormat|[`inputFormat`](./data-formats.md#input-format) to specify how to parse input data.|none|yes|
|appendToExisting|Creates segments as additional shards of the latest version, effectively appending to the segment set instead of replacing it. The current limitation is that you can append to any datasources regardless of their original partitioning scheme, but the appended segments should be partitioned using the `dynamic` partitionsSpec.|false|no|

### `tuningConfig`

The tuningConfig is optional and default parameters will be used if no tuningConfig is specified. See below for more details.

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|The task type, this should always be `index_parallel`.|none|yes|
|maxRowsPerSegment|Deprecated. Use `partitionsSpec` instead. Used in sharding. Determines how many rows are in each segment.|5000000|no|
|maxRowsInMemory|Used in determining when intermediate persists to disk should occur. Normally user does not need to set this, but depending on the nature of data, if rows are short in terms of bytes, user may not want to store a million rows in memory and this value should be set.|1000000|no|
|maxBytesInMemory|Used in determining when intermediate persists to disk should occur. Normally this is computed internally and user does not need to set it. This value represents number of bytes to aggregate in heap memory before persisting. This is based on a rough estimate of memory usage and not actual usage. The maximum heap memory usage for indexing is maxBytesInMemory * (2 + maxPendingPersists). Note that `maxBytesInMemory` also includes heap usage of artifacts created from intermediary persists. This means that after every persist, the amount of `maxBytesInMemory` until next persist will decreases, and task will fail when the sum of bytes of all intermediary persisted artifacts exceeds `maxBytesInMemory`.|1/6 of max JVM memory|no|
|maxColumnsToMerge|A parameter that limits how many segments can be merged in a single phase when merging segments for publishing. This limit is imposed on the total number of columns present in a set of segments being merged. If the limit is exceeded, segment merging will occur in multiple phases. At least 2 segments will be merged in a single phase, regardless of this setting.|-1 (unlimited)|no|
|maxTotalRows|Deprecated. Use `partitionsSpec` instead. Total number of rows in segments waiting for being pushed. Used in determining when intermediate pushing should occur.|20000000|no|
|numShards|Deprecated. Use `partitionsSpec` instead. Directly specify the number of shards to create when using a `hashed` `partitionsSpec`. If this is specified and `intervals` is specified in the `granularitySpec`, the index task can skip the determine intervals/partitions pass through the data. `numShards` cannot be specified if `maxRowsPerSegment` is set.|null|no|
|splitHintSpec|Used to give a hint to control the amount of data that each first phase task reads. This hint could be ignored depending on the implementation of the input source. See [Split hint spec](#split-hint-spec) for more details.|size-based split hint spec|no|
|partitionsSpec|Defines how to partition data in each timeChunk, see [PartitionsSpec](#partitionsspec)|`dynamic` if `forceGuaranteedRollup` = false, `hashed` or `single_dim` if `forceGuaranteedRollup` = true|no|
|indexSpec|Defines segment storage format options to be used at indexing time, see [IndexSpec](index.md#indexspec)|null|no|
|indexSpecForIntermediatePersists|Defines segment storage format options to be used at indexing time for intermediate persisted temporary segments. this can be used to disable dimension/metric compression on intermediate segments to reduce memory required for final merging. however, disabling compression on intermediate segments might increase page cache use while they are used before getting merged into final segment published, see [IndexSpec](index.md#indexspec) for possible values.|same as indexSpec|no|
|maxPendingPersists|Maximum number of persists that can be pending but not started. If this limit would be exceeded by a new intermediate persist, ingestion will block until the currently-running persist finishes. Maximum heap memory usage for indexing scales with maxRowsInMemory * (2 + maxPendingPersists).|0 (meaning one persist can be running concurrently with ingestion, and none can be queued up)|no|
|forceGuaranteedRollup|Forces guaranteeing the [perfect rollup](../ingestion/index.md#rollup). The perfect rollup optimizes the total size of generated segments and querying time while indexing time will be increased. If this is set to true, `intervals` in `granularitySpec` must be set and `hashed` or `single_dim` must be used for `partitionsSpec`. This flag cannot be used with `appendToExisting` of IOConfig. For more details, see the below __Segment pushing modes__ section.|false|no|
|reportParseExceptions|If true, exceptions encountered during parsing will be thrown and will halt ingestion; if false, unparseable rows and fields will be skipped.|false|no|
|pushTimeout|Milliseconds to wait for pushing segments. It must be >= 0, where 0 means to wait forever.|0|no|
|segmentWriteOutMediumFactory|Segment write-out medium to use when creating segments. See [SegmentWriteOutMediumFactory](#segmentwriteoutmediumfactory).|Not specified, the value from `druid.peon.defaultSegmentWriteOutMediumFactory.type` is used|no|
|maxNumConcurrentSubTasks|Maximum number of worker tasks which can be run in parallel at the same time. The supervisor task would spawn worker tasks up to `maxNumConcurrentSubTasks` regardless of the current available task slots. If this value is set to 1, the supervisor task processes data ingestion on its own instead of spawning worker tasks. If this value is set to too large, too many worker tasks can be created which might block other ingestion. Check [Capacity Planning](#capacity-planning) for more details.|1|no|
|maxRetry|Maximum number of retries on task failures.|3|no|
|maxNumSegmentsToMerge|Max limit for the number of segments that a single task can merge at the same time in the second phase. Used only `forceGuaranteedRollup` is set.|100|no|
|totalNumMergeTasks|Total number of tasks to merge segments in the merge phase when `partitionsSpec` is set to `hashed` or `single_dim`.|10|no|
|taskStatusCheckPeriodMs|Polling period in milliseconds to check running task statuses.|1000|no|
|chatHandlerTimeout|Timeout for reporting the pushed segments in worker tasks.|PT10S|no|
|chatHandlerNumRetries|Retries for reporting the pushed segments in worker tasks.|5|no|

### Split Hint Spec

The split hint spec is used to give a hint when the supervisor task creates input splits.
Note that each worker task processes a single input split. You can control the amount of data each worker task will read during the first phase.

#### Size-based Split Hint Spec

The size-based split hint spec is respected by all splittable input sources except for the HTTP input source and SQL input source.

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|This should always be `maxSize`.|none|yes|
|maxSplitSize|Maximum number of bytes of input files to process in a single subtask. If a single file is larger than this number, it will be processed by itself in a single subtask (Files are never split across tasks yet). Note that one subtask will not process more files than `maxNumFiles` even when their total size is smaller than `maxSplitSize`. [Human-readable format](../configuration/human-readable-byte.md) is supported.|1GiB|no|
|maxNumFiles|Maximum number of input files to process in a single subtask. This limit is to avoid task failures when the ingestion spec is too long. There are two known limits on the max size of serialized ingestion spec, i.e., the max ZNode size in ZooKeeper (`jute.maxbuffer`) and the max packet size in MySQL (`max_allowed_packet`). These can make ingestion tasks fail if the serialized ingestion spec size hits one of them. Note that one subtask will not process more data than `maxSplitSize` even when the total number of files is smaller than `maxNumFiles`.|1000|no|

#### Segments Split Hint Spec

The segments split hint spec is used only for [`DruidInputSource`](#druid-input-source) (and legacy [`IngestSegmentFirehose`](#ingestsegmentfirehose)).

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|This should always be `segments`.|none|yes|
|maxInputSegmentBytesPerTask|Maximum number of bytes of input segments to process in a single subtask. If a single segment is larger than this number, it will be processed by itself in a single subtask (input segments are never split across tasks). Note that one subtask will not process more segments than `maxNumSegments` even when their total size is smaller than `maxInputSegmentBytesPerTask`. [Human-readable format](../configuration/human-readable-byte.md) is supported.|1GiB|no|
|maxNumSegments|Maximum number of input segments to process in a single subtask. This limit is to avoid task failures when the ingestion spec is too long. There are two known limits on the max size of serialized ingestion spec, i.e., the max ZNode size in ZooKeeper (`jute.maxbuffer`) and the max packet size in MySQL (`max_allowed_packet`). These can make ingestion tasks fail if the serialized ingestion spec size hits one of them. Note that one subtask will not process more data than `maxInputSegmentBytesPerTask` even when the total number of segments is smaller than `maxNumSegments`.|1000|no|

### `partitionsSpec`

PartitionsSpec is used to describe the secondary partitioning method.
You should use different partitionsSpec depending on the [rollup mode](../ingestion/index.md#rollup) you want.
For perfect rollup, you should use either `hashed` (partitioning based on the hash of dimensions in each row) or
`single_dim` (based on ranges of a single dimension). For best-effort rollup, you should use `dynamic`.

The three `partitionsSpec` types have different characteristics.

| PartitionsSpec | Ingestion speed | Partitioning method | Supported rollup mode | Secondary partition pruning at query time |
|----------------|-----------------|---------------------|-----------------------|-------------------------------|
| `dynamic` | Fastest  | Partitioning based on number of rows in segment. | Best-effort rollup | N/A |
| `hashed`  | Moderate | Partitioning based on the hash value of partition dimensions. This partitioning may reduce your datasource size and query latency by improving data locality. See [Partitioning](./index.md#partitioning) for more details. | Perfect rollup | The broker can use the partition information to prune segments early to speed up queries. Since the broker knows how to hash `partitionDimensions` values to locate a segment, given a query including a filter on all the `partitionDimensions`, the broker can pick up only the segments holding the rows satisfying the filter on `partitionDimensions` for query processing.<br/><br/>Note that `partitionDimensions` must be set at ingestion time to enable secondary partition pruning at query time.|
| `single_dim` | Slowest | Range partitioning based on the value of the partition dimension. Segment sizes may be skewed depending on the partition key distribution. This may reduce your datasource size and query latency by improving data locality. See [Partitioning](./index.md#partitioning) for more details. | Perfect rollup | The broker can use the partition information to prune segments early to speed up queries. Since the broker knows the range of `partitionDimension` values in each segment, given a query including a filter on the `partitionDimension`, the broker can pick up only the segments holding the rows satisfying the filter on `partitionDimension` for query processing. |

The recommended use case for each partitionsSpec is:
- If your data has a uniformly distributed column which is frequently used in your queries,
consider using `single_dim` partitionsSpec to maximize the performance of most of your queries.
- If your data doesn't have a uniformly distributed column, but is expected to have a [high rollup ratio](./index.md#maximizing-rollup-ratio)
when you roll up with some dimensions, consider using `hashed` partitionsSpec.
It could reduce the size of datasource and query latency by improving data locality.
- If the above two scenarios are not the case or you don't need to roll up your datasource,
consider using `dynamic` partitionsSpec. 

#### Dynamic partitioning

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|This should always be `dynamic`|none|yes|
|maxRowsPerSegment|Used in sharding. Determines how many rows are in each segment.|5000000|no|
|maxTotalRows|Total number of rows across all segments waiting for being pushed. Used in determining when intermediate segment push should occur.|20000000|no|

With the Dynamic partitioning, the parallel index task runs in a single phase:
it will spawn multiple worker tasks (type `single_phase_sub_task`), each of which creates segments.
How the worker task creates segments is:

- The task creates a new segment whenever the number of rows in the current segment exceeds
  `maxRowsPerSegment`.
- Once the total number of rows in all segments across all time chunks reaches to `maxTotalRows`,
  the task pushes all segments created so far to the deep storage and creates new ones.

#### Hash-based partitioning

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|This should always be `hashed`|none|yes|
|numShards|Directly specify the number of shards to create. If this is specified and `intervals` is specified in the `granularitySpec`, the index task can skip the determine intervals/partitions pass through the data. This property and `targetRowsPerSegment` cannot both be set.|none|no|
|targetRowsPerSegment|A target row count for each partition. If `numShards` is left unspecified, the Parallel task will determine a partition count automatically such that each partition has a row count close to the target, assuming evenly distributed keys in the input data. A target per-segment row count of 5 million is used if both `numShards` and `targetRowsPerSegment` are null. |null (or 5,000,000 if both `numShards` and `targetRowsPerSegment` are null)|no|
|partitionDimensions|The dimensions to partition on. Leave blank to select all dimensions.|null|no|
|partitionFunction|A function to compute hash of partition dimensions. See [Hash partition function](#hash-partition-function)|`murmur3_32_abs`|no|

The Parallel task with hash-based partitioning is similar to [MapReduce](https://en.wikipedia.org/wiki/MapReduce).
The task runs in up to 3 phases: `partial dimension cardinality`, `partial segment generation` and `partial segment merge`.
- The `partial dimension cardinality` phase is an optional phase that only runs if `numShards` is not specified.
The Parallel task splits the input data and assigns them to worker tasks based on the split hint spec.
Each worker task (type `partial_dimension_cardinality`) gathers estimates of partitioning dimensions cardinality for
each time chunk. The Parallel task will aggregate these estimates from the worker tasks and determine the highest
cardinality across all of the time chunks in the input data, dividing this cardinality by `targetRowsPerSegment` to
automatically determine `numShards`.
- In the `partial segment generation` phase, just like the Map phase in MapReduce,
the Parallel task splits the input data based on the split hint spec
and assigns each split to a worker task. Each worker task (type `partial_index_generate`) reads the assigned split,
and partitions rows by the time chunk from `segmentGranularity` (primary partition key) in the `granularitySpec`
and then by the hash value of `partitionDimensions` (secondary partition key) in the `partitionsSpec`.
The partitioned data is stored in local storage of 
the [middleManager](../design/middlemanager.md) or the [indexer](../design/indexer.md).
- The `partial segment merge` phase is similar to the Reduce phase in MapReduce.
The Parallel task spawns a new set of worker tasks (type `partial_index_generic_merge`) to merge the partitioned data
created in the previous phase. Here, the partitioned data is shuffled based on
the time chunk and the hash value of `partitionDimensions` to be merged; each worker task reads the data
falling in the same time chunk and the same hash value from multiple MiddleManager/Indexer processes and merges
them to create the final segments. Finally, they push the final segments to the deep storage at once.

##### Hash partition function

In hash partitioning, the partition function is used to compute hash of partition dimensions. The partition dimension
values are first serialized into a byte array as a whole, and then the partition function is applied to compute hash of
the byte array.
Druid currently supports only one partition function.

|name|description|
|----|-----------|
|`murmur3_32_abs`|Applies an absolute value function to the result of [`murmur3_32`](https://guava.dev/releases/16.0/api/docs/com/google/common/hash/Hashing.html#murmur3_32()).|

#### Single-dimension range partitioning

> Single dimension range partitioning is currently not supported in the sequential mode of the Parallel task.
The Parallel task will use one subtask when you set `maxNumConcurrentSubTasks` to 1.

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|This should always be `single_dim`|none|yes|
|partitionDimension|The dimension to partition on. Only rows with a single dimension value are allowed.|none|yes|
|targetRowsPerSegment|Target number of rows to include in a partition, should be a number that targets segments of 500MB\~1GB.|none|either this or `maxRowsPerSegment`|
|maxRowsPerSegment|Soft max for the number of rows to include in a partition.|none|either this or `targetRowsPerSegment`|
|assumeGrouped|Assume that input data has already been grouped on time and dimensions. Ingestion will run faster, but may choose sub-optimal partitions if this assumption is violated.|false|no|

With `single-dim` partitioning, the Parallel task runs in 3 phases,
i.e., `partial dimension distribution`, `partial segment generation`, and `partial segment merge`.
The first phase is to collect some statistics to find
the best partitioning and the other 2 phases are to create partial segments
and to merge them, respectively, as in hash-based partitioning.
- In the `partial dimension distribution` phase, the Parallel task splits the input data and
assigns them to worker tasks based on the split hint spec. Each worker task (type `partial_dimension_distribution`) reads
the assigned split and builds a histogram for `partitionDimension`.
The Parallel task collects those histograms from worker tasks and finds
the best range partitioning based on `partitionDimension` to evenly
distribute rows across partitions. Note that either `targetRowsPerSegment`
or `maxRowsPerSegment` will be used to find the best partitioning.
- In the `partial segment generation` phase, the Parallel task spawns new worker tasks (type `partial_range_index_generate`)
to create partitioned data. Each worker task reads a split created as in the previous phase,
partitions rows by the time chunk from the `segmentGranularity` (primary partition key) in the `granularitySpec`
and then by the range partitioning found in the previous phase.
The partitioned data is stored in local storage of
the [middleManager](../design/middlemanager.md) or the [indexer](../design/indexer.md).
- In the `partial segment merge` phase, the parallel index task spawns a new set of worker tasks (type `partial_index_generic_merge`) to merge the partitioned
data created in the previous phase. Here, the partitioned data is shuffled based on
the time chunk and the value of `partitionDimension`; each worker task reads the segments
falling in the same partition of the same range from multiple MiddleManager/Indexer processes and merges
them to create the final segments. Finally, they push the final segments to the deep storage.

> Because the task with single-dimension range partitioning makes two passes over the input
> in `partial dimension distribution` and `partial segment generation` phases,
> the task may fail if the input changes in between the two passes.

### HTTP status endpoints

The supervisor task provides some HTTP endpoints to get running status.

* `http://{PEON_IP}:{PEON_PORT}/druid/worker/v1/chat/{SUPERVISOR_TASK_ID}/mode`

Returns 'parallel' if the indexing task is running in parallel. Otherwise, it returns 'sequential'.

* `http://{PEON_IP}:{PEON_PORT}/druid/worker/v1/chat/{SUPERVISOR_TASK_ID}/phase`

Returns the name of the current phase if the task running in the parallel mode.

* `http://{PEON_IP}:{PEON_PORT}/druid/worker/v1/chat/{SUPERVISOR_TASK_ID}/progress`

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

* `http://{PEON_IP}:{PEON_PORT}/druid/worker/v1/chat/{SUPERVISOR_TASK_ID}/subtasks/running`

Returns the task IDs of running worker tasks, or an empty list if the supervisor task is running in the sequential mode.

* `http://{PEON_IP}:{PEON_PORT}/druid/worker/v1/chat/{SUPERVISOR_TASK_ID}/subtaskspecs`

Returns all worker task specs, or an empty list if the supervisor task is running in the sequential mode.

* `http://{PEON_IP}:{PEON_PORT}/druid/worker/v1/chat/{SUPERVISOR_TASK_ID}/subtaskspecs/running`

Returns running worker task specs, or an empty list if the supervisor task is running in the sequential mode.

* `http://{PEON_IP}:{PEON_PORT}/druid/worker/v1/chat/{SUPERVISOR_TASK_ID}/subtaskspecs/complete`

Returns complete worker task specs, or an empty list if the supervisor task is running in the sequential mode.

* `http://{PEON_IP}:{PEON_PORT}/druid/worker/v1/chat/{SUPERVISOR_TASK_ID}/subtaskspec/{SUB_TASK_SPEC_ID}`

Returns the worker task spec of the given id, or HTTP 404 Not Found error if the supervisor task is running in the sequential mode.

* `http://{PEON_IP}:{PEON_PORT}/druid/worker/v1/chat/{SUPERVISOR_TASK_ID}/subtaskspec/{SUB_TASK_SPEC_ID}/state`

Returns the state of the worker task spec of the given id, or HTTP 404 Not Found error if the supervisor task is running in the sequential mode.
The returned result contains the worker task spec, a current task status if exists, and task attempt history.

An example of the result is

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
          "format": "tsv",
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
        "appendToExisting": false
      },
      "tuningConfig": {
        "type": "index_parallel",
        "maxRowsPerSegment": 5000000,
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
        "forceGuaranteedRollup": false,
        "buildV9Directly": true
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

* `http://{PEON_IP}:{PEON_PORT}/druid/worker/v1/chat/{SUPERVISOR_TASK_ID}/subtaskspec/{SUB_TASK_SPEC_ID}/history`

Returns the task attempt history of the worker task spec of the given id, or HTTP 404 Not Found error if the supervisor task is running in the sequential mode.

### Capacity planning

The supervisor task can create up to `maxNumConcurrentSubTasks` worker tasks no matter how many task slots are currently available.
As a result, total number of tasks which can be run at the same time is `(maxNumConcurrentSubTasks + 1)` (including the supervisor task).
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

## Simple task

The simple task (type `index`) is designed to be used for smaller data sets. The task executes within the indexing service.

### Task syntax

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
        "dimensions": ["page","language","user","unpatrolled","newPage","robot","anonymous","namespace","continent","country","region","city"],
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
      "maxRowsPerSegment" : 5000000,
      "maxRowsInMemory" : 1000000
    }
  }
}
```

|property|description|required?|
|--------|-----------|---------|
|type|The task type, this should always be `index`.|yes|
|id|The task ID. If this is not explicitly specified, Druid generates the task ID using task type, data source name, interval, and date-time stamp. |no|
|spec|The ingestion spec including the data schema, IOConfig, and TuningConfig. See below for more details. |yes|
|context|Context containing various task configuration parameters. See below for more details.|no|

### `dataSchema`

This field is required.

See the [`dataSchema`](../ingestion/index.md#dataschema) section of the ingestion docs for details.

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
|appendToExisting|Creates segments as additional shards of the latest version, effectively appending to the segment set instead of replacing it. The current limitation is that you can append to any datasources regardless of their original partitioning scheme, but the appended segments should be partitioned using the `dynamic` partitionsSpec.|false|no|

### `tuningConfig`

The tuningConfig is optional and default parameters will be used if no tuningConfig is specified. See below for more details.

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|The task type, this should always be "index".|none|yes|
|maxRowsPerSegment|Deprecated. Use `partitionsSpec` instead. Used in sharding. Determines how many rows are in each segment.|5000000|no|
|maxRowsInMemory|Used in determining when intermediate persists to disk should occur. Normally user does not need to set this, but depending on the nature of data, if rows are short in terms of bytes, user may not want to store a million rows in memory and this value should be set.|1000000|no|
|maxBytesInMemory|Used in determining when intermediate persists to disk should occur. Normally this is computed internally and user does not need to set it. This value represents number of bytes to aggregate in heap memory before persisting. This is based on a rough estimate of memory usage and not actual usage. The maximum heap memory usage for indexing is maxBytesInMemory * (2 + maxPendingPersists). Note that `maxBytesInMemory` also includes heap usage of artifacts created from intermediary persists. This means that after every persist, the amount of `maxBytesInMemory` until next persist will decreases, and task will fail when the sum of bytes of all intermediary persisted artifacts exceeds `maxBytesInMemory`.|1/6 of max JVM memory|no|
|maxTotalRows|Deprecated. Use `partitionsSpec` instead. Total number of rows in segments waiting for being pushed. Used in determining when intermediate pushing should occur.|20000000|no|
|numShards|Deprecated. Use `partitionsSpec` instead. Directly specify the number of shards to create. If this is specified and `intervals` is specified in the `granularitySpec`, the index task can skip the determine intervals/partitions pass through the data. `numShards` cannot be specified if `maxRowsPerSegment` is set.|null|no|
|partitionDimensions|Deprecated. Use `partitionsSpec` instead. The dimensions to partition on. Leave blank to select all dimensions. Only used with `forceGuaranteedRollup` = true, will be ignored otherwise.|null|no|
|partitionsSpec|Defines how to partition data in each timeChunk, see [PartitionsSpec](#partitionsspec)|`dynamic` if `forceGuaranteedRollup` = false, `hashed` if `forceGuaranteedRollup` = true|no|
|indexSpec|Defines segment storage format options to be used at indexing time, see [IndexSpec](index.md#indexspec)|null|no|
|indexSpecForIntermediatePersists|Defines segment storage format options to be used at indexing time for intermediate persisted temporary segments. this can be used to disable dimension/metric compression on intermediate segments to reduce memory required for final merging. however, disabling compression on intermediate segments might increase page cache use while they are used before getting merged into final segment published, see [IndexSpec](index.md#indexspec) for possible values.|same as indexSpec|no|
|maxPendingPersists|Maximum number of persists that can be pending but not started. If this limit would be exceeded by a new intermediate persist, ingestion will block until the currently-running persist finishes. Maximum heap memory usage for indexing scales with maxRowsInMemory * (2 + maxPendingPersists).|0 (meaning one persist can be running concurrently with ingestion, and none can be queued up)|no|
|forceGuaranteedRollup|Forces guaranteeing the [perfect rollup](../ingestion/index.md#rollup). The perfect rollup optimizes the total size of generated segments and querying time while indexing time will be increased. If this is set to true, the index task will read the entire input data twice: one for finding the optimal number of partitions per time chunk and one for generating segments. Note that the result segments would be hash-partitioned. This flag cannot be used with `appendToExisting` of IOConfig. For more details, see the below __Segment pushing modes__ section.|false|no|
|reportParseExceptions|DEPRECATED. If true, exceptions encountered during parsing will be thrown and will halt ingestion; if false, unparseable rows and fields will be skipped. Setting `reportParseExceptions` to true will override existing configurations for `maxParseExceptions` and `maxSavedParseExceptions`, setting `maxParseExceptions` to 0 and limiting `maxSavedParseExceptions` to no more than 1.|false|no|
|pushTimeout|Milliseconds to wait for pushing segments. It must be >= 0, where 0 means to wait forever.|0|no|
|segmentWriteOutMediumFactory|Segment write-out medium to use when creating segments. See [SegmentWriteOutMediumFactory](#segmentwriteoutmediumfactory).|Not specified, the value from `druid.peon.defaultSegmentWriteOutMediumFactory.type` is used|no|
|logParseExceptions|If true, log an error message when a parsing exception occurs, containing information about the row where the error occurred.|false|no|
|maxParseExceptions|The maximum number of parse exceptions that can occur before the task halts ingestion and fails. Overridden if `reportParseExceptions` is set.|unlimited|no|
|maxSavedParseExceptions|When a parse exception occurs, Druid can keep track of the most recent parse exceptions. "maxSavedParseExceptions" limits how many exception instances will be saved. These saved exceptions will be made available after the task finishes in the [task completion report](tasks.md#task-reports). Overridden if `reportParseExceptions` is set.|0|no|

### `partitionsSpec`

PartitionsSpec is to describe the secondary partitioning method.
You should use different partitionsSpec depending on the [rollup mode](../ingestion/index.md#rollup) you want.
For perfect rollup, you should use `hashed`.

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|This should always be `hashed`|none|yes|
|maxRowsPerSegment|Used in sharding. Determines how many rows are in each segment.|5000000|no|
|numShards|Directly specify the number of shards to create. If this is specified and `intervals` is specified in the `granularitySpec`, the index task can skip the determine intervals/partitions pass through the data. `numShards` cannot be specified if `maxRowsPerSegment` is set.|null|no|
|partitionDimensions|The dimensions to partition on. Leave blank to select all dimensions.|null|no|
|partitionFunction|A function to compute hash of partition dimensions. See [Hash partition function](#hash-partition-function)|`murmur3_32_abs`|no|

For best-effort rollup, you should use `dynamic`.

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|This should always be `dynamic`|none|yes|
|maxRowsPerSegment|Used in sharding. Determines how many rows are in each segment.|5000000|no|
|maxTotalRows|Total number of rows in segments waiting for being pushed.|20000000|no|

### `segmentWriteOutMediumFactory`

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|type|String|See [Additional Peon Configuration: SegmentWriteOutMediumFactory](../configuration/index.md#segmentwriteoutmediumfactory) for explanation and available options.|yes|

### Segment pushing modes

While ingesting data using the Index task, it creates segments from the input data and pushes them. For segment pushing,
the Index task supports two segment pushing modes, i.e., _bulk pushing mode_ and _incremental pushing mode_ for
[perfect rollup and best-effort rollup](../ingestion/index.md#rollup), respectively.

In the bulk pushing mode, every segment is pushed at the very end of the index task. Until then, created segments
are stored in the memory and local storage of the process running the index task. As a result, this mode might cause a
problem due to limited storage capacity, and is not recommended to use in production.

On the contrary, in the incremental pushing mode, segments are incrementally pushed, that is they can be pushed
in the middle of the index task. More precisely, the index task collects data and stores created segments in the memory
and disks of the process running that task until the total number of collected rows exceeds `maxTotalRows`. Once it exceeds,
the index task immediately pushes all segments created until that moment, cleans all pushed segments up, and
continues to ingest remaining data.

To enable bulk pushing mode, `forceGuaranteedRollup` should be set in the TuningConfig. Note that this option cannot
be used with `appendToExisting` of IOConfig.

## Input Sources

The input source is the place to define from where your index task reads data.
Only the native Parallel task and Simple task support the input source. 

### S3 Input Source

> You need to include the [`druid-s3-extensions`](../development/extensions-core/s3.md) as an extension to use the S3 input source. 

The S3 input source is to support reading objects directly from S3.
Objects can be specified either via a list of S3 URI strings or a list of
S3 location prefixes, which will attempt to list the contents and ingest
all objects contained in the locations. The S3 input source is splittable
and can be used by the [Parallel task](#parallel-task),
where each worker task of `index_parallel` will read one or multiple objects.

Sample specs:

```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "s3",
        "uris": ["s3://foo/bar/file.json", "s3://bar/foo/file2.json"]
      },
      "inputFormat": {
        "type": "json"
      },
      ...
    },
...
```

```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "s3",
        "prefixes": ["s3://foo/bar", "s3://bar/foo"]
      },
      "inputFormat": {
        "type": "json"
      },
      ...
    },
...
```


```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "s3",
        "objects": [
          { "bucket": "foo", "path": "bar/file1.json"},
          { "bucket": "bar", "path": "foo/file2.json"}
        ]
      },
      "inputFormat": {
        "type": "json"
      },
      ...
    },
...
```

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|This should be `s3`.|None|yes|
|uris|JSON array of URIs where S3 objects to be ingested are located.|None|`uris` or `prefixes` or `objects` must be set|
|prefixes|JSON array of URI prefixes for the locations of S3 objects to be ingested. Empty objects starting with one of the given prefixes will be skipped.|None|`uris` or `prefixes` or `objects` must be set|
|objects|JSON array of S3 Objects to be ingested.|None|`uris` or `prefixes` or `objects` must be set|
|properties|Properties Object for overriding the default S3 configuration. See below for more information.|None|No (defaults will be used if not given)

Note that the S3 input source will skip all empty objects only when `prefixes` is specified.

S3 Object:

|property|description|default|required?|
|--------|-----------|-------|---------|
|bucket|Name of the S3 bucket|None|yes|
|path|The path where data is located.|None|yes|

Properties Object:

|property|description|default|required?|
|--------|-----------|-------|---------|
|accessKeyId|The [Password Provider](../operations/password-provider.md) or plain text string of this S3 InputSource's access key|None|yes if secretAccessKey is given|
|secretAccessKey|The [Password Provider](../operations/password-provider.md) or plain text string of this S3 InputSource's secret key|None|yes if accessKeyId is given|

**Note :** *If accessKeyId and secretAccessKey are not given, the default [S3 credentials provider chain](../development/extensions-core/s3.md#s3-authentication-methods) is used.*

### Google Cloud Storage Input Source

> You need to include the [`druid-google-extensions`](../development/extensions-core/google.md) as an extension to use the Google Cloud Storage input source.

The Google Cloud Storage input source is to support reading objects directly
from Google Cloud Storage. Objects can be specified as list of Google
Cloud Storage URI strings. The Google Cloud Storage input source is splittable
and can be used by the [Parallel task](#parallel-task), where each worker task of `index_parallel` will read
one or multiple objects.

Sample specs:

```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "google",
        "uris": ["gs://foo/bar/file.json", "gs://bar/foo/file2.json"]
      },
      "inputFormat": {
        "type": "json"
      },
      ...
    },
...
```

```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "google",
        "prefixes": ["gs://foo/bar", "gs://bar/foo"]
      },
      "inputFormat": {
        "type": "json"
      },
      ...
    },
...
```


```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "google",
        "objects": [
          { "bucket": "foo", "path": "bar/file1.json"},
          { "bucket": "bar", "path": "foo/file2.json"}
        ]
      },
      "inputFormat": {
        "type": "json"
      },
      ...
    },
...
```

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|This should be `google`.|None|yes|
|uris|JSON array of URIs where Google Cloud Storage objects to be ingested are located.|None|`uris` or `prefixes` or `objects` must be set|
|prefixes|JSON array of URI prefixes for the locations of Google Cloud Storage objects to be ingested. Empty objects starting with one of the given prefixes will be skipped.|None|`uris` or `prefixes` or `objects` must be set|
|objects|JSON array of Google Cloud Storage objects to be ingested.|None|`uris` or `prefixes` or `objects` must be set|

Note that the Google Cloud Storage input source will skip all empty objects only when `prefixes` is specified.

Google Cloud Storage object:

|property|description|default|required?|
|--------|-----------|-------|---------|
|bucket|Name of the Google Cloud Storage bucket|None|yes|
|path|The path where data is located.|None|yes|

### Azure Input Source

> You need to include the [`druid-azure-extensions`](../development/extensions-core/azure.md) as an extension to use the Azure input source.

The Azure input source is to support reading objects directly from Azure Blob store. Objects can be
specified as list of Azure Blob store URI strings. The Azure input source is splittable and can be used
by the [Parallel task](#parallel-task), where each worker task of `index_parallel` will read
a single object.

Sample specs:

```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "azure",
        "uris": ["azure://container/prefix1/file.json", "azure://container/prefix2/file2.json"]
      },
      "inputFormat": {
        "type": "json"
      },
      ...
    },
...
```

```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "azure",
        "prefixes": ["azure://container/prefix1", "azure://container/prefix2"]
      },
      "inputFormat": {
        "type": "json"
      },
      ...
    },
...
```


```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "azure",
        "objects": [
          { "bucket": "container", "path": "prefix1/file1.json"},
          { "bucket": "container", "path": "prefix2/file2.json"}
        ]
      },
      "inputFormat": {
        "type": "json"
      },
      ...
    },
...
```

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|This should be `azure`.|None|yes|
|uris|JSON array of URIs where Azure Blob objects to be ingested are located. Should be in form "azure://\<container>/\<path-to-file\>"|None|`uris` or `prefixes` or `objects` must be set|
|prefixes|JSON array of URI prefixes for the locations of Azure Blob objects to be ingested. Should be in the form "azure://\<container>/\<prefix\>". Empty objects starting with one of the given prefixes will be skipped.|None|`uris` or `prefixes` or `objects` must be set|
|objects|JSON array of Azure Blob objects to be ingested.|None|`uris` or `prefixes` or `objects` must be set|

Note that the Azure input source will skip all empty objects only when `prefixes` is specified.

Azure Blob object:

|property|description|default|required?|
|--------|-----------|-------|---------|
|bucket|Name of the Azure Blob Storage container|None|yes|
|path|The path where data is located.|None|yes|

### HDFS Input Source

> You need to include the [`druid-hdfs-storage`](../development/extensions-core/hdfs.md) as an extension to use the HDFS input source.

The HDFS input source is to support reading files directly
from HDFS storage. File paths can be specified as an HDFS URI string or a list
of HDFS URI strings. The HDFS input source is splittable and can be used by the [Parallel task](#parallel-task),
where each worker task of `index_parallel` will read one or multiple files.

Sample specs:

```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "hdfs",
        "paths": "hdfs://foo/bar/", "hdfs://bar/foo"
      },
      "inputFormat": {
        "type": "json"
      },
      ...
    },
...
```

```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "hdfs",
        "paths": ["hdfs://foo/bar", "hdfs://bar/foo"]
      },
      "inputFormat": {
        "type": "json"
      },
      ...
    },
...
```

```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "hdfs",
        "paths": "hdfs://foo/bar/file.json", "hdfs://bar/foo/file2.json"
      },
      "inputFormat": {
        "type": "json"
      },
      ...
    },
...
```

```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "hdfs",
        "paths": ["hdfs://foo/bar/file.json", "hdfs://bar/foo/file2.json"]
      },
      "inputFormat": {
        "type": "json"
      },
      ...
    },
...
```

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|This should be `hdfs`.|None|yes|
|paths|HDFS paths. Can be either a JSON array or comma-separated string of paths. Wildcards like `*` are supported in these paths. Empty files located under one of the given paths will be skipped.|None|yes|

You can also ingest from cloud storage using the HDFS input source.
However, if you want to read from AWS S3 or Google Cloud Storage, consider using
the [S3 input source](#s3-input-source) or the [Google Cloud Storage input source](#google-cloud-storage-input-source) instead.

### HTTP Input Source

The HTTP input source is to support reading files directly
from remote sites via HTTP.
The HTTP input source is _splittable_ and can be used by the [Parallel task](#parallel-task),
where each worker task of `index_parallel` will read only one file. This input source does not support Split Hint Spec.

Sample specs:

```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "http",
        "uris": ["http://example.com/uri1", "http://example2.com/uri2"]
      },
      "inputFormat": {
        "type": "json"
      },
      ...
    },
...
```

Example with authentication fields using the DefaultPassword provider (this requires the password to be in the ingestion spec):

```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "http",
        "uris": ["http://example.com/uri1", "http://example2.com/uri2"],
        "httpAuthenticationUsername": "username",
        "httpAuthenticationPassword": "password123"
      },
      "inputFormat": {
        "type": "json"
      },
      ...
    },
...
```

You can also use the other existing Druid PasswordProviders. Here is an example using the EnvironmentVariablePasswordProvider:

```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "http",
        "uris": ["http://example.com/uri1", "http://example2.com/uri2"],
        "httpAuthenticationUsername": "username",
        "httpAuthenticationPassword": {
          "type": "environment",
          "variable": "HTTP_INPUT_SOURCE_PW"
        }
      },
      "inputFormat": {
        "type": "json"
      },
      ...
    },
...
}
```

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|This should be `http`|None|yes|
|uris|URIs of the input files.|None|yes|
|httpAuthenticationUsername|Username to use for authentication with specified URIs. Can be optionally used if the URIs specified in the spec require a Basic Authentication Header.|None|no|
|httpAuthenticationPassword|PasswordProvider to use with specified URIs. Can be optionally used if the URIs specified in the spec require a Basic Authentication Header.|None|no|

### Inline Input Source

The Inline input source can be used to read the data inlined in its own spec.
It can be used for demos or for quickly testing out parsing and schema.

Sample spec:

```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "inline",
        "data": "0,values,formatted\n1,as,CSV"
      },
      "inputFormat": {
        "type": "csv"
      },
      ...
    },
...
```

|property|description|required?|
|--------|-----------|---------|
|type|This should be "inline".|yes|
|data|Inlined data to ingest.|yes|

### Local Input Source

The Local input source is to support reading files directly from local storage,
and is mainly intended for proof-of-concept testing.
The Local input source is _splittable_ and can be used by the [Parallel task](#parallel-task),
where each worker task of `index_parallel` will read one or multiple files.

Sample spec:

```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "local",
        "filter" : "*.csv",
        "baseDir": "/data/directory",
        "files": ["/bar/foo", "/foo/bar"]
      },
      "inputFormat": {
        "type": "csv"
      },
      ...
    },
...
```

|property|description|required?|
|--------|-----------|---------|
|type|This should be "local".|yes|
|filter|A wildcard filter for files. See [here](http://commons.apache.org/proper/commons-io/apidocs/org/apache/commons/io/filefilter/WildcardFileFilter) for more information.|yes if `baseDir` is specified|
|baseDir|Directory to search recursively for files to be ingested. Empty files under the `baseDir` will be skipped.|At least one of `baseDir` or `files` should be specified|
|files|File paths to ingest. Some files can be ignored to avoid ingesting duplicate files if they are located under the specified `baseDir`. Empty files will be skipped.|At least one of `baseDir` or `files` should be specified|

### Druid Input Source

The Druid input source is to support reading data directly from existing Druid segments,
potentially using a new schema and changing the name, dimensions, metrics, rollup, etc. of the segment.
The Druid input source is _splittable_ and can be used by the [Parallel task](#parallel-task).
This input source has a fixed input format for reading from Druid segments;
no `inputFormat` field needs to be specified in the ingestion spec when using this input source.

|property|description|required?|
|--------|-----------|---------|
|type|This should be "druid".|yes|
|dataSource|A String defining the Druid datasource to fetch rows from|yes|
|interval|A String representing an ISO-8601 interval, which defines the time range to fetch the data over.|yes|
|dimensions|A list of Strings containing the names of dimension columns to select from the Druid datasource. If the list is empty, no dimensions are returned. If null, all dimensions are returned. |no|
|metrics|The list of Strings containing the names of metric columns to select. If the list is empty, no metrics are returned. If null, all metrics are returned.|no|
|filter| See [Filters](../querying/filters.md). Only rows that match the filter, if specified, will be returned.|no|

A minimal example DruidInputSource spec is shown below:

```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "druid",
        "dataSource": "wikipedia",
        "interval": "2013-01-01/2013-01-02"
      }
      ...
    },
...
```

The spec above will read all existing dimension and metric columns from
the `wikipedia` datasource, including all rows with a timestamp (the `__time` column)
within the interval `2013-01-01/2013-01-02`.

A spec that applies a filter and reads a subset of the original datasource's columns is shown below.

```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "druid",
        "dataSource": "wikipedia",
        "interval": "2013-01-01/2013-01-02",
        "dimensions": [
          "page",
          "user"
        ],
        "metrics": [
          "added"
        ],
        "filter": {
          "type": "selector",
          "dimension": "page",
          "value": "Druid"
        }
      }
      ...
    },
...
```

This spec above will only return the `page`, `user` dimensions and `added` metric.
Only rows where `page` = `Druid` will be returned.

### SQL Input Source

The SQL input source is used to read data directly from RDBMS.
The SQL input source is _splittable_ and can be used by the [Parallel task](#parallel-task), where each worker task will read from one SQL query from the list of queries.
This input source does not support Split Hint Spec.
Since this input source has a fixed input format for reading events, no `inputFormat` field needs to be specified in the ingestion spec when using this input source.
Please refer to the Recommended practices section below before using this input source.

|property|description|required?|
|--------|-----------|---------|
|type|This should be "sql".|Yes|
|database|Specifies the database connection details. The database type corresponds to the extension that supplies the `connectorConfig` support and this extension must be loaded into Druid. For database types `mysql` and `postgresql`, the `connectorConfig` support is provided by [mysql-metadata-storage](../development/extensions-core/mysql.md) and [postgresql-metadata-storage](../development/extensions-core/postgresql.md) extensions respectively.|Yes|
|foldCase|Toggle case folding of database column names. This may be enabled in cases where the database returns case insensitive column names in query results.|No|
|sqls|List of SQL queries where each SQL query would retrieve the data to be indexed.|Yes|

An example SqlInputSource spec is shown below:

```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "sql",
        "database": {
            "type": "mysql",
            "connectorConfig": {
                "connectURI": "jdbc:mysql://host:port/schema",
                "user": "user",
                "password": "password"
            }
        },
        "sqls": ["SELECT * FROM table1 WHERE timestamp BETWEEN '2013-01-01 00:00:00' AND '2013-01-01 11:59:59'", "SELECT * FROM table2 WHERE timestamp BETWEEN '2013-01-01 00:00:00' AND '2013-01-01 11:59:59'"]
      }
    },
...
```

The spec above will read all events from two separate SQLs for the interval `2013-01-01/2013-01-02`.
Each of the SQL queries will be run in its own sub-task and thus for the above example, there would be two sub-tasks.

**Recommended practices**

Compared to the other native batch InputSources, SQL InputSource behaves differently in terms of reading the input data and so it would be helpful to consider the following points before using this InputSource in a production environment:

* During indexing, each sub-task would execute one of the SQL queries and the results are stored locally on disk. The sub-tasks then proceed to read the data from these local input files and generate segments. Presently, there isn’t any restriction on the size of the generated files and this would require the MiddleManagers or Indexers to have sufficient disk capacity based on the volume of data being indexed.

* Filtering the SQL queries based on the intervals specified in the `granularitySpec` can avoid unwanted data being retrieved and stored locally by the indexing sub-tasks. For example, if the `intervals` specified in the `granularitySpec` is `["2013-01-01/2013-01-02"]` and the SQL query is `SELECT * FROM table1`, `SqlInputSource` will read all the data for `table1` based on the query, even though only data between the intervals specified will be indexed into Druid.

* Pagination may be used on the SQL queries to ensure that each query pulls a similar amount of data, thereby improving the efficiency of the sub-tasks.

* Similar to file-based input formats, any updates to existing data will replace the data in segments specific to the intervals specified in the `granularitySpec`.


### Combining Input Source

The Combining input source is used to read data from multiple InputSources. This input source should be only used if all the delegate input sources are
 _splittable_ and can be used by the [Parallel task](#parallel-task). This input source will identify the splits from its delegates and each split will be processed by a worker task. Similar to other input sources, this input source supports a single `inputFormat`. Therefore, please note that delegate input sources requiring an `inputFormat` must have the same format for input data.

|property|description|required?|
|--------|-----------|---------|
|type|This should be "combining".|Yes|
|delegates|List of _splittable_ InputSources to read data from.|Yes|

Sample spec:


```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "combining",
        "delegates" : [
         {
          "type": "local",
          "filter" : "*.csv",
          "baseDir": "/data/directory",
          "files": ["/bar/foo", "/foo/bar"]
         },
         {
          "type": "druid",
          "dataSource": "wikipedia",
          "interval": "2013-01-01/2013-01-02"
         }
        ]
      },
      "inputFormat": {
        "type": "csv"
      },
      ...
    },
...
```


###

## Firehoses (Deprecated)

Firehoses are deprecated in 0.17.0. It's highly recommended to use the [Input source](#input-sources) instead.
There are several firehoses readily available in Druid, some are meant for examples, others can be used directly in a production environment.

### StaticS3Firehose

> You need to include the [`druid-s3-extensions`](../development/extensions-core/s3.md) as an extension to use the StaticS3Firehose.

This firehose ingests events from a predefined list of S3 objects.
This firehose is _splittable_ and can be used by the [Parallel task](#parallel-task).
Since each split represents an object in this firehose, each worker task of `index_parallel` will read an object.

Sample spec:

```json
"firehose" : {
    "type" : "static-s3",
    "uris": ["s3://foo/bar/file.gz", "s3://bar/foo/file2.gz"]
}
```

This firehose provides caching and prefetching features. In the Simple task, a firehose can be read twice if intervals or
shardSpecs are not specified, and, in this case, caching can be useful. Prefetching is preferred when direct scan of objects is slow.
Note that prefetching or caching isn't that useful in the Parallel task.

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|This should be `static-s3`.|None|yes|
|uris|JSON array of URIs where s3 files to be ingested are located.|None|`uris` or `prefixes` must be set|
|prefixes|JSON array of URI prefixes for the locations of s3 files to be ingested.|None|`uris` or `prefixes` must be set|
|maxCacheCapacityBytes|Maximum size of the cache space in bytes. 0 means disabling cache. Cached files are not removed until the ingestion task completes.|1073741824|no|
|maxFetchCapacityBytes|Maximum size of the fetch space in bytes. 0 means disabling prefetch. Prefetched files are removed immediately once they are read.|1073741824|no|
|prefetchTriggerBytes|Threshold to trigger prefetching s3 objects.|maxFetchCapacityBytes / 2|no|
|fetchTimeout|Timeout for fetching an s3 object.|60000|no|
|maxFetchRetry|Maximum retry for fetching an s3 object.|3|no|

#### StaticGoogleBlobStoreFirehose

> You need to include the [`druid-google-extensions`](../development/extensions-core/google.md) as an extension to use the StaticGoogleBlobStoreFirehose.

This firehose ingests events, similar to the StaticS3Firehose, but from an Google Cloud Store.

As with the S3 blobstore, it is assumed to be gzipped if the extension ends in .gz

This firehose is _splittable_ and can be used by the [Parallel task](#parallel-task).
Since each split represents an object in this firehose, each worker task of `index_parallel` will read an object.

Sample spec:

```json
"firehose" : {
    "type" : "static-google-blobstore",
    "blobs": [
        {
          "bucket": "foo",
          "path": "/path/to/your/file.json"
        },
        {
          "bucket": "bar",
          "path": "/another/path.json"
        }
    ]
}
```

This firehose provides caching and prefetching features. In the Simple task, a firehose can be read twice if intervals or
shardSpecs are not specified, and, in this case, caching can be useful. Prefetching is preferred when direct scan of objects is slow.
Note that prefetching or caching isn't that useful in the Parallel task.

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|This should be `static-google-blobstore`.|None|yes|
|blobs|JSON array of Google Blobs.|None|yes|
|maxCacheCapacityBytes|Maximum size of the cache space in bytes. 0 means disabling cache. Cached files are not removed until the ingestion task completes.|1073741824|no|
|maxFetchCapacityBytes|Maximum size of the fetch space in bytes. 0 means disabling prefetch. Prefetched files are removed immediately once they are read.|1073741824|no|
|prefetchTriggerBytes|Threshold to trigger prefetching Google Blobs.|maxFetchCapacityBytes / 2|no|
|fetchTimeout|Timeout for fetching a Google Blob.|60000|no|
|maxFetchRetry|Maximum retry for fetching a Google Blob.|3|no|

Google Blobs:

|property|description|default|required?|
|--------|-----------|-------|---------|
|bucket|Name of the Google Cloud bucket|None|yes|
|path|The path where data is located.|None|yes|

### HDFSFirehose

> You need to include the [`druid-hdfs-storage`](../development/extensions-core/hdfs.md) as an extension to use the HDFSFirehose.

This firehose ingests events from a predefined list of files from the HDFS storage.
This firehose is _splittable_ and can be used by the [Parallel task](#parallel-task).
Since each split represents an HDFS file, each worker task of `index_parallel` will read files.

Sample spec:

```json
"firehose" : {
    "type" : "hdfs",
    "paths": "/foo/bar,/foo/baz"
}
```

This firehose provides caching and prefetching features. During native batch indexing, a firehose can be read twice if
`intervals` are not specified, and, in this case, caching can be useful. Prefetching is preferred when direct scanning
of files is slow.
Note that prefetching or caching isn't that useful in the Parallel task.

|Property|Description|Default|
|--------|-----------|-------|
|type|This should be `hdfs`.|none (required)|
|paths|HDFS paths. Can be either a JSON array or comma-separated string of paths. Wildcards like `*` are supported in these paths.|none (required)|
|maxCacheCapacityBytes|Maximum size of the cache space in bytes. 0 means disabling cache. Cached files are not removed until the ingestion task completes.|1073741824|
|maxFetchCapacityBytes|Maximum size of the fetch space in bytes. 0 means disabling prefetch. Prefetched files are removed immediately once they are read.|1073741824|
|prefetchTriggerBytes|Threshold to trigger prefetching files.|maxFetchCapacityBytes / 2|
|fetchTimeout|Timeout for fetching each file.|60000|
|maxFetchRetry|Maximum number of retries for fetching each file.|3|

### LocalFirehose

This Firehose can be used to read the data from files on local disk, and is mainly intended for proof-of-concept testing, and works with `string` typed parsers.
This Firehose is _splittable_ and can be used by [native parallel index tasks](native-batch.md#parallel-task).
Since each split represents a file in this Firehose, each worker task of `index_parallel` will read a file.
A sample local Firehose spec is shown below:

```json
{
    "type": "local",
    "filter" : "*.csv",
    "baseDir": "/data/directory"
}
```

|property|description|required?|
|--------|-----------|---------|
|type|This should be "local".|yes|
|filter|A wildcard filter for files. See [here](http://commons.apache.org/proper/commons-io/apidocs/org/apache/commons/io/filefilter/WildcardFileFilter) for more information.|yes|
|baseDir|directory to search recursively for files to be ingested. |yes|

<a name="http-firehose"></a>

### HttpFirehose

This Firehose can be used to read the data from remote sites via HTTP, and works with `string` typed parsers.
This Firehose is _splittable_ and can be used by [native parallel index tasks](native-batch.md#parallel-task).
Since each split represents a file in this Firehose, each worker task of `index_parallel` will read a file.
A sample HTTP Firehose spec is shown below:

```json
{
    "type": "http",
    "uris": ["http://example.com/uri1", "http://example2.com/uri2"]
}
```

The below configurations can be optionally used if the URIs specified in the spec require a Basic Authentication Header.
Omitting these fields from your spec will result in HTTP requests with no Basic Authentication Header.

|property|description|default|
|--------|-----------|-------|
|httpAuthenticationUsername|Username to use for authentication with specified URIs|None|
|httpAuthenticationPassword|PasswordProvider to use with specified URIs|None|

Example with authentication fields using the DefaultPassword provider (this requires the password to be in the ingestion spec):

```json
{
    "type": "http",
    "uris": ["http://example.com/uri1", "http://example2.com/uri2"],
    "httpAuthenticationUsername": "username",
    "httpAuthenticationPassword": "password123"
}
```

You can also use the other existing Druid PasswordProviders. Here is an example using the EnvironmentVariablePasswordProvider:

```json
{
    "type": "http",
    "uris": ["http://example.com/uri1", "http://example2.com/uri2"],
    "httpAuthenticationUsername": "username",
    "httpAuthenticationPassword": {
        "type": "environment",
        "variable": "HTTP_FIREHOSE_PW"
    }
}
```

The below configurations can optionally be used for tuning the Firehose performance.
Note that prefetching or caching isn't that useful in the Parallel task.

|property|description|default|
|--------|-----------|-------|
|maxCacheCapacityBytes|Maximum size of the cache space in bytes. 0 means disabling cache. Cached files are not removed until the ingestion task completes.|1073741824|
|maxFetchCapacityBytes|Maximum size of the fetch space in bytes. 0 means disabling prefetch. Prefetched files are removed immediately once they are read.|1073741824|
|prefetchTriggerBytes|Threshold to trigger prefetching HTTP objects.|maxFetchCapacityBytes / 2|
|fetchTimeout|Timeout for fetching an HTTP object.|60000|
|maxFetchRetry|Maximum retries for fetching an HTTP object.|3|

<a name="segment-firehose"></a>

### IngestSegmentFirehose

This Firehose can be used to read the data from existing druid segments, potentially using a new schema and changing the name, dimensions, metrics, rollup, etc. of the segment.
This Firehose is _splittable_ and can be used by [native parallel index tasks](native-batch.md#parallel-task).
This firehose will accept any type of parser, but will only utilize the list of dimensions and the timestamp specification.
 A sample ingest Firehose spec is shown below:

```json
{
    "type": "ingestSegment",
    "dataSource": "wikipedia",
    "interval": "2013-01-01/2013-01-02"
}
```

|property|description|required?|
|--------|-----------|---------|
|type|This should be "ingestSegment".|yes|
|dataSource|A String defining the data source to fetch rows from, very similar to a table in a relational database|yes|
|interval|A String representing the ISO-8601 interval. This defines the time range to fetch the data over.|yes|
|dimensions|The list of dimensions to select. If left empty, no dimensions are returned. If left null or not defined, all dimensions are returned. |no|
|metrics|The list of metrics to select. If left empty, no metrics are returned. If left null or not defined, all metrics are selected.|no|
|filter| See [Filters](../querying/filters.md)|no|
|maxInputSegmentBytesPerTask|Deprecated. Use [Segments Split Hint Spec](#segments-split-hint-spec) instead. When used with the native parallel index task, the maximum number of bytes of input segments to process in a single task. If a single segment is larger than this number, it will be processed by itself in a single task (input segments are never split across tasks). Defaults to 150MB.|no|

<a name="sql-firehose"></a>

### SqlFirehose

This Firehose can be used to ingest events residing in an RDBMS. The database connection information is provided as part of the ingestion spec.
For each query, the results are fetched locally and indexed.
If there are multiple queries from which data needs to be indexed, queries are prefetched in the background, up to `maxFetchCapacityBytes` bytes.
This Firehose is _splittable_ and can be used by [native parallel index tasks](native-batch.md#parallel-task).
This firehose will accept any type of parser, but will only utilize the list of dimensions and the timestamp specification. See the extension documentation for more detailed ingestion examples.

Requires one of the following extensions:
 * [MySQL Metadata Store](../development/extensions-core/mysql.md).
 * [PostgreSQL Metadata Store](../development/extensions-core/postgresql.md).


```json
{
    "type": "sql",
    "database": {
        "type": "mysql",
        "connectorConfig": {
            "connectURI": "jdbc:mysql://host:port/schema",
            "user": "user",
            "password": "password"
        }
     },
    "sqls": ["SELECT * FROM table1", "SELECT * FROM table2"]
}
```

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|This should be "sql".||Yes|
|database|Specifies the database connection details.||Yes|
|maxCacheCapacityBytes|Maximum size of the cache space in bytes. 0 means disabling cache. Cached files are not removed until the ingestion task completes.|1073741824|No|
|maxFetchCapacityBytes|Maximum size of the fetch space in bytes. 0 means disabling prefetch. Prefetched files are removed immediately once they are read.|1073741824|No|
|prefetchTriggerBytes|Threshold to trigger prefetching SQL result objects.|maxFetchCapacityBytes / 2|No|
|fetchTimeout|Timeout for fetching the result set.|60000|No|
|foldCase|Toggle case folding of database column names. This may be enabled in cases where the database returns case insensitive column names in query results.|false|No|
|sqls|List of SQL queries where each SQL query would retrieve the data to be indexed.||Yes|

#### Database

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|The type of database to query. Valid values are `mysql` and `postgresql`_||Yes|
|connectorConfig|Specify the database connection properties via `connectURI`, `user` and `password`||Yes|

### InlineFirehose

This Firehose can be used to read the data inlined in its own spec.
It can be used for demos or for quickly testing out parsing and schema, and works with `string` typed parsers.
A sample inline Firehose spec is shown below:

```json
{
    "type": "inline",
    "data": "0,values,formatted\n1,as,CSV"
}
```

|property|description|required?|
|--------|-----------|---------|
|type|This should be "inline".|yes|
|data|Inlined data to ingest.|yes|

### CombiningFirehose

This Firehose can be used to combine and merge data from a list of different Firehoses.

```json
{
    "type": "combining",
    "delegates": [ { firehose1 }, { firehose2 }, ... ]
}
```

|property|description|required?|
|--------|-----------|---------|
|type|This should be "combining"|yes|
|delegates|List of Firehoses to combine data from|yes|
