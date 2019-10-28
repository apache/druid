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


Apache Druid (incubating) currently has two types of native batch indexing tasks, `index_parallel` which can run
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
doesn't depend on other external systems like Hadoop. `index_parallel` task is a supervisor task which basically generates
multiple worker tasks and submits them to the Overlord. Each worker task reads input data and creates segments. Once they
successfully generate segments for all input data, they report the generated segment list to the supervisor task. 
The supervisor task periodically checks the status of worker tasks. If one of them fails, it retries the failed task
until the number of retries reaches the configured limit. If all worker tasks succeed, then it publishes the reported segments at once.

The parallel Index Task can run in two different modes depending on `forceGuaranteedRollup` in `tuningConfig`.
If `forceGuaranteedRollup` = false, it's executed in a single phase. In this mode,
each sub task creates segments individually and reports them to the supervisor task.

If `forceGuaranteedRollup` = true, it's executed in two phases with data shuffle which is similar to [MapReduce](https://en.wikipedia.org/wiki/MapReduce).
In the first phase, each sub task partitions input data based on `segmentGranularity` (primary partition key) in `granularitySpec`
and `partitionDimensions` (secondary partition key) in `partitionsSpec`. The partitioned data is served by
the [middleManager](../design/middlemanager.md) or the [indexer](../design/indexer.md)
where the first phase tasks ran. In the second phase, each sub task fetches
partitioned data from MiddleManagers or indexers and merges them to create the final segments.
As in the single phase execution, the created segments are reported to the supervisor task to publish at once.

To use this task, the `firehose` in `ioConfig` should be _splittable_ and `maxNumConcurrentSubTasks` should be set something larger than 1 in `tuningConfig`.
Otherwise, this task runs sequentially. Here is the list of currently splittable firehoses.

- [`LocalFirehose`](#local-firehose)
- [`IngestSegmentFirehose`](#segment-firehose)
- [`HttpFirehose`](#http-firehose)
- [`StaticS3Firehose`](../development/extensions-core/s3.md#firehose)
- [`StaticAzureBlobStoreFirehose`](../development/extensions-contrib/azure.md#firehose)
- [`StaticGoogleBlobStoreFirehose`](../development/extensions-core/google.md#firehose)
- [`StaticCloudFilesFirehose`](../development/extensions-contrib/cloudfiles.md#firehose)

The splittable firehose is responsible for generating _splits_. The supervisor task generates _worker task specs_ containing a split
and submits worker tasks using those specs. As a result, the number of worker tasks depends on
the implementation of splittable firehoses. Please note that multiple tasks can be created for the same worker task spec
if one of them fails.

You may want to consider the below things:

- The number of concurrent tasks run in parallel ingestion is determined by `maxNumConcurrentSubTasks` in the `tuningConfig`.
  The supervisor task checks the number of current running sub tasks and creates more if it's smaller than `maxNumConcurrentSubTasks` no matter how many task slots are currently available.
  This may affect to other ingestion performance. See the below [Capacity Planning](#capacity-planning) section for more details.
- By default, batch ingestion replaces all data (in your `granularitySpec`'s intervals) in any segment that it writes to.
  If you'd like to add to the segment instead, set the `appendToExisting` flag in `ioConfig`. Note that it only replaces
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
        },
        "parser": {
          "parseSpec": {
            "format" : "json",
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
            }
          }
        }
    },
    "ioConfig": {
        "type": "index_parallel",
        "firehose": {
          "type": "local",
          "baseDir": "examples/indexing/",
          "filter": "wikipedia_index_data*"
        }
    },
    "tuningconfig": {
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

If you specify `intervals` explicitly in your dataSchema's granularitySpec, batch ingestion will lock the full intervals
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
|firehose|Specify a [Firehose](#firehoses) here.|none|yes|
|appendToExisting|Creates segments as additional shards of the latest version, effectively appending to the segment set instead of replacing it. This will only work if the existing segment set has extendable-type shardSpecs.|false|no|

### `tuningConfig`

The tuningConfig is optional and default parameters will be used if no tuningConfig is specified. See below for more details.

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|The task type, this should always be `index_parallel`.|none|yes|
|maxRowsPerSegment|Deprecated. Use `partitionsSpec` instead. Used in sharding. Determines how many rows are in each segment.|5000000|no|
|maxRowsInMemory|Used in determining when intermediate persists to disk should occur. Normally user does not need to set this, but depending on the nature of data, if rows are short in terms of bytes, user may not want to store a million rows in memory and this value should be set.|1000000|no|
|maxBytesInMemory|Used in determining when intermediate persists to disk should occur. Normally this is computed internally and user does not need to set it. This value represents number of bytes to aggregate in heap memory before persisting. This is based on a rough estimate of memory usage and not actual usage. The maximum heap memory usage for indexing is maxBytesInMemory * (2 + maxPendingPersists)|1/6 of max JVM memory|no|
|maxTotalRows|Deprecated. Use `partitionsSpec` instead. Total number of rows in segments waiting for being pushed. Used in determining when intermediate pushing should occur.|20000000|no|
|numShards|Deprecated. Use `partitionsSpec` instead. Directly specify the number of shards to create. If this is specified and `intervals` is specified in the `granularitySpec`, the index task can skip the determine intervals/partitions pass through the data. `numShards` cannot be specified if `maxRowsPerSegment` is set.|null|no|
|splitHintSpec|Used to give a hint to control the amount of data that each first phase task reads. This hint could be ignored depending on the implementation of firehose. See [SplitHintSpec](#splithintspec) for more details.|null|no|
|partitionsSpec|Defines how to partition data in each timeChunk, see [PartitionsSpec](#partitionsspec)|`dynamic` if `forceGuaranteedRollup` = false, `hashed` if `forceGuaranteedRollup` = true|no|
|indexSpec|Defines segment storage format options to be used at indexing time, see [IndexSpec](index.md#indexspec)|null|no|
|indexSpecForIntermediatePersists|Defines segment storage format options to be used at indexing time for intermediate persisted temporary segments. this can be used to disable dimension/metric compression on intermediate segments to reduce memory required for final merging. however, disabling compression on intermediate segments might increase page cache use while they are used before getting merged into final segment published, see [IndexSpec](index.md#indexspec) for possible values.|same as indexSpec|no|
|maxPendingPersists|Maximum number of persists that can be pending but not started. If this limit would be exceeded by a new intermediate persist, ingestion will block until the currently-running persist finishes. Maximum heap memory usage for indexing scales with maxRowsInMemory * (2 + maxPendingPersists).|0 (meaning one persist can be running concurrently with ingestion, and none can be queued up)|no|
|forceGuaranteedRollup|Forces guaranteeing the [perfect rollup](../ingestion/index.md#rollup). The perfect rollup optimizes the total size of generated segments and querying time while indexing time will be increased. If this is set to true, `numShards` in `tuningConfig` and `intervals` in `granularitySpec` must be set. Note that the result segments would be hash-partitioned. This flag cannot be used with `appendToExisting` of IOConfig. For more details, see the below __Segment pushing modes__ section.|false|no|
|reportParseExceptions|If true, exceptions encountered during parsing will be thrown and will halt ingestion; if false, unparseable rows and fields will be skipped.|false|no|
|pushTimeout|Milliseconds to wait for pushing segments. It must be >= 0, where 0 means to wait forever.|0|no|
|segmentWriteOutMediumFactory|Segment write-out medium to use when creating segments. See [SegmentWriteOutMediumFactory](#segmentwriteoutmediumfactory).|Not specified, the value from `druid.peon.defaultSegmentWriteOutMediumFactory.type` is used|no|
|maxNumConcurrentSubTasks|Maximum number of sub tasks which can be run in parallel at the same time. The supervisor task would spawn worker tasks up to `maxNumConcurrentSubTasks` regardless of the current available task slots. If this value is set to 1, the supervisor task processes data ingestion on its own instead of spawning worker tasks. If this value is set to too large, too many worker tasks can be created which might block other ingestion. Check [Capacity Planning](#capacity-planning) for more details.|1|no|
|maxRetry|Maximum number of retries on task failures.|3|no|
|maxNumSegmentsToMerge|Max limit for the number of segments that a single task can merge at the same time in the second phase. Used only `forceGuaranteedRollup` is set.|100|no|
|totalNumMergeTasks|Total number of tasks to merge segments in the second phase when `forceGuaranteedRollup` is set.|10|no|
|taskStatusCheckPeriodMs|Polling period in milliseconds to check running task statuses.|1000|no|
|chatHandlerTimeout|Timeout for reporting the pushed segments in worker tasks.|PT10S|no|
|chatHandlerNumRetries|Retries for reporting the pushed segments in worker tasks.|5|no|

### `splitHintSpec`

`SplitHintSpec` is used to give a hint when the supervisor task creates input splits.
Note that each sub task processes a single input split. You can control the amount of data each sub task will read during the first phase.

Currently only one splitHintSpec, i.e., `segments`, is available.

#### `SegmentsSplitHintSpec`

`SegmentsSplitHintSpec` is used only for `IngestSegmentFirehose`.

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|This should always be `segments`.|none|yes|
|maxInputSegmentBytesPerTask|Maximum number of bytes of input segments to process in a single task. If a single segment is larger than this number, it will be processed by itself in a single task (input segments are never split across tasks).|150MB|no|

### `partitionsSpec`

PartitionsSpec is to describe the secondary partitioning method.
You should use different partitionsSpec depending on the [rollup mode](../ingestion/index.md#rollup) you want.
For perfect rollup, you should use `hashed`.

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|This should always be `hashed`|none|yes|
|targetRowsPerSegment|Target number of rows to include in a partition, should be a number that targets segments of 500MB\~1GB.|5000000 (if `numShards` is not set)|either this or `numShards`|
|numShards|Directly specify the number of shards to create. If this is specified and `intervals` is specified in the `granularitySpec`, the index task can skip the determine intervals/partitions pass through the data. `numShards` cannot be specified if `targetRowsPerSegment` is set.|null|no|
|partitionDimensions|The dimensions to partition on. Leave blank to select all dimensions. Only used with `numShards`, will be ignored when `targetRowsPerSegment` is set.|null|no|

For best-effort rollup, you should use `dynamic`.

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|This should always be `dynamic`|none|yes|
|maxRowsPerSegment|Used in sharding. Determines how many rows are in each segment.|5000000|no|
|maxTotalRows|Total number of rows in segments waiting for being pushed. Used in determining when intermediate segment push should occur.|20000000|no|

### HTTP status endpoints

The supervisor task provides some HTTP endpoints to get running status.

* `http://{PEON_IP}:{PEON_PORT}/druid/worker/v1/chat/{SUPERVISOR_TASK_ID}/mode`

Returns 'parallel' if the indexing task is running in parallel. Otherwise, it returns 'sequential'.

* `http://{PEON_IP}:{PEON_PORT}/druid/worker/v1/chat/{SUPERVISOR_TASK_ID}/phase`

Returns the name of the current phase if the task running in the parallel mode.

* `http://{PEON_IP}:{PEON_PORT}/druid/worker/v1/chat/{SUPERVISOR_TASK_ID}/progress`

Returns the current progress if the supervisor task is running in the parallel mode.

An example of the result is

```json
{
  "running":10,
  "succeeded":0,
  "failed":0,
  "complete":0,
  "total":10,
  "expectedSucceeded":10
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
        "parser": {
          "type": "hadoopyString",
          "parseSpec": {
            "format": "tsv",
            "delimiter": "|",
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
          }
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
        "firehose": {
          "type": "local",
          "baseDir": "/path/to/data/",
          "filter": "lineitem.tbl.5",
          "parser": null
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
            "type": "concise"
          },
          "dimensionCompression": "lz4",
          "metricCompression": "lz4",
          "longEncoding": "longs"
        },
        "indexSpecForIntermediatePersists": {
          "bitmap": {
            "type": "concise"
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
      "parser" : {
        "type" : "string",
        "parseSpec" : {
          "format" : "json",
          "timestampSpec" : {
            "column" : "timestamp",
            "format" : "auto"
          },
          "dimensionsSpec" : {
            "dimensions": ["page","language","user","unpatrolled","newPage","robot","anonymous","namespace","continent","country","region","city"],
            "dimensionExclusions" : [],
            "spatialDimensions" : []
          }
        }
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
      "firehose" : {
        "type" : "local",
        "baseDir" : "examples/indexing/",
        "filter" : "wikipedia_data.json"
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
|firehose|Specify a [Firehose](#firehoses) here.|none|yes|
|appendToExisting|Creates segments as additional shards of the latest version, effectively appending to the segment set instead of replacing it. This will only work if the existing segment set has extendable-type shardSpecs.|false|no|

### `tuningConfig`

The tuningConfig is optional and default parameters will be used if no tuningConfig is specified. See below for more details.

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|The task type, this should always be "index".|none|yes|
|maxRowsPerSegment|Deprecated. Use `partitionsSpec` instead. Used in sharding. Determines how many rows are in each segment.|5000000|no|
|maxRowsInMemory|Used in determining when intermediate persists to disk should occur. Normally user does not need to set this, but depending on the nature of data, if rows are short in terms of bytes, user may not want to store a million rows in memory and this value should be set.|1000000|no|
|maxBytesInMemory|Used in determining when intermediate persists to disk should occur. Normally this is computed internally and user does not need to set it. This value represents number of bytes to aggregate in heap memory before persisting. This is based on a rough estimate of memory usage and not actual usage. The maximum heap memory usage for indexing is maxBytesInMemory * (2 + maxPendingPersists)|1/6 of max JVM memory|no|
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

For best-effort rollup, you should use `dynamic`.

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|This should always be `dynamic`|none|yes|
|maxRowsPerSegment|Used in sharding. Determines how many rows are in each segment.|5000000|no|
|maxTotalRows|Total number of rows in segments waiting for being pushed. Used in determining when intermediate segment push should occur.|20000000|no|

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

Firehoses are pluggable and thus the configuration schema can and will vary based on the `type` of the firehose.

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| type | String | Specifies the type of firehose. Each value will have its own configuration schema, firehoses packaged with Druid are described below. | yes |

## Firehoses

There are several firehoses readily available in Druid, some are meant for examples, others can be used directly in a production environment.

For additional firehoses, please see our [extensions list](../development/extensions.md).

<a name="local-firehose"></a>

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
|filter|A wildcard filter for files. See [here](http://commons.apache.org/proper/commons-io/apidocs/org/apache/commons/io/filefilter/WildcardFileFilter.html) for more information.|yes|
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
|maxInputSegmentBytesPerTask|Deprecated. Use [SegmentsSplitHintSpec](#segmentssplithintspec) instead. When used with the native parallel index task, the maximum number of bytes of input segments to process in a single task. If a single segment is larger than this number, it will be processed by itself in a single task (input segments are never split across tasks). Defaults to 150MB.|no|

<a name="sql-firehose"></a>

### SqlFirehose

This Firehose can be used to ingest events residing in an RDBMS. The database connection information is provided as part of the ingestion spec.
For each query, the results are fetched locally and indexed.
If there are multiple queries from which data needs to be indexed, queries are prefetched in the background, up to `maxFetchCapacityBytes` bytes.
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

<a name="inline-firehose"></a>

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

<a name="combining-firehose"></a>

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
