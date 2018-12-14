---
layout: doc_page
title: "Native Index Tasks"
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

# Native Index Tasks

Druid currently has two types of native batch indexing tasks, `index_parallel` which runs tasks
in parallel on multiple middle manager nodes, and `index` which will run a single indexing task locally on a single
middle manager.

Parallel Index Task
--------------------------------

The Parallel Index Task is a task for parallel batch indexing. This task only uses Druid's resource and
doesn't depend on other external systems like Hadoop. This task currently works in a single phase without shuffling intermediate
data. `index_parallel` task is a supervisor task which basically generates multiple worker tasks and submits
them to overlords. Each worker task reads input data and makes segments. Once they successfully generate segments for all
input, they report the generated segment list to the supervisor task. The supervisor task periodically checks the worker
task statuses. If one of them fails, it retries the failed task until the retrying number reaches the configured limit.
If all worker tasks succeed, then it collects the reported list of generated segments and publishes those segments at once.

To use this task, the `firehose` in `ioConfig` should be _splittable_. If it's not, this task runs sequentially. The
current splittable fireshoses are [`LocalFirehose`](./firehose.html#localfirehose), [`HttpFirehose`](./firehose.html#httpfirehose)
, [`StaticS3Firehose`](../development/extensions-core/s3.html#statics3firehose), [`StaticAzureBlobStoreFirehose`](../development/extensions-contrib/azure.html#staticazureblobstorefirehose)
, [`StaticGoogleBlobStoreFirehose`](../development/extensions-contrib/google.html#staticgoogleblobstorefirehose), and [`StaticCloudFilesFirehose`](../development/extensions-contrib/cloudfiles.html#staticcloudfilesfirehose).

The splittable firehose is responsible for generating _splits_. The supervisor task generates _worker task specs_ each of
which specifies a split and submits worker tasks using those specs. As a result, the number of worker tasks depends on
the implementation of splittable firehoses. Please note that multiple tasks can be created for the same worker task spec
if one of them fails.

Since this task doesn't shuffle intermediate data, it isn't available for [perfect rollup](../ingestion/index.html#roll-up-modes). 

An example ingestion spec is:

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
    }
  }
}
```

#### Task Properties

|property|description|required?|
|--------|-----------|---------|
|type|The task type, this should always be `index_parallel`.|yes|
|id|The task ID. If this is not explicitly specified, Druid generates the task ID using task type, data source name, interval, and date-time stamp. |no|
|spec|The ingestion spec including the data schema, IOConfig, and TuningConfig. See below for more details. |yes|
|context|Context containing various task configuration parameters. See below for more details.|no|

#### DataSchema

This field is required.

See [Ingestion Spec DataSchema](../ingestion/ingestion-spec.html#dataschema)

#### IOConfig

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|The task type, this should always be `index_parallel`.|none|yes|
|firehose|Specify a [Firehose](../ingestion/firehose.html) here.|none|yes|
|appendToExisting|Creates segments as additional shards of the latest version, effectively appending to the segment set instead of replacing it. This will only work if the existing segment set has extendable-type shardSpecs (which can be forced by setting 'forceExtendableShardSpecs' in the tuning config).|false|no|

#### TuningConfig

The tuningConfig is optional and default parameters will be used if no tuningConfig is specified. See below for more details.

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|The task type, this should always be `index_parallel`.|none|yes|
|targetPartitionSize|Used in sharding. Determines how many rows are in each segment.|5000000|no|
|maxRowsInMemory|Used in determining when intermediate persists to disk should occur. Normally user does not need to set this, but depending on the nature of data, if rows are short in terms of bytes, user may not want to store a million rows in memory and this value should be set.|1000000|no|
|maxBytesInMemory|Used in determining when intermediate persists to disk should occur. Normally this is computed internally and user does not need to set it. This value represents number of bytes to aggregate in heap memory before persisting. This is based on a rough estimate of memory usage and not actual usage. The maximum heap memory usage for indexing is maxBytesInMemory * (2 + maxPendingPersists)|1/6 of max JVM memory|no|
|maxTotalRows|Total number of rows in segments waiting for being pushed. Used in determining when intermediate pushing should occur.|150000|no|
|numShards|Directly specify the number of shards to create. If this is specified and 'intervals' is specified in the granularitySpec, the index task can skip the determine intervals/partitions pass through the data. numShards cannot be specified if targetPartitionSize is set.|null|no|
|indexSpec|defines segment storage format options to be used at indexing time, see [IndexSpec](#indexspec)|null|no|
|maxPendingPersists|Maximum number of persists that can be pending but not started. If this limit would be exceeded by a new intermediate persist, ingestion will block until the currently-running persist finishes. Maximum heap memory usage for indexing scales with maxRowsInMemory * (2 + maxPendingPersists).|0 (meaning one persist can be running concurrently with ingestion, and none can be queued up)|no|
|forceExtendableShardSpecs|Forces use of extendable shardSpecs. Experimental feature intended for use with the [Kafka indexing service extension](../development/extensions-core/kafka-ingestion.html).|false|no|
|reportParseExceptions|If true, exceptions encountered during parsing will be thrown and will halt ingestion; if false, unparseable rows and fields will be skipped.|false|no|
|pushTimeout|Milliseconds to wait for pushing segments. It must be >= 0, where 0 means to wait forever.|0|no|
|segmentWriteOutMediumFactory|Segment write-out medium to use when creating segments. See [Additional Peon Configuration: SegmentWriteOutMediumFactory](../configuration/index.html#segmentwriteoutmediumfactory) for explanation and available options.|Not specified, the value from `druid.peon.defaultSegmentWriteOutMediumFactory` is used|no|
|maxNumSubTasks|Maximum number of tasks which can be run at the same time.|Integer.MAX_VALUE|no|
|maxRetry|Maximum number of retries on task failures.|3|no|
|taskStatusCheckPeriodMs|Polling period in milleseconds to check running task statuses.|1000|no|
|chatHandlerTimeout|Timeout for reporting the pushed segments in worker tasks.|PT10S|no|
|chatHandlerNumRetries|Retries for reporting the pushed segments in worker tasks.|5|no|

#### HTTP Endpoints

The supervisor task provides some HTTP endpoints to get running status.

* `http://{PEON_IP}:{PEON_PORT}/druid/worker/v1/chat/{SUPERVISOR_TASK_ID}/mode`

Returns 'parallel' if the indexing task is running in parallel. Otherwise, it returns 'sequential'.

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
        "targetPartitionSize": 5000000,
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
        "maxPendingPersists": 0,
        "forceExtendableShardSpecs": false,
        "reportParseExceptions": false,
        "pushTimeout": 0,
        "segmentWriteOutMediumFactory": null,
        "maxNumSubTasks": 2147483647,
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

Local Index Task
----------------

The Local Index Task is designed to be used for smaller data sets. The task executes within the indexing service. The grammar of the index task is as follows:

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
      "targetPartitionSize" : 5000000,
      "maxRowsInMemory" : 1000000
    }
  }
}
```

#### Task Properties

|property|description|required?|
|--------|-----------|---------|
|type|The task type, this should always be "index".|yes|
|id|The task ID. If this is not explicitly specified, Druid generates the task ID using task type, data source name, interval, and date-time stamp. |no|
|spec|The ingestion spec including the data schema, IOConfig, and TuningConfig. See below for more details. |yes|
|context|Context containing various task configuration parameters. See below for more details.|no|

#### DataSchema

This field is required.

See [Ingestion Spec DataSchema](../ingestion/ingestion-spec.html#dataschema)

#### IOConfig

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|The task type, this should always be "index".|none|yes|
|firehose|Specify a [Firehose](../ingestion/firehose.html) here.|none|yes|
|appendToExisting|Creates segments as additional shards of the latest version, effectively appending to the segment set instead of replacing it. This will only work if the existing segment set has extendable-type shardSpecs (which can be forced by setting 'forceExtendableShardSpecs' in the tuning config).|false|no|

#### TuningConfig

The tuningConfig is optional and default parameters will be used if no tuningConfig is specified. See below for more details.

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|The task type, this should always be "index".|none|yes|
|targetPartitionSize|Used in sharding. Determines how many rows are in each segment.|5000000|no|
|maxRowsInMemory|Used in determining when intermediate persists to disk should occur. Normally user does not need to set this, but depending on the nature of data, if rows are short in terms of bytes, user may not want to store a million rows in memory and this value should be set.|1000000|no|
|maxBytesInMemory|Used in determining when intermediate persists to disk should occur. Normally this is computed internally and user does not need to set it. This value represents number of bytes to aggregate in heap memory before persisting. This is based on a rough estimate of memory usage and not actual usage. The maximum heap memory usage for indexing is maxBytesInMemory * (2 + maxPendingPersists)|1/6 of max JVM memory|no|
|maxTotalRows|Total number of rows in segments waiting for being pushed. Used in determining when intermediate pushing should occur.|20000000|no|
|numShards|Directly specify the number of shards to create. If this is specified and 'intervals' is specified in the granularitySpec, the index task can skip the determine intervals/partitions pass through the data. numShards cannot be specified if targetPartitionSize is set.|null|no|
|partitionDimensions|The dimensions to partition on. Leave blank to select all dimensions. Only used with `forceGuaranteedRollup` = true, will be ignored otherwise.|null|no|
|indexSpec|defines segment storage format options to be used at indexing time, see [IndexSpec](#indexspec)|null|no|
|maxPendingPersists|Maximum number of persists that can be pending but not started. If this limit would be exceeded by a new intermediate persist, ingestion will block until the currently-running persist finishes. Maximum heap memory usage for indexing scales with maxRowsInMemory * (2 + maxPendingPersists).|0 (meaning one persist can be running concurrently with ingestion, and none can be queued up)|no|
|forceExtendableShardSpecs|Forces use of extendable shardSpecs. Experimental feature intended for use with the [Kafka indexing service extension](../development/extensions-core/kafka-ingestion.html).|false|no|
|forceGuaranteedRollup|Forces guaranteeing the [perfect rollup](../ingestion/index.html#roll-up-modes). The perfect rollup optimizes the total size of generated segments and querying time while indexing time will be increased. This flag cannot be used with either `appendToExisting` of IOConfig or `forceExtendableShardSpecs`. For more details, see the below __Segment pushing modes__ section.|false|no|
|reportParseExceptions|DEPRECATED. If true, exceptions encountered during parsing will be thrown and will halt ingestion; if false, unparseable rows and fields will be skipped. Setting `reportParseExceptions` to true will override existing configurations for `maxParseExceptions` and `maxSavedParseExceptions`, setting `maxParseExceptions` to 0 and limiting `maxSavedParseExceptions` to no more than 1.|false|no|
|pushTimeout|Milliseconds to wait for pushing segments. It must be >= 0, where 0 means to wait forever.|0|no|
|segmentWriteOutMediumFactory|Segment write-out medium to use when creating segments. See [Additional Peon Configuration: SegmentWriteOutMediumFactory](../configuration/index.html#segmentwriteoutmediumfactory) for explanation and available options.|Not specified, the value from `druid.peon.defaultSegmentWriteOutMediumFactory` is used|no|
|logParseExceptions|If true, log an error message when a parsing exception occurs, containing information about the row where the error occurred.|false|no|
|maxParseExceptions|The maximum number of parse exceptions that can occur before the task halts ingestion and fails. Overridden if `reportParseExceptions` is set.|unlimited|no|
|maxSavedParseExceptions|When a parse exception occurs, Druid can keep track of the most recent parse exceptions. "maxSavedParseExceptions" limits how many exception instances will be saved. These saved exceptions will be made available after the task finishes in the [task completion report](../ingestion/reports.html). Overridden if `reportParseExceptions` is set.|0|no|

#### IndexSpec

The indexSpec defines segment storage format options to be used at indexing time, such as bitmap type and column
compression formats. The indexSpec is optional and default parameters will be used if not specified.

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|bitmap|Object|Compression format for bitmap indexes. Should be a JSON object; see below for options.|no (defaults to Concise)|
|dimensionCompression|String|Compression format for dimension columns. Choose from `LZ4`, `LZF`, or `uncompressed`.|no (default == `LZ4`)|
|metricCompression|String|Compression format for metric columns. Choose from `LZ4`, `LZF`, `uncompressed`, or `none`.|no (default == `LZ4`)|
|longEncoding|String|Encoding format for metric and dimension columns with type long. Choose from `auto` or `longs`. `auto` encodes the values using offset or lookup table depending on column cardinality, and store them with variable size. `longs` stores the value as is with 8 bytes each.|no (default == `longs`)|

##### Bitmap types

For Concise bitmaps:

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|type|String|Must be `concise`.|yes|

For Roaring bitmaps:

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|type|String|Must be `roaring`.|yes|
|compressRunOnSerialization|Boolean|Use a run-length encoding where it is estimated as more space efficient.|no (default == `true`)|

#### Segment pushing modes

While ingesting data using the Index task, it creates segments from the input data and pushes them. For segment pushing,
the Index task supports two segment pushing modes, i.e., _bulk pushing mode_ and _incremental pushing mode_ for
[perfect rollup and best-effort rollup](../ingestion/index.html#roll-up-modes), respectively.

In the bulk pushing mode, every segment is pushed at the very end of the index task. Until then, created segments
are stored in the memory and local storage of the node running the index task. As a result, this mode might cause a
problem due to limited storage capacity, and is not recommended to use in production.

On the contrary, in the incremental pushing mode, segments are incrementally pushed, that is they can be pushed
in the middle of the index task. More precisely, the index task collects data and stores created segments in the memory
and disks of the node running that task until the total number of collected rows exceeds `maxTotalRows`. Once it exceeds,
the index task immediately pushes all segments created until that moment, cleans all pushed segments up, and
continues to ingest remaining data.

To enable bulk pushing mode, `forceGuaranteedRollup` should be set in the TuningConfig. Note that this option cannot
be used with either `forceExtendableShardSpecs` of TuningConfig or `appendToExisting` of IOConfig.
