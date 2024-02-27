---
id: rabbit-super-stream-injestion
title: "RabbitMQ superstream ingestion"
sidebar_label: "Rabbitmq superstream"
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

When you enable the rabbit stream indexing service, you can configure *supervisors* on the Overlord to manage the creation and lifetime of rabbit indexing tasks. These indexing tasks read events from a rabbit super-stream. The supervisor oversees the state of the indexing tasks to:
  - coordinate handoffs
  - manage failures
  - ensure that scalability and replication requirements are maintained.

  To use the rabbit stream indexing service, load the `druid-rabbit-indexing-service` community druid extension (see
[Including Extensions](../configuration/extensions.md#loading-extensions)).


## Submitting a Supervisor Spec

To use the rabbit stream indexing service, load the `druid-rabbit-indexing-service` extension on both the Overlord and the MiddleManagers. Druid starts a supervisor for a dataSource when you submit a supervisor spec. Submit your supervisor spec to the following endpoint:


`http://<OVERLORD_IP>:<OVERLORD_PORT>/druid/indexer/v1/supervisor`

For example:

```
curl -X POST -H 'Content-Type: application/json' -d @supervisor-spec.json http://localhost:8090/druid/indexer/v1/supervisor
```

Where the file `supervisor-spec.json` contains a rabbit supervisor spec:

```json
{
  "type": "rabbit",
  "spec": {
    "dataSchema": {
      "dataSource": "metrics-rabbit",
      "timestampSpec": {
        "column": "timestamp",
        "format": "auto"
      },
     "dimensionsSpec": {
        "dimensions": [],
        "dimensionExclusions": [
         "timestamp",
         "value"
       ]
      },
     "metricsSpec": [
        {
         "name": "count",
          "type": "count"
       },
       {
          "name": "value_sum",
          "fieldName": "value",
          "type": "doubleSum"
        },
       {
         "name": "value_min",
         "fieldName": "value",
         "type": "doubleMin"
       },
        {
          "name": "value_max",
         "fieldName": "value",
         "type": "doubleMax"
       }
     ],
     "granularitySpec": {
        "type": "uniform",
        "segmentGranularity": "HOUR",
        "queryGranularity": "NONE"
     }
   },
    "ioConfig": {
     "stream": "metrics",
     "inputFormat": {
       "type": "json"
      },
      "uri": "rabbitmq-stream://localhost:5552",
      "taskCount": 1,
      "replicas": 1,
      "taskDuration": "PT1H"
   },
   "tuningConfig": {
     "type": "rabbit",
     "maxRowsPerSegment": 5000000
   }
  }
}
```

## Supervisor Spec

|Field|Description|Required|
|--------|-----------|---------|
|`type`|The supervisor type; this should always be `rabbit`.|yes|
|`spec`|Container object for the supervisor configuration.|yes|
|`dataSchema`|The schema that will be used by the rabbit indexing task during ingestion. See [`dataSchema`](ingestion-spec.md#dataschema).|yes|
|`ioConfig`|An [`ioConfig`](#ioconfig) object for configuring rabbit super stream connection and I/O-related settings for the supervisor and indexing task.|yes|
|`tuningConfig`|A [`tuningConfig`](#tuningconfig) object for configuring performance-related settings for the supervisor and indexing tasks.|no|

### `ioConfig`

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|`stream`|String|The RabbitMQ super stream to read.|yes|
|`inputFormat`|Object|[`inputFormat`](data-formats.md#input-format) to specify how to parse input data. See [Specifying data format](data-formats.md#input-format) for details about specifying the input format.|yes|
|`uri`|String|The URI to connect to RabbitMQ with. |yes |
|`replicas`|Integer|The number of replica sets, where 1 means a single set of tasks (no replication). Replica tasks will always be assigned to different workers to provide resiliency against process failure.|no (default == 1)|
|`taskCount`|Integer|The maximum number of *reading* tasks in a *replica set*. This means that the maximum number of reading tasks will be `taskCount * replicas` and the total number of tasks (*reading* + *publishing*) will be higher than this. |no (default == 1)|
|`taskDuration`|ISO8601 Period|The length of time before tasks stop reading and begin publishing their segment.|no (default == PT1H)|
|`startDelay`|ISO8601 Period|The period to wait before the supervisor starts managing tasks.|no (default == PT5S)|
|`period`|ISO8601 Period|How often the supervisor will execute its management logic. Note that the supervisor will also run in response to certain events (such as tasks succeeding, failing, and reaching their taskDuration) so this value specifies the maximum time between iterations.|no (default == PT30S)|
|`useEarliestSequenceNumber`|Boolean|If a supervisor is managing a dataSource for the first time, it will obtain a set of starting sequence numbers from RabbitMQ. This flag determines whether it retrieves the earliest or latest sequence numbers in the stream. Under normal circumstances, subsequent tasks will start from where the previous segments ended so this flag will only be used on first run.|no (default == false)|
|`completionTimeout`|ISO8601 Period|The length of time to wait before declaring a publishing task as failed and terminating it. If this is set too low, your tasks may never publish. The publishing clock for a task begins roughly after `taskDuration` elapses.|no (default == PT6H)|
|`lateMessageRejectionPeriod`|ISO8601 Period|Configure tasks to reject messages with timestamps earlier than this period before the task was created; for example if this is set to `PT1H` and the supervisor creates a task at *2016-01-01T12:00Z*, messages with timestamps earlier than *2016-01-01T11:00Z* will be dropped. This may help prevent concurrency issues if your data stream has late messages and you have multiple pipelines that need to operate on the same segments (e.g. a realtime and a nightly batch ingestion pipeline).|no (default == none)|
|`earlyMessageRejectionPeriod`|ISO8601 Period|Configure tasks to reject messages with timestamps later than this period after the task reached its taskDuration; for example if this is set to `PT1H`, the taskDuration is set to `PT1H` and the supervisor creates a task at *2016-01-01T12:00Z*. Messages with timestamps later than *2016-01-01T14:00Z* will be dropped. **Note:** Tasks sometimes run past their task duration, for example, in cases of supervisor failover. Setting `earlyMessageRejectionPeriod` too low may cause messages to be dropped unexpectedly whenever a task runs past its originally configured task duration.|no (default == none)|
|`Consumer Properties`|Object| a dynamic map used to provide |no (default == none)|

<a name="tuningconfig"></a>

### `tuningConfig`

The `tuningConfig` is optional. If no `tuningConfig` is specified, default parameters are used.

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|`type`| String|The indexing task type, this should always be `rabbit`.|yes|
|`maxRowsInMemory`|Integer|The number of rows to aggregate before persisting. This number is the post-aggregation rows, so it is not equivalent to the number of input events, but the number of aggregated rows that those events result in. This is used to manage the required JVM heap size. Maximum heap memory usage for indexing scales with `maxRowsInMemory * (2 + maxPendingPersists)`.|no (default == 100000)|
|`maxBytesInMemory`|Long| The number of bytes to aggregate in heap memory before persisting. This is based on a rough estimate of memory usage and not actual usage. Normally, this is computed internally and user does not need to set it. The maximum heap memory usage for indexing is `maxBytesInMemory * (2 + maxPendingPersists)`.|no (default == One-sixth of max JVM memory)|
|`maxRowsPerSegment`|Integer|The number of rows to aggregate into a segment; this number is post-aggregation rows. Handoff will happen either if `maxRowsPerSegment` or `maxTotalRows` is hit or every `intermediateHandoffPeriod`, whichever happens earlier.|no (default == 5000000)|
|`maxTotalRows`|Long|The number of rows to aggregate across all segments; this number is post-aggregation rows. Handoff will happen either if `maxRowsPerSegment` or `maxTotalRows` is hit or every `intermediateHandoffPeriod`, whichever happens earlier.|no (default == unlimited)|
|`intermediatePersistPeriod`|ISO8601 Period|The period that determines the rate at which intermediate persists occur.|no (default == PT10M)|
|`maxPendingPersists`|Integer|Maximum number of persists that can be pending but not started. If this limit would be exceeded by a new intermediate persist, ingestion will block until the currently-running persist finishes. Maximum heap memory usage for indexing scales with `maxRowsInMemory * (2 + maxPendingPersists)`.|no (default == 0, meaning one persist can be running concurrently with ingestion, and none can be queued up)|
|`indexSpec`|Object|Tune how data is indexed. See [IndexSpec](#indexspec) for more information.|no|
|`indexSpecForIntermediatePersists`|Object|Defines segment storage format options to be used at indexing time for intermediate persisted temporary segments. This can be used to disable dimension/metric compression on intermediate segments to reduce memory required for final merging. However, disabling compression on intermediate segments might increase page cache use while they are used before getting merged into final segment published, see [IndexSpec](#indexspec) for possible values.| no (default = same as `indexSpec`)|
|`reportParseExceptions`|Boolean|If true, exceptions encountered during parsing will be thrown and will halt ingestion; if false, unparseable rows and fields will be skipped.|no (default == false)|
|`handoffConditionTimeout`|Long| Milliseconds to wait for segment handoff. It must be >= 0, where 0 means to wait forever.| no (default == 0)|
|`resetOffsetAutomatically`|Boolean|Controls behavior when Druid needs to read RabbitMQ messages that are no longer available. Not supported.  |no (default == false)|
|`skipSequenceNumberAvailabilityCheck`|Boolean|Whether to enable checking if the current sequence number is still available in a particular RabbitMQ stream. If set to false, the indexing task will attempt to reset the current sequence number (or not), depending on the value of `resetOffsetAutomatically`.|no (default == false)|
|`workerThreads`|Integer|The number of threads that the supervisor uses to handle requests/responses for worker tasks, along with any other internal asynchronous operation.|no (default == min(10, taskCount))|
|`chatAsync`|Boolean| If true, use asynchronous communication with indexing tasks, and ignore the `chatThreads` parameter. If false, use synchronous communication in a thread pool of size `chatThreads`.  | no (default == true) |
|`chatThreads`|Integer| The number of threads that will be used for communicating with indexing tasks. Ignored if `chatAsync` is `true` (the default).| no (default == min(10, taskCount * replicas))|
|`chatRetries`|Integer|The number of times HTTP requests to indexing tasks will be retried before considering tasks unresponsive.| no (default == 8)|
|`httpTimeout`|ISO8601 Period|How long to wait for a HTTP response from an indexing task.|no (default == PT10S)|
|`shutdownTimeout`|ISO8601 Period|How long to wait for the supervisor to attempt a graceful shutdown of tasks before exiting.|no (default == PT80S)|
|`recordBufferSize`|Integer|Size of the buffer (number of events) used between the RabbitMQ consumers and the main ingestion thread.|no ( default == 100 MB or an estimated 10% of available heap, whichever is smaller.)|
|`recordBufferOfferTimeout`|Integer|Length of time in milliseconds to wait for space to become available in the buffer before timing out.| no (default == 5000)|                                                 |
|`segmentWriteOutMediumFactory`|Object|Segment write-out medium to use when creating segments. See below for more information.|no (not specified by default, the value from `druid.peon.defaultSegmentWriteOutMediumFactory.type` is used)|
|`intermediateHandoffPeriod`|ISO8601 Period|How often the tasks should hand off segments. Handoff will happen either if `maxRowsPerSegment` or `maxTotalRows` is hit or every `intermediateHandoffPeriod`, whichever happens earlier.| no (default == P2147483647D)|
|`logParseExceptions`|Boolean|If true, log an error message when a parsing exception occurs, containing information about the row where the error occurred.|no, default == false|
|`maxParseExceptions`|Integer|The maximum number of parse exceptions that can occur before the task halts ingestion and fails. Overridden if `reportParseExceptions` is set.|no, unlimited default|
|`maxSavedParseExceptions`|Integer|When a parse exception occurs, Druid can keep track of the most recent parse exceptions. "maxSavedParseExceptions" limits how many exception instances will be saved. These saved exceptions will be made available after the task finishes in the [task completion report](tasks.md#task-reports). Overridden if `reportParseExceptions` is set.|no, default == 0|
|`maxRecordsPerPoll`|Integer|The maximum number of records/events to be fetched from buffer per poll. The actual maximum will be `Max(maxRecordsPerPoll, Max(bufferSize, 1))`|no, default = 100|
|`repartitionTransitionDuration`|ISO8601 Period|When shards are split or merged, the supervisor will recompute shard -> task group mappings, and signal any running tasks created under the old mappings to stop early at (current time + `repartitionTransitionDuration`). Stopping the tasks early allows Druid to begin reading from the new shards more quickly. The repartition transition wait time controlled by this property gives the stream additional time to write records to the new shards after the split/merge, which helps avoid the issues with empty shard handling described at https://github.com/apache/druid/issues/7600.|no, (default == PT2M)|
|`offsetFetchPeriod`|ISO8601 Period|How often the supervisor queries RabbitMQ and the indexing tasks to fetch current offsets and calculate lag. If the user-specified value is below the minimum value (`PT5S`), the supervisor ignores the value and uses the minimum value instead.|no (default == PT30S, min == PT5S)|


#### IndexSpec

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|bitmap|Object|Compression format for bitmap indexes. Should be a JSON object. See [Bitmap types](#bitmap-types) below for options.|no (defaults to Roaring)|
|dimensionCompression|String|Compression format for dimension columns. Choose from `LZ4`, `LZF`, or `uncompressed`.|no (default == `LZ4`)|
|metricCompression|String|Compression format for primitive type metric columns. Choose from `LZ4`, `LZF`, `uncompressed`, or `none`.|no (default == `LZ4`)|
|longEncoding|String|Encoding format for metric and dimension columns with type long. Choose from `auto` or `longs`. `auto` encodes the values using sequence number or lookup table depending on column cardinality, and store them with variable size. `longs` stores the value as is with 8 bytes each.|no (default == `longs`)|

##### Bitmap types

For Roaring bitmaps:

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|`type`|String|Must be `roaring`.|yes|

For Concise bitmaps:

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|`type`|String|Must be `concise`.|yes|

#### SegmentWriteOutMediumFactory

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|`type`|String|See [Additional Peon Configuration: SegmentWriteOutMediumFactory](../configuration/index.md#segmentwriteoutmediumfactory) for explanation and available options.|yes|



## Operations

This section describes how some supervisor APIs work in the Rabbit Stream Indexing Service.
For all supervisor APIs, check [Supervisor APIs](../api-reference/supervisor-api.md).

### RabbitMQ Authentication

To authenticate with RabbitMQ securely, you must provide a username and password, as well as configure
a certificate if you aren't using a standard certificate provider.

In order to configure these, use the dynamic configuration provider of the ioConfig
```
  "ioConfig": {
    "type": "rabbit",
    "stream": "api-audit",
    "uri": "rabbitmq-stream://localhost:5552",
    "taskCount": 1,
    "replicas": 1,
    "taskDuration": "PT1H",
    "consumerProperties": {
        "druid.dynamic.config.provider" : {
            "type": "environment",
            "variables": {
                "username": "RABBIT_USERNAME",
                "password": "RABBIT_PASSWORD"
            }
        }
    }
  },
  ```