---
id: kinesis-ingestion
title: "Amazon Kinesis ingestion"
sidebar_label: "Amazon Kinesis"
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

When you enable the Kinesis indexing service, you can configure *supervisors* on the Overlord to manage the creation and lifetime of Kinesis indexing tasks. These indexing tasks read events using Kinesis' own shard and sequence number mechanism to guarantee exactly-once ingestion. The supervisor oversees the state of the indexing tasks to:
  - coordinate handoffs
  - manage failures
  - ensure that scalability and replication requirements are maintained.


To use the Kinesis indexing service, load the `druid-kinesis-indexing-service` core Apache Druid extension (see
[Including Extensions](../../development/extensions.md#loading-extensions)).

> Before you deploy the Kinesis extension to production, read the [Kinesis known issues](#kinesis-known-issues).

## Submitting a Supervisor Spec

To use the Kinesis indexing service, load the `druid-kinesis-indexing-service` extension on both the Overlord and the MiddleManagers. Druid starts a supervisor for a dataSource when you submit a supervisor spec. Submit your supervisor spec to the following endpoint:


`http://<OVERLORD_IP>:<OVERLORD_PORT>/druid/indexer/v1/supervisor`

For example:

```
curl -X POST -H 'Content-Type: application/json' -d @supervisor-spec.json http://localhost:8090/druid/indexer/v1/supervisor
```

Where the file `supervisor-spec.json` contains a Kinesis supervisor spec:

```json
{
  "type": "kinesis",
  "spec": {
    "dataSchema": {
      "dataSource": "metrics-kinesis",
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
      "endpoint": "kinesis.us-east-1.amazonaws.com",
      "taskCount": 1,
      "replicas": 1,
      "taskDuration": "PT1H",
     "recordsPerFetch": 4000,
      "fetchDelayMillis": 0
   },
   "tuningConfig": {
     "type": "kinesis",
     "maxRowsPerSegment": 5000000
   }
  }
}
```


## Supervisor Spec

|Field|Description|Required|
|--------|-----------|---------|
|`type`|The supervisor type; this should always be `kinesis`.|yes|
|`spec`|Container object for the supervisor configuration.|yes|
|`dataSchema`|The schema that will be used by the Kinesis indexing task during ingestion. See [`dataSchema`](../../ingestion/ingestion-spec.md#dataschema).|yes|
|`ioConfig`|A `KinesisSupervisorIOConfig` object for configuring Kafka connection and I/O-related settings for the supervisor and indexing task. See [KinesisSupervisorIOConfig](#kinesissupervisorioconfig) below.|yes|
|`tuningConfig`|A `KinesisSupervisorTuningConfig` object for configuring performance-related settings for the supervisor and indexing tasks. See [KinesisSupervisorTuningConfig](#kinesissupervisortuningconfig) below.|no|

### KinesisSupervisorIOConfig

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|`stream`|String|The Kinesis stream to read.|yes|
|`inputFormat`|Object|[`inputFormat`](../../ingestion/data-formats.md#input-format) to specify how to parse input data. See [Specifying data format](#specifying-data-format) for details about specifying the input format.|yes|
|`endpoint`|String|The AWS Kinesis stream endpoint for a region. You can find a list of endpoints [here](http://docs.aws.amazon.com/general/latest/gr/rande.html#ak_region).|no (default == kinesis.us-east-1.amazonaws.com)|
|`replicas`|Integer|The number of replica sets, where 1 means a single set of tasks (no replication). Replica tasks will always be assigned to different workers to provide resiliency against process failure.|no (default == 1)|
|`taskCount`|Integer|The maximum number of *reading* tasks in a *replica set*. This means that the maximum number of reading tasks will be `taskCount * replicas` and the total number of tasks (*reading* + *publishing*) will be higher than this. See [Capacity Planning](#capacity-planning) below for more details. The number of reading tasks will be less than `taskCount` if `taskCount > {numKinesisShards}`.|no (default == 1)|
|`taskDuration`|ISO8601 Period|The length of time before tasks stop reading and begin publishing their segment.|no (default == PT1H)|
|`startDelay`|ISO8601 Period|The period to wait before the supervisor starts managing tasks.|no (default == PT5S)|
|`period`|ISO8601 Period|How often the supervisor will execute its management logic. Note that the supervisor will also run in response to certain events (such as tasks succeeding, failing, and reaching their taskDuration) so this value specifies the maximum time between iterations.|no (default == PT30S)|
|`useEarliestSequenceNumber`|Boolean|If a supervisor is managing a dataSource for the first time, it will obtain a set of starting sequence numbers from Kinesis. This flag determines whether it retrieves the earliest or latest sequence numbers in Kinesis. Under normal circumstances, subsequent tasks will start from where the previous segments ended so this flag will only be used on first run.|no (default == false)|
|`completionTimeout`|ISO8601 Period|The length of time to wait before declaring a publishing task as failed and terminating it. If this is set too low, your tasks may never publish. The publishing clock for a task begins roughly after `taskDuration` elapses.|no (default == PT6H)|
|`lateMessageRejectionPeriod`|ISO8601 Period|Configure tasks to reject messages with timestamps earlier than this period before the task was created; for example if this is set to `PT1H` and the supervisor creates a task at *2016-01-01T12:00Z*, messages with timestamps earlier than *2016-01-01T11:00Z* will be dropped. This may help prevent concurrency issues if your data stream has late messages and you have multiple pipelines that need to operate on the same segments (e.g. a realtime and a nightly batch ingestion pipeline).|no (default == none)|
|`earlyMessageRejectionPeriod`|ISO8601 Period|Configure tasks to reject messages with timestamps later than this period after the task reached its taskDuration; for example if this is set to `PT1H`, the taskDuration is set to `PT1H` and the supervisor creates a task at *2016-01-01T12:00Z*. Messages with timestamps later than *2016-01-01T14:00Z* will be dropped. **Note:** Tasks sometimes run past their task duration, for example, in cases of supervisor failover. Setting `earlyMessageRejectionPeriod` too low may cause messages to be dropped unexpectedly whenever a task runs past its originally configured task duration.|no (default == none)|
|`recordsPerFetch`|Integer|The number of records to request per call to fetch records from Kinesis. See [Determining fetch settings](#determining-fetch-settings).|no (default == 4000)|
|`fetchDelayMillis`|Integer|Time in milliseconds to wait between subsequent calls to fetch records from Kinesis. See [Determining fetch settings](#determining-fetch-settings).|no (default == 0)|
|`awsAssumedRoleArn`|String|The AWS assumed role to use for additional permissions.|no|
|`awsExternalId`|String|The AWS external id to use for additional permissions.|no|
|`deaggregate`|Boolean|Whether to use the de-aggregate function of the KCL. See below for details.|no|
|`autoScalerConfig`|Object|Defines auto scaling behavior for Kinesis ingest tasks. See [Tasks Autoscaler Properties](#task-autoscaler-properties).|no (default == null)|

#### Task Autoscaler Properties

> Note that Task AutoScaler is currently designated as experimental.

| Property | Description | Required |
| ------------- | ------------- | ------------- |
| `enableTaskAutoScaler` | Enable or disable the auto scaler. When false or absent, Druid disables the `autoScaler` even when `autoScalerConfig` is not null.| no (default == false) |
| `taskCountMax` | Maximum number of Kinesis ingestion tasks. Must be greater than or equal to `taskCountMin`. If greater than `{numKinesisShards}`, the maximum number of reading tasks is `{numKinesisShards}` and `taskCountMax` is ignored.  | yes |
| `taskCountMin` | Minimum number of Kinesis ingestion tasks. When you enable the auto scaler, Druid ignores the value of taskCount in `IOConfig` and uses`taskCountMin` for the initial number of tasks to launch.| yes |
| `minTriggerScaleActionFrequencyMillis` | Minimum time interval between two scale actions | no (default == 600000) |
| `autoScalerStrategy` | The algorithm of `autoScaler`. ONLY `lagBased` is supported for now. See [Lag Based AutoScaler Strategy Related Properties](#lag-based-autoscaler-strategy-related-properties) for details.| no (default == `lagBased`) |

##### Lag Based AutoScaler Strategy Related Properties

The Kinesis indexing service reports lag metrics measured in time milliseconds rather than message count which is used by Kafka.

| Property | Description | Required |
| ------------- | ------------- | ------------- |
| `lagCollectionIntervalMillis` | Period of lag points collection.  | no (default == 30000) |
| `lagCollectionRangeMillis` | The total time window of lag collection, Use with `lagCollectionIntervalMillis`，it means that in the recent `lagCollectionRangeMillis`, collect lag metric points every `lagCollectionIntervalMillis`. | no (default == 600000) |
| `scaleOutThreshold` | The Threshold of scale out action | no (default == 6000000) |
| `triggerScaleOutFractionThreshold` | If `triggerScaleOutFractionThreshold` percent of lag points are higher than `scaleOutThreshold`, then do scale out action. | no (default == 0.3) |
| `scaleInThreshold` | The Threshold of scale in action | no (default == 1000000) |
| `triggerScaleInFractionThreshold` | If `triggerScaleInFractionThreshold` percent of lag points are lower than `scaleOutThreshold`, then do scale in action. | no (default == 0.9) |
| `scaleActionStartDelayMillis` | Number of milliseconds to delay after the supervisor starts before the first scale logic check. | no (default == 300000) |
| `scaleActionPeriodMillis` | Frequency in milliseconds to check if a scale action is triggered | no (default == 60000) |
| `scaleInStep` | Number of tasks to reduce at a time when scaling down | no (default == 1) |
| `scaleOutStep` | Number of tasks to add at a time when scaling out | no (default == 2) |

The following example demonstrates a supervisor spec with `lagBased` autoScaler enabled:
```json
{
  "type": "kinesis",
  "dataSchema": {
    "dataSource": "metrics-kinesis",
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
    "autoScalerConfig": {
      "enableTaskAutoScaler": true,
      "taskCountMax": 6,
      "taskCountMin": 2,
      "minTriggerScaleActionFrequencyMillis": 600000,
      "autoScalerStrategy": "lagBased",
      "lagCollectionIntervalMillis": 30000,
      "lagCollectionRangeMillis": 600000,
      "scaleOutThreshold": 600000,
      "triggerScaleOutFractionThreshold": 0.3,
      "scaleInThreshold": 100000,
      "triggerScaleInFractionThreshold": 0.9,
      "scaleActionStartDelayMillis": 300000,
      "scaleActionPeriodMillis": 60000,
      "scaleInStep": 1,
      "scaleOutStep": 2
    },
    "inputFormat": {
      "type": "json"
    },
    "endpoint": "kinesis.us-east-1.amazonaws.com",
    "taskCount": 1,
    "replicas": 1,
    "taskDuration": "PT1H",
    "recordsPerFetch": 4000,
    "fetchDelayMillis": 0
  },
  "tuningConfig": {
    "type": "kinesis",
    "maxRowsPerSegment": 5000000
  }
}
```

#### Specifying data format

Kinesis indexing service supports both [`inputFormat`](../../ingestion/data-formats.md#input-format) and [`parser`](../../ingestion/data-formats.md#parser) to specify the data format.
Use the `inputFormat` to specify the data format for Kinesis indexing service unless you need a format only supported by the legacy `parser`.

Supported `inputFormat`s include:
- `csv`
- `delimited`
- `json`
- `avro_stream`
- `avro_ocf`
- `protobuf`

For more information, see [Data formats](../../ingestion/data-formats.md). You can also read [`thrift`](../extensions-contrib/thrift.md) formats using `parser`.

<a name="tuningconfig"></a>

### KinesisSupervisorTuningConfig

The `tuningConfig` is optional. If no `tuningConfig` is specified, default parameters are used.

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|`type`| String|The indexing task type, this should always be `kinesis`.|yes|
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
|`resetOffsetAutomatically`|Boolean|Controls behavior when Druid needs to read Kinesis messages that are no longer available.<br/><br/>If false, the exception will bubble up, which will cause your tasks to fail and ingestion to halt. If this occurs, manual intervention is required to correct the situation; potentially using the [Reset Supervisor API](../../operations/api-reference.md#supervisors). This mode is useful for production, since it will make you aware of issues with ingestion.<br/><br/>If true, Druid will automatically reset to the earlier or latest sequence number available in Kinesis, based on the value of the `useEarliestSequenceNumber` property (earliest if true, latest if false). Please note that this can lead to data being _DROPPED_ (if `useEarliestSequenceNumber` is false) or _DUPLICATED_ (if `useEarliestSequenceNumber` is true) without your knowledge. Messages will be logged indicating that a reset has occurred, but ingestion will continue. This mode is useful for non-production situations, since it will make Druid attempt to recover from problems automatically, even if they lead to quiet dropping or duplicating of data.|no (default == false)|
|`skipSequenceNumberAvailabilityCheck`|Boolean|Whether to enable checking if the current sequence number is still available in a particular Kinesis shard. If set to false, the indexing task will attempt to reset the current sequence number (or not), depending on the value of `resetOffsetAutomatically`.|no (default == false)|
|`workerThreads`|Integer|The number of threads that the supervisor uses to handle requests/responses for worker tasks, along with any other internal asynchronous operation.|no (default == min(10, taskCount))|
|`chatThreads`|Integer|The number of threads that will be used for communicating with indexing tasks.|no (default == min(10, taskCount * replicas))|
|`chatRetries`|Integer|The number of times HTTP requests to indexing tasks will be retried before considering tasks unresponsive.| no (default == 8)|
|`httpTimeout`|ISO8601 Period|How long to wait for a HTTP response from an indexing task.|no (default == PT10S)|
|`shutdownTimeout`|ISO8601 Period|How long to wait for the supervisor to attempt a graceful shutdown of tasks before exiting.|no (default == PT80S)|
|`recordBufferSize`|Integer|Size of the buffer (number of events) used between the Kinesis fetch threads and the main ingestion thread.|no (default == 10000)|
|`recordBufferOfferTimeout`|Integer|Length of time in milliseconds to wait for space to become available in the buffer before timing out.| no (default == 5000)|
|`recordBufferFullWait`|Integer|Length of time in milliseconds to wait for the buffer to drain before attempting to fetch records from Kinesis again.|no (default == 5000)|
|`fetchThreads`|Integer|Size of the pool of threads fetching data from Kinesis. There is no benefit in having more threads than Kinesis shards.|no (default == procs * 2, where "procs" is the number of processors on the server that the task is running on)                                                                  |
|`segmentWriteOutMediumFactory`|Object|Segment write-out medium to use when creating segments. See below for more information.|no (not specified by default, the value from `druid.peon.defaultSegmentWriteOutMediumFactory.type` is used)|
|`intermediateHandoffPeriod`|ISO8601 Period|How often the tasks should hand off segments. Handoff will happen either if `maxRowsPerSegment` or `maxTotalRows` is hit or every `intermediateHandoffPeriod`, whichever happens earlier.| no (default == P2147483647D)|
|`logParseExceptions`|Boolean|If true, log an error message when a parsing exception occurs, containing information about the row where the error occurred.|no, default == false|
|`maxParseExceptions`|Integer|The maximum number of parse exceptions that can occur before the task halts ingestion and fails. Overridden if `reportParseExceptions` is set.|no, unlimited default|
|`maxSavedParseExceptions`|Integer|When a parse exception occurs, Druid can keep track of the most recent parse exceptions. "maxSavedParseExceptions" limits how many exception instances will be saved. These saved exceptions will be made available after the task finishes in the [task completion report](../../ingestion/tasks.md#task-reports). Overridden if `reportParseExceptions` is set.|no, default == 0|
|`maxRecordsPerPoll`|Integer|The maximum number of records/events to be fetched from buffer per poll. The actual maximum will be `Max(maxRecordsPerPoll, Max(bufferSize, 1))`|no, default == 100|
|`repartitionTransitionDuration`|ISO8601 Period|When shards are split or merged, the supervisor will recompute shard -> task group mappings, and signal any running tasks created under the old mappings to stop early at (current time + `repartitionTransitionDuration`). Stopping the tasks early allows Druid to begin reading from the new shards more quickly. The repartition transition wait time controlled by this property gives the stream additional time to write records to the new shards after the split/merge, which helps avoid the issues with empty shard handling described at https://github.com/apache/druid/issues/7600.|no, (default == PT2M)|
|`offsetFetchPeriod`|ISO8601 Period|How often the supervisor queries Kinesis and the indexing tasks to fetch current offsets and calculate lag. If the user-specified value is below the minimum value (`PT5S`), the supervisor ignores the value and uses the minimum value instead.|no (default == PT30S, min == PT5S)|
|`useListShards`|Boolean|Indicates if `listShards` API of AWS Kinesis SDK can be used to prevent `LimitExceededException` during ingestion. Please note that the necessary `IAM` permissions must be set for this to work.|no (default == false)|

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
|`compressRunOnSerialization`|Boolean|Use a run-length encoding where it is estimated as more space efficient.|no (default == `true`)|

For Concise bitmaps:

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|`type`|String|Must be `concise`.|yes|

#### SegmentWriteOutMediumFactory

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|`type`|String|See [Additional Peon Configuration: SegmentWriteOutMediumFactory](../../configuration/index.md#segmentwriteoutmediumfactory) for explanation and available options.|yes|

## Operations

This section describes how some supervisor APIs work in Kinesis Indexing Service.
For all supervisor APIs, check [Supervisor APIs](../../operations/api-reference.md#supervisors).

### AWS Authentication

To authenticate with AWS, you must provide your AWS access key and AWS secret key via `runtime.properties`, for example:
```
-Ddruid.kinesis.accessKey=123 -Ddruid.kinesis.secretKey=456
```

The AWS access key ID and secret access key are used for Kinesis API requests. If this is not provided, the service will
look for credentials set in environment variables, via [Web Identity Token](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_providers_oidc.html), in the default profile configuration file, and from the EC2 instance
profile provider (in this order).

To ingest data from Kinesis, ensure that the policy attached to your IAM role contains the necessary permissions.
The permissions needed depend on the value of `useListShards`.

If the `useListShards` flag is set to `true`, you need following permissions:

* `ListStreams`: required to list your data streams
* `Get*`: required for `GetShardIterator`
* `GetRecords`: required to get data records from a data stream's shard
* `ListShards` : required to get the shards for a stream of interest

**Example policy**

```
[
  {
    "Effect": "Allow",
    "Action": ["kinesis:List*"],
    "Resource": ["*"]
  },
  {
    "Effect": "Allow",
    "Action": ["kinesis:Get*"],
    "Resource": [<ARN for shards to be ingested>]
  }
]
```

If the `useListShards` flag is set to `false`, you need following permissions:

* `ListStreams`: required to list your data streams
* `Get*`: required for `GetShardIterator`
* `GetRecords`: required to get data records from a data stream's shard
* `DescribeStream`: required to describe the specified data stream

**Example policy**

```    
[
  {
    "Effect": "Allow",
    "Action": ["kinesis:ListStreams"],
    "Resource": ["*"]
  },
  {
    "Effect": "Allow",
    "Action": ["kinesis:DescribeStreams"],
    "Resource": ["*"]
  },
  {
    "Effect": "Allow",
    "Action": ["kinesis:Get*"],
    "Resource": [<ARN for shards to be ingested>]
  }
]
```

### Getting Supervisor Status Report

`GET /druid/indexer/v1/supervisor/<supervisorId>/status` returns a snapshot report of the current state of the tasks
managed by the given supervisor. This includes the latest sequence numbers as reported by Kinesis. Unlike the Kafka
Indexing Service, Kinesis reports lag metrics measured in time difference in milliseconds between the current sequence number and latest sequence number, rather than message count.

The status report also contains the supervisor's state and a list of recently thrown exceptions (reported as
`recentErrors`, whose max size can be controlled using the `druid.supervisor.maxStoredExceptionEvents` configuration).
There are two fields related to the supervisor's state - `state` and `detailedState`. The `state` field will always be
one of a small number of generic states that are applicable to any type of supervisor, while the `detailedState` field
will contain a more descriptive, implementation-specific state that may provide more insight into the supervisor's
activities than the generic `state` field.

The list of possible `state` values are: [`PENDING`, `RUNNING`, `SUSPENDED`, `STOPPING`, `UNHEALTHY_SUPERVISOR`, `UNHEALTHY_TASKS`]

The list of `detailedState` values and their corresponding `state` mapping is as follows:

|Detailed State|Corresponding State|Description|
|--------------|-------------------|-----------|
|UNHEALTHY_SUPERVISOR|UNHEALTHY_SUPERVISOR|The supervisor has encountered errors on the past `druid.supervisor.unhealthinessThreshold` iterations|
|UNHEALTHY_TASKS|UNHEALTHY_TASKS|The last `druid.supervisor.taskUnhealthinessThreshold` tasks have all failed|
|UNABLE_TO_CONNECT_TO_STREAM|UNHEALTHY_SUPERVISOR|The supervisor is encountering connectivity issues with Kinesis and has not successfully connected in the past|
|LOST_CONTACT_WITH_STREAM|UNHEALTHY_SUPERVISOR|The supervisor is encountering connectivity issues with Kinesis but has successfully connected in the past|
|PENDING (first iteration only)|PENDING|The supervisor has been initialized and hasn't started connecting to the stream|
|CONNECTING_TO_STREAM (first iteration only)|RUNNING|The supervisor is trying to connect to the stream and update partition data|
|DISCOVERING_INITIAL_TASKS (first iteration only)|RUNNING|The supervisor is discovering already-running tasks|
|CREATING_TASKS (first iteration only)|RUNNING|The supervisor is creating tasks and discovering state|
|RUNNING|RUNNING|The supervisor has started tasks and is waiting for taskDuration to elapse|
|SUSPENDED|SUSPENDED|The supervisor has been suspended|
|STOPPING|STOPPING|The supervisor is stopping|

On each iteration of the supervisor's run loop, the supervisor completes the following tasks in sequence:
  1) Fetch the list of shards from Kinesis and determine the starting sequence number for each shard (either based on the
  last processed sequence number if continuing, or starting from the beginning or ending of the stream if this is a new stream).
  2) Discover any running indexing tasks that are writing to the supervisor's datasource and adopt them if they match
  the supervisor's configuration, else signal them to stop.
  3) Send a status request to each supervised task to update our view of the state of the tasks under our supervision.
  4) Handle tasks that have exceeded `taskDuration` and should transition from the reading to publishing state.
  5) Handle tasks that have finished publishing and signal redundant replica tasks to stop.
  6) Handle tasks that have failed and clean up the supervisor's internal state.
  7) Compare the list of healthy tasks to the requested `taskCount` and `replicas` configurations and create additional tasks if required.

The `detailedState` field will show additional values (those marked with "first iteration only") the first time the
supervisor executes this run loop after startup or after resuming from a suspension. This is intended to surface
initialization-type issues, where the supervisor is unable to reach a stable state (perhaps because it can't connect to
Kinesis, it can't read from the stream, or it can't communicate with existing tasks). Once the supervisor is stable -
that is, once it has completed a full execution without encountering any issues - `detailedState` will show a `RUNNING`
state until it is stopped, suspended, or hits a failure threshold and transitions to an unhealthy state.

### Updating Existing Supervisors

`POST /druid/indexer/v1/supervisor` can be used to update existing supervisor spec.
Calling this endpoint when there is already an existing supervisor for the same dataSource will cause:

- The running supervisor to signal its managed tasks to stop reading and begin publishing.
- The running supervisor to exit.
- A new supervisor to be created using the configuration provided in the request body. This supervisor will retain the
existing publishing tasks and will create new tasks starting at the sequence numbers the publishing tasks ended on.

Seamless schema migrations can thus be achieved by simply submitting the new schema using this endpoint.

### Suspending and Resuming Supervisors

You can suspend and resume a supervisor using `POST /druid/indexer/v1/supervisor/<supervisorId>/suspend` and `POST /druid/indexer/v1/supervisor/<supervisorId>/resume`, respectively.

Note that the supervisor itself will still be operating and emitting logs and metrics,
it will just ensure that no indexing tasks are running until the supervisor is resumed.

### Resetting Supervisors

The `POST /druid/indexer/v1/supervisor/<supervisorId>/reset` operation clears stored 
sequence numbers, causing the supervisor to start reading from either the earliest or 
latest sequence numbers in Kinesis (depending on the value of `useEarliestSequenceNumber`). 
After clearing stored sequence numbers, the supervisor kills and recreates active tasks, 
so that tasks begin reading from valid sequence numbers. 

Use care when using this operation! Resetting the supervisor may cause Kinesis messages 
to be skipped or read twice, resulting in missing or duplicate data. 

The reason for using this operation is to recover from a state in which the supervisor 
ceases operating due to missing sequence numbers. The indexing service keeps track of the latest 
persisted sequence number in order to provide exactly-once ingestion guarantees across 
tasks. 

Subsequent tasks must start reading from where the previous task completed in 
order for the generated segments to be accepted. If the messages at the expected starting sequence numbers are 
no longer available in Kinesis (typically because the message retention period has elapsed or the topic was 
removed and re-created) the supervisor will refuse to start and in-flight tasks will fail. This operation 
enables you to recover from this condition. 

Note that the supervisor must be running for this endpoint to be available.

### Terminating Supervisors

The `POST /druid/indexer/v1/supervisor/<supervisorId>/terminate` operation terminates a supervisor and causes
all associated indexing tasks managed by this supervisor to immediately stop and begin
publishing their segments. This supervisor will still exist in the metadata store and its history may be retrieved
with the supervisor history API, but will not be listed in the 'get supervisors' API response nor can its configuration
or status report be retrieved. The only way this supervisor can start again is by submitting a functioning supervisor
spec to the create API.

### Capacity Planning

Kinesis indexing tasks run on MiddleManagers and are thus limited by the resources available in the MiddleManager
cluster. In particular, you should make sure that you have sufficient worker capacity (configured using the
`druid.worker.capacity` property) to handle the configuration in the supervisor spec. Note that worker capacity is
shared across all types of indexing tasks, so you should plan your worker capacity to handle your total indexing load
(e.g. batch processing, realtime tasks, merging tasks, etc.). If your workers run out of capacity, Kinesis indexing tasks
will queue and wait for the next available worker. This may cause queries to return partial results but will not result
in data loss (assuming the tasks run before Kinesis purges those sequence numbers).

A running task will normally be in one of two states: *reading* or *publishing*. A task will remain in reading state for
`taskDuration`, at which point it will transition to publishing state. A task will remain in publishing state for as long
as it takes to generate segments, push segments to deep storage, and have them be loaded and served by a Historical process
(or until `completionTimeout` elapses).

The number of reading tasks is controlled by `replicas` and `taskCount`. In general, there will be `replicas * taskCount`
reading tasks, the exception being if taskCount > {numKinesisShards} in which case {numKinesisShards} tasks will
be used instead. When `taskDuration` elapses, these tasks will transition to publishing state and `replicas * taskCount`
new reading tasks will be created. Therefore to allow for reading tasks and publishing tasks to run concurrently, there
should be a minimum capacity of:

```
workerCapacity = 2 * replicas * taskCount
```

This value is for the ideal situation in which there is at most one set of tasks publishing while another set is reading.
In some circumstances, it is possible to have multiple sets of tasks publishing simultaneously. This would happen if the
time-to-publish (generate segment, push to deep storage, loaded on Historical) > `taskDuration`. This is a valid
scenario (correctness-wise) but requires additional worker capacity to support. In general, it is a good idea to have
`taskDuration` be large enough that the previous set of tasks finishes publishing before the current set begins.

### Supervisor Persistence

When a supervisor spec is submitted via the `POST /druid/indexer/v1/supervisor` endpoint, it is persisted in the
configured metadata database. There can only be a single supervisor per dataSource, and submitting a second spec for
the same dataSource will overwrite the previous one.

When an Overlord gains leadership, either by being started or as a result of another Overlord failing, it will spawn
a supervisor for each supervisor spec in the metadata database. The supervisor will then discover running Kinesis indexing
tasks and will attempt to adopt them if they are compatible with the supervisor's configuration. If they are not
compatible because they have a different ingestion spec or shard allocation, the tasks will be killed and the
supervisor will create a new set of tasks. In this way, the supervisors are persistent across Overlord restarts and
fail-overs.

A supervisor is stopped via the `POST /druid/indexer/v1/supervisor/<supervisorId>/terminate` endpoint. This places a
tombstone marker in the database (to prevent the supervisor from being reloaded on a restart) and then gracefully
shuts down the currently running supervisor. When a supervisor is shut down in this way, it will instruct its
managed tasks to stop reading and begin publishing their segments immediately. The call to the shutdown endpoint will
return after all tasks have been signalled to stop but before the tasks finish publishing their segments.

### Schema/Configuration Changes

Schema and configuration changes are handled by submitting the new supervisor spec via the same
`POST /druid/indexer/v1/supervisor` endpoint used to initially create the supervisor. The Overlord will initiate a
graceful shutdown of the existing supervisor which will cause the tasks being managed by that supervisor to stop reading
and begin publishing their segments. A new supervisor will then be started which will create a new set of tasks that
will start reading from the sequence numbers where the previous now-publishing tasks left off, but using the updated schema.
In this way, configuration changes can be applied without requiring any pause in ingestion.

### Deployment Notes

#### On the Subject of Segments

Each Kinesis Indexing Task puts events consumed from Kinesis Shards assigned to it in a single segment for each segment
granular interval until maxRowsPerSegment, maxTotalRows or intermediateHandoffPeriod limit is reached, at this point a new shard
for this segment granularity is created for further events. Kinesis Indexing Task also does incremental hand-offs which
means that all the segments created by a task will not be held up till the task duration is over. As soon as maxRowsPerSegment,
maxTotalRows or intermediateHandoffPeriod limit is hit, all the segments held by the task at that point in time will be handed-off
and new set of segments will be created for further events. This means that the task can run for longer durations of time
without accumulating old segments locally on Middle Manager processes and it is encouraged to do so.

Kinesis Indexing Service may still produce some small segments. Lets say the task duration is 4 hours, segment granularity
is set to an HOUR and Supervisor was started at 9:10 then after 4 hours at 13:10, new set of tasks will be started and
events for the interval 13:00 - 14:00 may be split across previous and new set of tasks. If you see it becoming a problem then
one can schedule re-indexing tasks be run to merge segments together into new segments of an ideal size (in the range of ~500-700 MB per segment).
Details on how to optimize the segment size can be found on [Segment size optimization](../../operations/segment-optimization.md).
There is also ongoing work to support automatic segment compaction of sharded segments as well as compaction not requiring
Hadoop (see [here](https://github.com/apache/druid/pull/5102)).

### Determining Fetch Settings
Internally, the Kinesis Indexing Service uses the Kinesis Record Supplier abstraction for fetching Kinesis data records and storing the records
locally. The way the Kinesis Record Supplier fetches records is to have a separate thread run the fetching operation per each Kinesis Shard, the
max number of threads is determined by `fetchThreads`. For example, a Kinesis stream with 3 shards will have 3 threads, each fetching from a shard separately.
There is a delay between each fetching operation, which is controlled by `fetchDelayMillis`. The maximum number of records to be fetched per thread per
operation is controlled by `recordsPerFetch`. Note that this is not the same as `maxRecordsPerPoll`.

The records fetched by each thread will be pushed to a queue in the order that they are fetched. The records are stored in this queue until `poll()` is called
by either the supervisor or the indexing task. `poll()` will attempt to drain the internal buffer queue up to a limit of `max(maxRecordsPerPoll, q.size())`.
Here `maxRecordsPerPoll` controls the theoretical maximum records to drain out of the buffer queue, so setting this parameter to a reasonable value is essential
in preventing the queue from overflowing or memory exceeding heap size.

Kinesis places the following restrictions on calls to fetch records:

- Each data record can be up to 1 MB in size.
- Each shard can support up to 5 transactions per second for reads.
- Each shard can read up to 2 MB per second.
- The maximum size of data that GetRecords can return is 10 MB.

Values for `recordsPerFetch` and `fetchDelayMillis` should be chosen to maximize throughput under the above constraints.
The values that you choose will depend on the average size of a record and the number of consumers you have reading from
a given shard (which will be `replicas` unless you have other consumers also reading from this Kinesis stream).

If the above limits are violated, AWS will throw ProvisionedThroughputExceededException errors on subsequent calls to
read data. When this happens, the Kinesis indexing service will pause by `fetchDelayMillis` and then attempt the call
again.

Internally, each indexing task maintains a buffer that stores the fetched but not yet processed record. `recordsPerFetch` and `fetchDelayMillis`
control this behavior. The number of records that the indexing task fetch from the buffer is controlled by `maxRecordsPerPoll`, which
determines the number of records to be processed per each ingestion loop in the task.

## Deaggregation
See [issue](https://github.com/apache/druid/issues/6714)

The Kinesis indexing service supports de-aggregation of multiple rows packed into a single record by the Kinesis
Producer Library's aggregate method for more efficient data transfer.

To enable this feature, set `deaggregate` to true when submitting a supervisor-spec.

## Resharding

When changing the shard count for a Kinesis stream, there will be a window of time around the resharding operation with early shutdown of Kinesis ingestion tasks and possible task failures.

The early shutdowns and task failures are expected, and they occur because the supervisor will update the shard -> task group mappings as shards are closed and fully read, to ensure that tasks are not running 
with an assignment of closed shards that have been fully read and to ensure a balanced distribution of active shards across tasks. 

This window with early task shutdowns and possible task failures will conclude when:
- All closed shards have been fully read and the Kinesis ingestion tasks have published the data from those shards, committing the "closed" state to metadata storage
- Any remaining tasks that had inactive shards in the assignment have been shutdown (these tasks would have been created before the closed shards were completely drained)

## Kinesis known issues

Before you deploy the Kinesis extension to production, consider the following known issues:

- Avoid implementing more than one Kinesis supervisor that read from the same Kinesis stream for ingestion. Kinesis has a per-shard read throughput limit and having multiple supervisors on the same stream can reduce available read throughput for an individual Supervisor's tasks. Additionally, multiple Supervisors ingesting to the same Druid Datasource can cause increased contention for locks on the Datasource.
- The only way to change the stream reset policy is to submit a new ingestion spec and set up a new supervisor.
- If ingestion tasks get stuck, the supervisor does not automatically recover. You should monitor ingestion tasks and investigate if your ingestion falls behind.
- A Kinesis supervisor can sometimes compare the checkpoint offset to retention window of the stream to see if it has fallen behind. These checks fetch the earliest sequence number for Kinesis which can result in `IteratorAgeMilliseconds` becoming very high in AWS CloudWatch.
