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

When you enable the Kinesis indexing service, you can configure supervisors on the Overlord to manage the creation and lifetime of Kinesis indexing tasks. These indexing tasks read events using Kinesis' own shard and sequence number mechanism to guarantee exactly-once ingestion. The supervisor oversees the state of the indexing tasks to coordinate handoffs, manage failures, and ensure that scalability and replication requirements are maintained.

This topic contains configuration reference information for the Kinesis indexing service supervisor for Apache Druid.

## Setup

To use the Kinesis indexing service, you must first load the `druid-kinesis-indexing-service` core extension on both the Overlord and the Middle Manager. See [Loading extensions](../../configuration/extensions.md#loading-extensions) for more information.
Review the [Kinesis known issues](#kinesis-known-issues) before deploying the `druid-kinesis-indexing-service` extension to production.

## Supervisor spec

The following table outlines the high-level configuration options for the Kinesis supervisor object. 
See [Supervisor API](../../api-reference/supervisor-api.md) for more information.

|Property|Type|Description|Required|
|--------|----|-----------|--------|
|`type`|String|The supervisor type; this should always be `kinesis`.|Yes|
|`spec`|Object|The container object for the supervisor configuration.|Yes|
|`ioConfig`|Object|The [I/O configuration](#supervisor-io-configuration) object for configuring Kafka connection and I/O-related settings for the supervisor and indexing task.|Yes|
|`dataSchema`|Object|The schema used by the Kinesis indexing task during ingestion. See [`dataSchema`](../../ingestion/ingestion-spec.md#dataschema) for more information.|Yes|
|`tuningConfig`|Object|The [tuning configuration](#supervisor-tuning-configuration) object for configuring performance-related settings for the supervisor and indexing tasks.|No|

Druid starts a new supervisor when you define a supervisor spec.
To create a supervisor, send a `POST` request to the `/druid/indexer/v1/supervisor` endpoint.
Once created, the supervisor persists in the configured metadata database. There can only be a single supervisor per datasource, and submitting a second spec for the same datasource overwrites the previous one.

When an Overlord gains leadership, either by being started or as a result of another Overlord failing, it spawns
a supervisor for each supervisor spec in the metadata database. The supervisor then discovers running Kinesis indexing
tasks and attempts to adopt them if they are compatible with the supervisor's configuration. If they are not
compatible because they have a different ingestion spec or shard allocation, the tasks are killed and the
supervisor creates a new set of tasks. In this way, the supervisors persist across Overlord restarts and failovers.

The following example shows how to submit a supervisor spec for a stream with the name `KinesisStream`.
In this example, `http://SERVICE_IP:SERVICE_PORT` is a placeholder for the server address of deployment and the service port.

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->
```shell
curl -X POST "http://SERVICE_IP:SERVICE_PORT/druid/indexer/v1/supervisor" \
-H "Content-Type: application/json" \
-d '{
  "type": "kinesis",
  "spec": {
    "ioConfig": {
      "type": "kinesis",
      "stream": "KinesisStream",
      "inputFormat": {
        "type": "json"
      },
      "useEarliestSequenceNumber": true
    },
    "tuningConfig": {
      "type": "kinesis"
    },
    "dataSchema": {
      "dataSource": "KinesisStream",
      "timestampSpec": {
        "column": "timestamp",
        "format": "iso"
      },
      "dimensionsSpec": {
        "dimensions": [
          "isRobot",
          "channel",
          "flags",
          "isUnpatrolled",
          "page",
          "diffUrl",
          {
            "type": "long",
            "name": "added"
          },
          "comment",
          {
            "type": "long",
            "name": "commentLength"
          },
          "isNew",
          "isMinor",
          {
            "type": "long",
            "name": "delta"
          },
          "isAnonymous",
          "user",
          {
            "type": "long",
            "name": "deltaBucket"
          },
          {
            "type": "long",
            "name": "deleted"
          },
          "namespace",
          "cityName",
          "countryName",
          "regionIsoCode",
          "metroCode",
          "countryIsoCode",
          "regionName"
        ]
      },
      "granularitySpec": {
        "queryGranularity": "none",
        "rollup": false,
        "segmentGranularity": "hour"
      }
    }
  }
}'
```
<!--HTTP-->
```HTTP
POST /druid/indexer/v1/supervisor
HTTP/1.1
Host: http://SERVICE_IP:SERVICE_PORT
Content-Type: application/json

{
  "type": "kinesis",
  "spec": {
    "ioConfig": {
      "type": "kinesis",
      "stream": "KinesisStream",
      "inputFormat": {
        "type": "json"
      },
      "useEarliestSequenceNumber": true
    },
    "tuningConfig": {
      "type": "kinesis"
    },
    "dataSchema": {
      "dataSource": "KinesisStream",
      "timestampSpec": {
        "column": "timestamp",
        "format": "iso"
      },
      "dimensionsSpec": {
        "dimensions": [
          "isRobot",
          "channel",
          "flags",
          "isUnpatrolled",
          "page",
          "diffUrl",
          {
            "type": "long",
            "name": "added"
          },
          "comment",
          {
            "type": "long",
            "name": "commentLength"
          },
          "isNew",
          "isMinor",
          {
            "type": "long",
            "name": "delta"
          },
          "isAnonymous",
          "user",
          {
            "type": "long",
            "name": "deltaBucket"
          },
          {
            "type": "long",
            "name": "deleted"
          },
          "namespace",
          "cityName",
          "countryName",
          "regionIsoCode",
          "metroCode",
          "countryIsoCode",
          "regionName"
        ]
      },
      "granularitySpec": {
        "queryGranularity": "none",
        "rollup": false,
        "segmentGranularity": "hour"
      }
    }
  }
}
```
<!--END_DOCUSAURUS_CODE_TABS-->

## Supervisor I/O configuration

The following table outlines the configuration options for `ioConfig`:

|Property|Type|Description|Required|Default|
|--------|----|-----------|--------|-------|
|`stream`|String|The Kinesis stream to read.|Yes||
|`inputFormat`|Object|The [input format](../../ingestion/data-formats.md#input-format) to specify how to parse input data. See [Specify data format](#specify-data-format) for more information.|Yes||
|`endpoint`|String|The AWS Kinesis stream endpoint for a region. You can find a list of endpoints in the [AWS service endpoints](http://docs.aws.amazon.com/general/latest/gr/rande.html#ak_region) document.|No|`kinesis.us-east-1.amazonaws.com`|
|`replicas`|Integer|The number of replica sets, where 1 is a single set of tasks (no replication). Druid always assigns replicate tasks to different workers to provide resiliency against process failure.|No|1|
|`taskCount`|Integer|The maximum number of reading tasks in a replica set. Multiply `taskCount` and `replicas` to measure the maximum number of reading tasks. <br />The total number of tasks (reading and publishing) is higher than the maximum number of reading tasks. See [Capacity planning](#capacity-planning) for more details. When `taskCount > {numKinesisShards}`, the actual number of reading tasks is less than the `taskCount` value.|No|1|
|`taskDuration`|ISO 8601 period|The length of time before tasks stop reading and begin publishing their segments.|No|PT1H|
|`startDelay`|ISO 8601 period|The period to wait before the supervisor starts managing tasks.|No|PT5S|
|`period`|ISO 8601 period|Determines how often the supervisor executes its management logic. Note that the supervisor also runs in response to certain events, such as tasks succeeding, failing, and reaching their task duration, so this value specifies the maximum time between iterations.|No|PT30S|
|`useEarliestSequenceNumber`|Boolean|If a supervisor is managing a datasource for the first time, it obtains a set of starting sequence numbers from Kinesis. This flag determines whether a supervisor retrieves the earliest or latest sequence numbers in Kinesis. Under normal circumstances, subsequent tasks start from where the previous segments ended so this flag is only used on the first run.|No|`false`|
|`completionTimeout`|ISO 8601 period|The length of time to wait before Druid declares a publishing task has failed and terminates it. If this is set too low, your tasks may never publish. The publishing clock for a task begins roughly after `taskDuration` elapses.|No|PT6H|
|`lateMessageRejectionPeriod`|ISO 8601 period|Configure tasks to reject messages with timestamps earlier than this period before the task is created. For example, if `lateMessageRejectionPeriod` is set to `PT1H` and the supervisor creates a task at `2016-01-01T12:00Z`, messages with timestamps earlier than `2016-01-01T11:00Z` are dropped. This may help prevent concurrency issues if your data stream has late messages and you have multiple pipelines that need to operate on the same segments, such as a streaming and a nightly batch ingestion pipeline.|No||
|`earlyMessageRejectionPeriod`|ISO 8601 period|Configure tasks to reject messages with timestamps later than this period after the task reached its `taskDuration`. For example, if `earlyMessageRejectionPeriod` is set to `PT1H`, the `taskDuration` is set to `PT1H` and the supervisor creates a task at `2016-01-01T12:00Z`. Messages with timestamps later than `2016-01-01T14:00Z` are dropped. **Note:** Tasks sometimes run past their task duration, for example, in cases of supervisor failover. Setting `earlyMessageRejectionPeriod` too low may cause messages to be dropped unexpectedly whenever a task runs past its originally configured task duration.|No||
|`recordsPerFetch`|Integer|The number of records to request per call to fetch records from Kinesis.|No| See [Determine fetch settings](#determine-fetch-settings) for defaults.|
|`fetchDelayMillis`|Integer|Time in milliseconds to wait between subsequent calls to fetch records from Kinesis. See [Determine fetch settings](#determine-fetch-settings).|No|0|
|`awsAssumedRoleArn`|String|The AWS assumed role to use for additional permissions.|No||
|`awsExternalId`|String|The AWS external ID to use for additional permissions.|No||
|`deaggregate`|Boolean|Whether to use the deaggregate function of the Kinesis Client Library (KCL).|No||
|`autoScalerConfig`|Object|Defines autoscaling behavior for Kinesis ingest tasks. See [Task autoscaler properties](#task-autoscaler-properties) for more information.|No|null|

### Task autoscaler properties

The following table outlines the configuration options for `autoScalerConfig`:

|Property|Description|Required|Default|
|--------|-----------|--------|-------|
|`enableTaskAutoScaler`|Enables the auto scaler. If not specified, Druid disables the auto scaler even when `autoScalerConfig` is not null.|No|`false`|
|`taskCountMax`|Maximum number of Kinesis ingestion tasks. Must be greater than or equal to `taskCountMin`. If greater than `{numKinesisShards}`, Druid sets the maximum number of reading tasks to `{numKinesisShards}` and ignores `taskCountMax`.|Yes||
|`taskCountMin`|Minimum number of Kinesis ingestion tasks. When you enable the auto scaler, Druid ignores the value of `taskCount` in `IOConfig` and uses `taskCountMin` for the initial number of tasks to launch.|Yes||
|`minTriggerScaleActionFrequencyMillis`|Minimum time interval between two scale actions.| No|600000|
|`autoScalerStrategy`|The algorithm of `autoScaler`. Druid only supports the `lagBased` strategy. See [Lag based autoscaler strategy related properties](#lag-based-autoscaler-strategy-related-properties) for more information.|No|Defaults to `lagBased`.|

### Lag based autoscaler strategy related properties

Unlike the Kafka indexing service, Kinesis reports lag metrics measured in time difference in milliseconds between the current sequence number and latest sequence number, rather than message count.

The following table outlines the configuration options for `autoScalerStrategy`:

|Property|Description|Required|Default|
|--------|-----------|--------|-------|
|`lagCollectionIntervalMillis`|The time period during which Druid collects lag metric points.|No|30000|
|`lagCollectionRangeMillis`|The total time window of lag collection. Use with `lagCollectionIntervalMillis` to specify the intervals at which to collect lag metric points.|No|600000|
|`scaleOutThreshold`|The threshold of scale out action. |No|6000000|
|`triggerScaleOutFractionThreshold`|Enables scale out action if `triggerScaleOutFractionThreshold` percent of lag points is higher than `scaleOutThreshold`.|No|0.3|
|`scaleInThreshold`|The threshold of scale in action.|No|1000000|
|`triggerScaleInFractionThreshold`|Enables scale in action if `triggerScaleInFractionThreshold` percent of lag points is lower than `scaleOutThreshold`.|No|0.9|
|`scaleActionStartDelayMillis`|The number of milliseconds to delay after the supervisor starts before the first scale logic check.|No|300000|
|`scaleActionPeriodMillis`|The frequency in milliseconds to check if a scale action is triggered.|No|60000|
|`scaleInStep`|The number of tasks to reduce at once when scaling down.|No|1|
|`scaleOutStep`|The number of tasks to add at once when scaling out.|No|2|

The following example shows a supervisor spec with `lagBased` auto scaler enabled.

<details>
    <summary>Click to view the example</summary>

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
    "taskDuration": "PT1H"
  },
  "tuningConfig": {
    "type": "kinesis",
    "maxRowsPerSegment": 5000000
  }
}
```

</details>

### Specify data format

The Kinesis indexing service supports both [`inputFormat`](../../ingestion/data-formats.md#input-format) and [`parser`](../../ingestion/data-formats.md#parser) to specify the data format.
Use the `inputFormat` to specify the data format for the Kinesis indexing service unless you need a format only supported by the legacy `parser`.

Supported values for `inputFormat` include:

- `csv`
- `delimited`
- `json`
- `avro_stream`
- `avro_ocf`
- `protobuf`

For more information, see [Data formats](../../ingestion/data-formats.md). You can also read [`thrift`](../extensions-contrib/thrift.md) formats using `parser`.

## Supervisor tuning configuration

The `tuningConfig` object is optional. If you don't specify the `tuningConfig` object, Druid uses the default configuration settings.

The following table outlines the configuration options for `tuningConfig`:

|Property|Type|Description|Required|Default|
|--------|----|-----------|--------|-------|
|`type`|String|The indexing task type. This should always be `kinesis`.|Yes||
|`maxRowsInMemory`|Integer|The number of rows to aggregate before persisting. This number represents the post-aggregation rows. It is not equivalent to the number of input events, but the resulting number of aggregated rows. Druid uses `maxRowsInMemory` to manage the required JVM heap size. The maximum heap memory usage for indexing scales is `maxRowsInMemory * (2 + maxPendingPersists)`.|No|100000|
|`maxBytesInMemory`|Long| The number of bytes to aggregate in heap memory before persisting. This is based on a rough estimate of memory usage and not actual usage. Normally, this is computed internally. The maximum heap memory usage for indexing is `maxBytesInMemory * (2 + maxPendingPersists)`.|No|One-sixth of max JVM memory|
|`skipBytesInMemoryOverheadCheck`|Boolean|The calculation of `maxBytesInMemory` takes into account overhead objects created during ingestion and each intermediate persist. To exclude the bytes of these overhead objects from the `maxBytesInMemory` check, set `skipBytesInMemoryOverheadCheck` to `true`.|No|`false`|
|`maxRowsPerSegment`|Integer|The number of rows to aggregate into a segment; this number represents the post-aggregation rows. Handoff occurs when `maxRowsPerSegment` or `maxTotalRows` is reached or every `intermediateHandoffPeriod`, whichever happens first.|No|5000000|
|`maxTotalRows`|Long|The number of rows to aggregate across all segments; this number represents the post-aggregation rows. Handoff occurs when `maxRowsPerSegment` or `maxTotalRows` is reached or every `intermediateHandoffPeriod`, whichever happens first.|No|unlimited|
|`intermediateHandoffPeriod`|ISO 8601 period|The period that determines how often tasks hand off segments. Handoff occurs if `maxRowsPerSegment` or `maxTotalRows` is reached or every `intermediateHandoffPeriod`, whichever happens first.|No|P2147483647D|
|`intermediatePersistPeriod`|ISO 8601 period|The period that determines the rate at which intermediate persists occur.|No|PT10M|
|`maxPendingPersists`|Integer|Maximum number of persists that can be pending but not started. If a new intermediate persist exceeds this limit, Druid blocks ingestion until the currently running persist finishes. One persist can be running concurrently with ingestion, and none can be queued up. The maximum heap memory usage for indexing scales is `maxRowsInMemory * (2 + maxPendingPersists)`.|No|0|
|`indexSpec`|Object|Defines how Druid indexes the data. See [IndexSpec](#indexspec) for more information.|No||
|`indexSpecForIntermediatePersists`|Object|Defines segment storage format options to use at indexing time for intermediate persisted temporary segments. You can use `indexSpecForIntermediatePersists` to disable dimension/metric compression on intermediate segments to reduce memory required for final merging. However, disabling compression on intermediate segments might increase page cache use while they are used before getting merged into final segment published. See [IndexSpec](#indexspec) for possible values.|No|Same as `indexSpec`|
|`reportParseExceptions`|Boolean|If `true`, Druid throws exceptions encountered during parsing causing ingestion to halt. If `false`, Druid skips unparseable rows and fields.|No|`false`|
|`handoffConditionTimeout`|Long|Number of milliseconds to wait for segment handoff. Set to a value >= 0, where 0 means to wait indefinitely.|No|0|
|`resetOffsetAutomatically`|Boolean|Controls behavior when Druid needs to read Kinesis messages that are no longer available.<br/>If `false`, the exception bubbles up causing tasks to fail and ingestion to halt. If this occurs, manual intervention is required to correct the situation, potentially using the [Reset Supervisor API](../../api-reference/supervisor-api.md). This mode is useful for production, since it highlights issues with ingestion.<br/>If `true`, Druid automatically resets to the earliest or latest sequence number available in Kinesis, based on the value of the `useEarliestSequenceNumber` property (earliest if `true`, latest if `false`). Note that this can lead to dropping data (if `useEarliestSequenceNumber` is `false`) or duplicating data (if `useEarliestSequenceNumber` is `true`) without your knowledge. Druid logs messages indicating that a reset has occurred without interrupting ingestion. This mode is useful for non-production situations since it enables Druid to recover from problems automatically, even if they lead to quiet dropping or duplicating of data.|No|`false`|
|`skipSequenceNumberAvailabilityCheck`|Boolean|Whether to enable checking if the current sequence number is still available in a particular Kinesis shard. If `false`, the indexing task attempts to reset the current sequence number, depending on the value of `resetOffsetAutomatically`.|No|`false`|
|`workerThreads`|Integer|The number of threads that the supervisor uses to handle requests/responses for worker tasks, along with any other internal asynchronous operation.|No| `min(10, taskCount)`|
|`chatRetries`|Integer|The number of times Druid retries HTTP requests to indexing tasks before considering tasks unresponsive.|No|8|
|`httpTimeout`|ISO 8601 period|The period of time to wait for a HTTP response from an indexing task.|No|PT10S|
|`shutdownTimeout`|ISO 8601 period|The period of time to wait for the supervisor to attempt a graceful shutdown of tasks before exiting.|No|PT80S|
|`recordBufferSize`|Integer|The size of the buffer (number of events) Druid uses between the Kinesis fetch threads and the main ingestion thread.|No|See [Determine fetch settings](#determine-fetch-settings) for defaults.|
|`recordBufferOfferTimeout`|Integer|The number of milliseconds to wait for space to become available in the buffer before timing out.|No|5000|
|`recordBufferFullWait`|Integer|The number of milliseconds to wait for the buffer to drain before Druid attempts to fetch records from Kinesis again.|No|5000|
|`fetchThreads`|Integer|The size of the pool of threads fetching data from Kinesis. There is no benefit in having more threads than Kinesis shards.|No| `procs * 2`, where `procs` is the number of processors available to the task.|
|`segmentWriteOutMediumFactory`|Object|The segment write-out medium to use when creating segments See [Additional Peon configuration: SegmentWriteOutMediumFactory](../../configuration/index.md#segmentwriteoutmediumfactory) for explanation and available options.|No|If not specified, Druid uses the value from `druid.peon.defaultSegmentWriteOutMediumFactory.type`.|
|`logParseExceptions`|Boolean|If `true`, Druid logs an error message when a parsing exception occurs, containing information about the row where the error occurred.|No|`false`|
|`maxParseExceptions`|Integer|The maximum number of parse exceptions that can occur before the task halts ingestion and fails. Overridden if `reportParseExceptions` is set.|No|unlimited|
|`maxSavedParseExceptions`|Integer|When a parse exception occurs, Druid keeps track of the most recent parse exceptions. `maxSavedParseExceptions` limits the number of saved exception instances. These saved exceptions are available after the task finishes in the [task completion report](../../ingestion/tasks.md#task-reports). Overridden if `reportParseExceptions` is set.|No|0|
|`maxRecordsPerPoll`|Integer|The maximum number of records to be fetched from buffer per poll. The actual maximum will be `Max(maxRecordsPerPoll, Max(bufferSize, 1))`.|No| See [Determine fetch settings](#determine-fetch-settings) for defaults.|
|`repartitionTransitionDuration`|ISO 8601 period|When shards are split or merged, the supervisor recomputes shard to task group mappings. The supervisor also signals any running tasks created under the old mappings to stop early at current time + `repartitionTransitionDuration`. Stopping the tasks early allows Druid to begin reading from the new shards more quickly. The repartition transition wait time controlled by this property gives the stream additional time to write records to the new shards after the split or merge, which helps avoid issues with [empty shard handling](https://github.com/apache/druid/issues/7600).|No|PT2M|
|`offsetFetchPeriod`|ISO 8601 period|Determines how often the supervisor queries Kinesis and the indexing tasks to fetch current offsets and calculate lag. If the user-specified value is below the minimum value of PT5S, the supervisor ignores the value and uses the minimum value instead.|No|PT30S|
|`useListShards`|Boolean|Indicates if `listShards` API of AWS Kinesis SDK can be used to prevent `LimitExceededException` during ingestion. You must set the necessary `IAM` permissions.|No|`false`|

### IndexSpec

The following table outlines the configuration options for `indexSpec`:

|Property|Type|Description|Required|Default|
|--------|----|-----------|--------|-------|
|`bitmap`|Object|Compression format for bitmap indexes. Druid supports roaring and concise bitmap types.|No|Roaring|
|`dimensionCompression`|String|Compression format for dimension columns. Choose from `LZ4`, `LZF`, or `uncompressed`.|No|`LZ4`|
|`metricCompression`|String|Compression format for primitive type metric columns. Choose from `LZ4`, `LZF`, `uncompressed`, or `none`.|No|`LZ4`|
|`longEncoding`|String|Encoding format for metric and dimension columns with type long. Choose from `auto` or `longs`. `auto` encodes the values using sequence number or lookup table depending on column cardinality and stores them with variable sizes. `longs` stores the value as is with 8 bytes each.|No|`longs`|

## Operations

This section describes how to use the [Supervisor API](../../api-reference/supervisor-api.md) with the Kinesis indexing service.

### AWS authentication

To authenticate with AWS, you must provide your AWS access key and AWS secret key using `runtime.properties`, for example:

```text
druid.kinesis.accessKey=AKIAWxxxxxxxxxx4NCKS
druid.kinesis.secretKey=Jbytxxxxxxxxxxx2+555
```

Druid uses the AWS access key and AWS secret key to authenticate Kinesis API requests. If not provided, the service looks for credentials set in environment variables, via [Web Identity Token](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_providers_oidc.html), in the default profile configuration file, and from the EC2 instance profile provider (in this order).

To ingest data from Kinesis, ensure that the policy attached to your IAM role contains the necessary permissions.
The required permissions depend on the value of `useListShards`.

If the `useListShards` flag is set to `true`, you need following permissions:

- `ListStreams` to list your data streams.
- `Get*` required for `GetShardIterator`.
- `GetRecords` to get data records from a data stream's shard.
- `ListShards` to get the shards for a stream of interest.

The following is an example policy:

```json
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

- `ListStreams` to list your data streams.
- `Get*` required for `GetShardIterator`.
- `GetRecords` to get data records from a data stream's shard.
- `DescribeStream` to describe the specified data stream.

The following is an example policy:

```json
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

### Get supervisor status report

To retrieve the current status report for a single supervisor, send a `GET` request to the `/druid/indexer/v1/supervisor/:supervisorId/status` endpoint.

The report contains the state of the supervisor tasks, the latest sequence numbers, and an array of recently thrown exceptions reported as `recentErrors`. You can control the maximum size of the exceptions using the `druid.supervisor.maxStoredExceptionEvents` configuration.

The two properties related to the supervisor's state are `state` and `detailedState`. The `state` property contains a small number of generic states that apply to any type of supervisor, while the `detailedState` property contains a more descriptive, implementation-specific state that may provide more insight into the supervisor's activities.

Possible `state` values are `PENDING`, `RUNNING`, `SUSPENDED`, `STOPPING`, `UNHEALTHY_SUPERVISOR`, and `UNHEALTHY_TASKS`.

The following table lists `detailedState` values and their corresponding `state` mapping:

|Detailed state|Corresponding state|Description|
|--------------|-------------------|-----------|
|`UNHEALTHY_SUPERVISOR`|`UNHEALTHY_SUPERVISOR`|The supervisor encountered errors on previous `druid.supervisor.unhealthinessThreshold` iterations.|
|`UNHEALTHY_TASKS`|`UNHEALTHY_TASKS`|The last `druid.supervisor.taskUnhealthinessThreshold` tasks all failed.|
|`UNABLE_TO_CONNECT_TO_STREAM`|`UNHEALTHY_SUPERVISOR`|The supervisor is encountering connectivity issues with Kinesis and has not successfully connected in the past.|
|`LOST_CONTACT_WITH_STREAM`|`UNHEALTHY_SUPERVISOR`|The supervisor is encountering connectivity issues with Kinesis but has successfully connected in the past.|
|`PENDING` (first iteration only)|`PENDING`|The supervisor has been initialized but hasn't started connecting to the stream.|
|`CONNECTING_TO_STREAM` (first iteration only)|`RUNNING`|The supervisor is trying to connect to the stream and update partition data.|
|`DISCOVERING_INITIAL_TASKS` (first iteration only)|`RUNNING`|The supervisor is discovering already-running tasks.|
|`CREATING_TASKS` (first iteration only)|`RUNNING`|The supervisor is creating tasks and discovering state.|
|`RUNNING`|`RUNNING`|The supervisor has started tasks and is waiting for `taskDuration` to elapse.|
|`SUSPENDED`|`SUSPENDED`|The supervisor is suspended.|
|`STOPPING`|`STOPPING`|The supervisor is stopping.|

On each iteration of the supervisor's run loop, the supervisor completes the following tasks in sequence:

1. Fetch the list of shards from Kinesis and determine the starting sequence number for each shard (either based on the last processed sequence number if continuing, or starting from the beginning or ending of the stream if this is a new stream).
2. Discover any running indexing tasks that are writing to the supervisor's datasource and adopt them if they match the supervisor's configuration, else signal them to stop.
3. Send a status request to each supervised task to update the view of the state of the tasks under supervision.
4. Handle tasks that have exceeded `taskDuration` and should transition from the reading to publishing state.
5. Handle tasks that have finished publishing and signal redundant replica tasks to stop.
6. Handle tasks that have failed and clean up the supervisor's internal state.
7. Compare the list of healthy tasks to the requested `taskCount` and `replicas` configurations and create additional tasks if required.

The `detailedState` property shows additional values (marked with "first iteration only" in the preceding table) the first time the
supervisor executes this run loop after startup or after resuming from a suspension. This is intended to surface
initialization-type issues, where the supervisor is unable to reach a stable state. For example, if the supervisor cannot connect to
Kinesis, if it's unable to read from the stream, or cannot communicate with existing tasks. Once the supervisor is stable;
that is, once it has completed a full execution without encountering any issues, `detailedState` will show a `RUNNING`
state until it is stopped, suspended, or hits a failure threshold and transitions to an unhealthy state.

### Update existing supervisors

To update an existing supervisor spec, send a `POST` request to the `/druid/indexer/v1/supervisor` endpoint.

When you call this endpoint on an existing supervisor for the same datasource, the running supervisor signals its tasks to stop reading and begin publishing their segments, exiting itself. Druid then uses the provided configuration from the request body to create a new supervisor with a new set of tasks that start reading from the sequence numbers, where the previous now-publishing tasks left off, but using the updated schema.
In this way, configuration changes can be applied without requiring any pause in ingestion.

You can achieve seamless schema migrations by submitting the new schema using the `/druid/indexer/v1/supervisor` endpoint.

### Suspend and resume a supervisor

To suspend a supervisor, send a `POST` request to the `/druid/indexer/v1/supervisor/:supervisorId/suspend` endpoint.
Suspending a supervisor does not prevent it from operating and emitting logs and metrics. It ensures that no indexing tasks are running until the supervisor resumes.

To resume a supervisor, send a `POST` request to the `/druid/indexer/v1/supervisor/:supervisorId/resume` endpoint.

### Reset a supervisor

The supervisor must be running for this endpoint to be available

To reset a supervisor, send a `POST` request to the `/druid/indexer/v1/supervisor/:supervisorId/reset` endpoint. This endpoint clears stored
sequence numbers, prompting the supervisor to start reading from either the earliest or the
latest sequence numbers in Kinesis (depending on the value of `useEarliestSequenceNumber`).
After clearing stored sequence numbers, the supervisor kills and recreates active tasks,
so that tasks begin reading from valid sequence numbers.

This endpoint is useful when you need to recover from a stopped state due to missing sequence numbers in Kinesis.
Use this endpoint with caution as it may result in skipped messages, leading to data loss or duplicate data.

The indexing service keeps track of the latest
persisted sequence number to provide exactly-once ingestion guarantees across
tasks.
Subsequent tasks must start reading from where the previous task completed
for the generated segments to be accepted. If the messages at the expected starting sequence numbers are
no longer available in Kinesis (typically because the message retention period has elapsed or the topic was
removed and re-created) the supervisor will refuse to start and in-flight tasks will fail. This endpoint enables you to recover from this condition.

### Terminate a supervisor

To terminate a supervisor and its associated indexing tasks, send a `POST` request to the `/druid/indexer/v1/supervisor/:supervisorId/terminate` endpoint.
This places a tombstone marker in the database to prevent the supervisor from being reloaded on a restart and then gracefully
shuts down the currently running supervisor.
The tasks stop reading and begin publishing their segments immediately.
The call returns after all tasks have been signaled to stop but before the tasks finish publishing their segments.

The terminated supervisor continues exists in the metadata store and its history can be retrieved.
The only way to restart a terminated supervisor is by submitting a functioning supervisor spec to `/druid/indexer/v1/supervisor`.

## Capacity planning

Kinesis indexing tasks run on Middle Managers and are limited by the resources available in the Middle Manager cluster. In particular, you should make sure that you have sufficient worker capacity, configured using the
`druid.worker.capacity` property, to handle the configuration in the supervisor spec. Note that worker capacity is
shared across all types of indexing tasks, so you should plan your worker capacity to handle your total indexing load, such as batch processing, streaming tasks, and merging tasks. If your workers run out of capacity, Kinesis indexing tasks queue and wait for the next available worker. This may cause queries to return partial results but will not result in data loss, assuming the tasks run before Kinesis purges those sequence numbers.

A running task can be in one of two states: reading or publishing. A task remains in reading state for the period defined in `taskDuration`, at which point it transitions to publishing state. A task remains in publishing state for as long as it takes to generate segments, push segments to deep storage, and have them loaded and served by a Historical process or until `completionTimeout` elapses.

The number of reading tasks is controlled by `replicas` and `taskCount`. In general, there are `replicas * taskCount` reading tasks. An exception occurs if `taskCount > {numKinesisShards}`, in which case Druid uses `{numKinesisShards}` tasks. When `taskDuration` elapses, these tasks transition to publishing state and `replicas * taskCount` new reading tasks are created. To allow for reading tasks and publishing tasks to run concurrently, there should be a minimum capacity of:

```text
workerCapacity = 2 * replicas * taskCount
```

This value is for the ideal situation in which there is at most one set of tasks publishing while another set is reading.
In some circumstances, it is possible to have multiple sets of tasks publishing simultaneously. This would happen if the
time-to-publish (generate segment, push to deep storage, load on Historical) is greater than `taskDuration`. This is a valid and correct scenario but requires additional worker capacity to support. In general, it is a good idea to have `taskDuration` be large enough that the previous set of tasks finishes publishing before the current set begins.

## Shards and segment handoff

Each Kinesis indexing task writes the events it consumes from Kinesis shards into a single segment for the segment granularity interval until it reaches one of the following limits: `maxRowsPerSegment`, `maxTotalRows`, or `intermediateHandoffPeriod`.
At this point, the task creates a new shard for this segment granularity to contain subsequent events.

The Kinesis indexing task also performs incremental hand-offs so that the segments created by the task are not held up until the task duration is over.
When the task reaches one of the `maxRowsPerSegment`, `maxTotalRows`, or `intermediateHandoffPeriod` limits, it hands off all the segments and creates a new set of segments for further events. This allows the task to run for longer durations
without accumulating old segments locally on Middle Manager processes.

The Kinesis indexing service may still produce some small segments.
For example, consider the following scenario:

- Task duration is 4 hours
- Segment granularity is set to an HOUR
- The supervisor was started at 9:10

After 4 hours at 13:10, Druid starts a new set of tasks. The events for the interval 13:00 - 14:00 may be split across existing tasks and the new set of tasks which could result in small segments. To merge them together into new segments of an ideal size (in the range of ~500-700 MB per segment), you can schedule re-indexing tasks, optionally with a different segment granularity.

For more detail, see [Segment size optimization](../../operations/segment-optimization.md).

## Determine fetch settings

Kinesis indexing tasks fetch records using `fetchThreads` threads.
If `fetchThreads` is higher than the number of Kinesis shards, the excess threads are unused.
Each fetch thread fetches up to `recordsPerFetch` records at once from a Kinesis shard, with a delay between fetches
of `fetchDelayMillis`.
The records fetched by each thread are pushed into a shared queue of size `recordBufferSize`.
The main runner thread for each task polls up to `maxRecordsPerPoll` records from the queue at once.

When using Kinesis Producer Library's aggregation feature, that is when [`deaggregate`](#deaggregation) is set,
each of these parameters refers to aggregated records rather than individual records.

The default values for these parameters are:

- `fetchThreads`: Twice the number of processors available to the task. The number of processors available to the task
is the total number of processors on the server, divided by `druid.worker.capacity` (the number of task slots on that
particular server).
- `fetchDelayMillis`: 0 (no delay between fetches).
- `recordsPerFetch`: 100 MB or an estimated 5% of available heap, whichever is smaller, divided by `fetchThreads`.
For estimation purposes, Druid uses a figure of 10 KB for regular records and 1 MB for [aggregated records](#deaggregation).
- `recordBufferSize`: 100 MB or an estimated 10% of available heap, whichever is smaller.
For estimation purposes, Druid uses a figure of 10 KB for regular records and 1 MB for [aggregated records](#deaggregation).
- `maxRecordsPerPoll`: 100 for regular records, 1 for [aggregated records](#deaggregation).

Kinesis places the following restrictions on calls to fetch records:

- Each data record can be up to 1 MB in size.
- Each shard can support up to 5 transactions per second for reads.
- Each shard can read up to 2 MB per second.
- The maximum size of data that GetRecords can return is 10 MB.

If the above limits are exceeded, Kinesis throws `ProvisionedThroughputExceededException` errors. If this happens, Druid
Kinesis tasks pause by `fetchDelayMillis` or 3 seconds, whichever is larger, and then attempt the call again.

In most cases, the default settings for fetch parameters are sufficient to achieve good performance without excessive
memory usage. However, in some cases, you may need to adjust these parameters to control fetch rate
and memory usage more finely. Optimal values depend on the average size of a record and the number of consumers you
have reading from a given shard, which will be `replicas` unless you have other consumers also reading from this
Kinesis stream.

## Deaggregation

The Kinesis indexing service supports de-aggregation of multiple rows packed into a single record by the Kinesis
Producer Library's aggregate method for more efficient data transfer.

To enable this feature, set `deaggregate` to true in your `ioConfig` when submitting a supervisor spec.

## Resharding

When changing the shard count for a Kinesis stream, there is a window of time around the resharding operation with early shutdown of Kinesis ingestion tasks and possible task failures.

The early shutdowns and task failures are expected. They occur because the supervisor updates the shard to task group mappings as shards are closed and fully read. This ensures that tasks are not running
with an assignment of closed shards that have been fully read and balances distribution of active shards across tasks.

This window with early task shutdowns and possible task failures concludes when:

- All closed shards have been fully read and the Kinesis ingestion tasks have published the data from those shards, committing the "closed" state to metadata storage.
- Any remaining tasks that had inactive shards in the assignment have been shut down. These tasks would have been created before the closed shards were completely drained.

## Kinesis known issues

Before you deploy the Kinesis extension to production, consider the following known issues:

- Avoid implementing more than one Kinesis supervisor that reads from the same Kinesis stream for ingestion. Kinesis has a per-shard read throughput limit and having multiple supervisors on the same stream can reduce available read throughput for an individual supervisor's tasks. Multiple supervisors ingesting to the same Druid datasource can also cause increased contention for locks on the datasource.
- The only way to change the stream reset policy is to submit a new ingestion spec and set up a new supervisor.
- If ingestion tasks get stuck, the supervisor does not automatically recover. You should monitor ingestion tasks and investigate if your ingestion falls behind.
- A Kinesis supervisor can sometimes compare the checkpoint offset to retention window of the stream to see if it has fallen behind. These checks fetch the earliest sequence number for Kinesis which can result in `IteratorAgeMilliseconds` becoming very high in AWS CloudWatch.
