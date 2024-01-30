---
id: kinesis-ingestion
title: "Amazon Kinesis ingestion"
sidebar_label: "Amazon Kinesis ingestion"
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';


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

When you enable the Kinesis indexing service, you can configure supervisors on the Overlord to manage the creation and lifetime of Kinesis indexing tasks. These indexing tasks read events using the Kinesis shard and sequence number mechanism to guarantee exactly-once ingestion. The supervisor oversees the state of the indexing tasks to coordinate handoffs, manage failures, and ensure that scalability and replication requirements are maintained.

This topic contains configuration reference information for the Kinesis indexing service supervisor for Apache Druid.

## Setup

To use the Kinesis indexing service, you must first load the `druid-kinesis-indexing-service` core extension on both the Overlord and the MiddleManager. See [Loading extensions](../configuration/extensions.md#loading-extensions) for more information.

Review [Kinesis known issues](#kinesis-known-issues) before deploying the `druid-kinesis-indexing-service` extension to production.

## Supervisor spec

The following table outlines the high-level configuration options for the Kinesis [supervisor spec](../ingestion/supervisor.md#supervisor-spec).

|Property|Type|Description|Required|
|--------|----|-----------|--------|
|`type`|String|The supervisor type; must be `kinesis`.|Yes|
|`spec`|Object|The container object for the supervisor configuration.|Yes|
|`ioConfig`|Object|The [I/O configuration](#supervisor-io-configuration) object for configuring Kinesis connection and I/O-related settings for the supervisor and indexing tasks.|Yes|
|`dataSchema`|Object|The schema used by the Kinesis indexing task during ingestion. See [`dataSchema`](../ingestion/ingestion-spec.md#dataschema) for more information.|Yes|
|`tuningConfig`|Object|The [tuning configuration](#supervisor-tuning-configuration) object for configuring performance-related settings for the supervisor and indexing tasks.|No|

The following example shows a supervisor spec for a stream with the name `KinesisStream`.

<details><summary>Click to view the example</summary>

```json
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
</details>

### I/O configuration

The following table outlines the configuration options for `ioConfig`:

|Property|Type|Description|Required|Default|
|--------|----|-----------|--------|-------|
|`stream`|String|The Kinesis stream to read.|Yes||
|`inputFormat`|Object|The [input format](../ingestion/data-formats.md#input-format) to specify how to parse input data.|Yes||
|`endpoint`|String|The AWS Kinesis stream endpoint for a region. You can find a list of endpoints in the [AWS service endpoints](http://docs.aws.amazon.com/general/latest/gr/rande.html#ak_region) document.|No|`kinesis.us-east-1.amazonaws.com`|
|`replicas`|Integer|The number of replica sets, where 1 is a single set of tasks (no replication). Druid always assigns replicate tasks to different workers to provide resiliency against process failure.|No|1|
|`taskCount`|Integer|The maximum number of reading tasks in a replica set. Multiply `taskCount` and `replicas` to measure the maximum number of reading tasks. <br />The total number of tasks (reading and publishing) is higher than the maximum number of reading tasks. See [Capacity planning](#capacity-planning) for more details. When `taskCount > {numKinesisShards}`, the actual number of reading tasks is less than the `taskCount` value.|No|1|
|`taskDuration`|ISO 8601 period|The length of time before tasks stop reading and begin publishing their segments.|No|PT1H|
|`startDelay`|ISO 8601 period|The period to wait before the supervisor starts managing tasks.|No|PT5S|
|`period`|ISO 8601 period|Determines how often the supervisor executes its management logic. Note that the supervisor also runs in response to certain events, such as tasks succeeding, failing, and reaching their task duration, so this value specifies the maximum time between iterations.|No|PT30S|
|`useEarliestSequenceNumber`|Boolean|If a supervisor is managing a datasource for the first time, it obtains a set of starting sequence numbers from Kinesis. This flag determines whether a supervisor retrieves the earliest or latest sequence numbers in Kinesis. Under normal circumstances, subsequent tasks start from where the previous segments ended so this flag is only used on the first run.|No|`false`|
|`completionTimeout`|ISO 8601 period|The length of time to wait before Druid declares a publishing task has failed and terminates it. If this is set too low, your tasks may never publish. The publishing clock for a task begins roughly after `taskDuration` elapses.|No|PT6H|
|`lateMessageRejectionPeriod`|ISO 8601 period|Configures tasks to reject messages with timestamps earlier than this period before the task is created. For example, if `lateMessageRejectionPeriod` is set to `PT1H` and the supervisor creates a task at `2016-01-01T12:00Z`, messages with timestamps earlier than `2016-01-01T11:00Z` are dropped. This may help prevent concurrency issues if your data stream has late messages and you have multiple pipelines that need to operate on the same segments, such as a streaming and a nightly batch ingestion pipeline.|No||
|`earlyMessageRejectionPeriod`|ISO 8601 period|Configures tasks to reject messages with timestamps later than this period after the task reached its `taskDuration`. For example, if `earlyMessageRejectionPeriod` is set to `PT1H`, the `taskDuration` is set to `PT1H` and the supervisor creates a task at `2016-01-01T12:00Z`. Messages with timestamps later than `2016-01-01T14:00Z` are dropped. **Note:** Tasks sometimes run past their task duration, for example, in cases of supervisor failover. Setting `earlyMessageRejectionPeriod` too low may cause messages to be dropped unexpectedly whenever a task runs past its originally configured task duration.|No||
|`recordsPerFetch`|Integer|The number of records to request per call to fetch records from Kinesis.|No| See [Determine fetch settings](#determine-fetch-settings) for defaults.|
|`fetchDelayMillis`|Integer|Time in milliseconds to wait between subsequent calls to fetch records from Kinesis. See [Determine fetch settings](#determine-fetch-settings).|No|0|
|`awsAssumedRoleArn`|String|The AWS assumed role to use for additional permissions.|No||
|`awsExternalId`|String|The AWS external ID to use for additional permissions.|No||
|`deaggregate`|Boolean|Whether to use the deaggregate function of the Kinesis Client Library (KCL).|No||
|`autoScalerConfig`|Object|Defines autoscaling behavior for ingestion tasks. See [Task autoscaler](../ingestion/supervisor.md#task-autoscaler) for more information.|No|null|

#### Task autoscaler

You can optionally configure autoscaling behavior for ingestion tasks using the `autoScalerConfig` property of the `ioConfig` object.

The following table outlines the autoscaler configuration options:

|Property|Description|Required|Default|
|--------|-----------|--------|-------|
|`enableTaskAutoScaler`|Enables the auto scaler. If not specified, Druid disables the auto scaler even when `autoScalerConfig` is not null.|No|`false`|
|`taskCountMax`|Maximum number of ingestion tasks. Must be greater than or equal to `taskCountMin`. If greater than `{numKinesisShards}`, Druid sets the maximum number of reading tasks to `{numKinesisShards}` and ignores `taskCountMax`.|Yes||
|`taskCountMin`|Minimum number of ingestion tasks. When you enable the autoscaler, Druid ignores the value of `taskCount` in `ioConfig` and starts with the `taskCountMin` number of tasks to launch.|Yes||
|`minTriggerScaleActionFrequencyMillis`|Minimum time interval between two scale actions.| No|600000|
|`autoScalerStrategy`|The algorithm of `autoScaler`. Druid only supports the `lagBased` strategy. See [Autoscaler strategy](#autoscaler-strategy) for more information.|No|`lagBased`|

##### Autoscaler strategy

:::info
Unlike the Kafka indexing service, Kinesis indexing service reports lag metrics measured in time difference in milliseconds between the current sequence number and latest sequence number, rather than message count.
:::

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

### Tuning configuration

The `tuningConfig` object is optional. If you don't specify the `tuningConfig` object, Druid uses the default configuration settings.

The following table outlines the configuration options for `tuningConfig`:

|Property|Type|Description|Required|Default|
|--------|----|-----------|--------|-------|
|`type`|String|The indexing task type; must be `kinesis`.|Yes||
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
|`resetOffsetAutomatically`|Boolean|Controls behavior when Druid needs to read Kinesis messages that are no longer available.<br/>If `false`, the exception bubbles up causing tasks to fail and ingestion to halt. If this occurs, manual intervention is required to correct the situation, potentially using the [Reset Supervisor API](../api-reference/supervisor-api.md). This mode is useful for production, since it highlights issues with ingestion.<br/>If `true`, Druid automatically resets to the earliest or latest sequence number available in Kinesis, based on the value of the `useEarliestSequenceNumber` property (earliest if `true`, latest if `false`). Note that this can lead to dropping data (if `useEarliestSequenceNumber` is `false`) or duplicating data (if `useEarliestSequenceNumber` is `true`) without your knowledge. Druid logs messages indicating that a reset has occurred without interrupting ingestion. This mode is useful for non-production situations since it enables Druid to recover from problems automatically, even if they lead to quiet dropping or duplicating of data.|No|`false`|
|`skipSequenceNumberAvailabilityCheck`|Boolean|Whether to enable checking if the current sequence number is still available in a particular Kinesis shard. If `false`, the indexing task attempts to reset the current sequence number, depending on the value of `resetOffsetAutomatically`.|No|`false`|
|`workerThreads`|Integer|The number of threads that the supervisor uses to handle requests/responses for worker tasks, along with any other internal asynchronous operation.|No| `min(10, taskCount)`|
|`chatRetries`|Integer|The number of times Druid retries HTTP requests to indexing tasks before considering tasks unresponsive.|No|8|
|`httpTimeout`|ISO 8601 period|The period of time to wait for a HTTP response from an indexing task.|No|PT10S|
|`shutdownTimeout`|ISO 8601 period|The period of time to wait for the supervisor to attempt a graceful shutdown of tasks before exiting.|No|PT80S|
|`recordBufferSize`|Integer|The size of the buffer (number of events) Druid uses between the Kinesis fetch threads and the main ingestion thread.|No|See [Determine fetch settings](#determine-fetch-settings) for defaults.|
|`recordBufferOfferTimeout`|Integer|The number of milliseconds to wait for space to become available in the buffer before timing out.|No|5000|
|`recordBufferFullWait`|Integer|The number of milliseconds to wait for the buffer to drain before Druid attempts to fetch records from Kinesis again.|No|5000|
|`fetchThreads`|Integer|The size of the pool of threads fetching data from Kinesis. There is no benefit in having more threads than Kinesis shards.|No| `procs * 2`, where `procs` is the number of processors available to the task.|
|`segmentWriteOutMediumFactory`|Object|The segment write-out medium to use when creating segments. See [Additional Peon configuration: SegmentWriteOutMediumFactory](../configuration/index.md#segmentwriteoutmediumfactory) for explanation and available options.|No|If not specified, Druid uses the value from `druid.peon.defaultSegmentWriteOutMediumFactory.type`.|
|`logParseExceptions`|Boolean|If `true`, Druid logs an error message when a parsing exception occurs, containing information about the row where the error occurred.|No|`false`|
|`maxParseExceptions`|Integer|The maximum number of parse exceptions that can occur before the task halts ingestion and fails. Overridden if `reportParseExceptions` is set.|No|unlimited|
|`maxSavedParseExceptions`|Integer|When a parse exception occurs, Druid keeps track of the most recent parse exceptions. `maxSavedParseExceptions` limits the number of saved exception instances. These saved exceptions are available after the task finishes in the [task completion report](../ingestion/tasks.md#task-reports). Overridden if `reportParseExceptions` is set.|No|0|
|`maxRecordsPerPoll`|Integer|The maximum number of records to be fetched from buffer per poll. The actual maximum will be `Max(maxRecordsPerPoll, Max(bufferSize, 1))`.|No| See [Determine fetch settings](#determine-fetch-settings) for defaults.|
|`repartitionTransitionDuration`|ISO 8601 period|When shards are split or merged, the supervisor recomputes shard to task group mappings. The supervisor also signals any running tasks created under the old mappings to stop early at current time + `repartitionTransitionDuration`. Stopping the tasks early allows Druid to begin reading from the new shards more quickly. The repartition transition wait time controlled by this property gives the stream additional time to write records to the new shards after the split or merge, which helps avoid issues with [empty shard handling](https://github.com/apache/druid/issues/7600).|No|PT2M|
|`offsetFetchPeriod`|ISO 8601 period|Determines how often the supervisor queries Kinesis and the indexing tasks to fetch current offsets and calculate lag. If the user-specified value is below the minimum value of PT5S, the supervisor ignores the value and uses the minimum value instead.|No|PT30S|
|`useListShards`|Boolean|Indicates if `listShards` API of AWS Kinesis SDK can be used to prevent `LimitExceededException` during ingestion. You must set the necessary `IAM` permissions.|No|`false`|

#### IndexSpec

The following table outlines the configuration options for `indexSpec`:

|Property|Type|Description|Required|Default|
|--------|----|-----------|--------|-------|
|`bitmap`|Object|Compression format for bitmap indexes. Druid supports roaring and concise bitmap types.|No|Roaring|
|`dimensionCompression`|String|Compression format for dimension columns. One of `LZ4`, `LZF`, or `uncompressed`.|No|`LZ4`|
|`metricCompression`|String|Compression format for primitive type metric columns. One of `LZ4`, `LZF`, `uncompressed`, or `none`.|No|`LZ4`|
|`longEncoding`|String|Encoding format for metric and dimension columns with type long. One of `auto` or `longs`. `auto` encodes the values using sequence number or lookup table depending on column cardinality and stores them with variable sizes. `longs` stores the value as is with 8 bytes each.|No|`longs`|

## AWS authentication

Druid uses AWS access and secret keys to authenticate Kinesis API requests. There are a few ways to provide this information to Druid:

1. Using roles or short-term credentials:

   Druid looks for credentials set in [environment variables](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html),
via [Web Identity Token](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_providers_oidc.html), in the
default [profile configuration file](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html), and from the
EC2 instance profile provider (in this order).

2. Using long-term security credentials:

   You can directly provide your AWS access key and AWS secret key in the `common.runtime.properties` file as shown in the example below:

```properties
druid.kinesis.accessKey=AKIAWxxxxxxxxxx4NCKS
druid.kinesis.secretKey=Jbytxxxxxxxxxxx2+555
```

:::info
AWS does not recommend providing long-term security credentials in configuration files since it might pose a security risk.
If you use this approach, it takes precedence over all other methods of providing credentials.
:::

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
    "Action": ["kinesis:DescribeStream"],
    "Resource": ["*"]
  },
  {
    "Effect": "Allow",
    "Action": ["kinesis:Get*"],
    "Resource": [<ARN for shards to be ingested>]
  }
]
```

## Shards and segment handoff

Each Kinesis indexing task writes the events it consumes from Kinesis shards into a single segment for the segment granularity interval until it reaches one of the following limits: `maxRowsPerSegment`, `maxTotalRows`, or `intermediateHandoffPeriod`.
At this point, the task creates a new shard for this segment granularity to contain subsequent events.

The Kinesis indexing task also performs incremental hand-offs so that the segments created by the task are not held up until the task duration is over.
When the task reaches one of the `maxRowsPerSegment`, `maxTotalRows`, or `intermediateHandoffPeriod` limits, it hands off all the segments and creates a new set of segments for further events. This allows the task to run for longer durations
without accumulating old segments locally on MiddleManager services.

The Kinesis indexing service may still produce some small segments.
For example, consider the following scenario:

- Task duration is 4 hours
- Segment granularity is set to an HOUR
- The supervisor was started at 9:10

After 4 hours at 13:10, Druid starts a new set of tasks. The events for the interval 13:00 - 14:00 may be split across existing tasks and the new set of tasks which could result in small segments. To merge them together into new segments of an ideal size (in the range of ~500-700 MB per segment), you can schedule re-indexing tasks, optionally with a different segment granularity.

For information on how to optimize the segment size, see [Segment size optimization](../operations/segment-optimization.md).

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

[Resharding](https://docs.aws.amazon.com/streams/latest/dev/kinesis-using-sdk-java-resharding.html) is an advanced operation that lets you adjust the number of shards in a stream to adapt to changes in the rate of data flowing through a stream.

When changing the shard count for a Kinesis stream, there is a window of time around the resharding operation with early shutdown of Kinesis ingestion tasks and possible task failures.

The early shutdowns and task failures are expected. They occur because the supervisor updates the shard to task group mappings as shards are closed and fully read. This ensures that tasks are not running
with an assignment of closed shards that have been fully read and balances distribution of active shards across tasks.

This window with early task shutdowns and possible task failures concludes when:

- All closed shards have been fully read and the Kinesis ingestion tasks have published the data from those shards, committing the "closed" state to metadata storage.
- Any remaining tasks that had inactive shards in the assignment have been shut down. These tasks would have been created before the closed shards were completely drained.

Note that when the supervisor is running and detects new partitions, tasks read new partitions from the earliest offsets, irrespective of the `useEarliestSequence` setting. This is because these new shards were immediately discovered and are therefore unlikely to experience a lag.

If resharding occurs when the supervisor is suspended and `useEarliestSequence` is set to `false`, resuming the supervisor causes tasks to read the new shards from the latest sequence. This is by design so that the consumer can catch up quickly with any lag accumulated while the supervisor was suspended. 

## Known issues

Before you deploy the `druid-kinesis-indexing-service` extension to production, consider the following known issues:

- Kinesis imposes a read throughput limit per shard. If you have multiple supervisors reading from the same Kinesis stream, consider adding more shards to ensure sufficient read throughput for all supervisors.
- A Kinesis supervisor can sometimes compare the checkpoint offset to retention window of the stream to see if it has fallen behind. These checks fetch the earliest sequence number for Kinesis which can result in `IteratorAgeMilliseconds` becoming very high in AWS CloudWatch.

## Learn more

See the following topics for more information:

* [Supervisor API](../api-reference/supervisor-api.md) for how to manage and monitor supervisors using the API.
* [Supervisor](../ingestion/supervisor.md) for supervisor status and capacity planning.