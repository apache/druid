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

When you enable the Kinesis indexing service, you can configure supervisors on the Overlord to manage the creation and lifetime of Kinesis indexing tasks. Kinesis indexing tasks read events using the Kinesis shard and sequence number mechanism to guarantee exactly-once ingestion. The supervisor oversees the state of the indexing tasks to coordinate handoffs, manage failures, and ensure that scalability and replication requirements are maintained.

This topic contains configuration information for the Kinesis indexing service supervisor for Apache Druid.

## Setup

To use the Kinesis indexing service, you must first load the `druid-kinesis-indexing-service` core extension on both the Overlord and the MiddleManager. See [Loading extensions](../configuration/extensions.md#loading-extensions) for more information.

Review [Known issues](#known-issues) before deploying the `druid-kinesis-indexing-service` extension to production.

## Supervisor spec configuration

This section outlines the configuration properties that are specific to the Amazon Kinesis streaming ingestion method. For configuration properties shared across all streaming ingestion methods supported by Druid, see [Supervisor spec](supervisor.md#supervisor-spec).

The following example shows a supervisor spec for a stream with the name `KinesisStream`:

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

The following table outlines the `ioConfig` configuration properties specific to Kinesis.
For configuration properties shared across all streaming ingestion methods, refer to [Supervisor I/O configuration](supervisor.md#io-configuration).

|Property|Type|Description|Required|Default|
|--------|----|-----------|--------|-------|
|`stream`|String|The Kinesis stream to read.|Yes||
|`endpoint`|String|The AWS Kinesis stream endpoint for a region. You can find a list of endpoints in the [AWS service endpoints](http://docs.aws.amazon.com/general/latest/gr/rande.html#ak_region) document.|No|`kinesis.us-east-1.amazonaws.com`|
|`useEarliestSequenceNumber`|Boolean|If a supervisor is managing a datasource for the first time, it obtains a set of starting sequence numbers from Kinesis. This flag determines whether a supervisor retrieves the earliest or latest sequence numbers in Kinesis. Under normal circumstances, subsequent tasks start from where the previous segments ended so this flag is only used on the first run.|No|`false`|
|`fetchDelayMillis`|Integer|Time in milliseconds to wait between subsequent calls to fetch records from Kinesis. See [Determine fetch settings](#determine-fetch-settings).|No|0|
|`awsAssumedRoleArn`|String|The AWS assumed role to use for additional permissions.|No||
|`awsExternalId`|String|The AWS external ID to use for additional permissions.|No||

#### Data format

The Kinesis indexing service supports both [`inputFormat`](data-formats.md#input-format) and [`parser`](data-formats.md#parser) to specify the data format. Use the `inputFormat` to specify the data format for the Kinesis indexing service unless you need a format only supported by the legacy `parser`. For more information, see [Source input formats](data-formats.md).

The Kinesis indexing service supports the following values for `inputFormat`:

* `csv`
* `tvs`
* `json`
* `avro_stream`
* `avro_ocf`
* `protobuf`

You can use `parser` to read [`thrift`](../development/extensions-contrib/thrift.md) formats.

### Tuning configuration

The following table outlines the `tuningConfig` configuration properties specific to Kinesis.
For configuration properties shared across all streaming ingestion methods, refer to [Supervisor tuning configuration](supervisor.md#tuning-configuration).

|Property|Type|Description|Required|Default|
|--------|----|-----------|--------|-------|
|`skipSequenceNumberAvailabilityCheck`|Boolean|Whether to enable checking if the current sequence number is still available in a particular Kinesis shard. If `false`, the indexing task attempts to reset the current sequence number, depending on the value of `resetOffsetAutomatically`.|No|`false`|
|`recordBufferSizeBytes`|Integer| The size of the buffer (heap memory bytes) Druid uses between the Kinesis fetch threads and the main ingestion thread.|No| See [Determine fetch settings](#determine-fetch-settings) for defaults.|
|`recordBufferOfferTimeout`|Integer|The number of milliseconds to wait for space to become available in the buffer before timing out.|No|5000|
|`recordBufferFullWait`|Integer|The number of milliseconds to wait for the buffer to drain before Druid attempts to fetch records from Kinesis again.|No|5000|
|`fetchThreads`|Integer|The size of the pool of threads fetching data from Kinesis. There is no benefit in having more threads than Kinesis shards.|No| `procs * 2`, where `procs` is the number of processors available to the task.|
|`maxBytesPerPoll`|Integer| The maximum number of bytes to be fetched from buffer per poll. At least one record is polled from the buffer regardless of this config.|No| 1000000 bytes|
|`repartitionTransitionDuration`|ISO 8601 period|When shards are split or merged, the supervisor recomputes shard to task group mappings. The supervisor also signals any running tasks created under the old mappings to stop early at current time + `repartitionTransitionDuration`. Stopping the tasks early allows Druid to begin reading from the new shards more quickly. The repartition transition wait time controlled by this property gives the stream additional time to write records to the new shards after the split or merge, which helps avoid issues with [empty shard handling](https://github.com/apache/druid/issues/7600).|No|`PT2M`|
|`useListShards`|Boolean|Indicates if `listShards` API of AWS Kinesis SDK can be used to prevent `LimitExceededException` during ingestion. You must set the necessary `IAM` permissions.|No|`false`|

## AWS authentication

Druid uses AWS access and secret keys to authenticate Kinesis API requests. There are a few ways to provide this information to Druid:

1. Using roles or short-term credentials:

  Druid looks for credentials set in [environment variables](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html), via [Web Identity Token](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_providers_oidc.html), in the default [profile configuration file](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html), and from the EC2 instance profile provider (in this order).

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
Each fetch thread fetches up to 10 MB of records at once from a Kinesis shard, with a delay between fetches of `fetchDelayMillis`.
The records fetched by each thread are pushed into a shared queue of size `recordBufferSizeBytes`.

The default values for these parameters are:

- `fetchThreads`: Twice the number of processors available to the task. The number of processors available to the task
is the total number of processors on the server, divided by `druid.worker.capacity` (the number of task slots on that
particular server). This value is further limited so that the total data record data fetched at a given time does not
exceed 5% of the max heap configured, assuming that each thread fetches 10 MB of records at once. If the value specified
for this configuration is higher than this limit, no failure occurs, but a warning is logged, and the value is
implicitly lowered to the max allowed by this constraint.
- `fetchDelayMillis`: 0 (no delay between fetches).
- `recordBufferSizeBytes`: 100 MB or an estimated 10% of available heap, whichever is smaller.
- `maxBytesPerPoll`: 1000000.

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

The Kinesis indexing service supports de-aggregation of multiple rows stored within a single [Kinesis Data Streams](https://docs.aws.amazon.com/streams/latest/dev/introduction.html) record for more efficient data transfer.

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