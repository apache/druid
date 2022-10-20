---
id: kafka-supervisor-reference
title: "Apache Kafka supervisor reference"
sidebar_label: "Apache Kafka supervisor"
description: "Reference topic for Apache Kafka supervisors"
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
This topic contains configuration reference information for the Apache Kafka supervisor for Apache Druid. The following table outlines the high-level configuration options:

|Field|Description|Required|
|--------|-----------|---------|
|`type`|Supervisor type. For Kafka streaming, set to `kafka`.|yes|
|`spec`| Container object for the supervisor configuration. | yes |
|`dataSchema`|Schema for the Kafka indexing task to use during ingestion.|yes|
|`ioConfig`|A `KafkaSupervisorIOConfig` object to define the Kafka connection and I/O-related settings for the supervisor and indexing task. See [KafkaSupervisorIOConfig](#kafkasupervisorioconfig).|yes|
|`tuningConfig`|A KafkaSupervisorTuningConfig object to define performance-related settings for the supervisor and indexing tasks. See [KafkaSupervisorTuningConfig](#kafkasupervisortuningconfig).|no|

## KafkaSupervisorIOConfig

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|`topic`|String|The Kafka topic to read from. Must be a specific topic. Topic patterns are not supported.|yes|
|`inputFormat`|Object|`inputFormat` to define input data parsing. See [Specifying data format](#specifying-data-format) for details about specifying the input format.|yes|
|`consumerProperties`|Map<String, Object>|A map of properties to pass to the Kafka consumer. See [More on consumer properties](#more-on-consumerproperties).|yes|
|`pollTimeout`|Long|The length of time to wait for the Kafka consumer to poll records, in milliseconds|no (default == 100)|
|`replicas`|Integer|The number of replica sets. "1" means a single set of tasks without replication. Druid always assigns replica tasks to different workers to provide resiliency against worker failure.|no (default == 1)|
|`taskCount`|Integer|The maximum number of *reading* tasks in a *replica set*. The maximum number of reading tasks equals `taskCount * replicas`. Therefore, the total number of tasks, *reading* + *publishing*, is greater than this count. See [Capacity Planning](./kafka-supervisor-operations.md#capacity-planning) for more details. When `taskCount > {numKafkaPartitions}`, the actual number of reading tasks is less than the `taskCount` value.|no (default == 1)|
|`taskDuration`|ISO8601 Period|The length of time before tasks stop reading and begin publishing segments.|no (default == PT1H)|
|`startDelay`|ISO8601 Period|The period to wait before the supervisor starts managing tasks.|no (default == PT5S)|
|`period`|ISO8601 Period|Frequency at which the supervisor executes its management logic. The supervisor also runs in response to certain events. For example, task success, task failure, and tasks reaching their `taskDuration`. The `period` value specifies the maximum time between iterations.|no (default == PT30S)|
|`useEarliestOffset`|Boolean|If a supervisor manages a `dataSource` for the first time, it obtains a set of starting offsets from Kafka. This flag determines whether it retrieves the earliest or latest offsets in Kafka. Under normal circumstances, subsequent tasks will start from where the previous segments ended. Therefore Druid only uses `useEarliestOffset` on first run.|no (default == false)|
|`completionTimeout`|ISO8601 Period|The length of time to wait before declaring a publishing task as failed and terminating it. If the value is too low, your tasks may never publish. The publishing clock for a task begins roughly after `taskDuration` elapses.|no (default == PT30M)|
|`lateMessageRejectionStartDateTime`|ISO8601 DateTime|Configure tasks to reject messages with timestamps earlier than this date time; for example if this is set to `2016-01-01T11:00Z` and the supervisor creates a task at *2016-01-01T12:00Z*, Druid drops messages with timestamps earlier than *2016-01-01T11:00Z*. This can prevent concurrency issues if your data stream has late messages and you have multiple pipelines that need to operate on the same segments (e.g. a realtime and a nightly batch ingestion pipeline).|no (default == none)|
|`lateMessageRejectionPeriod`|ISO8601 Period|Configure tasks to reject messages with timestamps earlier than this period before the task was created; for example if this is set to `PT1H` and the supervisor creates a task at *2016-01-01T12:00Z*, messages with timestamps earlier than *2016-01-01T11:00Z* will be dropped. This may help prevent concurrency issues if your data stream has late messages and you have multiple pipelines that need to operate on the same segments (e.g. a realtime and a nightly batch ingestion pipeline). Please note that only one of `lateMessageRejectionPeriod` or `lateMessageRejectionStartDateTime` can be specified.|no (default == none)|
|`earlyMessageRejectionPeriod`|ISO8601 Period|Configure tasks to reject messages with timestamps later than this period after the task reached its taskDuration; for example if this is set to `PT1H`, the taskDuration is set to `PT1H` and the supervisor creates a task at *2016-01-01T12:00Z*, messages with timestamps later than *2016-01-01T14:00Z* will be dropped. **Note:** Tasks sometimes run past their task duration, for example, in cases of supervisor failover. Setting earlyMessageRejectionPeriod too low may cause messages to be dropped unexpectedly whenever a task runs past its originally configured task duration.|no (default == none)|
|`autoScalerConfig`|Object|Defines auto scaling behavior for Kafka ingest tasks. See [Tasks Autoscaler Properties](#task-autoscaler-properties).|no (default == null)|
|`idleConfig`|Object|Defines how and when Kafka Supervisor can become idle. See [Idle Supervisor Configuration](#idle-supervisor-configuration) for more details.|no (default == null)|

## Task Autoscaler Properties

> Note that Task AutoScaler is currently designated as experimental.

| Property | Description | Required |
| ------------- | ------------- | ------------- |
| `enableTaskAutoScaler` | Enable or disable autoscaling. `false` or blank disables the `autoScaler` even when `autoScalerConfig` is not null| no (default == false) |
| `taskCountMax` | Maximum number of ingestion tasks. Set `taskCountMax >= taskCountMin`. If `taskCountMax > {numKafkaPartitions}`, Druid only scales reading tasks up to the `{numKafkaPartitions}`. In this case `taskCountMax` is ignored.  | yes |
| `taskCountMin` | Minimum number of ingestion tasks. When you enable autoscaler, Druid ignores the value of taskCount in `IOConfig` and starts with the `taskCountMin` number of tasks.| yes |
| `minTriggerScaleActionFrequencyMillis` | Minimum time interval between two scale actions. | no (default == 600000) |
| `autoScalerStrategy` | The algorithm of `autoScaler`. Only supports `lagBased`. See [Lag Based AutoScaler Strategy Related Properties](#lag-based-autoscaler-strategy-related-properties) for details.| no (default == `lagBased`) |

## Lag Based AutoScaler Strategy Related Properties
| Property | Description | Required |
| ------------- | ------------- | ------------- |
| `lagCollectionIntervalMillis` | Period of lag points collection.  | no (default == 30000) |
| `lagCollectionRangeMillis` | The total time window of lag collection. Use with `lagCollectionIntervalMillis`，it means that in the recent `lagCollectionRangeMillis`, collect lag metric points every `lagCollectionIntervalMillis`. | no (default == 600000) |
| `scaleOutThreshold` | The threshold of scale out action | no (default == 6000000) |
| `triggerScaleOutFractionThreshold` | If `triggerScaleOutFractionThreshold` percent of lag points are higher than `scaleOutThreshold`, then do scale out action. | no (default == 0.3) |
| `scaleInThreshold` | The Threshold of scale in action | no (default == 1000000) |
| `triggerScaleInFractionThreshold` | If `triggerScaleInFractionThreshold` percent of lag points are lower than `scaleOutThreshold`, then do scale in action. | no (default == 0.9) |
| `scaleActionStartDelayMillis` | Number of milliseconds after supervisor starts when first check scale logic. | no (default == 300000) |
| `scaleActionPeriodMillis` | The frequency of checking whether to do scale action in millis | no (default == 60000) |
| `scaleInStep` | How many tasks to reduce at a time | no (default == 1) |
| `scaleOutStep` | How many tasks to add at a time | no (default == 2) |

## Idle Supervisor Configuration

> Note that Idle state transitioning is currently designated as experimental.

| Property | Description | Required |
| ------------- | ------------- | ------------- |
| `enabled` | If `true`, Kafka supervisor will become idle if there is no data on input stream/topic for some time. | no (default == false) |
| `inactiveAfterMillis` | Supervisor is marked as idle if all existing data has been read from input topic and no new data has been published for `inactiveAfterMillis` milliseconds. | no (default == 600000) |

The following example demonstrates supervisor spec with `lagBased` autoScaler and idle config enabled:
```json
{
    "type": "kafka",
    "spec": {
      "dataSchema": {
        ...
      },
      "ioConfig": {
         "topic": "metrics",
         "inputFormat": {
             "type": "json"
         },
          "consumerProperties": {
             "bootstrap.servers": "localhost:9092"
          },
         "autoScalerConfig": {
             "enableTaskAutoScaler": true,
              "taskCountMax": 6,
              "taskCountMin": 2,
             "minTriggerScaleActionFrequencyMillis": 600000,
             "autoScalerStrategy": "lagBased",
              "lagCollectionIntervalMillis": 30000,
              "lagCollectionRangeMillis": 600000,
              "scaleOutThreshold": 6000000,
              "triggerScaleOutFractionThreshold": 0.3,
             "scaleInThreshold": 1000000,
             "triggerScaleInFractionThreshold": 0.9,
              "scaleActionStartDelayMillis": 300000,
              "scaleActionPeriodMillis": 60000,
              "scaleInStep": 1,
             "scaleOutStep": 2
         },
         "taskCount":1,
         "replicas":1,
         "taskDuration":"PT1H",
         "idleConfig": {
           "enabled": true,
           "inactiveAfterMillis": 600000 
         }
      },
     "tuningConfig":{
        ...
     }
    }
}
```

## More on consumerProperties

Consumer properties must contain a property `bootstrap.servers` with a list of Kafka brokers in the form: `<BROKER_1>:<PORT_1>,<BROKER_2>:<PORT_2>,...`.
By default, `isolation.level` is set to `read_committed`. If you use older versions of Kafka servers without transactions support or don't want Druid to consume only committed transactions, set `isolation.level` to `read_uncommitted`.

In some cases, you may need to fetch consumer properties at runtime. For example, when `bootstrap.servers` is not known upfront, or is not static. To enable SSL connections, you must provide passwords for `keystore`, `truststore` and `key` secretly. You can provide configurations at runtime with a dynamic config provider implementation like the environment variable config provider that comes with Druid. For more information, see [DynamicConfigProvider](../../operations/dynamic-config-provider.md).

For example, if you are using SASL and SSL with Kafka, set the following environment variables for the Druid user on the machines running the Overlord and the Peon services:

```
export KAFKA_JAAS_CONFIG="org.apache.kafka.common.security.plain.PlainLoginModule required username='admin_user' password='admin_password';"
export SSL_KEY_PASSWORD=mysecretkeypassword
export SSL_KEYSTORE_PASSWORD=mysecretkeystorepassword
export SSL_TRUSTSTORE_PASSWORD=mysecrettruststorepassword
```

```
        "druid.dynamic.config.provider": {
          "type": "environment",
          "variables": {
            "sasl.jaas.config": "KAFKA_JAAS_CONFIG",
            "ssl.key.password": "SSL_KEY_PASSWORD",
            "ssl.keystore.password": "SSL_KEYSTORE_PASSWORD",
            "ssl.truststore.password": "SSL_TRUSTSTORE_PASSWORD"
          }
        }
      }
```
Verify that you've changed the values for all configurations to match your own environment.  You can use the environment variable config provider syntax in the **Consumer properties** field on the **Connect tab** in the **Load Data** UI in the web console. When connecting to Kafka, Druid replaces the environment variables with their corresponding values.

Note: You can provide SSL connections with  [Password Provider](../../operations/password-provider.md) interface to define the `keystore`, `truststore`, and `key`, but this feature is deprecated.

## Specifying data format

Kafka indexing service supports both [`inputFormat`](../../ingestion/data-formats.md#input-format) and [`parser`](../../ingestion/data-formats.md#parser) to specify the data format.
Use the `inputFormat` to specify the data format for Kafka indexing service unless you need a format only supported by the legacy `parser`.

Supported `inputFormat`s include:
- `csv`
- `delimited`
- `json`
- `kafka`
- `avro_stream`
- `avro_ocf`
- `protobuf`

For more information, see [Data formats](../../ingestion/data-formats.md). You can also read [`thrift`](../extensions-contrib/thrift.md) formats using `parser`.

<a name="tuningconfig"></a>

## KafkaSupervisorTuningConfig

The `tuningConfig` is optional and default parameters will be used if no `tuningConfig` is specified.

| Field                             | Type           | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | Required                                                                                                     |
|-----------------------------------|----------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------|
| `type`                            | String         | The indexing task type, this should always be `kafka`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | yes                                                                                                          |
| `maxRowsInMemory`                 | Integer        | The number of rows to aggregate before persisting. This number is the post-aggregation rows, so it is not equivalent to the number of input events, but the number of aggregated rows that those events result in. This is used to manage the required JVM heap size. Maximum heap memory usage for indexing scales with `maxRowsInMemory` * (2 + `maxPendingPersists`). Normally user does not need to set this, but depending on the nature of data, if rows are short in terms of bytes, user may not want to store a million rows in memory and this value should be set.                                                                           | no (default == 1000000)                                                                                      |
| `maxBytesInMemory`                | Long           | The number of bytes to aggregate in heap memory before persisting. This is based on a rough estimate of memory usage and not actual usage. Normally this is computed internally and user does not need to set it. The maximum heap memory usage for indexing is `maxBytesInMemory` * (2 + `maxPendingPersists`).                                                                                                                                                                                                                                                                                                                                        | no (default == One-sixth of max JVM memory)                                                                  |
| `maxRowsPerSegment`               | Integer        | The number of rows to aggregate into a segment; this number is post-aggregation rows. Handoff will happen either if `maxRowsPerSegment` or `maxTotalRows` is hit or every `intermediateHandoffPeriod`, whichever happens earlier.                                                                                                                                                                                                                                                                                                                                                                                                                   | no (default == 5000000)                                                                                      |
| `maxTotalRows`                    | Long           | The number of rows to aggregate across all segments; this number is post-aggregation rows. Handoff will happen either if `maxRowsPerSegment` or `maxTotalRows` is hit or every `intermediateHandoffPeriod`, whichever happens earlier.                                                                                                                                                                                                                                                                                                                                                                                                              | no (default == unlimited)                                                                                    |
| `intermediatePersistPeriod`       | ISO8601 Period | The period that determines the rate at which intermediate persists occur.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           | no (default == PT10M)                                                                                        |
| `maxPendingPersists`              | Integer        | Maximum number of persists that can be pending but not started. If this limit would be exceeded by a new intermediate persist, ingestion will block until the currently-running persist finishes. Maximum heap memory usage for indexing scales with `maxRowsInMemory` * (2 + `maxPendingPersists`).                                                                                                                                                                                                                                                                                                                                                    | no (default == 0, meaning one persist can be running concurrently with ingestion, and none can be queued up) |
| `indexSpec`                       | Object         | Tune how data is indexed. See [IndexSpec](#indexspec) for more information.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | no                                                                                                           |
| `indexSpecForIntermediatePersists`|                | Defines segment storage format options to be used at indexing time for intermediate persisted temporary segments. This can be used to disable dimension/metric compression on intermediate segments to reduce memory required for final merging. However, disabling compression on intermediate segments might increase page cache use while they are used before getting merged into final segment published, see [IndexSpec](#indexspec) for possible values.                                                                                                                                                                                     | no (default = same as `indexSpec`)                                                                             |
| `reportParseExceptions`           | Boolean        | *DEPRECATED*. If true, exceptions encountered during parsing will be thrown and will halt ingestion; if false, unparseable rows and fields will be skipped. Setting `reportParseExceptions` to true will override existing configurations for `maxParseExceptions` and `maxSavedParseExceptions`, setting `maxParseExceptions` to 0 and limiting `maxSavedParseExceptions` to no more than 1.                                                                                                                                                                                                                                                       | no (default == false)                                                                                        |
| `handoffConditionTimeout`         | Long           | Milliseconds to wait for segment handoff. It must be >= 0, where 0 means to wait forever.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           | no (default == 0)                                                                                            |
| `resetOffsetAutomatically`        | Boolean        | Controls behavior when Druid needs to read Kafka messages that are no longer available (i.e. when `OffsetOutOfRangeException` is encountered).<br/><br/>If false, the exception will bubble up, which will cause your tasks to fail and ingestion to halt. If this occurs, manual intervention is required to correct the situation; potentially using the [Reset Supervisor API](../../operations/api-reference.md#supervisors). This mode is useful for production, since it will make you aware of issues with ingestion.<br/><br/>If true, Druid will automatically reset to the earlier or latest offset available in Kafka, based on the value of the `useEarliestOffset` property (earliest if true, latest if false). Note that this can lead to data being _DROPPED_ (if `useEarliestOffset` is false) or _DUPLICATED_ (if `useEarliestOffset` is true) without your knowledge. Messages will be logged indicating that a reset has occurred, but ingestion will continue. This mode is useful for non-production situations, since it will make Druid attempt to recover from problems automatically, even if they lead to quiet dropping or duplicating of data.<br/><br/>This feature behaves similarly to the Kafka `auto.offset.reset` consumer property. | no (default == false) |
| `workerThreads`                   | Integer        | The number of threads that the supervisor uses to handle requests/responses for worker tasks, along with any other internal asynchronous operation.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | no (default == min(10, taskCount))                                                                           |
| `chatThreads`                     | Integer        | The number of threads that will be used for communicating with indexing tasks.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | no (default == min(10, taskCount * replicas))                                                                |
| `chatRetries`                     | Integer        | The number of times HTTP requests to indexing tasks will be retried before considering tasks unresponsive.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | no (default == 8)                                                                                            |
| `httpTimeout`                     | ISO8601 Period | How long to wait for a HTTP response from an indexing task.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | no (default == PT10S)                                                                                        |
| `shutdownTimeout`                 | ISO8601 Period | How long to wait for the supervisor to attempt a graceful shutdown of tasks before exiting.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | no (default == PT80S)                                                                                        |
| `offsetFetchPeriod`               | ISO8601 Period | How often the supervisor queries Kafka and the indexing tasks to fetch current offsets and calculate lag. If the user-specified value is below the minimum value (`PT5S`), the supervisor ignores the value and uses the minimum value instead.                                                                                                                                                                                                                                                                                                                                                                                                          | no (default == PT30S, min == PT5S)                                                                           |
| `segmentWriteOutMediumFactory`    | Object         | Segment write-out medium to use when creating segments. See below for more information.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | no (not specified by default, the value from `druid.peon.defaultSegmentWriteOutMediumFactory.type` is used)  |
| `intermediateHandoffPeriod`       | ISO8601 Period | How often the tasks should hand off segments. Handoff will happen either if `maxRowsPerSegment` or `maxTotalRows` is hit or every `intermediateHandoffPeriod`, whichever happens earlier.                                                                                                                                                                                                                                                                                                                                                                                                                                                           | no (default == P2147483647D)                                                                                 |
| `logParseExceptions`              | Boolean        | If true, log an error message when a parsing exception occurs, containing information about the row where the error occurred.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | no, default == false                                                                                         |
| `maxParseExceptions`              | Integer        | The maximum number of parse exceptions that can occur before the task halts ingestion and fails. Overridden if `reportParseExceptions` is set.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | no, unlimited default                                                                                        |
| `maxSavedParseExceptions`         | Integer        | When a parse exception occurs, Druid can keep track of the most recent parse exceptions. `maxSavedParseExceptions` limits how many exception instances will be saved. These saved exceptions will be made available after the task finishes in the [task completion report](../../ingestion/tasks.md#reports). Overridden if `reportParseExceptions` is set.                                                                                                                                                                                                                                                                                            | no, default == 0                                                                                             |

#### IndexSpec

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|bitmap|Object|Compression format for bitmap indexes. Should be a JSON object. See [Bitmap types](#bitmap-types) below for options.|no (defaults to Roaring)|
|dimensionCompression|String|Compression format for dimension columns. Choose from `LZ4`, `LZF`, `ZSTD` or `uncompressed`.|no (default == `LZ4`)|
|metricCompression|String|Compression format for primitive type metric columns. Choose from `LZ4`, `LZF`, `ZSTD`, `uncompressed` or `none`.|no (default == `LZ4`)|
|longEncoding|String|Encoding format for metric and dimension columns with type long. Choose from `auto` or `longs`. `auto` encodes the values using offset or lookup table depending on column cardinality, and store them with variable size. `longs` stores the value as is with 8 bytes each.|no (default == `longs`)|

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
