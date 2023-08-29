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

This topic contains configuration reference information for the Apache Kafka supervisor for Apache Druid.

The following table outlines the high-level configuration options:

|Property|Type|Description|Required|
|--------|----|-----------|--------|
|`type`|String|The supervisor type. For Kafka streaming, set to `kafka`.|Yes|
|`spec`|Object|The container object for the supervisor configuration.|Yes|
|`ioConfig`|Object|The I/O configuration object to define the Kafka connection and I/O-related settings for the supervisor and indexing task. See [Supervisor I/O configuration](#supervisor-io-configuration).|Yes|
|`dataSchema`|Object|The schema for the Kafka indexing task to use during ingestion.|Yes|
|`tuningConfig`|Object|The tuning configuration object to define performance-related settings for the supervisor and indexing tasks. See [Supervisor tuning configuration](#supervisor-tuning-configuration).|No|

## Supervisor I/O configuration

The following table outlines the configuration options for `ioConfig`:

|Property|Type|Description|Required|Default|
|--------|----|-----------|--------|-------|
|`topic`|String|The Kafka topic to read from. Must be a specific topic. Druid does not support topic patterns.|Yes||
|`inputFormat`|Object|The input format to define input data parsing. See [Specifying data format](#specifying-data-format) for details about specifying the input format.|Yes||
|`consumerProperties`|String, Object|A map of properties to pass to the Kafka consumer. See [Consumer properties](#consumer-properties).|Yes||
|`pollTimeout`|Long|The length of time to wait for the Kafka consumer to poll records, in milliseconds.|No|100|
|`replicas`|Integer|The number of replica sets, where 1 is a single set of tasks (no replication). Druid always assigns replicate tasks to different workers to provide resiliency against process failure.|No|1|
|`taskCount`|Integer|The maximum number of reading tasks in a replica set. The maximum number of reading tasks equals `taskCount * replicas`. The total number of tasks, reading and publishing, is greater than this count. See [Capacity planning](./kafka-supervisor-operations.md#capacity-planning) for more details. When `taskCount > {numKafkaPartitions}`, the actual number of reading tasks is less than the `taskCount` value.|No|1|
|`taskDuration`|ISO 8601 period|The length of time before tasks stop reading and begin publishing segments.|No|PT1H|
|`startDelay`|ISO 8601 period|The period to wait before the supervisor starts managing tasks.|No|PT5S|
|`period`|ISO 8601 period|Determines how often the supervisor executes its management logic. Note that the supervisor also runs in response to certain events, such as tasks succeeding, failing, and reaching their task duration. The `period` value specifies the maximum time between iterations.|No|PT30S|
|`useEarliestOffset`|Boolean|If a supervisor manages a datasource for the first time, it obtains a set of starting offsets from Kafka. This flag determines whether it retrieves the earliest or latest offsets in Kafka. Under normal circumstances, subsequent tasks start from where the previous segments ended. Druid only uses `useEarliestOffset` on the first run.|No|`false`|
|`completionTimeout`|ISO 8601 period|The length of time to wait before declaring a publishing task as failed and terminating it. If the value is too low, your tasks may never publish. The publishing clock for a task begins roughly after `taskDuration` elapses.|No|PT30M|
|`lateMessageRejectionStartDateTime`|ISO 8601 date time|Configure tasks to reject messages with timestamps earlier than this date time. For example, if this property is set to `2016-01-01T11:00Z` and the supervisor creates a task at `2016-01-01T12:00Z`, Druid drops messages with timestamps earlier than `2016-01-01T11:00Z`. This can prevent concurrency issues if your data stream has late messages and you have multiple pipelines that need to operate on the same segments, such as a realtime and a nightly batch ingestion pipeline.|No||
|`lateMessageRejectionPeriod`|ISO 8601 period|Configure tasks to reject messages with timestamps earlier than this period before the task was created. For example, if this property is set to `PT1H` and the supervisor creates a task at `2016-01-01T12:00Z`, Druid drops messages with timestamps earlier than `2016-01-01T11:00Z`. This may help prevent concurrency issues if your data stream has late messages and you have multiple pipelines that need to operate on the same segments, such as a realtime and a nightly batch ingestion pipeline. Note that you can specify only one of the late message rejection properties.|No||
|`earlyMessageRejectionPeriod`|ISO 8601 period|Configure tasks to reject messages with timestamps later than this period after the task reached its task duration. For example, if this property is set to `PT1H`, the task duration is set to `PT1H` and the supervisor creates a task at `2016-01-01T12:00Z`, Druid drops messages with timestamps later than `2016-01-01T14:00Z`. Tasks sometimes run past their task duration, such as in cases of supervisor failover. Setting `earlyMessageRejectionPeriod` too low may cause Druid to drop messages unexpectedly whenever a task runs past its originally configured task duration.|No||
|`autoScalerConfig`|Object|Defines auto scaling behavior for Kafka ingest tasks. See [Task autoscaler properties](#task-autoscaler-properties).|No|null|
|`idleConfig`|Object|Defines how and when the Kafka supervisor can become idle. See [Idle supervisor configuration](#idle-supervisor-configuration) for more details.|No|null|

### Task autoscaler properties

The following table outlines the configuration options for `autoScalerConfig`:

|Property|Description|Required|Default|
|--------|-----------|--------|-------|
|`enableTaskAutoScaler`|Enable or disable autoscaling. `false` or blank disables the `autoScaler` even when `autoScalerConfig` is not null.|No|`false`|
|`taskCountMax`|Maximum number of ingestion tasks. Set `taskCountMax >= taskCountMin`. If `taskCountMax > {numKafkaPartitions}`, Druid only scales reading tasks up to the `{numKafkaPartitions}`. In this case, `taskCountMax` is ignored.|Yes||
|`taskCountMin`|Minimum number of ingestion tasks. When you enable the autoscaler, Druid ignores the value of `taskCount` in `ioConfig` and starts with the `taskCountMin` number of tasks.|Yes||
|`minTriggerScaleActionFrequencyMillis`|Minimum time interval between two scale actions.|No|600000|
|`autoScalerStrategy`|The algorithm of `autoScaler`. Only supports `lagBased`. See [Lag based autoscaler strategy related properties](#lag-based-autoscaler-strategy-related-properties) for details.|No|`lagBased`|

### Lag based autoscaler strategy related properties

The following table outlines the configuration options for `autoScalerStrategy`:

|Property|Description|Required|Default|
|--------|-----------|--------|-------|
|`lagCollectionIntervalMillis`|The time period during which Druid collects lag metric points.|No|30000|
|`lagCollectionRangeMillis`|The total time window of lag collection. Use with `lagCollectionIntervalMillis` to specify the intervals at which to collect lag metric points.|No|600000|
|`scaleOutThreshold`|The threshold of scale out action.|No|6000000|
|`triggerScaleOutFractionThreshold`|Enables scale out action if `triggerScaleOutFractionThreshold` percent of lag points is higher than `scaleOutThreshold`.|No|0.3|
|`scaleInThreshold`|The threshold of scale in action.|No|1000000|
|`triggerScaleInFractionThreshold`|Enables scale in action if `triggerScaleInFractionThreshold` percent of lag points is lower than `scaleOutThreshold`.|No|0.9|
|`scaleActionStartDelayMillis`|The number of milliseconds to delay after the supervisor starts before the first scale logic check.|No|300000|
|`scaleActionPeriodMillis`|The frequency in milliseconds to check if a scale action is triggered.|No|60000|
|`scaleInStep`|The number of tasks to reduce at once when scaling down.|No|1|
|`scaleOutStep`|The number of tasks to add at once when scaling out.|No|2|

### Ingesting from multiple topics

To ingest data from multiple topics, you have to set `topicPattern` in the supervisor I/O configuration and not set `topic`.
You can pass multiple topics as a regex pattern as the value for `topicPattern` in the I/O configuration. For example, to
ingest data from clicks and impressions, set `topicPattern` to `clicks|impressions` in the I/O configuration.
Similarly, you can use `metrics-.*` as the value for `topicPattern` if you want to ingest from all the topics that
start with `metrics-`. If new topics are added to the cluster that match the regex, Druid automatically starts
ingesting from those new topics. A topic name that only matches partially such as `my-metrics-12` will not be
included for ingestion. If you enable multi-topic ingestion for a datasource, downgrading to a version older than
28.0.0 will cause the ingestion for that datasource to fail.

When ingesting data from multiple topics, partitions are assigned based on the hashcode of the topic name and the
id of the partition within that topic. The partition assignment might not be uniform across all the tasks. It's also
assumed that partitions across individual topics have similar load. It is recommended that you have a higher number of
partitions for a high load topic and a lower number of partitions for a low load topic. Assuming that you want to
ingest from both high and low load topic in the same supervisor.

## Idle supervisor configuration

:::info
 Note that idle state transitioning is currently designated as experimental.
:::

|Property|Description|Required|
|--------|-----------|--------|
|`enabled`|If `true`, the supervisor becomes idle if there is no data on input stream/topic for some time.|No|`false`|
|`inactiveAfterMillis`|The supervisor becomes idle if all existing data has been read from input topic and no new data has been published for `inactiveAfterMillis` milliseconds.|No|`600_000`|

When the supervisor enters the idle state, no new tasks are launched subsequent to the completion of the currently executing tasks. This strategy may lead to reduced costs for cluster operators while using topics that get sporadic data.

The following example demonstrates supervisor spec with `lagBased` autoscaler and idle configuration enabled:

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

## Consumer properties

Consumer properties must contain a property `bootstrap.servers` with a list of Kafka brokers in the form: `<BROKER_1>:<PORT_1>,<BROKER_2>:<PORT_2>,...`.
By default, `isolation.level` is set to `read_committed`. If you use older versions of Kafka servers without transactions support or don't want Druid to consume only committed transactions, set `isolation.level` to `read_uncommitted`.

In some cases, you may need to fetch consumer properties at runtime. For example, when `bootstrap.servers` is not known upfront, or is not static. To enable SSL connections, you must provide passwords for `keystore`, `truststore` and `key` secretly. You can provide configurations at runtime with a dynamic config provider implementation like the environment variable config provider that comes with Druid. For more information, see [Dynamic config provider](../../operations/dynamic-config-provider.md).

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

Verify that you've changed the values for all configurations to match your own environment. You can use the environment variable config provider syntax in the **Consumer properties** field on the **Connect tab** in the **Load Data** UI in the web console. When connecting to Kafka, Druid replaces the environment variables with their corresponding values.

You can provide SSL connections with [Password provider](../../operations/password-provider.md) interface to define the `keystore`, `truststore`, and `key`, but this feature is deprecated.

## Specifying data format

The Kafka indexing service supports both [`inputFormat`](../../ingestion/data-formats.md#input-format) and [`parser`](../../ingestion/data-formats.md#parser) to specify the data format.
Use the `inputFormat` to specify the data format for Kafka indexing service unless you need a format only supported by the legacy `parser`.

Druid supports the following input formats:

- `csv`
- `tsv`
- `json`
- `kafka`
- `avro_stream`
- `avro_ocf`
- `protobuf`

For more information, see [Data formats](../../ingestion/data-formats.md). You can also read [`thrift`](../extensions-contrib/thrift.md) formats using `parser`.

## Supervisor tuning configuration

The `tuningConfig` object is optional. If you don't specify the `tuningConfig` object, Druid uses the default configuration settings.

|Property|Type|Description|Required|Default|
|--------|----|-----------|--------|-------|
|`type`|String|The indexing task type. This should always be `kafka`.|Yes||
|`maxRowsInMemory`|Integer|The number of rows to aggregate before persisting. This number represents the post-aggregation rows. It is not equivalent to the number of input events, but the resulting number of aggregated rows. Druid uses `maxRowsInMemory` to manage the required JVM heap size. The maximum heap memory usage for indexing scales is `maxRowsInMemory * (2 + maxPendingPersists)`. Normally, you do not need to set this, but depending on the nature of data, if rows are short in terms of bytes, you may not want to store a million rows in memory and this value should be set.|No|150000|
|`maxBytesInMemory`|Long|The number of bytes to aggregate in heap memory before persisting. This is based on a rough estimate of memory usage and not actual usage. Normally, this is computed internally. The maximum heap memory usage for indexing is `maxBytesInMemory * (2 + maxPendingPersists)`.|No|One-sixth of max JVM memory|
|`skipBytesInMemoryOverheadCheck`|Boolean|The calculation of `maxBytesInMemory` takes into account overhead objects created during ingestion and each intermediate persist. To exclude the bytes of these overhead objects from the `maxBytesInMemory` check, set `skipBytesInMemoryOverheadCheck` to `true`.|No|`false`|
|`maxRowsPerSegment`|Integer|The number of rows to store in a segment. This number is post-aggregation rows. Handoff occurs when `maxRowsPerSegment` or `maxTotalRows` is reached or every `intermediateHandoffPeriod`, whichever happens first.|No|5000000|
|`maxTotalRows`|Long|The number of rows to aggregate across all segments; this number is post-aggregation rows. Handoff happens either if `maxRowsPerSegment` or `maxTotalRows` is reached or every `intermediateHandoffPeriod`, whichever happens earlier.|No|20000000|
|`intermediateHandoffPeriod`|ISO 8601 period|The period that determines how often tasks hand off segments. Handoff occurs if `maxRowsPerSegment` or `maxTotalRows` is reached or every `intermediateHandoffPeriod`, whichever happens first.|No|P2147483647D|
|`intermediatePersistPeriod`|ISO 8601 period|The period that determines the rate at which intermediate persists occur.|No|PT10M|
|`maxPendingPersists`|Integer|Maximum number of persists that can be pending but not started. If a new intermediate persist exceeds this limit, Druid blocks ingestion until the currently running persist finishes. One persist can be running concurrently with ingestion, and none can be queued up. The maximum heap memory usage for indexing scales is `maxRowsInMemory * (2 + maxPendingPersists)`.|No|0|
|`indexSpec`|Object|Defines how Druid indexes the data. See [IndexSpec](#indexspec) for more information.|No||
|`indexSpecForIntermediatePersists`|Object|Defines segment storage format options to use at indexing time for intermediate persisted temporary segments. You can use `indexSpecForIntermediatePersists` to disable dimension/metric compression on intermediate segments to reduce memory required for final merging. However, disabling compression on intermediate segments might increase page cache use while they are used before getting merged into final segment published. See [IndexSpec](#indexspec) for possible values.|No|Same as `indexSpec`|
|`reportParseExceptions`|Boolean|DEPRECATED. If `true`, Druid throws exceptions encountered during parsing causing ingestion to halt. If `false`, Druid skips unparseable rows and fields. Setting `reportParseExceptions` to `true` overrides existing configurations for `maxParseExceptions` and `maxSavedParseExceptions`, setting `maxParseExceptions` to 0 and limiting `maxSavedParseExceptions` to not more than 1.|No|`false`|
|`handoffConditionTimeout`|Long|Number of milliseconds to wait for segment handoff. Set to a value >= 0, where 0 means to wait indefinitely.|No|900000 (15 minutes)|
|`resetOffsetAutomatically`|Boolean|Controls behavior when Druid needs to read Kafka messages that are no longer available, when `offsetOutOfRangeException` is encountered.<br/>If `false`, the exception bubbles up causing tasks to fail and ingestion to halt. If this occurs, manual intervention is required to correct the situation, potentially using the [Reset Supervisor API](../../api-reference/supervisor-api.md). This mode is useful for production, since it will make you aware of issues with ingestion.<br/>If `true`, Druid will automatically reset to the earlier or latest offset available in Kafka, based on the value of the `useEarliestOffset` property (earliest if `true`, latest if `false`). Note that this can lead to dropping data (if `useEarliestSequenceNumber` is `false`) or duplicating data (if `useEarliestSequenceNumber` is `true`) without your knowledge. Druid logs messages indicating that a reset has occurred without interrupting ingestion. This mode is useful for non-production situations since it enables Druid to recover from problems automatically, even if they lead to quiet dropping or duplicating of data. This feature behaves similarly to the Kafka `auto.offset.reset` consumer property.|No|`false`|
|`workerThreads`|Integer|The number of threads that the supervisor uses to handle requests/responses for worker tasks, along with any other internal asynchronous operation.|No|`min(10, taskCount)`|
|`chatAsync`|Boolean|If `true`, use asynchronous communication with indexing tasks, and ignore the `chatThreads` parameter. If `false`, use synchronous communication in a thread pool of size `chatThreads`.|No|`true`|
|`chatThreads`|Integer|The number of threads to use for communicating with indexing tasks. Ignored if `chatAsync` is `true`.|No|`min(10, taskCount * replicas)`|
|`chatRetries`|Integer|The number of times HTTP requests to indexing tasks are retried before considering tasks unresponsive.|No|8|
|`httpTimeout`| ISO 8601 period|The period of time to wait for a HTTP response from an indexing task.|No|PT10S|
|`shutdownTimeout`|ISO 8601 period|The period of time to wait for the supervisor to attempt a graceful shutdown of tasks before exiting.|No|PT80S|
|`offsetFetchPeriod`|ISO 8601 period|Determines how often the supervisor queries Kafka and the indexing tasks to fetch current offsets and calculate lag. If the user-specified value is below the minimum value of `PT5S`, the supervisor ignores the value and uses the minimum value instead.|No|PT30S|
|`segmentWriteOutMediumFactory`|Object|The segment write-out medium to use when creating segments. See [Additional Peon configuration: SegmentWriteOutMediumFactory](../../configuration/index.md#segmentwriteoutmediumfactory) for explanation and available options.|No|If not specified, Druid uses the value from `druid.peon.defaultSegmentWriteOutMediumFactory.type`.|
|`logParseExceptions`|Boolean|If `true`, Druid logs an error message when a parsing exception occurs, containing information about the row where the error occurred.|No|`false`|
|`maxParseExceptions`|Integer|The maximum number of parse exceptions that can occur before the task halts ingestion and fails. Overridden if `reportParseExceptions` is set.|No|unlimited|
|`maxSavedParseExceptions`|Integer|When a parse exception occurs, Druid keeps track of the most recent parse exceptions. `maxSavedParseExceptions` limits the number of saved exception instances. These saved exceptions are available after the task finishes in the [task completion report](../../ingestion/tasks.md#task-reports). Overridden if `reportParseExceptions` is set.|No|0|

### IndexSpec

The following table outlines the configuration options for `indexSpec`:

|Property|Type|Description|Required|Default|
|--------|----|-----------|--------|-------|
|`bitmap`|Object|Compression format for bitmap indexes. Druid supports roaring and concise bitmap types.|No|Roaring|
|`dimensionCompression`|String|Compression format for dimension columns. Choose from `LZ4`, `LZF`, `ZSTD` or `uncompressed`.|No|`LZ4`|
|`metricCompression`|String|Compression format for primitive type metric columns. Choose from `LZ4`, `LZF`, `ZSTD`, `uncompressed` or `none`.|No|`LZ4`|
|`longEncoding`|String|Encoding format for metric and dimension columns with type long. Choose from `auto` or `longs`. `auto` encodes the values using offset or lookup table depending on column cardinality, and store them with variable size. `longs` stores the value as is with 8 bytes each.|No|`longs`|