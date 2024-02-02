---
id: kafka-ingestion
title: "Apache Kafka ingestion"
sidebar_label: "Apache Kafka ingestion"
description: "Overview of the Kafka indexing service for Druid. Includes example supervisor specs to help you get started."
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

:::info
To use the Kafka indexing service, you must be on Apache Kafka version of 0.11.x or higher.
If you are using an older version, refer to the [Kafka upgrade guide](https://kafka.apache.org/documentation/#upgrade).
:::

When you enable the Kafka indexing service, you can configure supervisors on the Overlord to manage the creation and lifetime of Kafka indexing tasks.

Kafka indexing tasks read events using Kafka's own partition and offset mechanism to guarantee exactly-once ingestion. The supervisor oversees the state of the indexing tasks to coordinate handoffs, manage failures, and ensure that scalability and replication requirements are maintained.

This topic contains configuration information for the Kafka indexing service supervisor for Apache Druid.

## Setup

To use the Kafka indexing service, you must first load the `druid-kafka-indexing-service` extension on both the Overlord and the MiddleManager. See [Loading extensions](../configuration/extensions.md) for more information.

## Deployment notes on Kafka partitions and Druid segments

Druid assigns Kafka partitions to each Kafka indexing task. A task writes the events it consumes from Kafka into a single segment for the segment granularity interval until it reaches one of the following limits: `maxRowsPerSegment`, `maxTotalRows`, or `intermediateHandoffPeriod`. At this point, the task creates a new partition for this segment granularity to contain subsequent events.

The Kafka indexing task also does incremental hand-offs. Therefore, segments become available as they are ready and you don't have to wait for all segments until the end of the task duration. When the task reaches one of `maxRowsPerSegment`, `maxTotalRows`, or `intermediateHandoffPeriod`, it hands off all the segments and creates a new set of segments for further events. This allows the task to run for longer durations without accumulating old segments locally on MiddleManager services.

The Kafka indexing service may still produce some small segments. For example, consider the following scenario:
- Task duration is 4 hours.
- Segment granularity is set to an HOUR.
- The supervisor was started at 9:10.
After 4 hours at 13:10, Druid starts a new set of tasks. The events for the interval 13:00 - 14:00 may be split across existing tasks and the new set of tasks which could result in small segments. To merge them together into new segments of an ideal size (in the range of ~500-700 MB per segment), you can schedule re-indexing tasks, optionally with a different segment granularity.
For information on how to optimize the segment size, see [Segment size optimization](../operations/segment-optimization.md).

## Supervisor spec configuration

This section outlines the configuration properties that are specific to the Apache Kafka streaming ingestion method. For configuration properties shared across all streaming ingestion methods supported by Druid, see [Supervisor spec](supervisor.md#supervisor-spec).

The following example shows a supervisor spec for the Kafka indexing service:

<details>
  <summary>Click to view the example</summary>

```json
{
  "type": "kafka",
  "spec": {
    "dataSchema": {
      "dataSource": "metrics-kafka",
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
      "topic": "metrics",
      "inputFormat": {
        "type": "json"
      },
      "consumerProperties": {
        "bootstrap.servers": "localhost:9092"
      },
      "taskCount": 1,
      "replicas": 1,
      "taskDuration": "PT1H"
    },
    "tuningConfig": {
      "type": "kafka",
      "maxRowsPerSegment": 5000000
    }
  }
}
```

</details>

### I/O configuration

The following table outlines the Kafka-specific configuration properties for `ioConfig`:

|Property|Type|Description|Required|Default|
|--------|----|-----------|--------|-------|
|`topic`|String|Single Kafka topic to read from. To ingest data from multiple topic, use `topicPattern`. |Yes if `topicPattern` isn't set.||
|`topicPattern`|Multiple Kafka topics to read from, passed as a regex pattern. See [Ingest from multiple topics](#ingest-from-multiple-topics) for more information.|Yes if `topic` isn't set.||
|`consumerProperties`|String, Object|A map of properties to pass to the Kafka consumer. See [Consumer properties](#consumer-properties) for details.|Yes||
|`pollTimeout`|Long|The length of time to wait for the Kafka consumer to poll records, in milliseconds.|No|100|
|`useEarliestOffset`|Boolean|If a supervisor manages a datasource for the first time, it obtains a set of starting offsets from Kafka. This flag determines whether it retrieves the earliest or latest offsets in Kafka. Under normal circumstances, subsequent tasks start from where the previous segments ended. Druid only uses `useEarliestOffset` on the first run.|No|`false`|
|`idleConfig`|Object|Defines how and when the Kafka supervisor can become idle. See [Idle configuration](#idle-configuration) for more details.|No|null|

For configuration properties shared across all streaming ingestion methods supported by Druid, refer to [Supervisor I/O configuration](supervisor.md#io-configuration).


#### Ingest from multiple topics

:::info
If you enable multi-topic ingestion for a datasource, downgrading to a version older than
28.0.0 will cause the ingestion for that datasource to fail.
:::

You can ingest data from one or multiple topics.
When ingesting data from multiple topics, Druid assigns partitions based on the hashcode of the topic name and the ID of the partition within that topic. The partition assignment might not be uniform across all the tasks. Druid assumes that partitions across individual topics have similar load. If you want to ingest from both high and low load topics in the same supervisor, it is recommended that you have a higher number of partitions for a high load topic and a lower number of partitions for a low load topic.

To ingest data from multiple topics, use the `topicPattern` property instead of `topic`.
You pass multiple topics as a regex pattern. For example, to ingest data from clicks and impressions, set `topicPattern` to `clicks|impressions`.
Similarly, you can use `metrics-.*` as the value for `topicPattern` if you want to ingest from all the topics that start with `metrics-`. If you add a new topic that matches the regex to the cluster, Druid automatically starts ingesting from those new topics. Topic names that match partially, such as `my-metrics-12`, are not included for ingestion.

#### Consumer properties

Consumer properties must contain a property `bootstrap.servers` with a list of Kafka brokers in the form: `<BROKER_1>:<PORT_1>,<BROKER_2>:<PORT_2>,...`.
By default, `isolation.level` is set to `read_committed`. 

If you use older versions of Kafka servers without transactions support or don't want Druid to consume only committed transactions, set `isolation.level` to `read_uncommitted`.

Additionally, you can set `isolation.level` to `read_uncommitted` in `consumerProperties` if either:
- You don't need Druid to consume transactional topics.
- You need Druid to consume older versions of Kafka. Make sure offsets are sequential, since there is no offset gap check in Druid.

If your Kafka cluster enables consumer-group based ACLs, you can set `group.id` in `consumerProperties` to override the default auto generated group ID.

In some cases, you may need to fetch consumer properties at runtime. For example, when `bootstrap.servers` is not known upfront, or is not static. To enable SSL connections, you must provide passwords for `keystore`, `truststore`, and `key` secretly. You can provide configurations at runtime with a dynamic config provider implementation like the environment variable config provider that comes with Druid. For more information, see [Dynamic config provider](../operations/dynamic-config-provider.md).

For example, if you are using SASL and SSL with Kafka, set the following environment variables for the Druid user on the machines running the Overlord and the Peon services:

```
export KAFKA_JAAS_CONFIG="org.apache.kafka.common.security.plain.PlainLoginModule required username='admin_user' password='admin_password';"
export SSL_KEY_PASSWORD=mysecretkeypassword
export SSL_KEYSTORE_PASSWORD=mysecretkeystorepassword
export SSL_TRUSTSTORE_PASSWORD=mysecrettruststorepassword
```

```json
"druid.dynamic.config.provider": {
  "type": "environment",
  "variables": {
    "sasl.jaas.config": "KAFKA_JAAS_CONFIG",
    "ssl.key.password": "SSL_KEY_PASSWORD",
    "ssl.keystore.password": "SSL_KEYSTORE_PASSWORD",
    "ssl.truststore.password": "SSL_TRUSTSTORE_PASSWORD"
  }
}
```

Verify that you've changed the values for all configurations to match your own environment. In the Druid data loader interface, you can use the environment variable config provider syntax in the **Consumer properties** field on the **Connect tab**. When connecting to Kafka, Druid replaces the environment variables with their corresponding values.

#### Idle configuration

:::info
Idle state transitioning is currently designated as experimental.
:::

When the supervisor enters the idle state, no new tasks are launched subsequent to the completion of the currently executing tasks. This strategy may lead to reduced costs for cluster operators while using topics that get sporadic data.

The following table outlines the configuration options for `idleConfig`:

|Property|Description|Required|
|--------|-----------|--------|
|`enabled`|If `true`, the supervisor becomes idle if there is no data on input stream or topic for some time.|No|`false`|
|`inactiveAfterMillis`|The supervisor becomes idle if all existing data has been read from input topic and no new data has been published for `inactiveAfterMillis` milliseconds.|No|`600_000`|

The following example shows a supervisor spec with `lagBased` autoscaler and idle configuration enabled:

<details>
  <summary>Click to view the example</summary>

```json
{
  "type": "kafka",
  "spec": {
    "dataSchema": {...},
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
      "taskCount": 1,
      "replicas": 1,
      "taskDuration": "PT1H",
      "idleConfig": {
        "enabled": true,
        "inactiveAfterMillis": 600000
      }
    },
    "tuningConfig": {...}
  }
}
```
</details>


### Tuning configuration

The following table outlines the Kafka-specific configuration properties for `tuningConfig`:

|Property|Type|Description|Required|Default|
|--------|----|-----------|--------|-------|
|`chatAsync`|Boolean|If `true`, use asynchronous communication with indexing tasks, and ignore the `chatThreads` parameter. If `false`, use synchronous communication in a thread pool of size `chatThreads`.|No|`true`|
|`chatThreads`|Integer|The number of threads to use for communicating with indexing tasks. Ignored if `chatAsync` is `true`.|No|`min(10, taskCount * replicas)`|

For configuration properties shared across all streaming ingestion methods supported by Druid, refer to [Supervisor tuning configuration](supervisor.md#tuning-configuration).

## Learn more

See the following topics for more information:

* [Supervisor API](../api-reference/supervisor-api.md) for how to manage and monitor supervisors using the API.
* [Supervisor](../ingestion/supervisor.md) for supervisor status and capacity planning.
* [Loading from Apache Kafka](../tutorials/tutorial-kafka.md) for a tutorial on streaming data from Apache Kafka.
* [Kafka input format](../ingestion/data-formats.md#kafka) to learn about the `kafka` input format.