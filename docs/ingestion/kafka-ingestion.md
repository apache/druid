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
To use the Kafka indexing service, you must be on Apache Kafka version 0.11.x or higher.
If you are using an older version, refer to the [Apache Kafka upgrade guide](https://kafka.apache.org/documentation/#upgrade).
:::

When you enable the Kafka indexing service, you can configure supervisors on the Overlord to manage the creation and lifetime of Kafka indexing tasks.
Kafka indexing tasks read events using Kafka partition and offset mechanism to guarantee exactly-once ingestion. The supervisor oversees the state of the indexing tasks to coordinate handoffs, manage failures, and ensure that scalability and replication requirements are maintained.

This topic contains configuration information for the Kafka indexing service supervisor for Apache Druid.

## Setup

To use the Kafka indexing service, you must first load the `druid-kafka-indexing-service` extension on both the Overlord and the MiddleManager. See [Loading extensions](../configuration/extensions.md) for more information.

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

The following table outlines the `ioConfig` configuration properties specific to Kafka.
For configuration properties shared across all streaming ingestion methods, refer to [Supervisor I/O configuration](supervisor.md#io-configuration).

|Property|Type|Description|Required|Default|
|--------|----|-----------|--------|-------|
|`topic`|String|The Kafka topic to read from. To ingest data from multiple topic, use `topicPattern`. |Yes if `topicPattern` isn't set.||
|`topicPattern`|String|Multiple Kafka topics to read from, passed as a regex pattern. See [Ingest from multiple topics](#ingest-from-multiple-topics) for more information.|Yes if `topic` isn't set.||
|`consumerProperties`|String, Object|A map of properties to pass to the Kafka consumer. See [Consumer properties](#consumer-properties) for details.|Yes. At the minimum, you must set the `bootstrap.servers` property to establish the initial connection to the Kafka cluster.||
|`pollTimeout`|Long|The length of time to wait for the Kafka consumer to poll records, in milliseconds.|No|100|
|`useEarliestOffset`|Boolean|If a supervisor manages a datasource for the first time, it obtains a set of starting offsets from Kafka. This flag determines whether it retrieves the earliest or latest offsets in Kafka. Under normal circumstances, subsequent tasks start from where the previous segments ended. Druid only uses `useEarliestOffset` on the first run.|No|`false`|
|`idleConfig`|Object|Defines how and when the Kafka supervisor can become idle. See [Idle configuration](#idle-configuration) for more details.|No|null|

#### Ingest from multiple topics

:::info
If you enable multi-topic ingestion for a datasource, downgrading to a version older than
28.0.0 will cause the ingestion for that datasource to fail.
:::

You can ingest data from one or multiple topics.
When ingesting data from multiple topics, Druid assigns partitions based on the hashcode of the topic name and the ID of the partition within that topic. The partition assignment might not be uniform across all the tasks. Druid assumes that partitions across individual topics have similar load. If you want to ingest from both high and low load topics in the same supervisor, it is recommended that you have a higher number of partitions for a high load topic and a lower number of partitions for a low load topic.

To ingest data from multiple topics, use the `topicPattern` property instead of `topic`.
You pass multiple topics as a regex pattern. For example, to ingest data from clicks and impressions, set `topicPattern` to `clicks|impressions`.
Similarly, you can use `metrics-.*` as the value for `topicPattern` if you want to ingest from all the topics that start with `metrics-`. If you add a new topic that matches the regex to the cluster, Druid automatically starts ingesting from the new topic. Topic names that match partially, such as `my-metrics-12`, are not included for ingestion.

#### Consumer properties

Consumer properties control how a supervisor reads and processes event messages from a Kafka stream. For more information about consumers, refer to the [Apache Kafka documentation](https://kafka.apache.org/documentation/#consumerconfigs).

The `consumerProperties` object must contain a  `bootstrap.servers` property with a list of Kafka brokers in the form: `<BROKER_1>:<PORT_1>,<BROKER_2>:<PORT_2>,...`.
By default, `isolation.level` is set to `read_committed`.

If you use older versions of Kafka servers without transactions support or don't want Druid to consume only committed transactions, set `isolation.level` to `read_uncommitted`. If you need Druid to consume older versions of Kafka, make sure offsets are sequential, since there is no offset gap check in Druid.

If your Kafka cluster enables consumer-group based ACLs, you can set `group.id` in `consumerProperties` to override the default auto generated group ID.

In some cases, you may need to fetch consumer properties at runtime. For example, when `bootstrap.servers` is not known upfront or is not static. To enable SSL connections, you must provide passwords for `keystore`, `truststore`, and `key` secretly. You can provide configurations at runtime with a dynamic config provider implementation like the environment variable config provider that comes with Druid. For more information, see [Dynamic config provider](../operations/dynamic-config-provider.md).

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

You can provide SSL connections with [Password provider](../operations/password-provider.md) interface to define the `keystore`, `truststore`, and `key`, but this feature is deprecated.

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

The following example shows a supervisor spec with idle configuration enabled:

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

#### Data format

The Kafka indexing service supports both [`inputFormat`](data-formats.md#input-format) and [`parser`](data-formats.md#parser) to specify the data format. Use the `inputFormat` to specify the data format for the Kafka indexing service unless you need a format only supported by the legacy `parser`. For more information, see [Source input formats](data-formats.md).

The Kinesis indexing service supports the following values for `inputFormat`:

* `csv`
* `tvs`
* `json`
* `kafka`
* `avro_stream`
* `avro_ocf`
* `protobuf`

You can use `parser` to read [`thrift`](../development/extensions-contrib/thrift.md) formats.

##### Kafka input format supervisor spec example

The `kafka` input format lets you parse the Kafka metadata fields in addition to the Kafka payload value contents.

The `kafka` input format wraps around the payload parsing input format and augments the data it outputs with the Kafka event timestamp, the Kafka topic name, the Kafka event headers, and the key field that itself can be parsed using any available input format.

For example, consider the following structure for a Kafka message that represents a wiki edit in a development environment:

- **Kafka timestamp**: `1680795276351`
- **Kafka topic**: `wiki-edits`
- **Kafka headers**:
  - `env=development`
  - `zone=z1`
- **Kafka key**: `wiki-edit`
- **Kafka payload value**: `{"channel":"#sv.wikipedia","timestamp":"2016-06-27T00:00:11.080Z","page":"Salo Toraut","delta":31,"namespace":"Main"}`

Using `{ "type": "json" }` as the input format only parses the payload value.
To parse the Kafka metadata in addition to the payload, use the `kafka` input format.

You configure it as follows:

- `valueFormat`: Define how to parse the payload value. Set this to the payload parsing input format (`{ "type": "json" }`).
- `timestampColumnName`: Supply a custom name for the Kafka timestamp in the Druid schema to avoid conflicts with columns from the payload. The default is `kafka.timestamp`.
- `topicColumnName`: Supply a custom name for the Kafka topic in the Druid schema to avoid conflicts with columns from the payload. The default is `kafka.topic`. This field is useful when ingesting data from multiple topics into the same datasource.
- `headerFormat`: The default value `string` decodes strings in UTF-8 encoding from the Kafka header.  
   Other supported encoding formats include the following:
   - `ISO-8859-1`: ISO Latin Alphabet No. 1, that is, ISO-LATIN-1.
   - `US-ASCII`: Seven-bit ASCII. Also known as ISO646-US. The Basic Latin block of the Unicode character set.
   - `UTF-16`: Sixteen-bit UCS Transformation Format, byte order identified by an optional byte-order mark.
   - `UTF-16BE`: Sixteen-bit UCS Transformation Format, big-endian byte order.
   - `UTF-16LE`: Sixteen-bit UCS Transformation Format, little-endian byte order.
- `headerColumnPrefix`: Supply a prefix to the Kafka headers to avoid any conflicts with columns from the payload. The default is `kafka.header.`.
  Considering the header from the example, Druid maps the headers to the following columns: `kafka.header.env`, `kafka.header.zone`.
- `keyFormat`: Supply an input format to parse the key. Only the first value is used.
  If, as in the example, your key values are simple strings, then you can use the `tsv` format to parse them.
  ```json
  {
    "type": "tsv",
    "findColumnsFromHeader": false,
    "columns": ["x"]
  } 
  ```
  Note that for `tsv`,`csv`, and `regex` formats, you need to provide a `columns` array to make a valid input format. Only the first one is used, and its name will be ignored in favor of `keyColumnName`.
- `keyColumnName`: Supply the name for the Kafka key column to avoid conflicts with columns from the payload. The default is `kafka.key`.

The following input format uses default values for `timestampColumnName`, `topicColumnName`, `headerColumnPrefix`, and `keyColumnName`:

```json
{
  "type": "kafka",
  "valueFormat": {
    "type": "json"
  },
  "headerFormat": {
    "type": "string"
  },
  "keyFormat": {
    "type": "tsv",
    "findColumnsFromHeader": false,
    "columns": ["x"]
  }
}
```

It parses the example message as follows:

```json
{
  "channel": "#sv.wikipedia",
  "timestamp": "2016-06-27T00:00:11.080Z",
  "page": "Salo Toraut",
  "delta": 31,
  "namespace": "Main",
  "kafka.timestamp": 1680795276351,
  "kafka.topic": "wiki-edits",
  "kafka.header.env": "development",
  "kafka.header.zone": "z1",
  "kafka.key": "wiki-edit"
}
```

Finally, add these Kafka metadata columns to the `dimensionsSpec` or set your `dimensionsSpec` to auto-detect columns.
     
The following supervisor spec demonstrates how to ingest the Kafka header, key, timestamp, and topic into Druid dimensions:

<details>
  <summary>Click to view the example</summary>

```json
{
  "type": "kafka",
  "spec": {
    "ioConfig": {
      "type": "kafka",
      "consumerProperties": {
        "bootstrap.servers": "localhost:9092"
      },
      "topic": "wiki-edits",
      "inputFormat": {
        "type": "kafka",
        "valueFormat": {
          "type": "json"
        },
        "headerFormat": {
          "type": "string"
        },
        "keyFormat": {
          "type": "tsv",
          "findColumnsFromHeader": false,
          "columns": ["x"]
        }
      },
      "useEarliestOffset": true
    },
    "dataSchema": {
      "dataSource": "wikiticker",
      "timestampSpec": {
        "column": "timestamp",
        "format": "posix"
      },
      "dimensionsSpec":  "dimensionsSpec": {
        "useSchemaDiscovery": true,
        "includeAllDimensions": true
      },
      "granularitySpec": {
        "queryGranularity": "none",
        "rollup": false,
        "segmentGranularity": "day"
      }
    },
    "tuningConfig": {
      "type": "kafka"
    }
  }
}
```
</details>

After Druid ingests the data, you can query the Kafka metadata columns as follows:

```sql
SELECT
  "kafka.header.env",
  "kafka.key",
  "kafka.timestamp",
  "kafka.topic"
FROM "wikiticker"
```

This query returns:

|`kafka.header.env`|`kafka.key`|`kafka.timestamp`|`kafka.topic`|
|------------------|-----------|-----------------|-------------|
|`development`|`wiki-edit`|`1680795276351`|`wiki-edits`|

### Tuning configuration

The following table outlines the `tuningConfig` configuration properties specific to Kafka.
For configuration properties shared across all streaming ingestion methods, refer to [Supervisor tuning configuration](supervisor.md#tuning-configuration).

|Property|Type|Description|Required|Default|
|--------|----|-----------|--------|-------|
|`numPersistThreads`|Integer|The number of threads to use to create and persist incremental segments on the disk. Higher ingestion data throughput results in a larger number of incremental segments, causing significant CPU time to be spent on the creation of the incremental segments on the disk. For datasources with number of columns running into hundreds or thousands, creation of the incremental segments may take up significant time, in the order of multiple seconds. In both of these scenarios, ingestion can stall or pause frequently, causing it to fall behind. You can use additional threads to parallelize the segment creation without blocking ingestion as long as there are sufficient CPU resources available.|No|1|
|`chatAsync`|Boolean|If `true`, use asynchronous communication with indexing tasks, and ignore the `chatThreads` parameter. If `false`, use synchronous communication in a thread pool of size `chatThreads`.|No|`true`|
|`chatThreads`|Integer|The number of threads to use for communicating with indexing tasks. Ignored if `chatAsync` is `true`.|No|`min(10, taskCount * replicas)`|

## Deployment notes on Kafka partitions and Druid segments

Druid assigns Kafka partitions to each Kafka indexing task. A task writes the events it consumes from Kafka into a single segment for the segment granularity interval until it reaches one of the following limits: `maxRowsPerSegment`, `maxTotalRows`, or `intermediateHandoffPeriod`. At this point, the task creates a new partition for this segment granularity to contain subsequent events.

The Kafka indexing task also does incremental hand-offs. Therefore, segments become available as they are ready and you don't have to wait for all segments until the end of the task duration. When the task reaches one of `maxRowsPerSegment`, `maxTotalRows`, or `intermediateHandoffPeriod`, it hands off all the segments and creates a new set of segments for further events. This allows the task to run for longer durations without accumulating old segments locally on MiddleManager services.

The Kafka indexing service may still produce some small segments. For example, consider the following scenario:
- Task duration is 4 hours.
- Segment granularity is set to an HOUR.
- The supervisor was started at 9:10.
After 4 hours at 13:10, Druid starts a new set of tasks. The events for the interval 13:00 - 14:00 may be split across existing tasks and the new set of tasks which could result in small segments. To merge them together into new segments of an ideal size (in the range of ~500-700 MB per segment), you can schedule re-indexing tasks, optionally with a different segment granularity.

For information on how to optimize the segment size, see [Segment size optimization](../operations/segment-optimization.md).

## Learn more

See the following topics for more information:

* [Supervisor API](../api-reference/supervisor-api.md) for how to manage and monitor supervisors using the API.
* [Supervisor](../ingestion/supervisor.md) for supervisor status and capacity planning.
* [Loading from Apache Kafka](../tutorials/tutorial-kafka.md) for a tutorial on streaming data from Apache Kafka.
* [Kafka input format](../ingestion/data-formats.md#kafka) to learn about the `kafka` input format.