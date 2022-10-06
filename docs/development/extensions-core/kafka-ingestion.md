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

When you enable the Kafka indexing service, you can configure supervisors on the Overlord to manage the creation and lifetime of Kafka indexing tasks.

Kafka indexing tasks read events using Kafka's own partition and offset mechanism to guarantee exactly-once ingestion. The supervisor oversees the state of the indexing tasks to:
  - coordinate handoffs
  - manage failures
  - ensure that scalability and replication requirements are maintained.


This topic covers how to submit a supervisor spec to ingest event data, also known as message data, from Kafka. See the following for more information:
- For a reference of Kafka supervisor spec configuration options, see the [Kafka supervisor reference](./kafka-supervisor-reference.md).
- For operations reference information to help run and maintain Apache Kafka supervisors, see [Kafka supervisor operations](./kafka-supervisor-operations.md).
- For a walk-through, see the [Loading from Apache Kafka](../../tutorials/tutorial-kafka.md) tutorial.

## Kafka support
The Kafka indexing service supports transactional topics introduced in Kafka 0.11.x by default. The consumer for Kafka indexing service is incompatible with older Kafka brokers. If you are using an older version, refer to the [Kafka upgrade guide](https://kafka.apache.org/documentation/#upgrade).

Additionally, you can set `isolation.level` to `read_uncommitted` in `consumerProperties` if either:
- You don't need Druid to consume transactional topics.
- You need Druid to consume older versions of Kafka. Make sure offsets are sequential, since there is no offset gap check in Druid anymore.

If your Kafka cluster enables consumer-group based ACLs, you can set `group.id` in `consumerProperties` to override the default auto generated group id.

## Load the Kafka indexing service

To use the Kafka indexing service, load the `druid-kafka-indexing-service` extension on both the Overlord and the MiddleManagers. See [Loading extensions](../extensions.md#loading-extensions) for instructions on how to configure extensions.

## Define a supervisor spec
Similar to the ingestion spec for batch ingestion, the supervisor spec configures the data ingestion for Kafka streaming ingestion. A supervisor spec has the following sections:
- `dataSchema` to specify the Druid datasource name, primary timestamp, dimensions, metrics, transforms, and any necessary filters.
- `ioConfig` to configure Kafka connection settings and configure how Druid parses the data. Kafka-specific connection details go in the `consumerProperties`. The `ioConfig` is also where you define the input format (`inputFormat`) of your Kafka data. For supported formats for Kafka and information on how to configure the input format, see [Data formats](../../ingestion/data-formats.md).  
- `tuningConfig` to control various tuning parameters specific to each ingestion method.
For a full description of all the fields and parameters in a Kafka supervisor spec, see the [Kafka supervisor reference](./kafka-supervisor-reference.md).


The following sections contain examples to help you get started with supervisor specs.

### JSON input format supervisor spec example
The following example demonstrates a supervisor spec for Kafka that uses the `JSON` input format. In this case Druid parses the event contents in JSON format:

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

### Kafka input format supervisor spec example
If you want to ingest data from other fields in addition to the Kafka message contents, you can use the `kafka` input format. The `kafka` input format lets you ingest:
- the event key field
- event headers
- the Kafka event timestamp
- the Kafka event value that stores the payload.

For example, consider the following structure for a message that represents a fictitious wiki edit in a development environment:
- **Event headers**: {"environment": "development"}
- **Event key**: {"key: "wiki-edit"}
- **Event value**: \<JSON object with event payload containing the change details\>
- **Event timestamp**: "Nov. 10, 2021 at 14:06"

When you use the `kafka` input format, you configure the way that Druid names the dimensions created from the Kafka message:
- `headerLabelPrefix`: Supply a prefix to the Kafka headers to avoid any conflicts with named dimensions. The default is `kafka.header`. Considering the header from the example, Druid maps the header to the following column: `kafka.header.environment`.
- `timestampColumnName`: Supply a custom name for the Kafka timestamp in the Druid schema to avoid conflicts with other time columns. The default is `kafka.timestamp`.
- `keyColumnName`: Supply the name for the Kafka key column in Druid. The default is `kafka.key`.
Additionally, you must provide information about how Druid should parse the data in the Kafka message:
- `headerFormat`: The default "string" decodes UTF8-encoded strings from the Kafka header. If you need another format, you can implement your own parser.
- `keyFormat`: Takes a Druid `inputFormat` and uses the value for the first key it finds. According to the example the value is "wiki-edit". It discards the key name in this case. If you store the key as a string, use the `CSV` input format. For example, if you have simple string for the the key `wiki-edit`, you can use the following to parse the key:
  ```
  "keyFormat": {
    "type": "csv",
    "hasHeaderRow": false,
    "findColumnsFromHeader": false,
    "columns": ["key"]
    } 
  ```
- `valueFormat`: Define how to parse the message contents. You can use any of the Druid input formats that work for Kafka.

For more information on data formats, see [Data formats](../../ingestion/data-formats.md).

Finally, add the Kafka message columns to the `dimensionsSpec`. For the key and timestamp, you can use the dimension names you defined for `keyColumnName` and `timestampColumnName`. For header dimensions, append the header key to the `headerLabelPrefix`. For example `kafka.header.environment`.
     
The following supervisor spec demonstrates how to ingest the Kafka header, key, and timestamp into Druid dimensions:
```
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
        "headerLabelPrefix": "kafka.header.",
        "timestampColumnName": "kafka.timestamp",
        "keyColumnName": "kafka.key",
        "headerFormat": {
          "type": "string"
        },
        "keyFormat": {
          "type": "json"
        },
        "valueFormat": {
          "type": "json"
        },
        "findColumnsFromHeader": false
      },
      "useEarliestOffset": true
    },
    "tuningConfig": {
      "type": "kafka"
    },
    "dataSchema": {
      "dataSource": "wikiticker",
      "timestampSpec": {
        "column": "timestamp",
        "format": "posix"
      },
      "dimensionsSpec": {
        "dimensions": [
          {
            "type": "string",
            "name": "kafka.key"
          },
          {
            "type": "string",
            "name": "kafka.timestamp"
          },
          {
            "type": "string",
            "name": "kafka.header.environment"
          },
          "$schema",
          {
            "type": "long",
            "name": "id"
          },
          "type",
          {
            "type": "long",
            "name": "namespace"
          },
          "title",
          "comment",
          "user",]
        ]
      },
      "granularitySpec": {
        "queryGranularity": "none",
        "rollup": false,
        "segmentGranularity": "day"
      }
    }
  },
  "tuningConfig": {
    "type": "kafka"
  }
}
```
After Druid ingests the data, you can query the Kafka message columns as follows:
```unix
SELECT
  "kafka.header.environment",
  "kafka.key",
  "kafka.timestamp"
FROM "wikiticker"

kafka.header.environment  kafka.key	 kafka.timestamp
development               wiki-edit	 1636399229823
```
For more information, see [`kafka` data format](../../ingestion/data-formats.md#kafka).
## Submit a supervisor spec

Druid starts a supervisor for a dataSource when you submit a supervisor spec. You can use the data loader in the web console or you can submit a supervisor spec to the following endpoint:

`http://<OVERLORD_IP>:<OVERLORD_PORT>/druid/indexer/v1/supervisor`

For example: 

```
curl -X POST -H 'Content-Type: application/json' -d @supervisor-spec.json http://localhost:8090/druid/indexer/v1/supervisor
```

Where the file `supervisor-spec.json` contains your Kafka supervisor spec file.
