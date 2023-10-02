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

To use the Kafka indexing service, load the `druid-kafka-indexing-service` extension on both the Overlord and the MiddleManagers. See [Loading extensions](../../configuration/extensions.md) for instructions on how to configure extensions.

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

If you want to parse the Kafka metadata fields in addition to the Kafka payload value contents, you can use the `kafka` input format.

The `kafka` input format wraps around the payload parsing input format and augments the data it outputs with the Kafka event timestamp,
the Kafka topic name, the Kafka event headers, and the key field that itself can be parsed using any available InputFormat.

For example, consider the following structure for a Kafka message that represents a fictitious wiki edit in a development environment:

- **Kafka timestamp**: `1680795276351`
- **Kafka topic**: `wiki-edits`
- **Kafka headers**:
  - `env=development`
  - `zone=z1`
- **Kafka key**: `wiki-edit`
- **Kafka payload value**: `{"channel":"#sv.wikipedia","timestamp":"2016-06-27T00:00:11.080Z","page":"Salo Toraut","delta":31,"namespace":"Main"}`

Using `{ "type": "json" }` as the input format would only parse the payload value.
To parse the Kafka metadata in addition to the payload, use the `kafka` input format.

You would configure it as follows:

- `valueFormat`: Define how to parse the payload value. Set this to the payload parsing input format (`{ "type": "json" }`).
- `timestampColumnName`: Supply a custom name for the Kafka timestamp in the Druid schema to avoid conflicts with columns from the payload. The default is `kafka.timestamp`.
- `topicColumnName`: Supply a custom name for the Kafka topic in the Druid schema to avoid conflicts with columns from the payload. The default is `kafka.topic`. This field is useful when ingesting data from multiple topics into same datasource.
- `headerFormat`: The default value `string` decodes strings in UTF-8 encoding from the Kafka header.
   Other supported encoding formats include the following:
   - `ISO-8859-1`: ISO Latin Alphabet No. 1, that is, ISO-LATIN-1.
   - `US-ASCII`: Seven-bit ASCII. Also known as ISO646-US. The Basic Latin block of the Unicode character set.
   - `UTF-16`: Sixteen-bit UCS Transformation Format, byte order identified by an optional byte-order mark.
   - `UTF-16BE`: Sixteen-bit UCS Transformation Format, big-endian byte order.
   - `UTF-16LE`: Sixteen-bit UCS Transformation Format, little-endian byte order.
- `headerColumnPrefix`: Supply a prefix to the Kafka headers to avoid any conflicts with columns from the payload. The default is `kafka.header.`.
  Considering the header from the example, Druid maps the headers to the following columns: `kafka.header.env`, `kafka.header.zone`.
- `keyFormat`: Supply an input format to parse the key. Only the first value will be used.
  If, as in the example, your key values are simple strings, then you can use the `tsv` format to parse them.
  ```
  {
    "type": "tsv",
    "findColumnsFromHeader": false,
    "columns": ["x"]
  } 
  ```
  Note that for `tsv`,`csv`, and `regex` formats, you need to provide a `columns` array to make a valid input format. Only the first one is used, and its name will be ignored in favor of `keyColumnName`.
- `keyColumnName`: Supply the name for the Kafka key column to avoid conflicts with columns from the payload. The default is `kafka.key`.

Putting it together, the following input format (that uses the default values for `timestampColumnName`, `topicColumnName`, `headerColumnPrefix`, and `keyColumnName`)

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

would parse the example message as follows:

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

For more information on data formats, see [Data formats](../../ingestion/data-formats.md).

Finally, add these Kafka metadata columns to the `dimensionsSpec` or  set your `dimensionsSpec` to auto-detect columns.
     
The following supervisor spec demonstrates how to ingest the Kafka header, key, timestamp, and topic into Druid dimensions:

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

| `kafka.header.env` | `kafka.key` | `kafka.timestamp` | `kafka.topic` |
|--------------------|-----------|---------------|---------------|
| `development`      | `wiki-edit` | `1680795276351` | `wiki-edits`  |

For more information, see [`kafka` data format](../../ingestion/data-formats.md#kafka).

## Submit a supervisor spec

Druid starts a supervisor for a dataSource when you submit a supervisor spec. You can use the data loader in the web console or you can submit a supervisor spec to the following endpoint:

`http://<OVERLORD_IP>:<OVERLORD_PORT>/druid/indexer/v1/supervisor`

For example: 

```
curl -X POST -H 'Content-Type: application/json' -d @supervisor-spec.json http://localhost:8090/druid/indexer/v1/supervisor
```

Where the file `supervisor-spec.json` contains your Kafka supervisor spec file.
