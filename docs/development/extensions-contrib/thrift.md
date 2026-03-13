---
id: thrift
title: "Thrift"
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


To use this Apache Druid extension, [include](../../configuration/extensions.md#loading-extensions) `druid-thrift-extensions` in the extensions load list.

This extension enables Druid to ingest Thrift-encoded data from streaming sources such as Kafka and Kinesis.

You may want to use another version of thrift, change the dependency in pom and compile yourself.

## Thrift input format

Thrift-encoded data for streaming ingestion (Kafka, Kinesis) can be ingested using the Thrift [input format](../../ingestion/data-formats.md#input-format). It supports `flattenSpec` for extracting fields from nested Thrift structs using JSONPath expressions.

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| type | String | Must be `thrift` | yes |
| thriftClass | String | Fully qualified class name of the Thrift-generated `TBase` class to deserialize into. | yes |
| thriftJar | String | Path to a JAR file containing the Thrift class. If not provided, the class is looked up from the classpath. | no |
| flattenSpec | JSON Object | Specifies flattening of nested Thrift structs. See [Flattening nested data](../../ingestion/data-formats.md#flattenspec) for details. | no |

### Example: Kafka ingestion

Consider the following Thrift schema definition:

```
namespace java com.example.druid

struct Author {
  1: string firstName;
  2: string lastName;
}

struct Book {
  1: string date;
  2: double price;
  3: string title;
  4: Author author;
}
```

Compile it to produce `com.example.druid.Book` (and `com.example.druid.Author`) and make the resulting JAR available on the classpath of your Druid processes, or reference it via `thriftJar`.

The following Kafka supervisor spec ingests compact-encoded `Book` messages, using a `flattenSpec` to extract the nested `author.lastName` field:

```json
{
  "type": "kafka",
  "spec": {
    "dataSchema": {
      "dataSource": "books",
      "timestampSpec": {
        "column": "date",
        "format": "auto"
      },
      "dimensionsSpec": {
        "dimensions": [
          "title",
          "lastName"
        ]
      },
      "granularitySpec": {
        "type": "uniform",
        "segmentGranularity": "DAY",
        "queryGranularity": "NONE"
      }
    },
    "tuningConfig": {
      "type": "kafka"
    },
    "ioConfig": {
      "type": "kafka",
      "consumerProperties": {
        "bootstrap.servers": "localhost:9092"
      },
      "topic": "books",
      "inputFormat": {
        "type": "thrift",
        "thriftClass": "com.example.druid.Book",
        "flattenSpec": {
          "useFieldDiscovery": true,
          "fields": [
            {
              "type": "path",
              "name": "lastName",
              "expr": "$.author.lastName"
            }
          ]
        }
      },
      "taskCount": 1,
      "replicas": 1,
      "taskDuration": "PT1H"
    }
  }
}
```

