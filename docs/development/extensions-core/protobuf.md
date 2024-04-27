---
id: protobuf
title: "Protobuf"
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


This Apache Druid extension enables Druid to ingest and understand the Protobuf data format. Make sure to [include](../../configuration/extensions.md#loading-extensions) `druid-protobuf-extensions` in the extensions load list.

The `druid-protobuf-extensions` provides the [Protobuf Parser](../../ingestion/data-formats.md#protobuf-parser)
for [stream ingestion](../../ingestion/index.md#streaming). See corresponding docs for details.

## Example: Load Protobuf messages from Kafka

This example demonstrates how to load Protobuf messages from Kafka.  Please read the [Load from Kafka tutorial](../../tutorials/tutorial-kafka.md) first, and see [Kafka Indexing Service](../../ingestion/kafka-ingestion.md) documentation for more details.

The files used in this example are found at [`./examples/quickstart/protobuf` in your Druid directory](https://github.com/apache/druid/tree/master/examples/quickstart/protobuf).

For this example:
- Kafka broker host is `localhost:9092`
- Kafka topic is `metrics_pb`
- Datasource name is `metrics-protobuf`

Here is a JSON example of the 'metrics' data schema used in the example.

```json
{
  "unit": "milliseconds",
  "http_method": "GET",
  "value": 44,
  "timestamp": "2017-04-06T02:36:22Z",
  "http_code": "200",
  "page": "/",
  "metricType": "request/latency",
  "server": "www1.example.com"
}
```

### Proto file

The corresponding proto file for our 'metrics' dataset looks like this. You can use Protobuf `inputFormat` with a proto file or [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html).
```
syntax = "proto3";
message Metrics {
  string unit = 1;
  string http_method = 2;
  int32 value = 3;
  string timestamp = 4;
  string http_code = 5;
  string page = 6;
  string metricType = 7;
  string server = 8;
}
```

### When using a descriptor file

Next, we use the `protoc` Protobuf compiler to generate the descriptor file and save it as `metrics.desc`. The descriptor file must be either in the classpath or reachable by URL.  In this example the descriptor file was saved at `/tmp/metrics.desc`, however this file is also available in the example files. From your Druid install directory:

```
protoc -o /tmp/metrics.desc ./quickstart/protobuf/metrics.proto
```

### When using Schema Registry

Make sure your Schema Registry version is later than 5.5. Next, we can post a schema to add it to the registry:

```
POST /subjects/test/versions HTTP/1.1
Host: schemaregistry.example1.com
Accept: application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json

{
    "schemaType": "PROTOBUF",
    "schema": "syntax = \"proto3\";\nmessage Metrics {\n  string unit = 1;\n  string http_method = 2;\n  int32 value = 3;\n string timestamp = 4;\n string http_code = 5;\n string page = 6;\n string metricType = 7;\n string server = 8;\n}\n"
}
```

This feature uses Confluent's Protobuf provider which is not included in the Druid distribution and must be installed separately. You can fetch it and its dependencies from the Confluent repository and Maven Central at: 
- https://packages.confluent.io/maven/io/confluent/kafka-protobuf-provider/6.0.1/kafka-protobuf-provider-6.0.1.jar
- https://repo1.maven.org/maven2/org/jetbrains/kotlin/kotlin-stdlib/1.4.0/kotlin-stdlib-1.4.0.jar
- https://repo1.maven.org/maven2/com/squareup/wire/wire-schema/3.2.2/wire-schema-3.2.2.jar

Copy or symlink those files inside the folder `extensions/protobuf-extensions` under the distribution root directory.

## Create Kafka Supervisor

Below is the complete Supervisor spec JSON to be submitted to the Overlord.
Make sure these keys are properly configured for successful ingestion.

### When using a descriptor file

Important supervisor properties
- `protoBytesDecoder.descriptor` for the descriptor file URL
- `protoBytesDecoder.protoMessageType` from the proto definition
- `protoBytesDecoder.type` set to `file`, indicate use descriptor file to decode Protobuf file
- `inputFormat` should have `type` set to `protobuf`

```json
{
"type": "kafka",
"spec": {
    "dataSchema": {
        "dataSource": "metrics-protobuf",
        "timestampSpec": {
            "column": "timestamp",
            "format": "auto"
        },
        "dimensionsSpec": {
            "dimensions": [
                "unit",
                "http_method",
                "http_code",
                "page",
                "metricType",
                "server"
            ],
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
    "tuningConfig": {
        "type": "kafka",
        "maxRowsPerSegment": 5000000
    },
    "ioConfig": {
        "topic": "metrics_pb",
        "consumerProperties": {
            "bootstrap.servers": "localhost:9092"
        },
        "inputFormat": {
            "type": "protobuf",
            "protoBytesDecoder": {
                "type": "file",
                "descriptor": "file:///tmp/metrics.desc",
                "protoMessageType": "Metrics"
            },
            "flattenSpec": {
                "useFieldDiscovery": true
            },
            "binaryAsString": false
        },
        "taskCount": 1,
        "replicas": 1,
        "taskDuration": "PT1H",
        "type": "kafka"
    }
}
}
```

To adopt to old version. You can use old parser style, which also works.

```json
{
  "parser": {
    "type": "protobuf",
    "descriptor": "file:///tmp/metrics.desc",
    "protoMessageType": "Metrics"
  }
}
```

### When using Schema Registry

Important supervisor properties
- `protoBytesDecoder.url` for the schema registry URL with single instance.
- `protoBytesDecoder.urls` for the schema registry URLs with multi instances.
- `protoBytesDecoder.capacity` capacity for schema registry cached schemas.
- `protoBytesDecoder.config` to send additional configurations, configured for Schema Registry.
- `protoBytesDecoder.headers` to send headers to the Schema Registry.
- `protoBytesDecoder.type` set to `schema_registry`, indicate use schema registry to decode Protobuf file.
- `parser` should have `type` set to `protobuf`, but note that the `format` of the `parseSpec` must be `json`.

```json
{
  "parser": {
    "type": "protobuf",
    "protoBytesDecoder": {
      "urls": ["http://schemaregistry.example1.com:8081","http://schemaregistry.example2.com:8081"],
      "type": "schema_registry",
      "capacity": 100,
      "config" : {
           "basic.auth.credentials.source": "USER_INFO",
           "basic.auth.user.info": "fred:letmein",
           "schema.registry.ssl.truststore.location": "/some/secrets/kafka.client.truststore.jks",
           "schema.registry.ssl.truststore.password": "<password>",
           "schema.registry.ssl.keystore.location": "/some/secrets/kafka.client.keystore.jks",
           "schema.registry.ssl.keystore.password": "<password>",
           "schema.registry.ssl.key.password": "<password>",
             ... 
      },
      "headers": {
          "traceID" : "b29c5de2-0db4-490b-b421",
          "timeStamp" : "1577191871865",
          ...
      }
    }
  }
}
```

## Adding Protobuf messages to Kafka

If necessary, from your Kafka installation directory run the following command to create the Kafka topic

```
./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic metrics_pb
```

This example script requires `protobuf` and `kafka-python` modules. With the topic in place, messages can be inserted running the following command from your Druid installation directory

```
./bin/generate-example-metrics | python /quickstart/protobuf/pb_publisher.py
```

You can confirm that data has been inserted to your Kafka topic using the following command from your Kafka installation directory

```
./bin/kafka-console-consumer --zookeeper localhost --topic metrics_pb
```

which should print messages like this

```
millisecondsGETR"2017-04-06T03:23:56Z*2002/list:request/latencyBwww1.example.com
```

If your supervisor created in the previous step is running, the indexing tasks should begin producing the messages and the data will soon be available for querying in Druid.

## Generating the example files

The files provided in the example quickstart can be generated in the following manner starting with only `metrics.proto`.

### `metrics.desc`

The descriptor file is generated using `protoc` Protobuf compiler. Given a `.proto` file, a `.desc` file can be generated like so.

```
protoc -o metrics.desc metrics.proto
```

### `metrics_pb2.py`
`metrics_pb2.py` is also generated with `protoc`

```
 protoc -o metrics.desc metrics.proto --python_out=.
```

### `pb_publisher.py`
After `metrics_pb2.py` is generated, another script can be constructed to parse JSON data, convert it to Protobuf, and produce to a Kafka topic

```python
#!/usr/bin/env python

import sys
import json

from kafka import KafkaProducer
from metrics_pb2 import Metrics


producer = KafkaProducer(bootstrap_servers='localhost:9092')
topic = 'metrics_pb'

for row in iter(sys.stdin):
    d = json.loads(row)
    metrics = Metrics()
    for k, v in d.items():
        setattr(metrics, k, v)
    pb = metrics.SerializeToString()
    producer.send(topic, pb)

producer.flush()
```
