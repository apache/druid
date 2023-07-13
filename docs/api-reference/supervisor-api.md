---
id: supervisor-api
title: Supervisor API
sidebar_label: Supervisors
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

This document describes the API endpoints to manage and monitor Supervisors for Apache Druid.

In this document, `http://<SERVICE_IP>:<SERVICE_PORT>` is a placeholder for the server address of deployment and the service port. For example, on the quickstart configuration, replace `http://<ROUTER_IP>:<ROUTER_PORT>` with `http://localhost:8888`.

## Supervisor information

### Get an array of active Supervisors

#### URL

<code className="getAPI">GET</code> `/druid/indexer/v1/supervisor`

Retrieves an array of active Supervisors. Returns a response object that includes an array of strings representing the names of the active Supervisors. If there are no active Supervisors, it returns an empty array.

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved array of active Supervisor IDs* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->
```shell
curl "http://<ROUTER_IP>:<ROUTER_PORT>/druid/indexer/v1/supervisor"
```
<!--HTTP-->
```HTTP
GET /druid/indexer/v1/supervisor HTTP/1.1
Host: http://<ROUTER_IP>:<ROUTER_PORT>
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

<details>
  <summary>Click to show sample response</summary>
  
  ```json
  [
    "wikipedia_stream",
    "social_media"
  ]
  ```
</details>

### Get an array of active Supervisor objects

#### URL

<code className="getAPI">GET</code> `/druid/indexer/v1/supervisor?full`

Retrieves an array of active Supervisor objects. Each object has properties relevant to the Supervisor: `id`, `state`, `detailedState`, `healthy`, and `spec`. 

See the following table for details on properties within the response object:

|Field|Type|Description|
|---|---|---|
|`id`|String|Supervisor unique identifier|
|`state`|String|The basic state of the supervisor. Available states:`UNHEALTHY_SUPERVISOR`, `UNHEALTHY_TASKS`, `PENDING`, `RUNNING`, `SUSPENDED`, `STOPPING`. Check [Kafka Docs](../development/extensions-core/kafka-supervisor-operations.md) for details.|
|`detailedState`|String|The Supervisor specific state. See documentation of specific supervisor for details: [Kafka](../development/extensions-core/kafka-ingestion.md) or [Kinesis](../development/extensions-core/kinesis-ingestion.md)|
|`healthy`|Boolean|True or false indicator of overall Supervisor health|
|`spec`|SupervisorSpec|JSON specification of the Supervisor|

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved active Supervisor objects* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->
```shell
curl "http://<ROUTER_IP>:<ROUTER_PORT>/druid/indexer/v1/supervisor?full=null"
```
<!--HTTP-->
```HTTP
GET /druid/indexer/v1/supervisor?full=null HTTP/1.1
Host: http://<ROUTER_IP>:<ROUTER_PORT>
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

<details>
  <summary>Click to show sample response</summary>
  
  ```json
  [
    {
        "id": "wikipedia_stream",
        "state": "RUNNING",
        "detailedState": "CONNECTING_TO_STREAM",
        "healthy": true,
        "spec": {
            "type": "kafka",
            "spec": {
                "dataSchema": {
                    "dataSource": "wikipedia_stream",
                    "timestampSpec": {
                        "column": "__time",
                        "format": "iso",
                        "missingValue": null
                    },
                    "dimensionsSpec": {
                        "dimensions": [
                            {
                                "type": "string",
                                "name": "username",
                                "multiValueHandling": "SORTED_ARRAY",
                                "createBitmapIndex": true
                            },
                            {
                                "type": "string",
                                "name": "post_title",
                                "multiValueHandling": "SORTED_ARRAY",
                                "createBitmapIndex": true
                            },
                            {
                                "type": "long",
                                "name": "views",
                                "multiValueHandling": "SORTED_ARRAY",
                                "createBitmapIndex": false
                            },
                            {
                                "type": "long",
                                "name": "upvotes",
                                "multiValueHandling": "SORTED_ARRAY",
                                "createBitmapIndex": false
                            },
                            {
                                "type": "long",
                                "name": "comments",
                                "multiValueHandling": "SORTED_ARRAY",
                                "createBitmapIndex": false
                            },
                            {
                                "type": "string",
                                "name": "edited",
                                "multiValueHandling": "SORTED_ARRAY",
                                "createBitmapIndex": true
                            }
                        ],
                        "dimensionExclusions": [
                            "__time"
                        ],
                        "includeAllDimensions": false,
                        "useSchemaDiscovery": false
                    },
                    "metricsSpec": [],
                    "granularitySpec": {
                        "type": "uniform",
                        "segmentGranularity": "HOUR",
                        "queryGranularity": {
                            "type": "none"
                        },
                        "rollup": false,
                        "intervals": []
                    },
                    "transformSpec": {
                        "filter": null,
                        "transforms": []
                    }
                },
                "ioConfig": {
                    "topic": "social_media",
                    "inputFormat": {
                        "type": "json",
                        "keepNullColumns": false,
                        "assumeNewlineDelimited": false,
                        "useJsonNodeReader": false
                    },
                    "replicas": 1,
                    "taskCount": 1,
                    "taskDuration": "PT3600S",
                    "consumerProperties": {
                        "bootstrap.servers": "localhost:9042"
                    },
                    "autoScalerConfig": null,
                    "pollTimeout": 100,
                    "startDelay": "PT5S",
                    "period": "PT30S",
                    "useEarliestOffset": true,
                    "completionTimeout": "PT1800S",
                    "lateMessageRejectionPeriod": null,
                    "earlyMessageRejectionPeriod": null,
                    "lateMessageRejectionStartDateTime": null,
                    "configOverrides": null,
                    "idleConfig": null,
                    "stream": "social_media",
                    "useEarliestSequenceNumber": true
                },
                "tuningConfig": {
                    "type": "kafka",
                    "appendableIndexSpec": {
                        "type": "onheap",
                        "preserveExistingMetrics": false
                    },
                    "maxRowsInMemory": 150000,
                    "maxBytesInMemory": 0,
                    "skipBytesInMemoryOverheadCheck": false,
                    "maxRowsPerSegment": 5000000,
                    "maxTotalRows": null,
                    "intermediatePersistPeriod": "PT10M",
                    "maxPendingPersists": 0,
                    "indexSpec": {
                        "bitmap": {
                            "type": "roaring"
                        },
                        "dimensionCompression": "lz4",
                        "stringDictionaryEncoding": {
                            "type": "utf8"
                        },
                        "metricCompression": "lz4",
                        "longEncoding": "longs"
                    },
                    "indexSpecForIntermediatePersists": {
                        "bitmap": {
                            "type": "roaring"
                        },
                        "dimensionCompression": "lz4",
                        "stringDictionaryEncoding": {
                            "type": "utf8"
                        },
                        "metricCompression": "lz4",
                        "longEncoding": "longs"
                    },
                    "reportParseExceptions": false,
                    "handoffConditionTimeout": 0,
                    "resetOffsetAutomatically": false,
                    "segmentWriteOutMediumFactory": null,
                    "workerThreads": null,
                    "chatThreads": null,
                    "chatRetries": 8,
                    "httpTimeout": "PT10S",
                    "shutdownTimeout": "PT80S",
                    "offsetFetchPeriod": "PT30S",
                    "intermediateHandoffPeriod": "P2147483647D",
                    "logParseExceptions": false,
                    "maxParseExceptions": 2147483647,
                    "maxSavedParseExceptions": 0,
                    "skipSequenceNumberAvailabilityCheck": false,
                    "repartitionTransitionDuration": "PT120S"
                }
            },
            "dataSchema": {
                "dataSource": "wikipedia_stream",
                "timestampSpec": {
                    "column": "__time",
                    "format": "iso",
                    "missingValue": null
                },
                "dimensionsSpec": {
                    "dimensions": [
                        {
                            "type": "string",
                            "name": "username",
                            "multiValueHandling": "SORTED_ARRAY",
                            "createBitmapIndex": true
                        },
                        {
                            "type": "string",
                            "name": "post_title",
                            "multiValueHandling": "SORTED_ARRAY",
                            "createBitmapIndex": true
                        },
                        {
                            "type": "long",
                            "name": "views",
                            "multiValueHandling": "SORTED_ARRAY",
                            "createBitmapIndex": false
                        },
                        {
                            "type": "long",
                            "name": "upvotes",
                            "multiValueHandling": "SORTED_ARRAY",
                            "createBitmapIndex": false
                        },
                        {
                            "type": "long",
                            "name": "comments",
                            "multiValueHandling": "SORTED_ARRAY",
                            "createBitmapIndex": false
                        },
                        {
                            "type": "string",
                            "name": "edited",
                            "multiValueHandling": "SORTED_ARRAY",
                            "createBitmapIndex": true
                        }
                    ],
                    "dimensionExclusions": [
                        "__time"
                    ],
                    "includeAllDimensions": false,
                    "useSchemaDiscovery": false
                },
                "metricsSpec": [],
                "granularitySpec": {
                    "type": "uniform",
                    "segmentGranularity": "HOUR",
                    "queryGranularity": {
                        "type": "none"
                    },
                    "rollup": false,
                    "intervals": []
                },
                "transformSpec": {
                    "filter": null,
                    "transforms": []
                }
            },
            "tuningConfig": {
                "type": "kafka",
                "appendableIndexSpec": {
                    "type": "onheap",
                    "preserveExistingMetrics": false
                },
                "maxRowsInMemory": 150000,
                "maxBytesInMemory": 0,
                "skipBytesInMemoryOverheadCheck": false,
                "maxRowsPerSegment": 5000000,
                "maxTotalRows": null,
                "intermediatePersistPeriod": "PT10M",
                "maxPendingPersists": 0,
                "indexSpec": {
                    "bitmap": {
                        "type": "roaring"
                    },
                    "dimensionCompression": "lz4",
                    "stringDictionaryEncoding": {
                        "type": "utf8"
                    },
                    "metricCompression": "lz4",
                    "longEncoding": "longs"
                },
                "indexSpecForIntermediatePersists": {
                    "bitmap": {
                        "type": "roaring"
                    },
                    "dimensionCompression": "lz4",
                    "stringDictionaryEncoding": {
                        "type": "utf8"
                    },
                    "metricCompression": "lz4",
                    "longEncoding": "longs"
                },
                "reportParseExceptions": false,
                "handoffConditionTimeout": 0,
                "resetOffsetAutomatically": false,
                "segmentWriteOutMediumFactory": null,
                "workerThreads": null,
                "chatThreads": null,
                "chatRetries": 8,
                "httpTimeout": "PT10S",
                "shutdownTimeout": "PT80S",
                "offsetFetchPeriod": "PT30S",
                "intermediateHandoffPeriod": "P2147483647D",
                "logParseExceptions": false,
                "maxParseExceptions": 2147483647,
                "maxSavedParseExceptions": 0,
                "skipSequenceNumberAvailabilityCheck": false,
                "repartitionTransitionDuration": "PT120S"
            },
            "ioConfig": {
                "topic": "social_media",
                "inputFormat": {
                    "type": "json",
                    "keepNullColumns": false,
                    "assumeNewlineDelimited": false,
                    "useJsonNodeReader": false
                },
                "replicas": 1,
                "taskCount": 1,
                "taskDuration": "PT3600S",
                "consumerProperties": {
                    "bootstrap.servers": "localhost:9042"
                },
                "autoScalerConfig": null,
                "pollTimeout": 100,
                "startDelay": "PT5S",
                "period": "PT30S",
                "useEarliestOffset": true,
                "completionTimeout": "PT1800S",
                "lateMessageRejectionPeriod": null,
                "earlyMessageRejectionPeriod": null,
                "lateMessageRejectionStartDateTime": null,
                "configOverrides": null,
                "idleConfig": null,
                "stream": "social_media",
                "useEarliestSequenceNumber": true
            },
            "context": null,
            "suspended": false
        },
        "suspended": false
    },
    {
        "id": "social_media",
        "state": "RUNNING",
        "detailedState": "RUNNING",
        "healthy": true,
        "spec": {
            "type": "kafka",
            "spec": {
                "dataSchema": {
                    "dataSource": "social_media",
                    "timestampSpec": {
                        "column": "__time",
                        "format": "iso",
                        "missingValue": null
                    },
                    "dimensionsSpec": {
                        "dimensions": [
                            {
                                "type": "string",
                                "name": "username",
                                "multiValueHandling": "SORTED_ARRAY",
                                "createBitmapIndex": true
                            },
                            {
                                "type": "string",
                                "name": "post_title",
                                "multiValueHandling": "SORTED_ARRAY",
                                "createBitmapIndex": true
                            },
                            {
                                "type": "long",
                                "name": "views",
                                "multiValueHandling": "SORTED_ARRAY",
                                "createBitmapIndex": false
                            },
                            {
                                "type": "long",
                                "name": "upvotes",
                                "multiValueHandling": "SORTED_ARRAY",
                                "createBitmapIndex": false
                            },
                            {
                                "type": "long",
                                "name": "comments",
                                "multiValueHandling": "SORTED_ARRAY",
                                "createBitmapIndex": false
                            },
                            {
                                "type": "string",
                                "name": "edited",
                                "multiValueHandling": "SORTED_ARRAY",
                                "createBitmapIndex": true
                            }
                        ],
                        "dimensionExclusions": [
                            "__time"
                        ],
                        "includeAllDimensions": false,
                        "useSchemaDiscovery": false
                    },
                    "metricsSpec": [],
                    "granularitySpec": {
                        "type": "uniform",
                        "segmentGranularity": "HOUR",
                        "queryGranularity": {
                            "type": "none"
                        },
                        "rollup": false,
                        "intervals": []
                    },
                    "transformSpec": {
                        "filter": null,
                        "transforms": []
                    }
                },
                "ioConfig": {
                    "topic": "social_media",
                    "inputFormat": {
                        "type": "json",
                        "keepNullColumns": false,
                        "assumeNewlineDelimited": false,
                        "useJsonNodeReader": false
                    },
                    "replicas": 1,
                    "taskCount": 1,
                    "taskDuration": "PT3600S",
                    "consumerProperties": {
                        "bootstrap.servers": "localhost:9094"
                    },
                    "autoScalerConfig": null,
                    "pollTimeout": 100,
                    "startDelay": "PT5S",
                    "period": "PT30S",
                    "useEarliestOffset": true,
                    "completionTimeout": "PT1800S",
                    "lateMessageRejectionPeriod": null,
                    "earlyMessageRejectionPeriod": null,
                    "lateMessageRejectionStartDateTime": null,
                    "configOverrides": null,
                    "idleConfig": null,
                    "stream": "social_media",
                    "useEarliestSequenceNumber": true
                },
                "tuningConfig": {
                    "type": "kafka",
                    "appendableIndexSpec": {
                        "type": "onheap",
                        "preserveExistingMetrics": false
                    },
                    "maxRowsInMemory": 150000,
                    "maxBytesInMemory": 0,
                    "skipBytesInMemoryOverheadCheck": false,
                    "maxRowsPerSegment": 5000000,
                    "maxTotalRows": null,
                    "intermediatePersistPeriod": "PT10M",
                    "maxPendingPersists": 0,
                    "indexSpec": {
                        "bitmap": {
                            "type": "roaring"
                        },
                        "dimensionCompression": "lz4",
                        "stringDictionaryEncoding": {
                            "type": "utf8"
                        },
                        "metricCompression": "lz4",
                        "longEncoding": "longs"
                    },
                    "indexSpecForIntermediatePersists": {
                        "bitmap": {
                            "type": "roaring"
                        },
                        "dimensionCompression": "lz4",
                        "stringDictionaryEncoding": {
                            "type": "utf8"
                        },
                        "metricCompression": "lz4",
                        "longEncoding": "longs"
                    },
                    "reportParseExceptions": false,
                    "handoffConditionTimeout": 0,
                    "resetOffsetAutomatically": false,
                    "segmentWriteOutMediumFactory": null,
                    "workerThreads": null,
                    "chatThreads": null,
                    "chatRetries": 8,
                    "httpTimeout": "PT10S",
                    "shutdownTimeout": "PT80S",
                    "offsetFetchPeriod": "PT30S",
                    "intermediateHandoffPeriod": "P2147483647D",
                    "logParseExceptions": false,
                    "maxParseExceptions": 2147483647,
                    "maxSavedParseExceptions": 0,
                    "skipSequenceNumberAvailabilityCheck": false,
                    "repartitionTransitionDuration": "PT120S"
                }
            },
            "dataSchema": {
                "dataSource": "social_media",
                "timestampSpec": {
                    "column": "__time",
                    "format": "iso",
                    "missingValue": null
                },
                "dimensionsSpec": {
                    "dimensions": [
                        {
                            "type": "string",
                            "name": "username",
                            "multiValueHandling": "SORTED_ARRAY",
                            "createBitmapIndex": true
                        },
                        {
                            "type": "string",
                            "name": "post_title",
                            "multiValueHandling": "SORTED_ARRAY",
                            "createBitmapIndex": true
                        },
                        {
                            "type": "long",
                            "name": "views",
                            "multiValueHandling": "SORTED_ARRAY",
                            "createBitmapIndex": false
                        },
                        {
                            "type": "long",
                            "name": "upvotes",
                            "multiValueHandling": "SORTED_ARRAY",
                            "createBitmapIndex": false
                        },
                        {
                            "type": "long",
                            "name": "comments",
                            "multiValueHandling": "SORTED_ARRAY",
                            "createBitmapIndex": false
                        },
                        {
                            "type": "string",
                            "name": "edited",
                            "multiValueHandling": "SORTED_ARRAY",
                            "createBitmapIndex": true
                        }
                    ],
                    "dimensionExclusions": [
                        "__time"
                    ],
                    "includeAllDimensions": false,
                    "useSchemaDiscovery": false
                },
                "metricsSpec": [],
                "granularitySpec": {
                    "type": "uniform",
                    "segmentGranularity": "HOUR",
                    "queryGranularity": {
                        "type": "none"
                    },
                    "rollup": false,
                    "intervals": []
                },
                "transformSpec": {
                    "filter": null,
                    "transforms": []
                }
            },
            "tuningConfig": {
                "type": "kafka",
                "appendableIndexSpec": {
                    "type": "onheap",
                    "preserveExistingMetrics": false
                },
                "maxRowsInMemory": 150000,
                "maxBytesInMemory": 0,
                "skipBytesInMemoryOverheadCheck": false,
                "maxRowsPerSegment": 5000000,
                "maxTotalRows": null,
                "intermediatePersistPeriod": "PT10M",
                "maxPendingPersists": 0,
                "indexSpec": {
                    "bitmap": {
                        "type": "roaring"
                    },
                    "dimensionCompression": "lz4",
                    "stringDictionaryEncoding": {
                        "type": "utf8"
                    },
                    "metricCompression": "lz4",
                    "longEncoding": "longs"
                },
                "indexSpecForIntermediatePersists": {
                    "bitmap": {
                        "type": "roaring"
                    },
                    "dimensionCompression": "lz4",
                    "stringDictionaryEncoding": {
                        "type": "utf8"
                    },
                    "metricCompression": "lz4",
                    "longEncoding": "longs"
                },
                "reportParseExceptions": false,
                "handoffConditionTimeout": 0,
                "resetOffsetAutomatically": false,
                "segmentWriteOutMediumFactory": null,
                "workerThreads": null,
                "chatThreads": null,
                "chatRetries": 8,
                "httpTimeout": "PT10S",
                "shutdownTimeout": "PT80S",
                "offsetFetchPeriod": "PT30S",
                "intermediateHandoffPeriod": "P2147483647D",
                "logParseExceptions": false,
                "maxParseExceptions": 2147483647,
                "maxSavedParseExceptions": 0,
                "skipSequenceNumberAvailabilityCheck": false,
                "repartitionTransitionDuration": "PT120S"
            },
            "ioConfig": {
                "topic": "social_media",
                "inputFormat": {
                    "type": "json",
                    "keepNullColumns": false,
                    "assumeNewlineDelimited": false,
                    "useJsonNodeReader": false
                },
                "replicas": 1,
                "taskCount": 1,
                "taskDuration": "PT3600S",
                "consumerProperties": {
                    "bootstrap.servers": "localhost:9094"
                },
                "autoScalerConfig": null,
                "pollTimeout": 100,
                "startDelay": "PT5S",
                "period": "PT30S",
                "useEarliestOffset": true,
                "completionTimeout": "PT1800S",
                "lateMessageRejectionPeriod": null,
                "earlyMessageRejectionPeriod": null,
                "lateMessageRejectionStartDateTime": null,
                "configOverrides": null,
                "idleConfig": null,
                "stream": "social_media",
                "useEarliestSequenceNumber": true
            },
            "context": null,
            "suspended": false
        },
        "suspended": false
    }
  ]
  ```
</details>

### Get Supervisor state 

<code className="getAPI">GET</code> `/druid/indexer/v1/supervisor?state=true`

Retrieves an array of objects of the currently active Supervisors and their current state. If there are no active Supervisors, it returns an empty array.

See the following table for details on properties within the response object:
|Field|Type|Description|
|---|---|---|
|`id`|String|Supervisor unique identifier|
|`state`|String|The basic state of the Supervisor. Available states: `UNHEALTHY_SUPERVISOR`, `UNHEALTHY_TASKS`, `PENDING`, `RUNNING`, `SUSPENDED`, `STOPPING`. Check [Kafka Docs](../development/extensions-core/kafka-supervisor-operations.md) for details.|
|`detailedState`|String|Supervisor specific state. See documentation of the specific Supervisor for details: [Kafka](../development/extensions-core/kafka-ingestion.md) or [Kinesis](../development/extensions-core/kinesis-ingestion.md)|
|`healthy`|Boolean|True or false indicator of overall Supervisor health|
|`suspended`|Boolean|True or false indicator of whether the Supervisor is in suspended state|

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved Supervisor state objects*  

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->
```shell
curl "http://<ROUTER_IP>:<ROUTER_PORT>/druid/indexer/v1/supervisor?state=true"
```
<!--HTTP-->
```HTTP
GET /druid/indexer/v1/supervisor?state=true HTTP/1.1
Host: http://<ROUTER_IP>:<ROUTER_PORT>
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

<details>
  <summary>Click to show sample response</summary>
  
  ```json
  [
    {
        "id": "wikipedia_stream",
        "state": "UNHEALTHY_SUPERVISOR",
        "detailedState": "UNABLE_TO_CONNECT_TO_STREAM",
        "healthy": false,
        "suspended": false
    },
    {
        "id": "social_media",
        "state": "RUNNING",
        "detailedState": "RUNNING",
        "healthy": true,
        "suspended": false
    }
  ]
  ```

</details>

### Get Supervisor specification

#### URL

<code className="getAPI">GET</code> `/druid/indexer/v1/supervisor/{supervisorId}`

Retrieves the current spec for the Supervisor with the provided ID. The returned Supervisor spec specifies the `dataSchema`, `ioConfig`, and `tuningConfig`. 

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved Supervisor specification* 
<!--404 NOT FOUND-->
<br/>
*Invalid Supervisor ID* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

The following example shows how to retrieve the specification of the `wikipedia_stream` Supervisor.

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->
```shell
curl "http://<ROUTER_IP>:<ROUTER_PORT>/druid/indexer/v1/supervisor/wikipedia_stream"
```
<!--HTTP-->
```HTTP
GET /druid/indexer/v1/supervisor/wikipedia_stream HTTP/1.1
Host: http://<ROUTER_IP>:<ROUTER_PORT>
```
<!--END_DOCUSAURUS_CODE_TABS-->


#### Sample response

<details>
  <summary>Click to show sample response</summary>

  ```json
{
    "type": "kafka",
    "spec": {
        "dataSchema": {
            "dataSource": "social_media",
            "timestampSpec": {
                "column": "__time",
                "format": "iso",
                "missingValue": null
            },
            "dimensionsSpec": {
                "dimensions": [
                    {
                        "type": "string",
                        "name": "username",
                        "multiValueHandling": "SORTED_ARRAY",
                        "createBitmapIndex": true
                    },
                    {
                        "type": "string",
                        "name": "post_title",
                        "multiValueHandling": "SORTED_ARRAY",
                        "createBitmapIndex": true
                    },
                    {
                        "type": "long",
                        "name": "views",
                        "multiValueHandling": "SORTED_ARRAY",
                        "createBitmapIndex": false
                    },
                    {
                        "type": "long",
                        "name": "upvotes",
                        "multiValueHandling": "SORTED_ARRAY",
                        "createBitmapIndex": false
                    },
                    {
                        "type": "long",
                        "name": "comments",
                        "multiValueHandling": "SORTED_ARRAY",
                        "createBitmapIndex": false
                    },
                    {
                        "type": "string",
                        "name": "edited",
                        "multiValueHandling": "SORTED_ARRAY",
                        "createBitmapIndex": true
                    }
                ],
                "dimensionExclusions": [
                    "__time"
                ],
                "includeAllDimensions": false,
                "useSchemaDiscovery": false
            },
            "metricsSpec": [],
            "granularitySpec": {
                "type": "uniform",
                "segmentGranularity": "HOUR",
                "queryGranularity": {
                    "type": "none"
                },
                "rollup": false,
                "intervals": []
            },
            "transformSpec": {
                "filter": null,
                "transforms": []
            }
        },
        "ioConfig": {
            "topic": "social_media",
            "inputFormat": {
                "type": "json",
                "keepNullColumns": false,
                "assumeNewlineDelimited": false,
                "useJsonNodeReader": false
            },
            "replicas": 1,
            "taskCount": 1,
            "taskDuration": "PT3600S",
            "consumerProperties": {
                "bootstrap.servers": "localhost:9094"
            },
            "autoScalerConfig": null,
            "pollTimeout": 100,
            "startDelay": "PT5S",
            "period": "PT30S",
            "useEarliestOffset": true,
            "completionTimeout": "PT1800S",
            "lateMessageRejectionPeriod": null,
            "earlyMessageRejectionPeriod": null,
            "lateMessageRejectionStartDateTime": null,
            "configOverrides": null,
            "idleConfig": null,
            "stream": "social_media",
            "useEarliestSequenceNumber": true
        },
        "tuningConfig": {
            "type": "kafka",
            "appendableIndexSpec": {
                "type": "onheap",
                "preserveExistingMetrics": false
            },
            "maxRowsInMemory": 150000,
            "maxBytesInMemory": 0,
            "skipBytesInMemoryOverheadCheck": false,
            "maxRowsPerSegment": 5000000,
            "maxTotalRows": null,
            "intermediatePersistPeriod": "PT10M",
            "maxPendingPersists": 0,
            "indexSpec": {
                "bitmap": {
                    "type": "roaring"
                },
                "dimensionCompression": "lz4",
                "stringDictionaryEncoding": {
                    "type": "utf8"
                },
                "metricCompression": "lz4",
                "longEncoding": "longs"
            },
            "indexSpecForIntermediatePersists": {
                "bitmap": {
                    "type": "roaring"
                },
                "dimensionCompression": "lz4",
                "stringDictionaryEncoding": {
                    "type": "utf8"
                },
                "metricCompression": "lz4",
                "longEncoding": "longs"
            },
            "reportParseExceptions": false,
            "handoffConditionTimeout": 0,
            "resetOffsetAutomatically": false,
            "segmentWriteOutMediumFactory": null,
            "workerThreads": null,
            "chatThreads": null,
            "chatRetries": 8,
            "httpTimeout": "PT10S",
            "shutdownTimeout": "PT80S",
            "offsetFetchPeriod": "PT30S",
            "intermediateHandoffPeriod": "P2147483647D",
            "logParseExceptions": false,
            "maxParseExceptions": 2147483647,
            "maxSavedParseExceptions": 0,
            "skipSequenceNumberAvailabilityCheck": false,
            "repartitionTransitionDuration": "PT120S"
        }
    },
    "dataSchema": {
        "dataSource": "social_media",
        "timestampSpec": {
            "column": "__time",
            "format": "iso",
            "missingValue": null
        },
        "dimensionsSpec": {
            "dimensions": [
                {
                    "type": "string",
                    "name": "username",
                    "multiValueHandling": "SORTED_ARRAY",
                    "createBitmapIndex": true
                },
                {
                    "type": "string",
                    "name": "post_title",
                    "multiValueHandling": "SORTED_ARRAY",
                    "createBitmapIndex": true
                },
                {
                    "type": "long",
                    "name": "views",
                    "multiValueHandling": "SORTED_ARRAY",
                    "createBitmapIndex": false
                },
                {
                    "type": "long",
                    "name": "upvotes",
                    "multiValueHandling": "SORTED_ARRAY",
                    "createBitmapIndex": false
                },
                {
                    "type": "long",
                    "name": "comments",
                    "multiValueHandling": "SORTED_ARRAY",
                    "createBitmapIndex": false
                },
                {
                    "type": "string",
                    "name": "edited",
                    "multiValueHandling": "SORTED_ARRAY",
                    "createBitmapIndex": true
                }
            ],
            "dimensionExclusions": [
                "__time"
            ],
            "includeAllDimensions": false,
            "useSchemaDiscovery": false
        },
        "metricsSpec": [],
        "granularitySpec": {
            "type": "uniform",
            "segmentGranularity": "HOUR",
            "queryGranularity": {
                "type": "none"
            },
            "rollup": false,
            "intervals": []
        },
        "transformSpec": {
            "filter": null,
            "transforms": []
        }
    },
    "tuningConfig": {
        "type": "kafka",
        "appendableIndexSpec": {
            "type": "onheap",
            "preserveExistingMetrics": false
        },
        "maxRowsInMemory": 150000,
        "maxBytesInMemory": 0,
        "skipBytesInMemoryOverheadCheck": false,
        "maxRowsPerSegment": 5000000,
        "maxTotalRows": null,
        "intermediatePersistPeriod": "PT10M",
        "maxPendingPersists": 0,
        "indexSpec": {
            "bitmap": {
                "type": "roaring"
            },
            "dimensionCompression": "lz4",
            "stringDictionaryEncoding": {
                "type": "utf8"
            },
            "metricCompression": "lz4",
            "longEncoding": "longs"
        },
        "indexSpecForIntermediatePersists": {
            "bitmap": {
                "type": "roaring"
            },
            "dimensionCompression": "lz4",
            "stringDictionaryEncoding": {
                "type": "utf8"
            },
            "metricCompression": "lz4",
            "longEncoding": "longs"
        },
        "reportParseExceptions": false,
        "handoffConditionTimeout": 0,
        "resetOffsetAutomatically": false,
        "segmentWriteOutMediumFactory": null,
        "workerThreads": null,
        "chatThreads": null,
        "chatRetries": 8,
        "httpTimeout": "PT10S",
        "shutdownTimeout": "PT80S",
        "offsetFetchPeriod": "PT30S",
        "intermediateHandoffPeriod": "P2147483647D",
        "logParseExceptions": false,
        "maxParseExceptions": 2147483647,
        "maxSavedParseExceptions": 0,
        "skipSequenceNumberAvailabilityCheck": false,
        "repartitionTransitionDuration": "PT120S"
    },
    "ioConfig": {
        "topic": "social_media",
        "inputFormat": {
            "type": "json",
            "keepNullColumns": false,
            "assumeNewlineDelimited": false,
            "useJsonNodeReader": false
        },
        "replicas": 1,
        "taskCount": 1,
        "taskDuration": "PT3600S",
        "consumerProperties": {
            "bootstrap.servers": "localhost:9094"
        },
        "autoScalerConfig": null,
        "pollTimeout": 100,
        "startDelay": "PT5S",
        "period": "PT30S",
        "useEarliestOffset": true,
        "completionTimeout": "PT1800S",
        "lateMessageRejectionPeriod": null,
        "earlyMessageRejectionPeriod": null,
        "lateMessageRejectionStartDateTime": null,
        "configOverrides": null,
        "idleConfig": null,
        "stream": "social_media",
        "useEarliestSequenceNumber": true
    },
    "context": null,
    "suspended": false
}
  ```
</details>

### Get Supervisor status

#### URL
<code className="getAPI">GET</code> `/druid/indexer/v1/supervisor/{supervisorId}/status`

Retrieves a current status report of the Supervisor with the provided ID. The report contains the state of the tasks managed by the Supervisor and an array of recently thrown exceptions. The possible `state` values are: [`PENDING`, `RUNNING`, `SUSPENDED`, `STOPPING`, `UNHEALTHY_SUPERVISOR`, `UNHEALTHY_TASKS`].

 For additional information about the status report, see the documentation for each streaming ingestion methods:
* [Amazon Kinesis](../development/extensions-core/kinesis-ingestion.md#getting-supervisor-status-report)
* [Apache Kafka](../development/extensions-core/kafka-supervisor-operations.md#getting-supervisor-status-report)

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved Supervisor status* 
<!--404 NOT FOUND-->
<br/>
*Invalid Supervisor ID* 
<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

The following example shows retrieving the status of the `social_media` Supervisor. 

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->
```shell
curl "http://<ROUTER_IP>:<ROUTER_PORT>/druid/indexer/v1/supervisor/social_media/status"
```
<!--HTTP-->
```HTTP
GET /druid/indexer/v1/supervisor/social_media/status HTTP/1.1
Host: http://<ROUTER_IP>:<ROUTER_PORT>
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

<details>
  <summary>Click to show sample response</summary>

  ```json
  {
      "id": "social_media",
      "generationTime": "2023-07-05T23:24:43.934Z",
      "payload": {
          "dataSource": "social_media",
          "stream": "social_media",
          "partitions": 1,
          "replicas": 1,
          "durationSeconds": 3600,
          "activeTasks": [
              {
                  "id": "index_kafka_social_media_ab72ae4127c591c_flcbhdlh",
                  "startingOffsets": {
                      "0": 3176381
                  },
                  "startTime": "2023-07-05T23:21:39.321Z",
                  "remainingSeconds": 3415,
                  "type": "ACTIVE",
                  "currentOffsets": {
                      "0": 3296632
                  },
                  "lag": {
                      "0": 3
                  }
              }
          ],
          "publishingTasks": [],
          "latestOffsets": {
              "0": 3296635
          },
          "minimumLag": {
              "0": 3
          },
          "aggregateLag": 3,
          "offsetsLastUpdated": "2023-07-05T23:24:30.212Z",
          "suspended": false,
          "healthy": true,
          "state": "RUNNING",
          "detailedState": "RUNNING",
          "recentErrors": []
      }
  }
  ```
</details>

## Audit history

### Get audit history for all Supervisors

#### URL

<code className="getAPI">GET</code> `/druid/indexer/v1/supervisor/history`

Retrieve an audit history of specs for all Supervisors (current and past).

An audit history of a Supervisor provides a comprehensive log of events, including its configuration, creation, suspension, and modification history.

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved audit history* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->
```shell
curl "http://<ROUTER_IP>:<ROUTER_PORT>/druid/indexer/v1/supervisor/history"
```
<!--HTTP-->
```HTTP
GET /druid/indexer/v1/supervisor/history HTTP/1.1
Host: http://<ROUTER_IP>:<ROUTER_PORT>
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

<details>
  <summary>Click to show sample response</summary>

  ```json
{
    "social_media": [
        {
            "spec": {
                "type": "kafka",
                "spec": {
                    "dataSchema": {
                        "dataSource": "social_media",
                        "timestampSpec": {
                            "column": "__time",
                            "format": "iso",
                            "missingValue": null
                        },
                        "dimensionsSpec": {
                            "dimensions": [
                                {
                                    "type": "string",
                                    "name": "username",
                                    "multiValueHandling": "SORTED_ARRAY",
                                    "createBitmapIndex": true
                                },
                                {
                                    "type": "string",
                                    "name": "post_title",
                                    "multiValueHandling": "SORTED_ARRAY",
                                    "createBitmapIndex": true
                                },
                                {
                                    "type": "long",
                                    "name": "views",
                                    "multiValueHandling": "SORTED_ARRAY",
                                    "createBitmapIndex": false
                                },
                                {
                                    "type": "long",
                                    "name": "upvotes",
                                    "multiValueHandling": "SORTED_ARRAY",
                                    "createBitmapIndex": false
                                },
                                {
                                    "type": "long",
                                    "name": "comments",
                                    "multiValueHandling": "SORTED_ARRAY",
                                    "createBitmapIndex": false
                                },
                                {
                                    "type": "string",
                                    "name": "edited",
                                    "multiValueHandling": "SORTED_ARRAY",
                                    "createBitmapIndex": true
                                }
                            ],
                            "dimensionExclusions": [
                                "__time"
                            ],
                            "includeAllDimensions": false,
                            "useSchemaDiscovery": false
                        },
                        "metricsSpec": [],
                        "granularitySpec": {
                            "type": "uniform",
                            "segmentGranularity": "HOUR",
                            "queryGranularity": {
                                "type": "none"
                            },
                            "rollup": false,
                            "intervals": []
                        },
                        "transformSpec": {
                            "filter": null,
                            "transforms": []
                        }
                    },
                    "ioConfig": {
                        "topic": "social_media",
                        "inputFormat": {
                            "type": "json",
                            "keepNullColumns": false,
                            "assumeNewlineDelimited": false,
                            "useJsonNodeReader": false
                        },
                        "replicas": 1,
                        "taskCount": 1,
                        "taskDuration": "PT3600S",
                        "consumerProperties": {
                            "bootstrap.servers": "localhost:9094"
                        },
                        "autoScalerConfig": null,
                        "pollTimeout": 100,
                        "startDelay": "PT5S",
                        "period": "PT30S",
                        "useEarliestOffset": true,
                        "completionTimeout": "PT1800S",
                        "lateMessageRejectionPeriod": null,
                        "earlyMessageRejectionPeriod": null,
                        "lateMessageRejectionStartDateTime": null,
                        "configOverrides": null,
                        "idleConfig": null,
                        "stream": "social_media",
                        "useEarliestSequenceNumber": true
                    },
                    "tuningConfig": {
                        "type": "kafka",
                        "appendableIndexSpec": {
                            "type": "onheap",
                            "preserveExistingMetrics": false
                        },
                        "maxRowsInMemory": 150000,
                        "maxBytesInMemory": 0,
                        "skipBytesInMemoryOverheadCheck": false,
                        "maxRowsPerSegment": 5000000,
                        "maxTotalRows": null,
                        "intermediatePersistPeriod": "PT10M",
                        "maxPendingPersists": 0,
                        "indexSpec": {
                            "bitmap": {
                                "type": "roaring"
                            },
                            "dimensionCompression": "lz4",
                            "stringDictionaryEncoding": {
                                "type": "utf8"
                            },
                            "metricCompression": "lz4",
                            "longEncoding": "longs"
                        },
                        "indexSpecForIntermediatePersists": {
                            "bitmap": {
                                "type": "roaring"
                            },
                            "dimensionCompression": "lz4",
                            "stringDictionaryEncoding": {
                                "type": "utf8"
                            },
                            "metricCompression": "lz4",
                            "longEncoding": "longs"
                        },
                        "reportParseExceptions": false,
                        "handoffConditionTimeout": 0,
                        "resetOffsetAutomatically": false,
                        "segmentWriteOutMediumFactory": null,
                        "workerThreads": null,
                        "chatThreads": null,
                        "chatRetries": 8,
                        "httpTimeout": "PT10S",
                        "shutdownTimeout": "PT80S",
                        "offsetFetchPeriod": "PT30S",
                        "intermediateHandoffPeriod": "P2147483647D",
                        "logParseExceptions": false,
                        "maxParseExceptions": 2147483647,
                        "maxSavedParseExceptions": 0,
                        "skipSequenceNumberAvailabilityCheck": false,
                        "repartitionTransitionDuration": "PT120S"
                    }
                },
                "dataSchema": {
                    "dataSource": "social_media",
                    "timestampSpec": {
                        "column": "__time",
                        "format": "iso",
                        "missingValue": null
                    },
                    "dimensionsSpec": {
                        "dimensions": [
                            {
                                "type": "string",
                                "name": "username",
                                "multiValueHandling": "SORTED_ARRAY",
                                "createBitmapIndex": true
                            },
                            {
                                "type": "string",
                                "name": "post_title",
                                "multiValueHandling": "SORTED_ARRAY",
                                "createBitmapIndex": true
                            },
                            {
                                "type": "long",
                                "name": "views",
                                "multiValueHandling": "SORTED_ARRAY",
                                "createBitmapIndex": false
                            },
                            {
                                "type": "long",
                                "name": "upvotes",
                                "multiValueHandling": "SORTED_ARRAY",
                                "createBitmapIndex": false
                            },
                            {
                                "type": "long",
                                "name": "comments",
                                "multiValueHandling": "SORTED_ARRAY",
                                "createBitmapIndex": false
                            },
                            {
                                "type": "string",
                                "name": "edited",
                                "multiValueHandling": "SORTED_ARRAY",
                                "createBitmapIndex": true
                            }
                        ],
                        "dimensionExclusions": [
                            "__time"
                        ],
                        "includeAllDimensions": false,
                        "useSchemaDiscovery": false
                    },
                    "metricsSpec": [],
                    "granularitySpec": {
                        "type": "uniform",
                        "segmentGranularity": "HOUR",
                        "queryGranularity": {
                            "type": "none"
                        },
                        "rollup": false,
                        "intervals": []
                    },
                    "transformSpec": {
                        "filter": null,
                        "transforms": []
                    }
                },
                "tuningConfig": {
                    "type": "kafka",
                    "appendableIndexSpec": {
                        "type": "onheap",
                        "preserveExistingMetrics": false
                    },
                    "maxRowsInMemory": 150000,
                    "maxBytesInMemory": 0,
                    "skipBytesInMemoryOverheadCheck": false,
                    "maxRowsPerSegment": 5000000,
                    "maxTotalRows": null,
                    "intermediatePersistPeriod": "PT10M",
                    "maxPendingPersists": 0,
                    "indexSpec": {
                        "bitmap": {
                            "type": "roaring"
                        },
                        "dimensionCompression": "lz4",
                        "stringDictionaryEncoding": {
                            "type": "utf8"
                        },
                        "metricCompression": "lz4",
                        "longEncoding": "longs"
                    },
                    "indexSpecForIntermediatePersists": {
                        "bitmap": {
                            "type": "roaring"
                        },
                        "dimensionCompression": "lz4",
                        "stringDictionaryEncoding": {
                            "type": "utf8"
                        },
                        "metricCompression": "lz4",
                        "longEncoding": "longs"
                    },
                    "reportParseExceptions": false,
                    "handoffConditionTimeout": 0,
                    "resetOffsetAutomatically": false,
                    "segmentWriteOutMediumFactory": null,
                    "workerThreads": null,
                    "chatThreads": null,
                    "chatRetries": 8,
                    "httpTimeout": "PT10S",
                    "shutdownTimeout": "PT80S",
                    "offsetFetchPeriod": "PT30S",
                    "intermediateHandoffPeriod": "P2147483647D",
                    "logParseExceptions": false,
                    "maxParseExceptions": 2147483647,
                    "maxSavedParseExceptions": 0,
                    "skipSequenceNumberAvailabilityCheck": false,
                    "repartitionTransitionDuration": "PT120S"
                },
                "ioConfig": {
                    "topic": "social_media",
                    "inputFormat": {
                        "type": "json",
                        "keepNullColumns": false,
                        "assumeNewlineDelimited": false,
                        "useJsonNodeReader": false
                    },
                    "replicas": 1,
                    "taskCount": 1,
                    "taskDuration": "PT3600S",
                    "consumerProperties": {
                        "bootstrap.servers": "localhost:9094"
                    },
                    "autoScalerConfig": null,
                    "pollTimeout": 100,
                    "startDelay": "PT5S",
                    "period": "PT30S",
                    "useEarliestOffset": true,
                    "completionTimeout": "PT1800S",
                    "lateMessageRejectionPeriod": null,
                    "earlyMessageRejectionPeriod": null,
                    "lateMessageRejectionStartDateTime": null,
                    "configOverrides": null,
                    "idleConfig": null,
                    "stream": "social_media",
                    "useEarliestSequenceNumber": true
                },
                "context": null,
                "suspended": false
            },
            "version": "2023-07-03T18:51:02.970Z"
        }
    ]
}
  ```
</details>

### Get audit history for specific Supervisor

#### URL

<code className="getAPI">GET</code> `/druid/indexer/v1/supervisor/{supervisorId}/history`

Retrieve an audit history of specs for the Supervisor with the provided ID.

An audit history of a Supervisor provides a comprehensive log of events, including its configuration, creation, suspension, and modification history. 

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved audit history for Supervisor* 
<!--404 NOT FOUND-->
<br/>
*Invalid supervisor ID* 
<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

The following example shows how to retrieve the audit history of the `wikipedia_stream` Supervisor.

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->
```shell
curl "http://<ROUTER_IP>:<ROUTER_PORT>/druid/indexer/v1/supervisor/wikipedia_stream/history"
```
<!--HTTP-->
```HTTP
GET /druid/indexer/v1/supervisor/wikipedia_stream/history HTTP/1.1
Host: http://<ROUTER_IP>:<ROUTER_PORT>
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

<details>
  <summary>Click to show sample response</summary>

  ```json
[
    {
        "spec": {
            "type": "kafka",
            "spec": {
                "dataSchema": {
                    "dataSource": "wikipedia_stream",
                    "timestampSpec": {
                        "column": "__time",
                        "format": "iso",
                        "missingValue": null
                    },
                    "dimensionsSpec": {
                        "dimensions": [
                            {
                                "type": "string",
                                "name": "username",
                                "multiValueHandling": "SORTED_ARRAY",
                                "createBitmapIndex": true
                            },
                            {
                                "type": "string",
                                "name": "post_title",
                                "multiValueHandling": "SORTED_ARRAY",
                                "createBitmapIndex": true
                            },
                            {
                                "type": "long",
                                "name": "views",
                                "multiValueHandling": "SORTED_ARRAY",
                                "createBitmapIndex": false
                            },
                            {
                                "type": "long",
                                "name": "upvotes",
                                "multiValueHandling": "SORTED_ARRAY",
                                "createBitmapIndex": false
                            },
                            {
                                "type": "long",
                                "name": "comments",
                                "multiValueHandling": "SORTED_ARRAY",
                                "createBitmapIndex": false
                            },
                            {
                                "type": "string",
                                "name": "edited",
                                "multiValueHandling": "SORTED_ARRAY",
                                "createBitmapIndex": true
                            }
                        ],
                        "dimensionExclusions": [
                            "__time"
                        ],
                        "includeAllDimensions": false,
                        "useSchemaDiscovery": false
                    },
                    "metricsSpec": [],
                    "granularitySpec": {
                        "type": "uniform",
                        "segmentGranularity": "HOUR",
                        "queryGranularity": {
                            "type": "none"
                        },
                        "rollup": false,
                        "intervals": []
                    },
                    "transformSpec": {
                        "filter": null,
                        "transforms": []
                    }
                },
                "ioConfig": {
                    "topic": "social_media",
                    "inputFormat": {
                        "type": "json",
                        "keepNullColumns": false,
                        "assumeNewlineDelimited": false,
                        "useJsonNodeReader": false
                    },
                    "replicas": 1,
                    "taskCount": 1,
                    "taskDuration": "PT3600S",
                    "consumerProperties": {
                        "bootstrap.servers": "localhost:9042"
                    },
                    "autoScalerConfig": null,
                    "pollTimeout": 100,
                    "startDelay": "PT5S",
                    "period": "PT30S",
                    "useEarliestOffset": true,
                    "completionTimeout": "PT1800S",
                    "lateMessageRejectionPeriod": null,
                    "earlyMessageRejectionPeriod": null,
                    "lateMessageRejectionStartDateTime": null,
                    "configOverrides": null,
                    "idleConfig": null,
                    "stream": "social_media",
                    "useEarliestSequenceNumber": true
                },
                "tuningConfig": {
                    "type": "kafka",
                    "appendableIndexSpec": {
                        "type": "onheap",
                        "preserveExistingMetrics": false
                    },
                    "maxRowsInMemory": 150000,
                    "maxBytesInMemory": 0,
                    "skipBytesInMemoryOverheadCheck": false,
                    "maxRowsPerSegment": 5000000,
                    "maxTotalRows": null,
                    "intermediatePersistPeriod": "PT10M",
                    "maxPendingPersists": 0,
                    "indexSpec": {
                        "bitmap": {
                            "type": "roaring"
                        },
                        "dimensionCompression": "lz4",
                        "stringDictionaryEncoding": {
                            "type": "utf8"
                        },
                        "metricCompression": "lz4",
                        "longEncoding": "longs"
                    },
                    "indexSpecForIntermediatePersists": {
                        "bitmap": {
                            "type": "roaring"
                        },
                        "dimensionCompression": "lz4",
                        "stringDictionaryEncoding": {
                            "type": "utf8"
                        },
                        "metricCompression": "lz4",
                        "longEncoding": "longs"
                    },
                    "reportParseExceptions": false,
                    "handoffConditionTimeout": 0,
                    "resetOffsetAutomatically": false,
                    "segmentWriteOutMediumFactory": null,
                    "workerThreads": null,
                    "chatThreads": null,
                    "chatRetries": 8,
                    "httpTimeout": "PT10S",
                    "shutdownTimeout": "PT80S",
                    "offsetFetchPeriod": "PT30S",
                    "intermediateHandoffPeriod": "P2147483647D",
                    "logParseExceptions": false,
                    "maxParseExceptions": 2147483647,
                    "maxSavedParseExceptions": 0,
                    "skipSequenceNumberAvailabilityCheck": false,
                    "repartitionTransitionDuration": "PT120S"
                }
            },
            "dataSchema": {
                "dataSource": "wikipedia_stream",
                "timestampSpec": {
                    "column": "__time",
                    "format": "iso",
                    "missingValue": null
                },
                "dimensionsSpec": {
                    "dimensions": [
                        {
                            "type": "string",
                            "name": "username",
                            "multiValueHandling": "SORTED_ARRAY",
                            "createBitmapIndex": true
                        },
                        {
                            "type": "string",
                            "name": "post_title",
                            "multiValueHandling": "SORTED_ARRAY",
                            "createBitmapIndex": true
                        },
                        {
                            "type": "long",
                            "name": "views",
                            "multiValueHandling": "SORTED_ARRAY",
                            "createBitmapIndex": false
                        },
                        {
                            "type": "long",
                            "name": "upvotes",
                            "multiValueHandling": "SORTED_ARRAY",
                            "createBitmapIndex": false
                        },
                        {
                            "type": "long",
                            "name": "comments",
                            "multiValueHandling": "SORTED_ARRAY",
                            "createBitmapIndex": false
                        },
                        {
                            "type": "string",
                            "name": "edited",
                            "multiValueHandling": "SORTED_ARRAY",
                            "createBitmapIndex": true
                        }
                    ],
                    "dimensionExclusions": [
                        "__time"
                    ],
                    "includeAllDimensions": false,
                    "useSchemaDiscovery": false
                },
                "metricsSpec": [],
                "granularitySpec": {
                    "type": "uniform",
                    "segmentGranularity": "HOUR",
                    "queryGranularity": {
                        "type": "none"
                    },
                    "rollup": false,
                    "intervals": []
                },
                "transformSpec": {
                    "filter": null,
                    "transforms": []
                }
            },
            "tuningConfig": {
                "type": "kafka",
                "appendableIndexSpec": {
                    "type": "onheap",
                    "preserveExistingMetrics": false
                },
                "maxRowsInMemory": 150000,
                "maxBytesInMemory": 0,
                "skipBytesInMemoryOverheadCheck": false,
                "maxRowsPerSegment": 5000000,
                "maxTotalRows": null,
                "intermediatePersistPeriod": "PT10M",
                "maxPendingPersists": 0,
                "indexSpec": {
                    "bitmap": {
                        "type": "roaring"
                    },
                    "dimensionCompression": "lz4",
                    "stringDictionaryEncoding": {
                        "type": "utf8"
                    },
                    "metricCompression": "lz4",
                    "longEncoding": "longs"
                },
                "indexSpecForIntermediatePersists": {
                    "bitmap": {
                        "type": "roaring"
                    },
                    "dimensionCompression": "lz4",
                    "stringDictionaryEncoding": {
                        "type": "utf8"
                    },
                    "metricCompression": "lz4",
                    "longEncoding": "longs"
                },
                "reportParseExceptions": false,
                "handoffConditionTimeout": 0,
                "resetOffsetAutomatically": false,
                "segmentWriteOutMediumFactory": null,
                "workerThreads": null,
                "chatThreads": null,
                "chatRetries": 8,
                "httpTimeout": "PT10S",
                "shutdownTimeout": "PT80S",
                "offsetFetchPeriod": "PT30S",
                "intermediateHandoffPeriod": "P2147483647D",
                "logParseExceptions": false,
                "maxParseExceptions": 2147483647,
                "maxSavedParseExceptions": 0,
                "skipSequenceNumberAvailabilityCheck": false,
                "repartitionTransitionDuration": "PT120S"
            },
            "ioConfig": {
                "topic": "social_media",
                "inputFormat": {
                    "type": "json",
                    "keepNullColumns": false,
                    "assumeNewlineDelimited": false,
                    "useJsonNodeReader": false
                },
                "replicas": 1,
                "taskCount": 1,
                "taskDuration": "PT3600S",
                "consumerProperties": {
                    "bootstrap.servers": "localhost:9042"
                },
                "autoScalerConfig": null,
                "pollTimeout": 100,
                "startDelay": "PT5S",
                "period": "PT30S",
                "useEarliestOffset": true,
                "completionTimeout": "PT1800S",
                "lateMessageRejectionPeriod": null,
                "earlyMessageRejectionPeriod": null,
                "lateMessageRejectionStartDateTime": null,
                "configOverrides": null,
                "idleConfig": null,
                "stream": "social_media",
                "useEarliestSequenceNumber": true
            },
            "context": null,
            "suspended": false
        },
        "version": "2023-07-05T20:59:16.872Z"
    }
]
  ```
</details>

## Managing Supervisors

### Create a new Supervisor or update an existing Supervisor

#### URL

<code className="postAPI">POST</code> `/druid/indexer/v1/supervisor`

Creates a new Supervisor or updates an existing one for the same datasource with a new schema and configuration. 

You can create a Supervisor spec using [Apache Kafka ingestion streaming](../development/extensions-core/kafka-ingestion.md#define-a-supervisor-spec) or [Amazon Kinesis ingestion](../development/extensions-core/kinesis-ingestion.md#submitting-a-supervisor-spec). Once created, a Supervisor will persist in the metadata database.

Using this endpoint on an existing Supervisor for the same datasource will result in the running Supervisor signaling its tasks to stop reading and begin publishing, exiting itself, and a new Supervisor being created with the provided configuration from the request body. This submits a new schema while retaining existing publishing tasks and starting new tasks at the previous task offsets. 

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully created a new Supervisor or updated an existing Supervisor* 
<!--415 UNSUPPORTED MEDIA TYPE-->
<br/>
*Request body content type is not in JSON format* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->
```shell
curl "http://<ROUTER_IP>:<ROUTER_PORT>/druid/indexer/v1/supervisor" \
--header "Content-Type: application/json" \
--data "{
    \"type\": \"kafka\",
    \"spec\": {
        \"ioConfig\": {
            \"type\": \"kafka\",
            \"consumerProperties\": {
                \"bootstrap.servers\": \"localhost:9094\"
            },
            \"topic\": \"social_media\",
            \"inputFormat\": {
                \"type\": \"json\"
            },
            \"useEarliestOffset\": true
        },
        \"tuningConfig\": {
            \"type\": \"kafka\"
        },
        \"dataSchema\": {
            \"dataSource\": \"social_media\",
            \"timestampSpec\": {
                \"column\": \"__time\",
                \"format\": \"iso\"
            },
            \"dimensionsSpec\": {
                \"dimensions\": [
                    \"username\",
                    \"post_title\",
                    {
                        \"type\": \"long\",
                        \"name\": \"views\"
                    },
                    {
                        \"type\": \"long\",
                        \"name\": \"upvotes\"
                    },
                    {
                        \"type\": \"long\",
                        \"name\": \"comments\"
                    },
                    \"edited\"
                ]
            },
            \"granularitySpec\": {
                \"queryGranularity\": \"none\",
                \"rollup\": false,
                \"segmentGranularity\": \"hour\"
            }
        }
    }
}"
```
<!--HTTP-->
```HTTP
POST /druid/indexer/v1/supervisor HTTP/1.1
Host: http://<ROUTER_IP>:<ROUTER_PORT>
Content-Type: application/json
Content-Length: 1359

{
    "type": "kafka",
    "spec": {
        "ioConfig": {
            "type": "kafka",
            "consumerProperties": {
                "bootstrap.servers": "localhost:9094"
            },
            "topic": "social_media",
            "inputFormat": {
                "type": "json"
            },
            "useEarliestOffset": true
        },
        "tuningConfig": {
            "type": "kafka"
        },
        "dataSchema": {
            "dataSource": "social_media",
            "timestampSpec": {
                "column": "__time",
                "format": "iso"
            },
            "dimensionsSpec": {
                "dimensions": [
                    "username",
                    "post_title",
                    {
                        "type": "long",
                        "name": "views"
                    },
                    {
                        "type": "long",
                        "name": "upvotes"
                    },
                    {
                        "type": "long",
                        "name": "comments"
                    },
                    "edited"
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
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

<details>
  <summary>Click to show sample response</summary>

  ```json
{
    "id": "social_media"
}
  ```
</details>

### Suspend a running Supervisor

#### URL
<code className="postAPI">POST</code> `/druid/indexer/v1/supervisor/{supervisorId}/suspend`

Suspends the current running Supervisor of the provided ID. Returns the updated Supervisor spec where the `suspended` property is set to `true`. A suspended Supervisor will continue to emit logs and metrics.

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully shut down Supervisor* 
<!--400 BAD REQUEST-->
<br/>
*Supervisor already suspended* 
<!--404 NOT FOUND-->
<br/>
*Invalid Supervisor ID* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

The following example shows how to suspend a running Supervisor with task ID `social_media`.

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->
```shell
curl --request POST "http://<ROUTER_IP>:<ROUTER_PORT>/druid/indexer/v1/supervisor/social_media/suspend"
```
<!--HTTP-->
```HTTP
POST /druid/indexer/v1/supervisor/social_media/suspend HTTP/1.1
Host: http://<ROUTER_IP>:<ROUTER_PORT>
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

<details>
  <summary>Click to show sample response</summary>

  ```json
{
    "type": "kafka",
    "spec": {
        "dataSchema": {
            "dataSource": "social_media",
            "timestampSpec": {
                "column": "__time",
                "format": "iso",
                "missingValue": null
            },
            "dimensionsSpec": {
                "dimensions": [
                    {
                        "type": "string",
                        "name": "username",
                        "multiValueHandling": "SORTED_ARRAY",
                        "createBitmapIndex": true
                    },
                    {
                        "type": "string",
                        "name": "post_title",
                        "multiValueHandling": "SORTED_ARRAY",
                        "createBitmapIndex": true
                    },
                    {
                        "type": "long",
                        "name": "views",
                        "multiValueHandling": "SORTED_ARRAY",
                        "createBitmapIndex": false
                    },
                    {
                        "type": "long",
                        "name": "upvotes",
                        "multiValueHandling": "SORTED_ARRAY",
                        "createBitmapIndex": false
                    },
                    {
                        "type": "long",
                        "name": "comments",
                        "multiValueHandling": "SORTED_ARRAY",
                        "createBitmapIndex": false
                    },
                    {
                        "type": "string",
                        "name": "edited",
                        "multiValueHandling": "SORTED_ARRAY",
                        "createBitmapIndex": true
                    }
                ],
                "dimensionExclusions": [
                    "__time"
                ],
                "includeAllDimensions": false,
                "useSchemaDiscovery": false
            },
            "metricsSpec": [],
            "granularitySpec": {
                "type": "uniform",
                "segmentGranularity": "HOUR",
                "queryGranularity": {
                    "type": "none"
                },
                "rollup": false,
                "intervals": []
            },
            "transformSpec": {
                "filter": null,
                "transforms": []
            }
        },
        "ioConfig": {
            "topic": "social_media",
            "inputFormat": {
                "type": "json",
                "keepNullColumns": false,
                "assumeNewlineDelimited": false,
                "useJsonNodeReader": false
            },
            "replicas": 1,
            "taskCount": 1,
            "taskDuration": "PT3600S",
            "consumerProperties": {
                "bootstrap.servers": "localhost:9094"
            },
            "autoScalerConfig": null,
            "pollTimeout": 100,
            "startDelay": "PT5S",
            "period": "PT30S",
            "useEarliestOffset": true,
            "completionTimeout": "PT1800S",
            "lateMessageRejectionPeriod": null,
            "earlyMessageRejectionPeriod": null,
            "lateMessageRejectionStartDateTime": null,
            "configOverrides": null,
            "idleConfig": null,
            "stream": "social_media",
            "useEarliestSequenceNumber": true
        },
        "tuningConfig": {
            "type": "kafka",
            "appendableIndexSpec": {
                "type": "onheap",
                "preserveExistingMetrics": false
            },
            "maxRowsInMemory": 150000,
            "maxBytesInMemory": 0,
            "skipBytesInMemoryOverheadCheck": false,
            "maxRowsPerSegment": 5000000,
            "maxTotalRows": null,
            "intermediatePersistPeriod": "PT10M",
            "maxPendingPersists": 0,
            "indexSpec": {
                "bitmap": {
                    "type": "roaring"
                },
                "dimensionCompression": "lz4",
                "stringDictionaryEncoding": {
                    "type": "utf8"
                },
                "metricCompression": "lz4",
                "longEncoding": "longs"
            },
            "indexSpecForIntermediatePersists": {
                "bitmap": {
                    "type": "roaring"
                },
                "dimensionCompression": "lz4",
                "stringDictionaryEncoding": {
                    "type": "utf8"
                },
                "metricCompression": "lz4",
                "longEncoding": "longs"
            },
            "reportParseExceptions": false,
            "handoffConditionTimeout": 0,
            "resetOffsetAutomatically": false,
            "segmentWriteOutMediumFactory": null,
            "workerThreads": null,
            "chatThreads": null,
            "chatRetries": 8,
            "httpTimeout": "PT10S",
            "shutdownTimeout": "PT80S",
            "offsetFetchPeriod": "PT30S",
            "intermediateHandoffPeriod": "P2147483647D",
            "logParseExceptions": false,
            "maxParseExceptions": 2147483647,
            "maxSavedParseExceptions": 0,
            "skipSequenceNumberAvailabilityCheck": false,
            "repartitionTransitionDuration": "PT120S"
        }
    },
    "dataSchema": {
        "dataSource": "social_media",
        "timestampSpec": {
            "column": "__time",
            "format": "iso",
            "missingValue": null
        },
        "dimensionsSpec": {
            "dimensions": [
                {
                    "type": "string",
                    "name": "username",
                    "multiValueHandling": "SORTED_ARRAY",
                    "createBitmapIndex": true
                },
                {
                    "type": "string",
                    "name": "post_title",
                    "multiValueHandling": "SORTED_ARRAY",
                    "createBitmapIndex": true
                },
                {
                    "type": "long",
                    "name": "views",
                    "multiValueHandling": "SORTED_ARRAY",
                    "createBitmapIndex": false
                },
                {
                    "type": "long",
                    "name": "upvotes",
                    "multiValueHandling": "SORTED_ARRAY",
                    "createBitmapIndex": false
                },
                {
                    "type": "long",
                    "name": "comments",
                    "multiValueHandling": "SORTED_ARRAY",
                    "createBitmapIndex": false
                },
                {
                    "type": "string",
                    "name": "edited",
                    "multiValueHandling": "SORTED_ARRAY",
                    "createBitmapIndex": true
                }
            ],
            "dimensionExclusions": [
                "__time"
            ],
            "includeAllDimensions": false,
            "useSchemaDiscovery": false
        },
        "metricsSpec": [],
        "granularitySpec": {
            "type": "uniform",
            "segmentGranularity": "HOUR",
            "queryGranularity": {
                "type": "none"
            },
            "rollup": false,
            "intervals": []
        },
        "transformSpec": {
            "filter": null,
            "transforms": []
        }
    },
    "tuningConfig": {
        "type": "kafka",
        "appendableIndexSpec": {
            "type": "onheap",
            "preserveExistingMetrics": false
        },
        "maxRowsInMemory": 150000,
        "maxBytesInMemory": 0,
        "skipBytesInMemoryOverheadCheck": false,
        "maxRowsPerSegment": 5000000,
        "maxTotalRows": null,
        "intermediatePersistPeriod": "PT10M",
        "maxPendingPersists": 0,
        "indexSpec": {
            "bitmap": {
                "type": "roaring"
            },
            "dimensionCompression": "lz4",
            "stringDictionaryEncoding": {
                "type": "utf8"
            },
            "metricCompression": "lz4",
            "longEncoding": "longs"
        },
        "indexSpecForIntermediatePersists": {
            "bitmap": {
                "type": "roaring"
            },
            "dimensionCompression": "lz4",
            "stringDictionaryEncoding": {
                "type": "utf8"
            },
            "metricCompression": "lz4",
            "longEncoding": "longs"
        },
        "reportParseExceptions": false,
        "handoffConditionTimeout": 0,
        "resetOffsetAutomatically": false,
        "segmentWriteOutMediumFactory": null,
        "workerThreads": null,
        "chatThreads": null,
        "chatRetries": 8,
        "httpTimeout": "PT10S",
        "shutdownTimeout": "PT80S",
        "offsetFetchPeriod": "PT30S",
        "intermediateHandoffPeriod": "P2147483647D",
        "logParseExceptions": false,
        "maxParseExceptions": 2147483647,
        "maxSavedParseExceptions": 0,
        "skipSequenceNumberAvailabilityCheck": false,
        "repartitionTransitionDuration": "PT120S"
    },
    "ioConfig": {
        "topic": "social_media",
        "inputFormat": {
            "type": "json",
            "keepNullColumns": false,
            "assumeNewlineDelimited": false,
            "useJsonNodeReader": false
        },
        "replicas": 1,
        "taskCount": 1,
        "taskDuration": "PT3600S",
        "consumerProperties": {
            "bootstrap.servers": "localhost:9094"
        },
        "autoScalerConfig": null,
        "pollTimeout": 100,
        "startDelay": "PT5S",
        "period": "PT30S",
        "useEarliestOffset": true,
        "completionTimeout": "PT1800S",
        "lateMessageRejectionPeriod": null,
        "earlyMessageRejectionPeriod": null,
        "lateMessageRejectionStartDateTime": null,
        "configOverrides": null,
        "idleConfig": null,
        "stream": "social_media",
        "useEarliestSequenceNumber": true
    },
    "context": null,
    "suspended": true
}
  ```
</details>

### Suspend all Supervisors

#### URL
<code className="postAPI">POST</code> `/druid/indexer/v1/supervisor/suspendAll`

Suspends all Supervisors.

Note that this endpoint will return a "success" message even if there are no Supervisors or running Supervisors.

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully suspended all Supervisors* 

<!--END_DOCUSAURUS_CODE_TABS-->

---
#### Sample request

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->
```shell
curl --request POST "http://<ROUTER_IP>:<ROUTER_PORT>/druid/indexer/v1/supervisor/suspendAll"
```
<!--HTTP-->
```HTTP
POST /druid/indexer/v1/supervisor/suspendAll HTTP/1.1
Host: http://<ROUTER_IP>:<ROUTER_PORT>
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

<details>
  <summary>Click to show sample response</summary>

  ```json
{
    "status": "success"
}
  ```
</details>

### Resume a Supervisor

#### URL

<code className="postAPI">POST</code> `/druid/indexer/v1/supervisor/{supervisorId}/resume`

Resumes indexing tasks for a Supervisor. Returns an updated Supervisor spec with the `suspended` property set to `false`.

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully resumed Supervisor* 
<!--400 BAD REQUEST-->
<br/>
*Supervisor already running* 
<!--404 NOT FOUND-->
<br/>
*Invalid Supervisor ID* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

The following example resumes a previously suspended Supervisor with ID `social_media`.

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->
```shell
curl --request POST "http://<ROUTER_IP>:<ROUTER_PORT>/druid/indexer/v1/supervisor/social_media/resume"
```
<!--HTTP-->
```HTTP
POST /druid/indexer/v1/supervisor/social_media/resume HTTP/1.1
Host: http://<ROUTER_IP>:<ROUTER_PORT>
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

<details>
  <summary>Click to show sample response</summary>

  ```json
{
    "type": "kafka",
    "spec": {
        "dataSchema": {
            "dataSource": "social_media",
            "timestampSpec": {
                "column": "__time",
                "format": "iso",
                "missingValue": null
            },
            "dimensionsSpec": {
                "dimensions": [
                    {
                        "type": "string",
                        "name": "username",
                        "multiValueHandling": "SORTED_ARRAY",
                        "createBitmapIndex": true
                    },
                    {
                        "type": "string",
                        "name": "post_title",
                        "multiValueHandling": "SORTED_ARRAY",
                        "createBitmapIndex": true
                    },
                    {
                        "type": "long",
                        "name": "views",
                        "multiValueHandling": "SORTED_ARRAY",
                        "createBitmapIndex": false
                    },
                    {
                        "type": "long",
                        "name": "upvotes",
                        "multiValueHandling": "SORTED_ARRAY",
                        "createBitmapIndex": false
                    },
                    {
                        "type": "long",
                        "name": "comments",
                        "multiValueHandling": "SORTED_ARRAY",
                        "createBitmapIndex": false
                    },
                    {
                        "type": "string",
                        "name": "edited",
                        "multiValueHandling": "SORTED_ARRAY",
                        "createBitmapIndex": true
                    }
                ],
                "dimensionExclusions": [
                    "__time"
                ],
                "includeAllDimensions": false,
                "useSchemaDiscovery": false
            },
            "metricsSpec": [],
            "granularitySpec": {
                "type": "uniform",
                "segmentGranularity": "HOUR",
                "queryGranularity": {
                    "type": "none"
                },
                "rollup": false,
                "intervals": []
            },
            "transformSpec": {
                "filter": null,
                "transforms": []
            }
        },
        "ioConfig": {
            "topic": "social_media",
            "inputFormat": {
                "type": "json",
                "keepNullColumns": false,
                "assumeNewlineDelimited": false,
                "useJsonNodeReader": false
            },
            "replicas": 1,
            "taskCount": 1,
            "taskDuration": "PT3600S",
            "consumerProperties": {
                "bootstrap.servers": "localhost:9094"
            },
            "autoScalerConfig": null,
            "pollTimeout": 100,
            "startDelay": "PT5S",
            "period": "PT30S",
            "useEarliestOffset": true,
            "completionTimeout": "PT1800S",
            "lateMessageRejectionPeriod": null,
            "earlyMessageRejectionPeriod": null,
            "lateMessageRejectionStartDateTime": null,
            "configOverrides": null,
            "idleConfig": null,
            "stream": "social_media",
            "useEarliestSequenceNumber": true
        },
        "tuningConfig": {
            "type": "kafka",
            "appendableIndexSpec": {
                "type": "onheap",
                "preserveExistingMetrics": false
            },
            "maxRowsInMemory": 150000,
            "maxBytesInMemory": 0,
            "skipBytesInMemoryOverheadCheck": false,
            "maxRowsPerSegment": 5000000,
            "maxTotalRows": null,
            "intermediatePersistPeriod": "PT10M",
            "maxPendingPersists": 0,
            "indexSpec": {
                "bitmap": {
                    "type": "roaring"
                },
                "dimensionCompression": "lz4",
                "stringDictionaryEncoding": {
                    "type": "utf8"
                },
                "metricCompression": "lz4",
                "longEncoding": "longs"
            },
            "indexSpecForIntermediatePersists": {
                "bitmap": {
                    "type": "roaring"
                },
                "dimensionCompression": "lz4",
                "stringDictionaryEncoding": {
                    "type": "utf8"
                },
                "metricCompression": "lz4",
                "longEncoding": "longs"
            },
            "reportParseExceptions": false,
            "handoffConditionTimeout": 0,
            "resetOffsetAutomatically": false,
            "segmentWriteOutMediumFactory": null,
            "workerThreads": null,
            "chatThreads": null,
            "chatRetries": 8,
            "httpTimeout": "PT10S",
            "shutdownTimeout": "PT80S",
            "offsetFetchPeriod": "PT30S",
            "intermediateHandoffPeriod": "P2147483647D",
            "logParseExceptions": false,
            "maxParseExceptions": 2147483647,
            "maxSavedParseExceptions": 0,
            "skipSequenceNumberAvailabilityCheck": false,
            "repartitionTransitionDuration": "PT120S"
        }
    },
    "dataSchema": {
        "dataSource": "social_media",
        "timestampSpec": {
            "column": "__time",
            "format": "iso",
            "missingValue": null
        },
        "dimensionsSpec": {
            "dimensions": [
                {
                    "type": "string",
                    "name": "username",
                    "multiValueHandling": "SORTED_ARRAY",
                    "createBitmapIndex": true
                },
                {
                    "type": "string",
                    "name": "post_title",
                    "multiValueHandling": "SORTED_ARRAY",
                    "createBitmapIndex": true
                },
                {
                    "type": "long",
                    "name": "views",
                    "multiValueHandling": "SORTED_ARRAY",
                    "createBitmapIndex": false
                },
                {
                    "type": "long",
                    "name": "upvotes",
                    "multiValueHandling": "SORTED_ARRAY",
                    "createBitmapIndex": false
                },
                {
                    "type": "long",
                    "name": "comments",
                    "multiValueHandling": "SORTED_ARRAY",
                    "createBitmapIndex": false
                },
                {
                    "type": "string",
                    "name": "edited",
                    "multiValueHandling": "SORTED_ARRAY",
                    "createBitmapIndex": true
                }
            ],
            "dimensionExclusions": [
                "__time"
            ],
            "includeAllDimensions": false,
            "useSchemaDiscovery": false
        },
        "metricsSpec": [],
        "granularitySpec": {
            "type": "uniform",
            "segmentGranularity": "HOUR",
            "queryGranularity": {
                "type": "none"
            },
            "rollup": false,
            "intervals": []
        },
        "transformSpec": {
            "filter": null,
            "transforms": []
        }
    },
    "tuningConfig": {
        "type": "kafka",
        "appendableIndexSpec": {
            "type": "onheap",
            "preserveExistingMetrics": false
        },
        "maxRowsInMemory": 150000,
        "maxBytesInMemory": 0,
        "skipBytesInMemoryOverheadCheck": false,
        "maxRowsPerSegment": 5000000,
        "maxTotalRows": null,
        "intermediatePersistPeriod": "PT10M",
        "maxPendingPersists": 0,
        "indexSpec": {
            "bitmap": {
                "type": "roaring"
            },
            "dimensionCompression": "lz4",
            "stringDictionaryEncoding": {
                "type": "utf8"
            },
            "metricCompression": "lz4",
            "longEncoding": "longs"
        },
        "indexSpecForIntermediatePersists": {
            "bitmap": {
                "type": "roaring"
            },
            "dimensionCompression": "lz4",
            "stringDictionaryEncoding": {
                "type": "utf8"
            },
            "metricCompression": "lz4",
            "longEncoding": "longs"
        },
        "reportParseExceptions": false,
        "handoffConditionTimeout": 0,
        "resetOffsetAutomatically": false,
        "segmentWriteOutMediumFactory": null,
        "workerThreads": null,
        "chatThreads": null,
        "chatRetries": 8,
        "httpTimeout": "PT10S",
        "shutdownTimeout": "PT80S",
        "offsetFetchPeriod": "PT30S",
        "intermediateHandoffPeriod": "P2147483647D",
        "logParseExceptions": false,
        "maxParseExceptions": 2147483647,
        "maxSavedParseExceptions": 0,
        "skipSequenceNumberAvailabilityCheck": false,
        "repartitionTransitionDuration": "PT120S"
    },
    "ioConfig": {
        "topic": "social_media",
        "inputFormat": {
            "type": "json",
            "keepNullColumns": false,
            "assumeNewlineDelimited": false,
            "useJsonNodeReader": false
        },
        "replicas": 1,
        "taskCount": 1,
        "taskDuration": "PT3600S",
        "consumerProperties": {
            "bootstrap.servers": "localhost:9094"
        },
        "autoScalerConfig": null,
        "pollTimeout": 100,
        "startDelay": "PT5S",
        "period": "PT30S",
        "useEarliestOffset": true,
        "completionTimeout": "PT1800S",
        "lateMessageRejectionPeriod": null,
        "earlyMessageRejectionPeriod": null,
        "lateMessageRejectionStartDateTime": null,
        "configOverrides": null,
        "idleConfig": null,
        "stream": "social_media",
        "useEarliestSequenceNumber": true
    },
    "context": null,
    "suspended": false
}
  ```
</details>

### Resume all Supervisors

#### URL
<code className="postAPI">POST</code> `/druid/indexer/v1/supervisor/resumeAll`

Resumes all Supervisors. 

Note that this endpoint will return a "success" message even if there are no Supervisors or suspended Supervisors.

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully resumed all Supervisors* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->
```shell
curl --request POST "http://<ROUTER_IP>:<ROUTER_PORT>/druid/indexer/v1/supervisor/resumeAll"
```
<!--HTTP-->
```HTTP
POST /druid/indexer/v1/supervisor/resumeAll HTTP/1.1
Host: http://<ROUTER_IP>:<ROUTER_PORT>
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

<details>
  <summary>Click to show sample response</summary>

  ```json
{
    "status": "success"
}
  ```
</details>

### Reset a Supervisor

#### URL
<code className="postAPI">POST</code> `/druid/indexer/v1/supervisor/{supervisorId}/reset`

Resets the specified supervisor. This endpoint clears stored offsets, causing the Supervisor to start reading offsets from either the earliest or the latest offsets in Kafka. It kills and recreates active tasks to read from valid offsets. 

Use this endpoint for recovering when the Supervisor stops due to missing offsets. Use this endpoint with caution as it may result in skipped, resulting in data loss, or duplicate data. 

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully reset Supervisor* 
<!--404 NOT FOUND-->
<br/>
*Invalid Supervisor ID* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->
```shell
curl --request POST "http://<ROUTER_IP>:<ROUTER_PORT>/druid/indexer/v1/supervisor/social_media/reset"
```
<!--HTTP-->
```HTTP
POST /druid/indexer/v1/supervisor/social_media/reset HTTP/1.1
Host: http://<ROUTER_IP>:<ROUTER_PORT>
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

<details>
  <summary>Click to show sample response</summary>

  ```json
{
    "id": "social_media"
}
  ```
</details>

### Terminate a Supervisor

#### URL 
<code className="postAPI">POST</code> `/druid/indexer/v1/supervisor/{supervisorId}/terminate`

Terminate a Supervisor of the provided ID and its associated indexing tasks, triggering the publishing of their segments. When terminated, a tombstone marker will be placed in the database to prevent reloading on restart. 

The terminated Supervisor will still exist in the metadata store and its history can be retrieved.

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully terminated a Supervisor* 
<!--404 NOT FOUND-->
<br/>
*Invalid Supervisor ID or Supervisor not running* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->
```shell
curl --request POST "http://<ROUTER_IP>:<ROUTER_PORT>/druid/indexer/v1/supervisor/social_media/terminate"
```
<!--HTTP-->
```HTTP
POST /druid/indexer/v1/supervisor/social_media/terminate HTTP/1.1
Host: http://<ROUTER_IP>:<ROUTER_PORT>
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

<details>
  <summary>Click to show sample response</summary>

  ```json
{
    "id": "social_media"
}
  ```
</details>

### Terminate all Supervisors

#### URL

<code className="postAPI">POST</code> `/druid/indexer/v1/supervisor/terminateAll`

Terminate all Supervisors. Terminated Supervisors will still exist in the metadata store and their history can be retrieved.

Note that this endpoint will return a "success" message even if there are no Supervisors or running Supervisors to terminate.

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully terminated all Supervisors* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->
```shell
curl --request POST "http://<ROUTER_IP>:<ROUTER_PORT>/druid/indexer/v1/supervisor/terminateAll"
```
<!--HTTP-->
```HTTP
POST /druid/indexer/v1/supervisor/terminateAll HTTP/1.1
Host: http://<ROUTER_IP>:<ROUTER_PORT>
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

<details>
  <summary>Click to show sample response</summary>

  ```json
{
    "status": "success"
}
  ```
</details>

### Shut down a Supervisor

#### URL

<code className="postAPI">POST</code> `/druid/indexer/v1/supervisor/{supervisorId}/shutdown`

Shuts down a supervisor. This API is depreciated and will be removed in future releases. Use the equivalent [terminate](#terminate-a-supervisor) endpoint instead. 