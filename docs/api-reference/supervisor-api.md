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

This document describes the API endpoints to manage and monitor supervisors for Apache Druid.

## Supervisors

### Get a list of active supervisors

#### URL

<code className="getAPI">GET</code> `/druid/indexer/v1/supervisor`

Retrieves a list of active supervisors. The response object is a list of strings containing the names of the supervisors. 

This endpoint can be queried with the Coordinator service or the Router service.

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved list of active supervisor IDs* 

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

### Get list of active supervisor objects

#### URL

`GET /druid/indexer/v1/supervisor?full`

Retrieves a list of active supervisor objects. Each object has properties relevant to the supervisor: `id`, `state`, `detailedState`, `healthy`, and `spec`. 

This endpoint can be queried with the Coordinator service or the Router service.

See the following table for details on properties within the response object:

|Field|Type|Description|
|---|---|---|
|`id`|String|Supervisor unique identifier|
|`state`|String|The basic state of the supervisor. Available states:`UNHEALTHY_SUPERVISOR`, `UNHEALTHY_TASKS`, `PENDING`, `RUNNING`, `SUSPENDED`, `STOPPING`. Check [Kafka Docs](../development/extensions-core/kafka-supervisor-operations.md) for details.|
|`detailedState`|String|The supervisor specific state. See documentation of specific supervisor for details: [Kafka](../development/extensions-core/kafka-ingestion.md) or [Kinesis](../development/extensions-core/kinesis-ingestion.md)|
|`healthy`|Boolean|True or false indicator of overall supervisor health|
|`spec`|SupervisorSpec|JSON specification of the supervisor|

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved active supervisor objects* 

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

### Get supervisor state 

`GET /druid/indexer/v1/supervisor?state=true`

Retrieves a list of objects of the currently active supervisors and their current state.

This endpoint can be queried with the Coordinator service or the Router service.

See the following table for details on properties within the response object:
|Field|Type|Description|
|---|---|---|
|`id`|String|Supervisor unique identifier|
|`state`|String|The basic state of the supervisor. Available states: `UNHEALTHY_SUPERVISOR`, `UNHEALTHY_TASKS`, `PENDING`, `RUNNING`, `SUSPENDED`, `STOPPING`. Check [Kafka Docs](../development/extensions-core/kafka-supervisor-operations.md) for details.|
|`detailedState`|String|Supervisor specific state. See documentation of the specific supervisor for details: [Kafka](../development/extensions-core/kafka-ingestion.md) or [Kinesis](../development/extensions-core/kinesis-ingestion.md)|
|`healthy`|Boolean|True or false indicator of overall supervisor health|
|`suspended`|Boolean|True or false indicator of whether the supervisor is in suspended state|

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved supervisor state objects*  

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

### Get supervisor specification

#### URL

`GET /druid/indexer/v1/supervisor/{supervisorId}`

Retrieves the current spec for the supervisor with the provided ID.

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved specification* 
<!--404 NOT FOUND-->
<br/>
*Invalid supervisor ID* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

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

### Get supervisor status

#### URL
`GET /druid/indexer/v1/supervisor/{supervisorId}/status`

Retrieves the current status of the supervisor with the provided ID. 

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved supervisor status* 
<!--404 NOT FOUND-->
<br/>
*Invalid supervisor ID* 
<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

The following example shows retrieving the status of the `social_media` supervisor. 

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

### Get audit history for all supervisors

#### URL

`GET /druid/indexer/v1/supervisor/history`

Retrieve an audit history of specs for all supervisors (current and past).

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

### Get audit history for specific supervisor

#### URL

`GET /druid/indexer/v1/supervisor/{supervisorId}/history`

Retrieve an audit history of specs for the supervisor with the provided ID.

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved audit history for supervisor* 
<!--404 NOT FOUND-->
<br/>
*Invalid supervisor ID* 
<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

The following example shows how to retrieve the audit history of the `wikipedia_stream` supervisor.

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


### Create a new supervisor or update an existing supervisor

#### URL

`POST /druid/indexer/v1/supervisor`

Creates a new supervisor or updates an existing one. 

To create a supervisor spec using Kafka streaming ingestion, refer to [Apache Kafka ingestion](../development/extensions-core/kafka-ingestion.md#define-a-supervisor-spec).

To create a supervisor spec using Kinesis ingestion, refer to [Amazon Kinesis ingestion](../development/extensions-core/kinesis-ingestion.md#submitting-a-supervisor-spec). 

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully created a new supervisor or updated an existing supervisor* 
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

### Suspend a running supervisor

#### URL
`POST /druid/indexer/v1/supervisor/{supervisorId}/suspend`

Suspend the current running supervisor of the provided ID. Responds with updated SupervisorSpec.

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully shut down supervisor* 
<!--404 NOT FOUND-->
<br/>
*Invalid supervisor ID* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

The following example shows how to suspend a running supervisor with task ID `social_media`.

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

`POST /druid/indexer/v1/supervisor/suspendAll`

Suspend all supervisors at once.

`POST /druid/indexer/v1/supervisor/<supervisorId>/resume`

Resume indexing tasks for a supervisor. Responds with updated SupervisorSpec.

`POST /druid/indexer/v1/supervisor/resumeAll`

Resume all supervisors at once.

`POST /druid/indexer/v1/supervisor/<supervisorId>/reset`

Reset the specified supervisor.

`POST /druid/indexer/v1/supervisor/<supervisorId>/terminate`

Terminate a supervisor of the provided ID.

`POST /druid/indexer/v1/supervisor/terminateAll`

Terminate all supervisors at once.

`POST /druid/indexer/v1/supervisor/<supervisorId>/shutdown`

> This API is deprecated and will be removed in future releases.
> Please use the equivalent `terminate` instead.

Shutdown a supervisor.