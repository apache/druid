---
id: supervisor-api
title: Supervisor API
sidebar_label: Supervisors
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';


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

This topic describes the API endpoints to manage and monitor supervisors for Apache Druid.

In this topic, `http://ROUTER_IP:ROUTER_PORT` is a placeholder for your Router service address and port. Replace it with the information for your deployment. For example, use `http://localhost:8888` for quickstart deployments.

## Supervisor information

The following table lists the properties of a supervisor object:

|Property|Type|Description|
|---|---|---|
|`id`|String|Unique identifier.|
|`state`|String|Generic state of the supervisor. Available states:`UNHEALTHY_SUPERVISOR`, `UNHEALTHY_TASKS`, `PENDING`, `RUNNING`, `SUSPENDED`, `STOPPING`. See [Apache Kafka operations](../development/extensions-core/kafka-supervisor-operations.md) for details.|
|`detailedState`|String|Detailed state of the supervisor. This property contains a more descriptive, implementation-specific state that may provide more insight into the supervisor's activities than the `state` property. See [Apache Kafka ingestion](../development/extensions-core/kafka-ingestion.md) and [Amazon Kinesis ingestion](../development/extensions-core/kinesis-ingestion.md) for supervisor-specific states.|
|`healthy`|Boolean|Supervisor health indicator.|
|`spec`|Object|Container object for the supervisor configuration.|
|`suspended`|Boolean|Indicates whether the supervisor is in a suspended state.|

### Get an array of active supervisor IDs

Returns an array of strings representing the names of active supervisors. If there are no active supervisors, it returns an empty array.

#### URL

<code class="getAPI">GET</code> <code>/druid/indexer/v1/supervisor</code>

#### Responses

<Tabs>

<TabItem value="1" label="200 SUCCESS">


*Successfully retrieved array of active supervisor IDs*

</TabItem>
</Tabs>

---

#### Sample request

<Tabs>

<TabItem value="2" label="cURL">


```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/supervisor"
```

</TabItem>
<TabItem value="3" label="HTTP">


```HTTP
GET /druid/indexer/v1/supervisor HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

</TabItem>
</Tabs>

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

### Get an array of active supervisor objects

Retrieves an array of active supervisor objects. If there are no active supervisors, it returns an empty array. For reference on the supervisor object properties, see the preceding [table](#supervisor-information).

#### URL

<code class="getAPI">GET</code> <code>/druid/indexer/v1/supervisor?full</code>

#### Responses

<Tabs>

<TabItem value="4" label="200 SUCCESS">


*Successfully retrieved supervisor objects*

</TabItem>
</Tabs>

---

#### Sample request

<Tabs>

<TabItem value="5" label="cURL">


```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/supervisor?full=null"
```

</TabItem>
<TabItem value="6" label="HTTP">


```HTTP
GET /druid/indexer/v1/supervisor?full=null HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

</TabItem>
</Tabs>

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

### Get an array of supervisor states

Retrieves an array of objects representing active supervisors and their current state. If there are no active supervisors, it returns an empty array. For reference on the supervisor object properties, see the preceding [table](#supervisor-information).

#### URL

<code class="getAPI">GET</code> <code>/druid/indexer/v1/supervisor?state=true</code>

#### Responses

<Tabs>

<TabItem value="7" label="200 SUCCESS">


*Successfully retrieved supervisor state objects*

</TabItem>
</Tabs>

---

#### Sample request

<Tabs>

<TabItem value="8" label="cURL">


```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/supervisor?state=true"
```

</TabItem>
<TabItem value="9" label="HTTP">


```HTTP
GET /druid/indexer/v1/supervisor?state=true HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

</TabItem>
</Tabs>

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

Retrieves the specification for a single supervisor. The returned specification includes the `dataSchema`, `ioConfig`, and `tuningConfig` objects.

#### URL

<code class="getAPI">GET</code> <code>/druid/indexer/v1/supervisor/:supervisorId</code>

#### Responses

<Tabs>

<TabItem value="10" label="200 SUCCESS">


*Successfully retrieved supervisor spec*

</TabItem>
<TabItem value="11" label="404 NOT FOUND">


*Invalid supervisor ID*

</TabItem>
</Tabs>

---

#### Sample request

The following example shows how to retrieve the specification of a supervisor with the name `wikipedia_stream`.

<Tabs>

<TabItem value="12" label="cURL">


```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/supervisor/wikipedia_stream"
```

</TabItem>
<TabItem value="13" label="HTTP">


```HTTP
GET /druid/indexer/v1/supervisor/wikipedia_stream HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

</TabItem>
</Tabs>


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

Retrieves the current status report for a single supervisor. The report contains the state of the supervisor tasks and an array of recently thrown exceptions.

For additional information about the status report, see the topic for each streaming ingestion methods:
* [Amazon Kinesis](../development/extensions-core/kinesis-ingestion.md#get-supervisor-status-report)
* [Apache Kafka](../development/extensions-core/kafka-supervisor-operations.md#getting-supervisor-status-report)

#### URL

<code class="getAPI">GET</code> <code>/druid/indexer/v1/supervisor/:supervisorId/status</code>

#### Responses

<Tabs>

<TabItem value="14" label="200 SUCCESS">


*Successfully retrieved supervisor status*

</TabItem>
<TabItem value="15" label="404 NOT FOUND">


*Invalid supervisor ID*

</TabItem>
</Tabs>

---

#### Sample request

The following example shows how to retrieve the status of a supervisor with the name `social_media`.

<Tabs>

<TabItem value="16" label="cURL">


```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/supervisor/social_media/status"
```

</TabItem>
<TabItem value="17" label="HTTP">


```HTTP
GET /druid/indexer/v1/supervisor/social_media/status HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

</TabItem>
</Tabs>

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

An audit history provides a comprehensive log of events, including supervisor configuration, creation, suspension, and modification history.

### Get audit history for all supervisors

Retrieve an audit history of specs for all supervisors.

#### URL

<code class="getAPI">GET</code> <code>/druid/indexer/v1/supervisor/history</code>

#### Responses

<Tabs>

<TabItem value="18" label="200 SUCCESS">


*Successfully retrieved audit history*

</TabItem>
</Tabs>

---

#### Sample request

<Tabs>

<TabItem value="19" label="cURL">


```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/supervisor/history"
```

</TabItem>
<TabItem value="20" label="HTTP">


```HTTP
GET /druid/indexer/v1/supervisor/history HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

</TabItem>
</Tabs>

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

### Get audit history for a specific supervisor

Retrieves an audit history of specs for a single supervisor.

#### URL

<code class="getAPI">GET</code> <code>/druid/indexer/v1/supervisor/:supervisorId/history</code>


#### Responses

<Tabs>

<TabItem value="21" label="200 SUCCESS">


*Successfully retrieved supervisor audit history*

</TabItem>
<TabItem value="22" label="404 NOT FOUND">


*Invalid supervisor ID*

</TabItem>
</Tabs>

---

#### Sample request

The following example shows how to retrieve the audit history of a supervisor with the name `wikipedia_stream`.

<Tabs>

<TabItem value="23" label="cURL">


```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/supervisor/wikipedia_stream/history"
```

</TabItem>
<TabItem value="24" label="HTTP">


```HTTP
GET /druid/indexer/v1/supervisor/wikipedia_stream/history HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

</TabItem>
</Tabs>

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

## Manage supervisors

### Create or update a supervisor

Creates a new supervisor or updates an existing one for the same datasource with a new schema and configuration.

You can define a supervisor spec for [Apache Kafka](../development/extensions-core/kafka-ingestion.md#define-a-supervisor-spec) or [Amazon Kinesis](../development/extensions-core/kinesis-ingestion.md#supervisor-spec) streaming ingestion methods. Once created, the supervisor persists in the metadata database.

When you call this endpoint on an existing supervisor for the same datasource, the running supervisor signals its tasks to stop reading and begin publishing, exiting itself. Druid then uses the provided configuration from the request body to create a new supervisor. Druid submits a new schema while retaining existing publishing tasks and starts new tasks at the previous task offsets.

#### URL

<code class="postAPI">POST</code> <code>/druid/indexer/v1/supervisor</code>

#### Responses

<Tabs>

<TabItem value="25" label="200 SUCCESS">


*Successfully created a new supervisor or updated an existing supervisor*

</TabItem>
<TabItem value="26" label="415 UNSUPPORTED MEDIA TYPE">


*Request body content type is not in JSON format*

</TabItem>
</Tabs>

---

#### Sample request

The following example uses JSON input format to create a supervisor spec for Kafka with a `social_media` datasource and `social_media` topic.

<Tabs>

<TabItem value="27" label="cURL">


```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/supervisor" \
--header 'Content-Type: application/json' \
--data '{
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
}'
```

</TabItem>
<TabItem value="28" label="HTTP">


```HTTP
POST /druid/indexer/v1/supervisor HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
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

</TabItem>
</Tabs>

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

Suspends a single running supervisor. Returns the updated supervisor spec, where the `suspended` property is set to `true`. The suspended supervisor continues to emit logs and metrics.

#### URL
<code class="postAPI">POST</code> <code>/druid/indexer/v1/supervisor/:supervisorId/suspend</code>

#### Responses

<Tabs>

<TabItem value="29" label="200 SUCCESS">


*Successfully shut down supervisor*

</TabItem>
<TabItem value="30" label="400 BAD REQUEST">


*Supervisor already suspended*

</TabItem>
<TabItem value="31" label="404 NOT FOUND">


*Invalid supervisor ID*

</TabItem>
</Tabs>

---

#### Sample request

The following example shows how to suspend a running supervisor with the name `social_media`.

<Tabs>

<TabItem value="32" label="cURL">


```shell
curl --request POST "http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/supervisor/social_media/suspend"
```

</TabItem>
<TabItem value="33" label="HTTP">


```HTTP
POST /druid/indexer/v1/supervisor/social_media/suspend HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

</TabItem>
</Tabs>

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

### Suspend all supervisors

Suspends all supervisors. Note that this endpoint returns an HTTP `200 Success` code message even if there are no supervisors or running supervisors to suspend.

#### URL
<code class="postAPI">POST</code> <code>/druid/indexer/v1/supervisor/suspendAll</code>

#### Responses

<Tabs>

<TabItem value="34" label="200 SUCCESS">


*Successfully suspended all supervisors*

</TabItem>
</Tabs>

---

#### Sample request

<Tabs>

<TabItem value="35" label="cURL">


```shell
curl --request POST "http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/supervisor/suspendAll"
```

</TabItem>
<TabItem value="36" label="HTTP">


```HTTP
POST /druid/indexer/v1/supervisor/suspendAll HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

</TabItem>
</Tabs>

#### Sample response

<details>
  <summary>Click to show sample response</summary>

  ```json
{
    "status": "success"
}
  ```
</details>

### Resume a supervisor

Resumes indexing tasks for a supervisor. Returns an updated supervisor spec with the `suspended` property set to `false`.

#### URL

<code class="postAPI">POST</code> <code>/druid/indexer/v1/supervisor/:supervisorId/resume</code>

#### Responses

<Tabs>

<TabItem value="37" label="200 SUCCESS">


*Successfully resumed supervisor*

</TabItem>
<TabItem value="38" label="400 BAD REQUEST">


*Supervisor already running*

</TabItem>
<TabItem value="39" label="404 NOT FOUND">


*Invalid supervisor ID*

</TabItem>
</Tabs>

---

#### Sample request

The following example resumes a previously suspended supervisor with name `social_media`.

<Tabs>

<TabItem value="40" label="cURL">


```shell
curl --request POST "http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/supervisor/social_media/resume"
```

</TabItem>
<TabItem value="41" label="HTTP">


```HTTP
POST /druid/indexer/v1/supervisor/social_media/resume HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

</TabItem>
</Tabs>

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

### Resume all supervisors

Resumes all supervisors. Note that this endpoint returns an HTTP `200 Success` code even if there are no supervisors or suspended supervisors to resume.

#### URL

<code class="postAPI">POST</code> <code>/druid/indexer/v1/supervisor/resumeAll</code>

#### Responses

<Tabs>

<TabItem value="42" label="200 SUCCESS">


*Successfully resumed all supervisors*

</TabItem>
</Tabs>

---

#### Sample request

<Tabs>

<TabItem value="43" label="cURL">


```shell
curl --request POST "http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/supervisor/resumeAll"
```

</TabItem>
<TabItem value="44" label="HTTP">


```HTTP
POST /druid/indexer/v1/supervisor/resumeAll HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

</TabItem>
</Tabs>

#### Sample response

<details>
  <summary>Click to show sample response</summary>

  ```json
{
    "status": "success"
}
  ```
</details>

### Reset a supervisor

Resets the specified supervisor. This endpoint clears _all_ stored offsets in Kafka or sequence numbers in Kinesis, prompting the supervisor to resume data reading. The supervisor will start from the earliest or latest available position, depending on the platform (offsets in Kafka or sequence numbers in Kinesis). It kills and recreates active tasks to read from valid positions.

Use this endpoint to recover from a stopped state due to missing offsets in Kafka or sequence numbers in Kinesis. Use this endpoint with caution as it may result in skipped messages and lead to data loss or duplicate data.

#### URL

<code class="postAPI">POST</code> <code>/druid/indexer/v1/supervisor/:supervisorId/reset</code>

#### Responses

<Tabs>

<TabItem value="45" label="200 SUCCESS">


*Successfully reset supervisor*

</TabItem>
<TabItem value="46" label="404 NOT FOUND">


*Invalid supervisor ID*

</TabItem>
</Tabs>

---

#### Sample request

The following example shows how to reset a supervisor with the name `social_media`.

<Tabs>

<TabItem value="47" label="cURL">


```shell
curl --request POST "http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/supervisor/social_media/reset"
```

</TabItem>
<TabItem value="48" label="HTTP">


```HTTP
POST /druid/indexer/v1/supervisor/social_media/reset HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

</TabItem>
</Tabs>

#### Sample response

<details>
  <summary>Click to show sample response</summary>

  ```json
{
    "id": "social_media"
}
  ```
</details>

### Reset Offsets for a supervisor

Resets the specified offsets for a supervisor. This endpoint clears _only_ the specified offsets in Kafka or sequence numbers in Kinesis, prompting the supervisor to resume data reading.
If there are no stored offsets, the specified offsets will be set in the metadata store. The supervisor will start from the reset offsets for the partitions specified and for the other partitions from the stored offset.
It kills and recreates active tasks pertaining to the partitions specified to read from valid offsets.

Use this endpoint to selectively reset offsets for partitions without resetting the entire set.

#### URL

<code class="postAPI">POST</code> <code>/druid/indexer/v1/supervisor/:supervisorId/resetOffsets</code>

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->

*Successfully reset offsets*

<!--404 NOT FOUND-->

*Invalid supervisor ID*

<!--END_DOCUSAURUS_CODE_TABS-->

---
#### Reset Offsets Metadata

This section presents the structure and details of the reset offsets metadata payload.

| Field | Type | Description | Required |
|---------|---------|---------|---------|
| `type` | String | The type of reset offsets metadata payload. It must match the supervisor's `type`. Possible values: `kafka` or `kinesis`. | Yes |
| `partitions` | Object | An object representing the reset metadata. See below for details. | Yes |

#### Partitions

The following table defines the fields within the `partitions` object in the reset offsets metadata payload.

| Field | Type | Description | Required |
|---------|---------|---------|---------|
| `type` | String | Must be set as `end`.  Indicates the end sequence numbers for the reset offsets. | Yes |
| `stream` | String | The stream to be reset. It must be a valid stream consumed by the supervisor. | Yes |
| `partitionOffsetMap` | Object | A map of partitions to corresponding offsets for the stream to be reset.| Yes |

#### Sample request

The following example shows how to reset offsets for a kafka supervisor with the name `social_media`. Let's say the supervisor is reading
from a kafka topic `ads_media_stream` and has the stored offsets: `{"0": 0, "1": 10, "2": 20, "3": 40}`.

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->

```shell
curl --request POST "http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/supervisor/social_media/resetOffsets"
--header 'Content-Type: application/json'
--data-raw '{"type":"kafka","partitions":{"type":"end","stream":"ads_media_stream","partitionOffsetMap":{"0":100, "2": 650}}}'
```

<!--HTTP-->

```HTTP
POST /druid/indexer/v1/supervisor/social_media/resetOffsets HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
Content-Type: application/json

{
  "type": "kafka",
  "partitions": {
    "type": "end",
    "stream": "ads_media_stream",
    "partitionOffsetMap": {
      "0": 100,
      "2": 650
    }
  }
}
```

The above operation will reset offsets only for partitions 0 and 2 to 100 and 650 respectively. After a successful reset,
when the supervisor's tasks restart, they will resume reading from `{"0": 100, "1": 10, "2": 650, "3": 40}`.

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

### Terminate a supervisor

Terminates a supervisor and its associated indexing tasks, triggering the publishing of their segments. When terminated, a tombstone marker is placed in the database to prevent reloading on restart.

The terminated supervisor still exists in the metadata store and its history can be retrieved.

#### URL

<code class="postAPI">POST</code> <code>/druid/indexer/v1/supervisor/:supervisorId/terminate</code>

#### Responses

<Tabs>

<TabItem value="49" label="200 SUCCESS">


*Successfully terminated a supervisor*

</TabItem>
<TabItem value="50" label="404 NOT FOUND">


*Invalid supervisor ID or supervisor not running*

</TabItem>
</Tabs>

---

#### Sample request

<Tabs>

<TabItem value="51" label="cURL">


```shell
curl --request POST "http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/supervisor/social_media/terminate"
```

</TabItem>
<TabItem value="52" label="HTTP">


```HTTP
POST /druid/indexer/v1/supervisor/social_media/terminate HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

</TabItem>
</Tabs>

#### Sample response

<details>
  <summary>Click to show sample response</summary>

  ```json
{
    "id": "social_media"
}
  ```
</details>

### Terminate all supervisors

Terminates all supervisors. Terminated supervisors still exist in the metadata store and their history can be retrieved. Note that this endpoint returns an HTTP `200 Success` code even if there are no supervisors or running supervisors to terminate.

#### URL

<code class="postAPI">POST</code> <code>/druid/indexer/v1/supervisor/terminateAll</code>

#### Responses

<Tabs>

<TabItem value="53" label="200 SUCCESS">


*Successfully terminated all supervisors*

</TabItem>
</Tabs>

---

#### Sample request

<Tabs>

<TabItem value="54" label="cURL">


```shell
curl --request POST "http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/supervisor/terminateAll"
```

</TabItem>
<TabItem value="55" label="HTTP">


```HTTP
POST /druid/indexer/v1/supervisor/terminateAll HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

</TabItem>
</Tabs>

#### Sample response

<details>
  <summary>Click to show sample response</summary>

  ```json
{
    "status": "success"
}
  ```
</details>

### Shut down a supervisor

Shuts down a supervisor. This endpoint is deprecated and will be removed in future releases. Use the equivalent [terminate](#terminate-a-supervisor) endpoint instead.

#### URL

<code class="postAPI">POST</code> <code>/druid/indexer/v1/supervisor/:supervisorId/shutdown</code>
