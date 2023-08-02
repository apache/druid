---
id: tasks-api
title: Tasks API
sidebar_label: Tasks
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

This document describes the API endpoints for task retrieval, submission, and deletion for Apache Druid. Tasks are individual jobs performed by Druid to complete operations such as ingestion, querying, and compaction.  

In this topic, `http://ROUTER_IP:ROUTER_PORT` is a placeholder for the Router service address and port. For example, on the quickstart configuration, use `http://localhost:8888`.

## Task information and retrieval 

### Get an array of tasks

Retrieves an array of all tasks in the Druid cluster. Each task object includes information on its ID, status, associated datasource, and other metadata. For definitions of the response properties, see the [Tasks table](../querying/sql-metadata-tables.md#tasks-table).

#### URL

<code class="getAPI">GET</code> <code>/druid/indexer/v1/tasks</code>

#### Query parameters

The endpoint supports a set of optional query parameters to filter results. 

|Parameter|Type|Description|
|---|---|---|
|`state`|String|Filter list of tasks by task state, valid options are `running`, `complete`, `waiting`, and `pending`.|
| `datasource`|String| Return tasks filtered by Druid datasource.|
| `createdTimeInterval`|String (ISO-8601)| Return tasks created within the specified interval. Use `_` as the delimiter for the interval string. Do not use `/`. For example, `2023-06-27_2023-06-28`.|
| `max`|Integer|Maximum number of `complete` tasks to return. Only applies when `state` is set to `complete`.|
| `type`|String|Filter tasks by task type. See [task documentation](../ingestion/tasks.md) for more details.|

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->

*Successfully retrieved list of tasks* 

<!--400 BAD REQUEST-->

*Invalid `state` query parameter value* 

<!--500 SERVER ERROR-->

*Invalid query parameter* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

The following example shows how to retrieve a list of tasks filtered with the following query parameters:
* State: `complete`
* Datasource: `wikipedia_api`
* Time interval: between `2015-09-12` and `2015-09-13`
* Max entries returned: `10` 
* Task type: `query_worker`

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->

```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/tasks/?state=complete&datasource=wikipedia_api&createdTimeInterval=2015-09-12_2015-09-13&max=10&type=query_worker"
```

<!--HTTP-->

```HTTP
GET /druid/indexer/v1/tasks/?state=complete&datasource=wikipedia_api&createdTimeInterval=2015-09-12_2015-09-13&max=10&type=query_worker HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

<details>
  <summary>Click to show sample response</summary>
  
  ```json
  [
    {
        "id": "query-223549f8-b993-4483-b028-1b0d54713cad-worker0_0",
        "groupId": "query-223549f8-b993-4483-b028-1b0d54713cad",
        "type": "query_worker",
        "createdTime": "2023-06-22T22:11:37.012Z",
        "queueInsertionTime": "1970-01-01T00:00:00.000Z",
        "statusCode": "SUCCESS",
        "status": "SUCCESS",
        "runnerStatusCode": "NONE",
        "duration": 17897,
        "location": {
            "host": "localhost",
            "port": 8101,
            "tlsPort": -1
        },
        "dataSource": "wikipedia_api",
        "errorMsg": null
    },
    {
        "id": "query-fa82fa40-4c8c-4777-b832-cabbee5f519f-worker0_0",
        "groupId": "query-fa82fa40-4c8c-4777-b832-cabbee5f519f",
        "type": "query_worker",
        "createdTime": "2023-06-20T22:51:21.302Z",
        "queueInsertionTime": "1970-01-01T00:00:00.000Z",
        "statusCode": "SUCCESS",
        "status": "SUCCESS",
        "runnerStatusCode": "NONE",
        "duration": 16911,
        "location": {
            "host": "localhost",
            "port": 8101,
            "tlsPort": -1
        },
        "dataSource": "wikipedia_api",
        "errorMsg": null
    },
    {
        "id": "query-5419da7a-b270-492f-90e6-920ecfba766a-worker0_0",
        "groupId": "query-5419da7a-b270-492f-90e6-920ecfba766a",
        "type": "query_worker",
        "createdTime": "2023-06-20T22:45:53.909Z",
        "queueInsertionTime": "1970-01-01T00:00:00.000Z",
        "statusCode": "SUCCESS",
        "status": "SUCCESS",
        "runnerStatusCode": "NONE",
        "duration": 17030,
        "location": {
            "host": "localhost",
            "port": 8101,
            "tlsPort": -1
        },
        "dataSource": "wikipedia_api",
        "errorMsg": null
    }
  ]
  ```

</details>

### Get an array of complete tasks

Retrieves an array of completed tasks in the Druid cluster. This is functionally equivalent to `/druid/indexer/v1/tasks?state=complete`. For definitions of the response properties, see the [Tasks table](../querying/sql-metadata-tables.md#tasks-table).

#### URL

<code class="getAPI">GET</code> <code>/druid/indexer/v1/completeTasks</code>

#### Query parameters

The endpoint supports a set of optional query parameters to filter results. 

|Parameter|Type|Description|
|---|---|---|
| `datasource`|String| Return tasks filtered by Druid datasource.|
| `createdTimeInterval`|String (ISO-8601)| Return tasks created within the specified interval. The interval string should be delimited by `_` instead of `/`. For example, `2023-06-27_2023-06-28`.|
| `max`|Integer|Maximum number of `complete` tasks to return. Only applies when `state` is set to `complete`.|
| `type`|String|Filter tasks by task type. See [task documentation](../ingestion/tasks.md) for more details.|

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->

*Successfully retrieved list of complete tasks* 

<!--404 NOT FOUND-->

*Request sent to incorrect service* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->

```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/completeTasks"
```

<!--HTTP-->

```HTTP
GET /druid/indexer/v1/completeTasks HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

<details>
  <summary>Click to show sample response</summary>
  
  ```json
  [
    {
        "id": "query-223549f8-b993-4483-b028-1b0d54713cad-worker0_0",
        "groupId": "query-223549f8-b993-4483-b028-1b0d54713cad",
        "type": "query_worker",
        "createdTime": "2023-06-22T22:11:37.012Z",
        "queueInsertionTime": "1970-01-01T00:00:00.000Z",
        "statusCode": "SUCCESS",
        "status": "SUCCESS",
        "runnerStatusCode": "NONE",
        "duration": 17897,
        "location": {
            "host": "localhost",
            "port": 8101,
            "tlsPort": -1
        },
        "dataSource": "wikipedia_api",
        "errorMsg": null
    },
    {
        "id": "query-223549f8-b993-4483-b028-1b0d54713cad",
        "groupId": "query-223549f8-b993-4483-b028-1b0d54713cad",
        "type": "query_controller",
        "createdTime": "2023-06-22T22:11:28.367Z",
        "queueInsertionTime": "1970-01-01T00:00:00.000Z",
        "statusCode": "SUCCESS",
        "status": "SUCCESS",
        "runnerStatusCode": "NONE",
        "duration": 30317,
        "location": {
            "host": "localhost",
            "port": 8100,
            "tlsPort": -1
        },
        "dataSource": "wikipedia_api",
        "errorMsg": null
    }
  ]
  ```

</details>

### Get an array of running tasks

Retrieves an array of running task objects in the Druid cluster. It is functionally equivalent to `/druid/indexer/v1/tasks?state=running`. For definitions of the response properties, see the [Tasks table](../querying/sql-metadata-tables.md#tasks-table).

#### URL

<code class="getAPI">GET</code> <code>/druid/indexer/v1/runningTasks</code>

#### Query parameters

The endpoint supports a set of optional query parameters to filter results. 

|Parameter|Type|Description|
|---|---|---|
| `datasource`|String| Return tasks filtered by Druid datasource.|
| `createdTimeInterval`|String (ISO-8601)| Return tasks created within the specified interval. The interval string should be delimited by `_` instead of `/`. For example, `2023-06-27_2023-06-28`.|
| `max`|Integer|Maximum number of `complete` tasks to return. Only applies when `state` is set to `complete`.|
| `type`|String|Filter tasks by task type. See [task documentation](../ingestion/tasks.md) for more details.|

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->

*Successfully retrieved list of running tasks* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->

```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/runningTasks"
```

<!--HTTP-->

```HTTP
GET /druid/indexer/v1/runningTasks HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

<!--END_DOCUSAURUS_CODE_TABS-->


#### Sample response

<details>
  <summary>Click to show sample response</summary>
  
  ```json
  [
    {
        "id": "query-32663269-ead9-405a-8eb6-0817a952ef47",
        "groupId": "query-32663269-ead9-405a-8eb6-0817a952ef47",
        "type": "query_controller",
        "createdTime": "2023-06-22T22:54:43.170Z",
        "queueInsertionTime": "2023-06-22T22:54:43.170Z",
        "statusCode": "RUNNING",
        "status": "RUNNING",
        "runnerStatusCode": "RUNNING",
        "duration": -1,
        "location": {
            "host": "localhost",
            "port": 8100,
            "tlsPort": -1
        },
        "dataSource": "wikipedia_api",
        "errorMsg": null
    }
  ]
  ```

</details>

### Get an array of waiting tasks

Retrieves an array of waiting tasks in the Druid cluster. It is functionally equivalent to `/druid/indexer/v1/tasks?state=waiting`. For definitions of the response properties, see the [Tasks table](../querying/sql-metadata-tables.md#tasks-table).

#### URL

<code class="getAPI">GET</code> <code>/druid/indexer/v1/waitingTasks</code>

#### Query parameters

The endpoint supports a set of optional query parameters to filter results. 

|Parameter|Type|Description|
|---|---|---|
| `datasource`|String| Return tasks filtered by Druid datasource.|
| `createdTimeInterval`|String (ISO-8601)| Return tasks created within the specified interval. The interval string should be delimited by `_` instead of `/`. For example, `2023-06-27_2023-06-28`.|
| `max`|Integer|Maximum number of `complete` tasks to return. Only applies when `state` is set to `complete`.|
| `type`|String|Filter tasks by task type. See [task documentation](../ingestion/tasks.md) for more details.|

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->

*Successfully retrieved list of waiting tasks* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->

```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/waitingTasks"
```

<!--HTTP-->

```HTTP
GET /druid/indexer/v1/waitingTasks HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

<details>
  <summary>Click to show sample response</summary>
  
  ```json
  [
    {
        "id": "index_parallel_wikipedia_auto_biahcbmf_2023-06-26T21:08:05.216Z",
        "groupId": "index_parallel_wikipedia_auto_biahcbmf_2023-06-26T21:08:05.216Z",
        "type": "index_parallel",
        "createdTime": "2023-06-26T21:08:05.217Z",
        "queueInsertionTime": "1970-01-01T00:00:00.000Z",
        "statusCode": "RUNNING",
        "status": "RUNNING",
        "runnerStatusCode": "WAITING",
        "duration": -1,
        "location": {
            "host": null,
            "port": -1,
            "tlsPort": -1
        },
        "dataSource": "wikipedia_auto",
        "errorMsg": null
    },
    {
        "id": "index_parallel_wikipedia_auto_afggfiec_2023-06-26T21:08:05.546Z",
        "groupId": "index_parallel_wikipedia_auto_afggfiec_2023-06-26T21:08:05.546Z",
        "type": "index_parallel",
        "createdTime": "2023-06-26T21:08:05.548Z",
        "queueInsertionTime": "1970-01-01T00:00:00.000Z",
        "statusCode": "RUNNING",
        "status": "RUNNING",
        "runnerStatusCode": "WAITING",
        "duration": -1,
        "location": {
            "host": null,
            "port": -1,
            "tlsPort": -1
        },
        "dataSource": "wikipedia_auto",
        "errorMsg": null
    },
    {
        "id": "index_parallel_wikipedia_auto_jmmddihf_2023-06-26T21:08:06.644Z",
        "groupId": "index_parallel_wikipedia_auto_jmmddihf_2023-06-26T21:08:06.644Z",
        "type": "index_parallel",
        "createdTime": "2023-06-26T21:08:06.671Z",
        "queueInsertionTime": "1970-01-01T00:00:00.000Z",
        "statusCode": "RUNNING",
        "status": "RUNNING",
        "runnerStatusCode": "WAITING",
        "duration": -1,
        "location": {
            "host": null,
            "port": -1,
            "tlsPort": -1
        },
        "dataSource": "wikipedia_auto",
        "errorMsg": null
    }
  ]
  ```

</details>

### Get an array of pending tasks

Retrieves an array of pending tasks in the Druid cluster. It is functionally equivalent to `/druid/indexer/v1/tasks?state=pending`. For definitions of the response properties, see the [Tasks table](../querying/sql-metadata-tables.md#tasks-table).

#### URL

<code class="getAPI">GET</code> <code>/druid/indexer/v1/pendingTasks</code>

#### Query parameters

The endpoint supports a set of optional query parameters to filter results. 

|Parameter|Type|Description|
|---|---|---|
| `datasource`|String| Return tasks filtered by Druid datasource.|
| `createdTimeInterval`|String (ISO-8601)| Return tasks created within the specified interval. The interval string should be delimited by `_` instead of `/`. For example, `2023-06-27_2023-06-28`.|
| `max`|Integer|Maximum number of `complete` tasks to return. Only applies when `state` is set to `complete`.|
| `type`|String|Filter tasks by task type. See [task documentation](../ingestion/tasks.md) for more details.|

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->

*Successfully retrieved list of pending tasks* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->

```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/pendingTasks"
```

<!--HTTP-->

```HTTP
GET /druid/indexer/v1/pendingTasks HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

<details>
  <summary>Click to show sample response</summary>
  
  ```json
  [
    {
        "id": "query-7b37c315-50a0-4b68-aaa8-b1ef1f060e67",
        "groupId": "query-7b37c315-50a0-4b68-aaa8-b1ef1f060e67",
        "type": "query_controller",
        "createdTime": "2023-06-23T19:53:06.037Z",
        "queueInsertionTime": "2023-06-23T19:53:06.037Z",
        "statusCode": "RUNNING",
        "status": "RUNNING",
        "runnerStatusCode": "PENDING",
        "duration": -1,
        "location": {
            "host": null,
            "port": -1,
            "tlsPort": -1
        },
        "dataSource": "wikipedia_api",
        "errorMsg": null
    },
    {
        "id": "query-544f0c41-f81d-4504-b98b-f9ab8b36ef36",
        "groupId": "query-544f0c41-f81d-4504-b98b-f9ab8b36ef36",
        "type": "query_controller",
        "createdTime": "2023-06-23T19:53:06.616Z",
        "queueInsertionTime": "2023-06-23T19:53:06.616Z",
        "statusCode": "RUNNING",
        "status": "RUNNING",
        "runnerStatusCode": "PENDING",
        "duration": -1,
        "location": {
            "host": null,
            "port": -1,
            "tlsPort": -1
        },
        "dataSource": "wikipedia_api",
        "errorMsg": null
    }
  ]
  ```

</details>

### Get task payload

Retrieves the payload of a task given the task ID. It returns a JSON object with the task ID and payload that includes task configuration details and relevant specifications associated with the execution of the task.

#### URL

<code class="getAPI">GET</code> <code>/druid/indexer/v1/task/:taskId</code>

#### Responses 

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->

*Successfully retrieved payload of task* 

<!--404 NOT FOUND-->

*Cannot find task with ID* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

The following examples shows how to retrieve the task payload of a task with the specified ID `index_parallel_wikipedia_short_iajoonnd_2023-07-07T17:53:12.174Z`.

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->

```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/task/index_parallel_wikipedia_short_iajoonnd_2023-07-07T17:53:12.174Z"
```

<!--HTTP-->

```HTTP
GET /druid/indexer/v1/task/index_parallel_wikipedia_short_iajoonnd_2023-07-07T17:53:12.174Z HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

<!--END_DOCUSAURUS_CODE_TABS-->


#### Sample response

<details>
  <summary>Click to show sample response</summary>
  
  ```json
  {
    "task": "index_parallel_wikipedia_short_iajoonnd_2023-07-07T17:53:12.174Z",
    "payload": {
        "type": "index_parallel",
        "id": "index_parallel_wikipedia_short_iajoonnd_2023-07-07T17:53:12.174Z",
        "groupId": "index_parallel_wikipedia_short_iajoonnd_2023-07-07T17:53:12.174Z",
        "resource": {
            "availabilityGroup": "index_parallel_wikipedia_short_iajoonnd_2023-07-07T17:53:12.174Z",
            "requiredCapacity": 1
        },
        "spec": {
            "dataSchema": {
                "dataSource": "wikipedia_short",
                "timestampSpec": {
                    "column": "time",
                    "format": "iso",
                    "missingValue": null
                },
                "dimensionsSpec": {
                    "dimensions": [
                        {
                            "type": "string",
                            "name": "cityName",
                            "multiValueHandling": "SORTED_ARRAY",
                            "createBitmapIndex": true
                        },
                        {
                            "type": "string",
                            "name": "countryName",
                            "multiValueHandling": "SORTED_ARRAY",
                            "createBitmapIndex": true
                        },
                        {
                            "type": "string",
                            "name": "regionName",
                            "multiValueHandling": "SORTED_ARRAY",
                            "createBitmapIndex": true
                        }
                    ],
                    "dimensionExclusions": [
                        "__time",
                        "time"
                    ],
                    "includeAllDimensions": false,
                    "useSchemaDiscovery": false
                },
                "metricsSpec": [],
                "granularitySpec": {
                    "type": "uniform",
                    "segmentGranularity": "DAY",
                    "queryGranularity": {
                        "type": "none"
                    },
                    "rollup": false,
                    "intervals": [
                        "2015-09-12T00:00:00.000Z/2015-09-13T00:00:00.000Z"
                    ]
                },
                "transformSpec": {
                    "filter": null,
                    "transforms": []
                }
            },
            "ioConfig": {
                "type": "index_parallel",
                "inputSource": {
                    "type": "local",
                    "baseDir": "quickstart/tutorial",
                    "filter": "wikiticker-2015-09-12-sampled.json.gz"
                },
                "inputFormat": {
                    "type": "json",
                    "keepNullColumns": false,
                    "assumeNewlineDelimited": false,
                    "useJsonNodeReader": false
                },
                "appendToExisting": false,
                "dropExisting": false
            },
            "tuningConfig": {
                "type": "index_parallel",
                "maxRowsPerSegment": 5000000,
                "appendableIndexSpec": {
                    "type": "onheap",
                    "preserveExistingMetrics": false
                },
                "maxRowsInMemory": 25000,
                "maxBytesInMemory": 0,
                "skipBytesInMemoryOverheadCheck": false,
                "maxTotalRows": null,
                "numShards": null,
                "splitHintSpec": null,
                "partitionsSpec": {
                    "type": "dynamic",
                    "maxRowsPerSegment": 5000000,
                    "maxTotalRows": null
                },
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
                "maxPendingPersists": 0,
                "forceGuaranteedRollup": false,
                "reportParseExceptions": false,
                "pushTimeout": 0,
                "segmentWriteOutMediumFactory": null,
                "maxNumConcurrentSubTasks": 1,
                "maxRetry": 3,
                "taskStatusCheckPeriodMs": 1000,
                "chatHandlerTimeout": "PT10S",
                "chatHandlerNumRetries": 5,
                "maxNumSegmentsToMerge": 100,
                "totalNumMergeTasks": 10,
                "logParseExceptions": false,
                "maxParseExceptions": 2147483647,
                "maxSavedParseExceptions": 0,
                "maxColumnsToMerge": -1,
                "awaitSegmentAvailabilityTimeoutMillis": 0,
                "maxAllowedLockCount": -1,
                "partitionDimensions": []
            }
        },
        "context": {
            "forceTimeChunkLock": true,
            "useLineageBasedSegmentAllocation": true
        },
        "dataSource": "wikipedia_short"
    }
}
  ```

</details>

### Get task status

Retrieves the status of a task given the task ID. It returns a JSON object with the task's status code, runner status, task type, datasource, and other relevant metadata.

#### URL

<code class="getAPI">GET</code> <code>/druid/indexer/v1/task/:taskId/status</code>

#### Responses 

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->

*Successfully retrieved task status* 

<!--404 NOT FOUND-->

*Cannot find task with ID* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

The following examples shows how to retrieve the status of a task with the specified ID `query-223549f8-b993-4483-b028-1b0d54713cad`.

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->

```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/task/query-223549f8-b993-4483-b028-1b0d54713cad/status"
```

<!--HTTP-->

```HTTP
GET /druid/indexer/v1/task/query-223549f8-b993-4483-b028-1b0d54713cad/status HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

<!--END_DOCUSAURUS_CODE_TABS-->


#### Sample response

<details>
  <summary>Click to show sample response</summary>

  ```json
  {
    'task': 'query-223549f8-b993-4483-b028-1b0d54713cad',
    'status': {
      'id': 'query-223549f8-b993-4483-b028-1b0d54713cad',
      'groupId': 'query-223549f8-b993-4483-b028-1b0d54713cad',
      'type': 'query_controller',
      'createdTime': '2023-06-22T22:11:28.367Z',
      'queueInsertionTime': '1970-01-01T00:00:00.000Z',
      'statusCode': 'RUNNING',
      'status': 'RUNNING',
      'runnerStatusCode': 'RUNNING',
      'duration': -1,
      'location': {'host': 'localhost', 'port': 8100, 'tlsPort': -1},
      'dataSource': 'wikipedia_api',
      'errorMsg': None
    }
  }
  ```

</details>

### Get task segments

> This API is deprecated and will be removed in future releases.

Retrieves information about segments generated by the task given the task ID. To hit this endpoint, make sure to enable the audit log config on the Overlord with `druid.indexer.auditLog.enabled = true`.

In addition to enabling audit logs, configure a cleanup strategy to prevent overloading the metadata store with old audit logs which may cause performance issues. To enable automated cleanup of audit logs on the Coordinator, set `druid.coordinator.kill.audit.on`. You may also manually export the audit logs to external storage. For more information, see [Audit records](../operations/clean-metadata-store.md#audit-records).

#### URL

<code class="getAPI">GET</code> <code>/druid/indexer/v1/task/:taskId/segments</code>

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->

*Successfully retrieved task segments* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

The following examples shows how to retrieve the task segment of the task with the specified ID `query-52a8aafe-7265-4427-89fe-dc51275cc470`.

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->

```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/task/query-52a8aafe-7265-4427-89fe-dc51275cc470/reports"
```

<!--HTTP-->

```HTTP
GET /druid/indexer/v1/task/query-52a8aafe-7265-4427-89fe-dc51275cc470/reports HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

A successful request returns a `200 OK` response and an array of the task segments.

### Get task log

Retrieves the event log associated with a task. It returns a list of logged events during the lifecycle of the task. The endpoint is useful for providing information about the execution of the task, including any errors or warnings raised. 

Task logs are automatically retrieved from the Middle Manager/Indexer or in long-term storage. For reference, see [Task logs](../ingestion/tasks.md#task-logs).

#### URL

<code class="getAPI">GET</code> <code>/druid/indexer/v1/task/:taskId/log</code>

#### Query parameters

* `offset` (optional)
    * Type: Int
    * Exclude the first passed in number of entries from the response.

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->

*Successfully retrieved task log* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

The following examples shows how to retrieve the task log of a task with the specified ID `index_kafka_social_media_0e905aa31037879_nommnaeg`.

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->

```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/task/index_kafka_social_media_0e905aa31037879_nommnaeg/log"
```

<!--HTTP-->

```HTTP
GET /druid/indexer/v1/task/index_kafka_social_media_0e905aa31037879_nommnaeg/log HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

<details>
  <summary>Click to show sample response</summary>
  
  ```json
    2023-07-03T22:11:17,891 INFO [qtp1251996697-122] org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner - Sequence[index_kafka_social_media_0e905aa31037879_0] end offsets updated from [{0=9223372036854775807}] to [{0=230985}].
    2023-07-03T22:11:17,900 INFO [qtp1251996697-122] org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner - Saved sequence metadata to disk: [SequenceMetadata{sequenceId=0, sequenceName='index_kafka_social_media_0e905aa31037879_0', assignments=[0], startOffsets={0=230985}, exclusiveStartPartitions=[], endOffsets={0=230985}, sentinel=false, checkpointed=true}]
    2023-07-03T22:11:17,901 INFO [task-runner-0-priority-0] org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner - Received resume command, resuming ingestion.
    2023-07-03T22:11:17,901 INFO [task-runner-0-priority-0] org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner - Finished reading partition[0], up to[230985].
    2023-07-03T22:11:17,902 INFO [task-runner-0-priority-0] org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=consumer-kafka-supervisor-dcanhmig-1, groupId=kafka-supervisor-dcanhmig] Resetting generation and member id due to: consumer pro-actively leaving the group
    2023-07-03T22:11:17,902 INFO [task-runner-0-priority-0] org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=consumer-kafka-supervisor-dcanhmig-1, groupId=kafka-supervisor-dcanhmig] Request joining group due to: consumer pro-actively leaving the group
    2023-07-03T22:11:17,902 INFO [task-runner-0-priority-0] org.apache.kafka.clients.consumer.KafkaConsumer - [Consumer clientId=consumer-kafka-supervisor-dcanhmig-1, groupId=kafka-supervisor-dcanhmig] Unsubscribed all topics or patterns and assigned partitions
    2023-07-03T22:11:17,912 INFO [task-runner-0-priority-0] org.apache.druid.segment.realtime.appenderator.StreamAppenderator - Persisted rows[0] and (estimated) bytes[0]
    2023-07-03T22:11:17,916 INFO [[index_kafka_social_media_0e905aa31037879_nommnaeg]-appenderator-persist] org.apache.druid.segment.realtime.appenderator.StreamAppenderator - Flushed in-memory data with commit metadata [AppenderatorDriverMetadata{segments={}, lastSegmentIds={}, callerMetadata={nextPartitions=SeekableStreamEndSequenceNumbers{stream='social_media', partitionSequenceNumberMap={0=230985}}}}] for segments: 
    2023-07-03T22:11:17,917 INFO [[index_kafka_social_media_0e905aa31037879_nommnaeg]-appenderator-persist] org.apache.druid.segment.realtime.appenderator.StreamAppenderator - Persisted stats: processed rows: [0], persisted rows[0], sinks: [0], total fireHydrants (across sinks): [0], persisted fireHydrants (across sinks): [0]
    2023-07-03T22:11:17,919 INFO [task-runner-0-priority-0] org.apache.druid.segment.realtime.appenderator.BaseAppenderatorDriver - Pushing [0] segments in background
    2023-07-03T22:11:17,921 INFO [task-runner-0-priority-0] org.apache.druid.segment.realtime.appenderator.StreamAppenderator - Persisted rows[0] and (estimated) bytes[0]
    2023-07-03T22:11:17,924 INFO [[index_kafka_social_media_0e905aa31037879_nommnaeg]-appenderator-persist] org.apache.druid.segment.realtime.appenderator.StreamAppenderator - Flushed in-memory data with commit metadata [AppenderatorDriverMetadata{segments={}, lastSegmentIds={}, callerMetadata={nextPartitions=SeekableStreamStartSequenceNumbers{stream='social_media', partitionSequenceNumberMap={0=230985}, exclusivePartitions=[]}, publishPartitions=SeekableStreamEndSequenceNumbers{stream='social_media', partitionSequenceNumberMap={0=230985}}}}] for segments: 
    2023-07-03T22:11:17,924 INFO [[index_kafka_social_media_0e905aa31037879_nommnaeg]-appenderator-persist] org.apache.druid.segment.realtime.appenderator.StreamAppenderator - Persisted stats: processed rows: [0], persisted rows[0], sinks: [0], total fireHydrants (across sinks): [0], persisted fireHydrants (across sinks): [0]
    2023-07-03T22:11:17,925 INFO [[index_kafka_social_media_0e905aa31037879_nommnaeg]-appenderator-merge] org.apache.druid.segment.realtime.appenderator.StreamAppenderator - Preparing to push (stats): processed rows: [0], sinks: [0], fireHydrants (across sinks): [0]
    2023-07-03T22:11:17,925 INFO [[index_kafka_social_media_0e905aa31037879_nommnaeg]-appenderator-merge] org.apache.druid.segment.realtime.appenderator.StreamAppenderator - Push complete...
    2023-07-03T22:11:17,929 INFO [[index_kafka_social_media_0e905aa31037879_nommnaeg]-publish] org.apache.druid.indexing.seekablestream.SequenceMetadata - With empty segment set, start offsets [SeekableStreamStartSequenceNumbers{stream='social_media', partitionSequenceNumberMap={0=230985}, exclusivePartitions=[]}] and end offsets [SeekableStreamEndSequenceNumbers{stream='social_media', partitionSequenceNumberMap={0=230985}}] are the same, skipping metadata commit.
    2023-07-03T22:11:17,930 INFO [[index_kafka_social_media_0e905aa31037879_nommnaeg]-publish] org.apache.druid.segment.realtime.appenderator.BaseAppenderatorDriver - Published [0] segments with commit metadata [{nextPartitions=SeekableStreamStartSequenceNumbers{stream='social_media', partitionSequenceNumberMap={0=230985}, exclusivePartitions=[]}, publishPartitions=SeekableStreamEndSequenceNumbers{stream='social_media', partitionSequenceNumberMap={0=230985}}}]
    2023-07-03T22:11:17,930 INFO [[index_kafka_social_media_0e905aa31037879_nommnaeg]-publish] org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner - Published 0 segments for sequence [index_kafka_social_media_0e905aa31037879_0] with metadata [AppenderatorDriverMetadata{segments={}, lastSegmentIds={}, callerMetadata={nextPartitions=SeekableStreamStartSequenceNumbers{stream='social_media', partitionSequenceNumberMap={0=230985}, exclusivePartitions=[]}, publishPartitions=SeekableStreamEndSequenceNumbers{stream='social_media', partitionSequenceNumberMap={0=230985}}}}].
    2023-07-03T22:11:17,931 INFO [[index_kafka_social_media_0e905aa31037879_nommnaeg]-publish] org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner - Saved sequence metadata to disk: []
    2023-07-03T22:11:17,932 INFO [task-runner-0-priority-0] org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner - Handoff complete for segments: 
    2023-07-03T22:11:17,932 INFO [task-runner-0-priority-0] org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=consumer-kafka-supervisor-dcanhmig-1, groupId=kafka-supervisor-dcanhmig] Resetting generation and member id due to: consumer pro-actively leaving the group
    2023-07-03T22:11:17,932 INFO [task-runner-0-priority-0] org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=consumer-kafka-supervisor-dcanhmig-1, groupId=kafka-supervisor-dcanhmig] Request joining group due to: consumer pro-actively leaving the group
    2023-07-03T22:11:17,933 INFO [task-runner-0-priority-0] org.apache.kafka.common.metrics.Metrics - Metrics scheduler closed
    2023-07-03T22:11:17,933 INFO [task-runner-0-priority-0] org.apache.kafka.common.metrics.Metrics - Closing reporter org.apache.kafka.common.metrics.JmxReporter
    2023-07-03T22:11:17,933 INFO [task-runner-0-priority-0] org.apache.kafka.common.metrics.Metrics - Metrics reporters closed
    2023-07-03T22:11:17,935 INFO [task-runner-0-priority-0] org.apache.kafka.common.utils.AppInfoParser - App info kafka.consumer for consumer-kafka-supervisor-dcanhmig-1 unregistered
    2023-07-03T22:11:17,936 INFO [task-runner-0-priority-0] org.apache.druid.curator.announcement.Announcer - Unannouncing [/druid/internal-discovery/PEON/localhost:8100]
    2023-07-03T22:11:17,972 INFO [task-runner-0-priority-0] org.apache.druid.curator.discovery.CuratorDruidNodeAnnouncer - Unannounced self [{"druidNode":{"service":"druid/middleManager","host":"localhost","bindOnHost":false,"plaintextPort":8100,"port":-1,"tlsPort":-1,"enablePlaintextPort":true,"enableTlsPort":false},"nodeType":"peon","services":{"dataNodeService":{"type":"dataNodeService","tier":"_default_tier","maxSize":0,"type":"indexer-executor","serverType":"indexer-executor","priority":0},"lookupNodeService":{"type":"lookupNodeService","lookupTier":"__default"}}}].
    2023-07-03T22:11:17,972 INFO [task-runner-0-priority-0] org.apache.druid.curator.announcement.Announcer - Unannouncing [/druid/announcements/localhost:8100]
    2023-07-03T22:11:17,996 INFO [task-runner-0-priority-0] org.apache.druid.indexing.worker.executor.ExecutorLifecycle - Task completed with status: {
    "id" : "index_kafka_social_media_0e905aa31037879_nommnaeg",
    "status" : "SUCCESS",
    "duration" : 3601130,
    "errorMsg" : null,
    "location" : {
        "host" : null,
        "port" : -1,
        "tlsPort" : -1
    }
    }
    2023-07-03T22:11:17,998 INFO [main] org.apache.druid.java.util.common.lifecycle.Lifecycle - Stopping lifecycle [module] stage [ANNOUNCEMENTS]
    2023-07-03T22:11:18,005 INFO [main] org.apache.druid.java.util.common.lifecycle.Lifecycle - Stopping lifecycle [module] stage [SERVER]
    2023-07-03T22:11:18,009 INFO [main] org.eclipse.jetty.server.AbstractConnector - Stopped ServerConnector@6491006{HTTP/1.1, (http/1.1)}{0.0.0.0:8100}
    2023-07-03T22:11:18,009 INFO [main] org.eclipse.jetty.server.session - node0 Stopped scavenging
    2023-07-03T22:11:18,012 INFO [main] org.eclipse.jetty.server.handler.ContextHandler - Stopped o.e.j.s.ServletContextHandler@742aa00a{/,null,STOPPED}
    2023-07-03T22:11:18,014 INFO [main] org.apache.druid.java.util.common.lifecycle.Lifecycle - Stopping lifecycle [module] stage [NORMAL]
    2023-07-03T22:11:18,014 INFO [main] org.apache.druid.server.coordination.ZkCoordinator - Stopping ZkCoordinator for [DruidServerMetadata{name='localhost:8100', hostAndPort='localhost:8100', hostAndTlsPort='null', maxSize=0, tier='_default_tier', type=indexer-executor, priority=0}]
    2023-07-03T22:11:18,014 INFO [main] org.apache.druid.server.coordination.SegmentLoadDropHandler - Stopping...
    2023-07-03T22:11:18,014 INFO [main] org.apache.druid.server.coordination.SegmentLoadDropHandler - Stopped.
    2023-07-03T22:11:18,014 INFO [main] org.apache.druid.indexing.overlord.SingleTaskBackgroundRunner - Starting graceful shutdown of task[index_kafka_social_media_0e905aa31037879_nommnaeg].
    2023-07-03T22:11:18,014 INFO [main] org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner - Stopping forcefully (status: [PUBLISHING])
    2023-07-03T22:11:18,019 INFO [LookupExtractorFactoryContainerProvider-MainThread] org.apache.druid.query.lookup.LookupReferencesManager - Lookup Management loop exited. Lookup notices are not handled anymore.
    2023-07-03T22:11:18,020 INFO [main] org.apache.druid.query.lookup.LookupReferencesManager - Closed lookup [name].
    2023-07-03T22:11:18,020 INFO [Curator-Framework-0] org.apache.curator.framework.imps.CuratorFrameworkImpl - backgroundOperationsLoop exiting
    2023-07-03T22:11:18,147 INFO [main] org.apache.zookeeper.ZooKeeper - Session: 0x1000097ceaf0007 closed
    2023-07-03T22:11:18,147 INFO [main-EventThread] org.apache.zookeeper.ClientCnxn - EventThread shut down for session: 0x1000097ceaf0007
    2023-07-03T22:11:18,151 INFO [main] org.apache.druid.java.util.common.lifecycle.Lifecycle - Stopping lifecycle [module] stage [INIT]
    Finished peon task
  ```

</details>

### Get task completion report

Retrieves a [task completion report](../ingestion/tasks.md#task-reports) for a task. It returns a JSON object with information about the number of rows ingested, and any parse exceptions that Druid raised.

#### URL

<code class="getAPI">GET</code> <code>/druid/indexer/v1/task/:taskId/reports</code>

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->

*Successfully retrieved task report* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

The following examples shows how to retrieve the completion report of a task with the specified ID `query-52a8aafe-7265-4427-89fe-dc51275cc470`.

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->

```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/task/query-52a8aafe-7265-4427-89fe-dc51275cc470/reports"
```

<!--HTTP-->

```HTTP
GET /druid/indexer/v1/task/query-52a8aafe-7265-4427-89fe-dc51275cc470/reports HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

<details>
  <summary>Click to show sample response</summary>
  
  ```json
  {
    "ingestionStatsAndErrors": {
        "type": "ingestionStatsAndErrors",
        "taskId": "query-52a8aafe-7265-4427-89fe-dc51275cc470",
        "payload": {
            "ingestionState": "COMPLETED",
            "unparseableEvents": {},
            "rowStats": {
                "determinePartitions": {
                    "processed": 0,
                    "processedBytes": 0,
                    "processedWithError": 0,
                    "thrownAway": 0,
                    "unparseable": 0
                },
                "buildSegments": {
                    "processed": 39244,
                    "processedBytes": 17106256,
                    "processedWithError": 0,
                    "thrownAway": 0,
                    "unparseable": 0
                }
            },
            "errorMsg": null,
            "segmentAvailabilityConfirmed": false,
            "segmentAvailabilityWaitTimeMs": 0
        }
    }
  }
  ```

</details>

## Task operations

### Submit a task

Submits a JSON-based ingestion spec or supervisor spec to the Overlord. It returns the task ID of the submitted task. For information on creating an ingestion spec, refer to the [ingestion spec reference](../ingestion/ingestion-spec.md).

Note that for most batch ingestion use cases, you should use the [SQL-ingestion API](./sql-ingestion-api.md) instead of JSON-based batch ingestion.

#### URL

<code class="postAPI">POST</code> <code>/druid/indexer/v1/task</code>

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->

*Successfully submitted task* 

<!--400 BAD REQUEST-->

*Missing information in query* 

<!--415 UNSUPPORTED MEDIA TYPE-->

*Incorrect request body media type* 

<!--500 Server Error-->

*Unexpected token or characters in request body* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

The following request is an example of submitting a task to create a datasource named `"wikipedia auto"`.

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->

```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/task" \
--header 'Content-Type: application/json' \
--data '{
  "type" : "index_parallel",
  "spec" : {
    "dataSchema" : {
      "dataSource" : "wikipedia_auto",
      "timestampSpec": {
        "column": "time",
        "format": "iso"
      },
      "dimensionsSpec" : {
        "useSchemaDiscovery": true
      },
      "metricsSpec" : [],
      "granularitySpec" : {
        "type" : "uniform",
        "segmentGranularity" : "day",
        "queryGranularity" : "none",
        "intervals" : ["2015-09-12/2015-09-13"],
        "rollup" : false
      }
    },
    "ioConfig" : {
      "type" : "index_parallel",
      "inputSource" : {
        "type" : "local",
        "baseDir" : "quickstart/tutorial/",
        "filter" : "wikiticker-2015-09-12-sampled.json.gz"
      },
      "inputFormat" : {
        "type" : "json"
      },
      "appendToExisting" : false
    },
    "tuningConfig" : {
      "type" : "index_parallel",
      "maxRowsPerSegment" : 5000000,
      "maxRowsInMemory" : 25000
    }
  }
}'

```
<!--HTTP-->

```HTTP
POST /druid/indexer/v1/task HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
Content-Type: application/json
Content-Length: 952

{
  "type" : "index_parallel",
  "spec" : {
    "dataSchema" : {
      "dataSource" : "wikipedia_auto",
      "timestampSpec": {
        "column": "time",
        "format": "iso"
      },
      "dimensionsSpec" : {
        "useSchemaDiscovery": true
      },
      "metricsSpec" : [],
      "granularitySpec" : {
        "type" : "uniform",
        "segmentGranularity" : "day",
        "queryGranularity" : "none",
        "intervals" : ["2015-09-12/2015-09-13"],
        "rollup" : false
      }
    },
    "ioConfig" : {
      "type" : "index_parallel",
      "inputSource" : {
        "type" : "local",
        "baseDir" : "quickstart/tutorial/",
        "filter" : "wikiticker-2015-09-12-sampled.json.gz"
      },
      "inputFormat" : {
        "type" : "json"
      },
      "appendToExisting" : false
    },
    "tuningConfig" : {
      "type" : "index_parallel",
      "maxRowsPerSegment" : 5000000,
      "maxRowsInMemory" : 25000
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
      "task": "index_parallel_wikipedia_odofhkle_2023-06-23T21:07:28.226Z"
  }
  ```

</details>

### Shut down a task

Shuts down a task if it not already complete. Returns a JSON object with the ID of the task that was shut down successfully.

#### URL

<code class="postAPI">POST</code> <code>/druid/indexer/v1/task/:taskId/shutdown</code>

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->

*Successfully shut down task* 

<!--404 NOT FOUND-->

*Cannot find task with ID or task is no longer running* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

The following request shows how to shut down a task with the ID `query-52as 8aafe-7265-4427-89fe-dc51275cc470`.

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->

```shell
curl --request POST "http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/task/query-52as 8aafe-7265-4427-89fe-dc51275cc470/shutdown"
```

<!--HTTP-->

```HTTP
POST /druid/indexer/v1/task/query-52as 8aafe-7265-4427-89fe-dc51275cc470/shutdown HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

<details>
  <summary>Click to show sample response</summary>

  ```json
  {
    'task': 'query-577a83dd-a14e-4380-bd01-c942b781236b'
  }
  ```

</details>

### Shut down all tasks for a datasource

Shuts down all tasks for a specified datasource. If successful, it returns a JSON object with the name of the datasource whose tasks are shut down.

#### URL

<code class="postAPI">POST</code> <code>/druid/indexer/v1/datasources/:datasource/shutdownAllTasks</code>

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->

*Successfully shut down tasks* 

<!--404 NOT FOUND-->

*Error or datasource does not have a running task* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

The following request is an example of shutting down all tasks for datasource `wikipedia_auto`.

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->

```shell
curl --request POST "http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/datasources/wikipedia_auto/shutdownAllTasks"
```

<!--HTTP-->

```HTTP
POST /druid/indexer/v1/datasources/wikipedia_auto/shutdownAllTasks HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

<details>
  <summary>Click to show sample response</summary>
  
  ```json
  {
    "dataSource": "wikipedia_api"
  }
  ```

</details>

## Task management

### Retrieve status objects for tasks

Retrieves list of task status objects for list of task ID strings in request body. It returns a set of JSON objects with the status, duration, location of each task, and any error messages.

#### URL

<code class="postAPI">POST</code> <code>/druid/indexer/v1/taskStatus</code>

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->

*Successfully retrieved status objects* 

<!--415 UNSUPPORTED MEDIA TYPE-->

*Missing request body or incorrect request body type* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

The following request is an example of retrieving status objects for task ID `index_parallel_wikipedia_auto_jndhkpbo_2023-06-26T17:23:05.308Z` and `index_parallel_wikipedia_auto_jbgiianh_2023-06-26T23:17:56.769Z` .

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->

```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/taskStatus" \
--header 'Content-Type: application/json' \
--data '["index_parallel_wikipedia_auto_jndhkpbo_2023-06-26T17:23:05.308Z","index_parallel_wikipedia_auto_jbgiianh_2023-06-26T23:17:56.769Z"]'
```

<!--HTTP-->

```HTTP
POST /druid/indexer/v1/taskStatus HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
Content-Type: application/json
Content-Length: 134

["index_parallel_wikipedia_auto_jndhkpbo_2023-06-26T17:23:05.308Z", "index_parallel_wikipedia_auto_jbgiianh_2023-06-26T23:17:56.769Z"]
```

<!--END_DOCUSAURUS_CODE_TABS-->


#### Sample response

<details>
  <summary>Click to show sample response</summary>
  
  ```json
  {
    "index_parallel_wikipedia_auto_jbgiianh_2023-06-26T23:17:56.769Z": {
        "id": "index_parallel_wikipedia_auto_jbgiianh_2023-06-26T23:17:56.769Z",
        "status": "SUCCESS",
        "duration": 10630,
        "errorMsg": null,
        "location": {
            "host": "localhost",
            "port": 8100,
            "tlsPort": -1
        }
    },
    "index_parallel_wikipedia_auto_jndhkpbo_2023-06-26T17:23:05.308Z": {
        "id": "index_parallel_wikipedia_auto_jndhkpbo_2023-06-26T17:23:05.308Z",
        "status": "SUCCESS",
        "duration": 11012,
        "errorMsg": null,
        "location": {
            "host": "localhost",
            "port": 8100,
            "tlsPort": -1
        }
    }
  }
  ```

</details>

### Clean up pending segments for a datasource

Manually clean up pending segments table in metadata storage for `datasource`. It returns a JSON object response with
`numDeleted` for the number of rows deleted from the pending segments table. This API is used by the
`druid.coordinator.kill.pendingSegments.on` [Coordinator setting](../configuration/index.md#coordinator-operation)
which automates this operation to perform periodically.

#### URL

<code class="deleteAPI">DELETE</code> <code>/druid/indexer/v1/pendingSegments/:datasource</code>

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->

*Successfully deleted pending segments* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

The following request is an example of cleaning up pending segments for the `wikipedia_api` datasource.

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->

```shell
curl --request DELETE "http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/pendingSegments/wikipedia_api"
```

<!--HTTP-->

```HTTP
DELETE /druid/indexer/v1/pendingSegments/wikipedia_api HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

<details>
  <summary>Click to show sample response</summary>
  
  ```json
  {
    "numDeleted": 2
  }
  ```

</details>