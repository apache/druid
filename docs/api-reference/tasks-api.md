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

This document describes the API endpoints for task retrieval, submission, and deletion for Apache Druid.

## Tasks

Note that all _interval_ URL parameters are ISO 8601 strings delimited by a `_` instead of a `/`
as in `2016-06-27_2016-06-28`.

### Get an array of tasks

#### URL
<code class="getAPI">GET</code> `/druid/indexer/v1/tasks`

This endpoint retrieves an array of all task objects currently running or executed in the current Druid cluster. It provides information about each task such as its task id, task status, associated data source, and other metadata. It supports a set of optional query parameters to filter results. 

#### Query parameters
|Parameter|Type|Description|
|---|---|---|
|`state`|String|Filter list of tasks by task state, valid options are `running`, `complete`, `waiting`, and `pending`.|
| `datasource`|String| Return tasks filtered by Druid datasource.|
| `createdTimeInterval`|String (ISO-8601)| Return tasks created within the specified interval. |
| `max`|Integer|Maximum number of `"complete"` tasks to return. Only applies when `state` is set to `"complete"`.|
| `type`|String|Filter tasks by task type. See [task documentation](../ingestion/tasks.md) for more details.|

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved list of tasks* 
<!--400 BAD REQUEST-->
<br/>
*Invalid `state` query parameter value* 
<!--404 NOT FOUND-->
<br/>
*Resource not found* 
<!--500 SERVER ERROR-->
<br/>
*Invalid query parameter* 
<!--END_DOCUSAURUS_CODE_TABS-->

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
curl "{domain}/druid/indexer/v1/tasks/?state=complete&datasource=wikipedia_api&createdTimeInterval=2015-09-12T00%3A00%3A00Z%2F2015-09-13T23%3A59%3A59Z&max=10&type=query_worker"
```
<!--HTTP-->
```HTTP
GET /druid/indexer/v1/tasks/?state=complete&datasource=wikipedia_api&createdTimeInterval=2015-09-12T00%3A00%3A00Z%2F2015-09-13T23%3A59%3A59Z&max=10&type=query_worker HTTP/1.1
Host: {domain}
```

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

<details>
  <summary>Toggle to show sample response</summary>
  <pre><code>[
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
]</code></pre>
</details>

### Get an array of complete tasks

#### URL
<code class="getAPI">GET</code> `/druid/indexer/v1/completeTasks`

This endpoint retrieves an array of completed task objects in the current Druid cluster. This is functionally equivalent to `/druid/indexer/v1/tasks?state=complete`. It supports a set of optional query parameters to filter results. 

#### Query parameters
|Parameter|Type|Description|
|---|---|---|
| `datasource`|String| Return tasks filtered by Druid datasource.|
| `createdTimeInterval`|String (ISO-8601)| Return tasks created within the specified interval. |
| `max`|Integer|Maximum number of `"complete"` tasks to return. Only applies when `state` is set to `"complete"`.|
| `type`|String|Filter tasks by task type. See [task documentation](../ingestion/tasks.md) for more details.|

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved list of complete tasks* 
<!--404 NOT FOUND-->
<br/>
*Request sent to incorrect service* 

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->
```shell
curl "{domain}/druid/indexer/v1/completeTasks"
```
<!--HTTP-->
```HTTP
GET /druid/indexer/v1/completeTasks HTTP/1.1
Host: {domain}
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

<details>
  <summary>Toggle to show sample response</summary>
  <pre><code>[
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
]</code></pre>
</details>

### Get an array of running tasks

#### URL
<code class="getAPI">GET</code> `/druid/indexer/v1/runningTasks`

This endpoint retrieves an array of running task objects in the current Druid cluster. It is functionally equivalent to `/druid/indexer/v1/tasks?state=running`. It supports a set of optional query parameters to filter results. 

#### Query parameters
|Parameter|Type|Description|
|---|---|---|
| `datasource`|String| Return tasks filtered by Druid datasource.|
| `createdTimeInterval`|String (ISO-8601)| Return tasks created within the specified interval. |
| `max`|Integer|Maximum number of `"complete"` tasks to return. Only applies when `state` is set to `"complete"`.|
| `type`|String|Filter tasks by task type. See [task documentation](../ingestion/tasks.md) for more details.|

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved list of running tasks* 

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->
```shell
curl "{domain}/druid/indexer/v1/runningTasks"
```
<!--HTTP-->
```HTTP
GET /druid/indexer/v1/runningTasks HTTP/1.1
Host: {domain}
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

<details>
  <summary>Toggle to show sample response</summary>
  <pre><code>[
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
]</code></pre>
</details>

### Get an array of waiting tasks

#### URL
<code class="getAPI">GET</code> `/druid/indexer/v1/waitingTasks`

This endpoint retrieves an array of waiting task objects in the current Druid cluster. It is functionally equivalent to `/druid/indexer/v1/tasks?state=waiting`. It supports a set of optional query parameters to filter results. 

#### Query parameters
|Parameter|Type|Description|
|---|---|---|
| `datasource`|String| Return tasks filtered by Druid datasource.|
| `createdTimeInterval`|String (ISO-8601)| Return tasks created within the specified interval. |
| `max`|Integer|Maximum number of `"complete"` tasks to return. Only applies when `state` is set to `"complete"`.|
| `type`|String|Filter tasks by task type. See [task documentation](../ingestion/tasks.md) for more details.|

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved list of waiting tasks* 

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->
```shell
curl "{domain}/druid/indexer/v1/waitingTasks"
```
<!--HTTP-->
```HTTP
GET /druid/indexer/v1/waitingTasks HTTP/1.1
Host: {domain}
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

<details>
  <summary>Toggle to show sample response</summary>
  <pre><code>[
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
]</code></pre>
</details>

### Get an array of pending tasks

#### URL

<code class="getAPI">GET</code> `/druid/indexer/v1/pendingTasks`

This endpoint retrieves an array of pending task objects in the current Druid cluster. It is functionally equivalent to `/druid/indexer/v1/tasks?state=pending`. It supports a set of optional query parameters to filter results. 

#### Query parameters
|Parameter|Type|Description|
|---|---|---|
| `datasource`|String| Return tasks filtered by Druid datasource.|
| `createdTimeInterval`|String (ISO-8601)| Return tasks created within the specified interval. |
| `max`|Integer|Maximum number of `"complete"` tasks to return. Only applies when `state` is set to `"complete"`.|
| `type`|String|Filter tasks by task type. See [task documentation](../ingestion/tasks.md) for more details.|

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved list of pending tasks* 

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->
```shell
curl "{domain}/druid/indexer/v1/pendingTasks"
```
<!--HTTP-->
```HTTP
GET /druid/indexer/v1/pendingTasks HTTP/1.1
Host: {domain}
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

<details>
  <summary>Toggle to show sample response</summary>
  <pre><code>[
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
]</code></pre>
</details>

### Get task payload

#### URL
<code class="getAPI">GET</code> `/druid/indexer/v1/task/{taskId}`

Retrieve the 'payload' of a task.
#### Responses 
<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved payload of task* 
<!--404 NOT FOUND-->
<br/>
*Cannot find task with id* 

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample request

The following examples shows how to retrieve the task payload of a task with the specified id `query-32663269-ead9-405a-8eb6-0817a952ef47`.

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->
```shell
curl "{domain}/druid/indexer/v1/task/query-32663269-ead9-405a-8eb6-0817a952ef47"
```
<!--HTTP-->
```HTTP
GET /druid/indexer/v1/task/query-32663269-ead9-405a-8eb6-0817a952ef47 HTTP/1.1
Host: {domain}
```
<!--END_DOCUSAURUS_CODE_TABS-->


#### Sample response

<details>
  <summary>Toggle to show sample response</summary>
  <pre><code>{
    "task": "query-32663269-ead9-405a-8eb6-0817a952ef47",
    "payload": {
        "type": "query_controller",
        "id": "query-32663269-ead9-405a-8eb6-0817a952ef47",
        "spec": {
            "query": {
                "queryType": "scan",
                "dataSource": {
                    "type": "external",
                    "inputSource": {
                        "type": "http",
                        "uris": [
                            "https://druid.apache.org/data/wikipedia.json.gz"
                        ]
                    },
                    "inputFormat": {
                        "type": "json",
                        "keepNullColumns": false,
                        "assumeNewlineDelimited": false,
                        "useJsonNodeReader": false
                    },
                    "signature": [
                        {
                            "name": "added",
                            "type": "LONG"
                        },
                        {
                            "name": "channel",
                            "type": "STRING"
                        },
                        {
                            "name": "cityName",
                            "type": "STRING"
                        },
                        {
                            "name": "comment",
                            "type": "STRING"
                        },
                        {
                            "name": "commentLength",
                            "type": "LONG"
                        },
                        {
                            "name": "countryIsoCode",
                            "type": "STRING"
                        },
                        {
                            "name": "countryName",
                            "type": "STRING"
                        },
                        {
                            "name": "deleted",
                            "type": "LONG"
                        },
                        {
                            "name": "delta",
                            "type": "LONG"
                        },
                        {
                            "name": "deltaBucket",
                            "type": "STRING"
                        },
                        {
                            "name": "diffUrl",
                            "type": "STRING"
                        },
                        {
                            "name": "flags",
                            "type": "STRING"
                        },
                        {
                            "name": "isAnonymous",
                            "type": "STRING"
                        },
                        {
                            "name": "isMinor",
                            "type": "STRING"
                        },
                        {
                            "name": "isNew",
                            "type": "STRING"
                        },
                        {
                            "name": "isRobot",
                            "type": "STRING"
                        },
                        {
                            "name": "isUnpatrolled",
                            "type": "STRING"
                        },
                        {
                            "name": "metroCode",
                            "type": "STRING"
                        },
                        {
                            "name": "namespace",
                            "type": "STRING"
                        },
                        {
                            "name": "page",
                            "type": "STRING"
                        },
                        {
                            "name": "regionIsoCode",
                            "type": "STRING"
                        },
                        {
                            "name": "regionName",
                            "type": "STRING"
                        },
                        {
                            "name": "timestamp",
                            "type": "STRING"
                        },
                        {
                            "name": "user",
                            "type": "STRING"
                        }
                    ]
                },
                "intervals": {
                    "type": "intervals",
                    "intervals": [
                        "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z"
                    ]
                },
                "virtualColumns": [
                    {
                        "type": "expression",
                        "name": "v0",
                        "expression": "timestamp_parse(\"timestamp\",null,'UTC')",
                        "outputType": "LONG"
                    }
                ],
                "resultFormat": "compactedList",
                "columns": [
                    "added",
                    "channel",
                    "cityName",
                    "comment",
                    "commentLength",
                    "countryIsoCode",
                    "countryName",
                    "deleted",
                    "delta",
                    "deltaBucket",
                    "diffUrl",
                    "flags",
                    "isAnonymous",
                    "isMinor",
                    "isNew",
                    "isRobot",
                    "isUnpatrolled",
                    "metroCode",
                    "namespace",
                    "page",
                    "regionIsoCode",
                    "regionName",
                    "timestamp",
                    "user",
                    "v0"
                ],
                "legacy": false,
                "context": {
                    "finalize": true,
                    "maxNumTasks": 3,
                    "maxParseExceptions": 0,
                    "queryId": "32663269-ead9-405a-8eb6-0817a952ef47",
                    "scanSignature": "[{\"name\":\"added\",\"type\":\"LONG\"},{\"name\":\"channel\",\"type\":\"STRING\"},{\"name\":\"cityName\",\"type\":\"STRING\"},{\"name\":\"comment\",\"type\":\"STRING\"},{\"name\":\"commentLength\",\"type\":\"LONG\"},{\"name\":\"countryIsoCode\",\"type\":\"STRING\"},{\"name\":\"countryName\",\"type\":\"STRING\"},{\"name\":\"deleted\",\"type\":\"LONG\"},{\"name\":\"delta\",\"type\":\"LONG\"},{\"name\":\"deltaBucket\",\"type\":\"STRING\"},{\"name\":\"diffUrl\",\"type\":\"STRING\"},{\"name\":\"flags\",\"type\":\"STRING\"},{\"name\":\"isAnonymous\",\"type\":\"STRING\"},{\"name\":\"isMinor\",\"type\":\"STRING\"},{\"name\":\"isNew\",\"type\":\"STRING\"},{\"name\":\"isRobot\",\"type\":\"STRING\"},{\"name\":\"isUnpatrolled\",\"type\":\"STRING\"},{\"name\":\"metroCode\",\"type\":\"STRING\"},{\"name\":\"namespace\",\"type\":\"STRING\"},{\"name\":\"page\",\"type\":\"STRING\"},{\"name\":\"regionIsoCode\",\"type\":\"STRING\"},{\"name\":\"regionName\",\"type\":\"STRING\"},{\"name\":\"timestamp\",\"type\":\"STRING\"},{\"name\":\"user\",\"type\":\"STRING\"},{\"name\":\"v0\",\"type\":\"LONG\"}]",
                    "sqlInsertSegmentGranularity": "\"DAY\"",
                    "sqlQueryId": "32663269-ead9-405a-8eb6-0817a952ef47"
                },
                "granularity": {
                    "type": "all"
                }
            },
            "columnMappings": [
                {
                    "queryColumn": "v0",
                    "outputColumn": "__time"
                },
                {
                    "queryColumn": "added",
                    "outputColumn": "added"
                },
                {
                    "queryColumn": "channel",
                    "outputColumn": "channel"
                },
                {
                    "queryColumn": "cityName",
                    "outputColumn": "cityName"
                },
                {
                    "queryColumn": "comment",
                    "outputColumn": "comment"
                },
                {
                    "queryColumn": "commentLength",
                    "outputColumn": "commentLength"
                },
                {
                    "queryColumn": "countryIsoCode",
                    "outputColumn": "countryIsoCode"
                },
                {
                    "queryColumn": "countryName",
                    "outputColumn": "countryName"
                },
                {
                    "queryColumn": "deleted",
                    "outputColumn": "deleted"
                },
                {
                    "queryColumn": "delta",
                    "outputColumn": "delta"
                },
                {
                    "queryColumn": "deltaBucket",
                    "outputColumn": "deltaBucket"
                },
                {
                    "queryColumn": "diffUrl",
                    "outputColumn": "diffUrl"
                },
                {
                    "queryColumn": "flags",
                    "outputColumn": "flags"
                },
                {
                    "queryColumn": "isAnonymous",
                    "outputColumn": "isAnonymous"
                },
                {
                    "queryColumn": "isMinor",
                    "outputColumn": "isMinor"
                },
                {
                    "queryColumn": "isNew",
                    "outputColumn": "isNew"
                },
                {
                    "queryColumn": "isRobot",
                    "outputColumn": "isRobot"
                },
                {
                    "queryColumn": "isUnpatrolled",
                    "outputColumn": "isUnpatrolled"
                },
                {
                    "queryColumn": "metroCode",
                    "outputColumn": "metroCode"
                },
                {
                    "queryColumn": "namespace",
                    "outputColumn": "namespace"
                },
                {
                    "queryColumn": "page",
                    "outputColumn": "page"
                },
                {
                    "queryColumn": "regionIsoCode",
                    "outputColumn": "regionIsoCode"
                },
                {
                    "queryColumn": "regionName",
                    "outputColumn": "regionName"
                },
                {
                    "queryColumn": "timestamp",
                    "outputColumn": "timestamp"
                },
                {
                    "queryColumn": "user",
                    "outputColumn": "user"
                }
            ],
            "destination": {
                "type": "dataSource",
                "dataSource": "wikipedia_api",
                "segmentGranularity": "DAY"
            },
            "assignmentStrategy": "max",
            "tuningConfig": {
                "maxNumWorkers": 2,
                "maxRowsInMemory": 100000,
                "rowsPerSegment": 3000000
            }
        },
        "sqlQuery": "\nINSERT INTO wikipedia_api \nSELECT \n  TIME_PARSE(\"timestamp\") AS __time,\n  * \nFROM TABLE(EXTERN(\n  '{\"type\": \"http\", \"uris\": [\"https://druid.apache.org/data/wikipedia.json.gz\"]}', \n  '{\"type\": \"json\"}', \n  '[{\"name\": \"added\", \"type\": \"long\"}, {\"name\": \"channel\", \"type\": \"string\"}, {\"name\": \"cityName\", \"type\": \"string\"}, {\"name\": \"comment\", \"type\": \"string\"}, {\"name\": \"commentLength\", \"type\": \"long\"}, {\"name\": \"countryIsoCode\", \"type\": \"string\"}, {\"name\": \"countryName\", \"type\": \"string\"}, {\"name\": \"deleted\", \"type\": \"long\"}, {\"name\": \"delta\", \"type\": \"long\"}, {\"name\": \"deltaBucket\", \"type\": \"string\"}, {\"name\": \"diffUrl\", \"type\": \"string\"}, {\"name\": \"flags\", \"type\": \"string\"}, {\"name\": \"isAnonymous\", \"type\": \"string\"}, {\"name\": \"isMinor\", \"type\": \"string\"}, {\"name\": \"isNew\", \"type\": \"string\"}, {\"name\": \"isRobot\", \"type\": \"string\"}, {\"name\": \"isUnpatrolled\", \"type\": \"string\"}, {\"name\": \"metroCode\", \"type\": \"string\"}, {\"name\": \"namespace\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"regionIsoCode\", \"type\": \"string\"}, {\"name\": \"regionName\", \"type\": \"string\"}, {\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\n  ))\nPARTITIONED BY DAY\n",
        "sqlQueryContext": {
            "sqlQueryId": "32663269-ead9-405a-8eb6-0817a952ef47",
            "sqlInsertSegmentGranularity": "\"DAY\"",
            "maxNumTasks": 3,
            "queryId": "32663269-ead9-405a-8eb6-0817a952ef47"
        },
        "sqlResultsContext": {
            "timeZone": "UTC",
            "serializeComplexValues": true,
            "stringifyArrays": true
        },
        "sqlTypeNames": [
            "TIMESTAMP",
            "BIGINT",
            "VARCHAR",
            "VARCHAR",
            "VARCHAR",
            "BIGINT",
            "VARCHAR",
            "VARCHAR",
            "BIGINT",
            "BIGINT",
            "VARCHAR",
            "VARCHAR",
            "VARCHAR",
            "VARCHAR",
            "VARCHAR",
            "VARCHAR",
            "VARCHAR",
            "VARCHAR",
            "VARCHAR",
            "VARCHAR",
            "VARCHAR",
            "VARCHAR",
            "VARCHAR",
            "VARCHAR",
            "VARCHAR"
        ],
        "context": {
            "forceTimeChunkLock": true,
            "useLineageBasedSegmentAllocation": true
        },
        "groupId": "query-32663269-ead9-405a-8eb6-0817a952ef47",
        "dataSource": "wikipedia_api",
        "resource": {
            "availabilityGroup": "query-32663269-ead9-405a-8eb6-0817a952ef47",
            "requiredCapacity": 1
        }
    }
}</code></pre>
</details>

### Get task status

#### URL
<code class="getAPI">GET</code> `/druid/indexer/v1/task/{taskId}/status`

Retrieve the status of a task.

#### Responses 
<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved task status* 
<!--404 NOT FOUND-->
<br/>
*Cannot find task with id* 
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample request

The following examples shows how to retrieve the status of a task with the specified id `query-223549f8-b993-4483-b028-1b0d54713cad`.

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->
```shell
curl "{domain}/druid/indexer/v1/task/query-223549f8-b993-4483-b028-1b0d54713cad/status"
```
<!--HTTP-->
```HTTP
GET /druid/indexer/v1/task/query-223549f8-b993-4483-b028-1b0d54713cad/status HTTP/1.1
Host: {domain}
```
<!--END_DOCUSAURUS_CODE_TABS-->


#### Sample response
<details>
  <summary>Toggle to show sample response</summary>
  <pre><code>{'task': 'query-223549f8-b993-4483-b028-1b0d54713cad',
 'status': {'id': 'query-223549f8-b993-4483-b028-1b0d54713cad',
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
  'errorMsg': None}
}</code></pre>
</details>

### Get task segments

#### URL

<code class="getAPI">GET</code> `/druid/indexer/v1/task/{taskId}/segments`

> This API is deprecated and will be removed in future releases.

Retrieve information about the segments of a task.

#### Responses
<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved task segments* 
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample request

The following examples shows how to retrieve the task segment of the task with the specified id `query-52a8aafe-7265-4427-89fe-dc51275cc470`.

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->
```shell
curl "{domain}/druid/indexer/v1/task/query-52a8aafe-7265-4427-89fe-dc51275cc470/reports"
```
<!--HTTP-->
```HTTP
GET /druid/indexer/v1/task/query-52a8aafe-7265-4427-89fe-dc51275cc470/reports HTTP/1.1
Host: {domain}
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

A successful request returns a `200 OK` response and an array of the task segments.

### Get task completion report

#### URL

<code class="getAPI">GET</code> `/druid/indexer/v1/task/{taskId}/reports`

Retrieve a [task completion report](../ingestion/tasks.md#task-reports) for a task. Only works for completed tasks.

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved task report* 
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample request

The following examples shows how to retrieve the completion report of a task with the specified id `query-52a8aafe-7265-4427-89fe-dc51275cc470`.

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->
```shell
curl "{domain}/druid/indexer/v1/task/query-52a8aafe-7265-4427-89fe-dc51275cc470/reports"
```
<!--HTTP-->
```HTTP
GET /druid/indexer/v1/task/query-52a8aafe-7265-4427-89fe-dc51275cc470/reports HTTP/1.1
Host: {domain}
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response
<details>
  <summary>Toggle to show sample response</summary>
  <pre><code>{
    "multiStageQuery": {
        "type": "multiStageQuery",
        "taskId": "query-32663269-ead9-405a-8eb6-0817a952ef47",
        "payload": {
            "status": {
                "status": "SUCCESS",
                "startTime": "2023-06-22T22:54:51.514Z",
                "durationMs": 18228,
                "pendingTasks": 0,
                "runningTasks": 2
            },
            "stages": [
                {
                    "stageNumber": 0,
                    "definition": {
                        "id": "bc2df410-080f-4f82-bf34-73725489c7fc_0",
                        "input": [
                            {
                                "type": "external",
                                "inputSource": {
                                    "type": "http",
                                    "uris": [
                                        "https://druid.apache.org/data/wikipedia.json.gz"
                                    ]
                                },
                                "inputFormat": {
                                    "type": "json",
                                    "keepNullColumns": false,
                                    "assumeNewlineDelimited": false,
                                    "useJsonNodeReader": false
                                },
                                "signature": [
                                    {
                                        "name": "added",
                                        "type": "LONG"
                                    },
                                    {
                                        "name": "channel",
                                        "type": "STRING"
                                    },
                                    {
                                        "name": "cityName",
                                        "type": "STRING"
                                    },
                                    {
                                        "name": "comment",
                                        "type": "STRING"
                                    },
                                    {
                                        "name": "commentLength",
                                        "type": "LONG"
                                    },
                                    {
                                        "name": "countryIsoCode",
                                        "type": "STRING"
                                    },
                                    {
                                        "name": "countryName",
                                        "type": "STRING"
                                    },
                                    {
                                        "name": "deleted",
                                        "type": "LONG"
                                    },
                                    {
                                        "name": "delta",
                                        "type": "LONG"
                                    },
                                    {
                                        "name": "deltaBucket",
                                        "type": "STRING"
                                    },
                                    {
                                        "name": "diffUrl",
                                        "type": "STRING"
                                    },
                                    {
                                        "name": "flags",
                                        "type": "STRING"
                                    },
                                    {
                                        "name": "isAnonymous",
                                        "type": "STRING"
                                    },
                                    {
                                        "name": "isMinor",
                                        "type": "STRING"
                                    },
                                    {
                                        "name": "isNew",
                                        "type": "STRING"
                                    },
                                    {
                                        "name": "isRobot",
                                        "type": "STRING"
                                    },
                                    {
                                        "name": "isUnpatrolled",
                                        "type": "STRING"
                                    },
                                    {
                                        "name": "metroCode",
                                        "type": "STRING"
                                    },
                                    {
                                        "name": "namespace",
                                        "type": "STRING"
                                    },
                                    {
                                        "name": "page",
                                        "type": "STRING"
                                    },
                                    {
                                        "name": "regionIsoCode",
                                        "type": "STRING"
                                    },
                                    {
                                        "name": "regionName",
                                        "type": "STRING"
                                    },
                                    {
                                        "name": "timestamp",
                                        "type": "STRING"
                                    },
                                    {
                                        "name": "user",
                                        "type": "STRING"
                                    }
                                ]
                            }
                        ],
                        "processor": {
                            "type": "scan",
                            "query": {
                                "queryType": "scan",
                                "dataSource": {
                                    "type": "inputNumber",
                                    "inputNumber": 0
                                },
                                "intervals": {
                                    "type": "intervals",
                                    "intervals": [
                                        "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z"
                                    ]
                                },
                                "virtualColumns": [
                                    {
                                        "type": "expression",
                                        "name": "v0",
                                        "expression": "timestamp_parse(\"timestamp\",null,'UTC')",
                                        "outputType": "LONG"
                                    }
                                ],
                                "resultFormat": "compactedList",
                                "columns": [
                                    "added",
                                    "channel",
                                    "cityName",
                                    "comment",
                                    "commentLength",
                                    "countryIsoCode",
                                    "countryName",
                                    "deleted",
                                    "delta",
                                    "deltaBucket",
                                    "diffUrl",
                                    "flags",
                                    "isAnonymous",
                                    "isMinor",
                                    "isNew",
                                    "isRobot",
                                    "isUnpatrolled",
                                    "metroCode",
                                    "namespace",
                                    "page",
                                    "regionIsoCode",
                                    "regionName",
                                    "timestamp",
                                    "user",
                                    "v0"
                                ],
                                "legacy": false,
                                "context": {
                                    "__timeColumn": "v0",
                                    "finalize": true,
                                    "maxNumTasks": 3,
                                    "maxParseExceptions": 0,
                                    "queryId": "32663269-ead9-405a-8eb6-0817a952ef47",
                                    "scanSignature": "[{\"name\":\"added\",\"type\":\"LONG\"},{\"name\":\"channel\",\"type\":\"STRING\"},{\"name\":\"cityName\",\"type\":\"STRING\"},{\"name\":\"comment\",\"type\":\"STRING\"},{\"name\":\"commentLength\",\"type\":\"LONG\"},{\"name\":\"countryIsoCode\",\"type\":\"STRING\"},{\"name\":\"countryName\",\"type\":\"STRING\"},{\"name\":\"deleted\",\"type\":\"LONG\"},{\"name\":\"delta\",\"type\":\"LONG\"},{\"name\":\"deltaBucket\",\"type\":\"STRING\"},{\"name\":\"diffUrl\",\"type\":\"STRING\"},{\"name\":\"flags\",\"type\":\"STRING\"},{\"name\":\"isAnonymous\",\"type\":\"STRING\"},{\"name\":\"isMinor\",\"type\":\"STRING\"},{\"name\":\"isNew\",\"type\":\"STRING\"},{\"name\":\"isRobot\",\"type\":\"STRING\"},{\"name\":\"isUnpatrolled\",\"type\":\"STRING\"},{\"name\":\"metroCode\",\"type\":\"STRING\"},{\"name\":\"namespace\",\"type\":\"STRING\"},{\"name\":\"page\",\"type\":\"STRING\"},{\"name\":\"regionIsoCode\",\"type\":\"STRING\"},{\"name\":\"regionName\",\"type\":\"STRING\"},{\"name\":\"timestamp\",\"type\":\"STRING\"},{\"name\":\"user\",\"type\":\"STRING\"},{\"name\":\"v0\",\"type\":\"LONG\"}]",
                                    "sqlInsertSegmentGranularity": "\"DAY\"",
                                    "sqlQueryId": "32663269-ead9-405a-8eb6-0817a952ef47"
                                },
                                "granularity": {
                                    "type": "all"
                                }
                            }
                        },
                        "signature": [
                            {
                                "name": "__bucket",
                                "type": "LONG"
                            },
                            {
                                "name": "__boost",
                                "type": "LONG"
                            },
                            {
                                "name": "added",
                                "type": "LONG"
                            },
                            {
                                "name": "channel",
                                "type": "STRING"
                            },
                            {
                                "name": "cityName",
                                "type": "STRING"
                            },
                            {
                                "name": "comment",
                                "type": "STRING"
                            },
                            {
                                "name": "commentLength",
                                "type": "LONG"
                            },
                            {
                                "name": "countryIsoCode",
                                "type": "STRING"
                            },
                            {
                                "name": "countryName",
                                "type": "STRING"
                            },
                            {
                                "name": "deleted",
                                "type": "LONG"
                            },
                            {
                                "name": "delta",
                                "type": "LONG"
                            },
                            {
                                "name": "deltaBucket",
                                "type": "STRING"
                            },
                            {
                                "name": "diffUrl",
                                "type": "STRING"
                            },
                            {
                                "name": "flags",
                                "type": "STRING"
                            },
                            {
                                "name": "isAnonymous",
                                "type": "STRING"
                            },
                            {
                                "name": "isMinor",
                                "type": "STRING"
                            },
                            {
                                "name": "isNew",
                                "type": "STRING"
                            },
                            {
                                "name": "isRobot",
                                "type": "STRING"
                            },
                            {
                                "name": "isUnpatrolled",
                                "type": "STRING"
                            },
                            {
                                "name": "metroCode",
                                "type": "STRING"
                            },
                            {
                                "name": "namespace",
                                "type": "STRING"
                            },
                            {
                                "name": "page",
                                "type": "STRING"
                            },
                            {
                                "name": "regionIsoCode",
                                "type": "STRING"
                            },
                            {
                                "name": "regionName",
                                "type": "STRING"
                            },
                            {
                                "name": "timestamp",
                                "type": "STRING"
                            },
                            {
                                "name": "user",
                                "type": "STRING"
                            },
                            {
                                "name": "v0",
                                "type": "LONG"
                            }
                        ],
                        "shuffleSpec": {
                            "type": "targetSize",
                            "clusterBy": {
                                "columns": [
                                    {
                                        "columnName": "__bucket",
                                        "order": "ASCENDING"
                                    },
                                    {
                                        "columnName": "__boost",
                                        "order": "ASCENDING"
                                    }
                                ],
                                "bucketByCount": 1
                            },
                            "targetSize": 3000000
                        },
                        "maxWorkerCount": 2,
                        "shuffleCheckHasMultipleValues": true,
                        "maxInputBytesPerWorker": 10737418240
                    },
                    "phase": "FINISHED",
                    "workerCount": 1,
                    "partitionCount": 1,
                    "startTime": "2023-06-22T22:54:51.910Z",
                    "duration": 16482,
                    "sort": true
                },
                {
                    "stageNumber": 1,
                    "definition": {
                        "id": "bc2df410-080f-4f82-bf34-73725489c7fc_1",
                        "input": [
                            {
                                "type": "stage",
                                "stage": 0
                            }
                        ],
                        "processor": {
                            "type": "segmentGenerator",
                            "dataSchema": {
                                "dataSource": "wikipedia_api",
                                "timestampSpec": {
                                    "column": "__time",
                                    "format": "millis",
                                    "missingValue": null
                                },
                                "dimensionsSpec": {
                                    "dimensions": [
                                        {
                                            "type": "long",
                                            "name": "added",
                                            "multiValueHandling": "SORTED_ARRAY",
                                            "createBitmapIndex": false
                                        },
                                        {
                                            "type": "string",
                                            "name": "channel",
                                            "multiValueHandling": "SORTED_ARRAY",
                                            "createBitmapIndex": true
                                        },
                                        {
                                            "type": "string",
                                            "name": "cityName",
                                            "multiValueHandling": "SORTED_ARRAY",
                                            "createBitmapIndex": true
                                        },
                                        {
                                            "type": "string",
                                            "name": "comment",
                                            "multiValueHandling": "SORTED_ARRAY",
                                            "createBitmapIndex": true
                                        },
                                        {
                                            "type": "long",
                                            "name": "commentLength",
                                            "multiValueHandling": "SORTED_ARRAY",
                                            "createBitmapIndex": false
                                        },
                                        {
                                            "type": "string",
                                            "name": "countryIsoCode",
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
                                            "type": "long",
                                            "name": "deleted",
                                            "multiValueHandling": "SORTED_ARRAY",
                                            "createBitmapIndex": false
                                        },
                                        {
                                            "type": "long",
                                            "name": "delta",
                                            "multiValueHandling": "SORTED_ARRAY",
                                            "createBitmapIndex": false
                                        },
                                        {
                                            "type": "string",
                                            "name": "deltaBucket",
                                            "multiValueHandling": "SORTED_ARRAY",
                                            "createBitmapIndex": true
                                        },
                                        {
                                            "type": "string",
                                            "name": "diffUrl",
                                            "multiValueHandling": "SORTED_ARRAY",
                                            "createBitmapIndex": true
                                        },
                                        {
                                            "type": "string",
                                            "name": "flags",
                                            "multiValueHandling": "SORTED_ARRAY",
                                            "createBitmapIndex": true
                                        },
                                        {
                                            "type": "string",
                                            "name": "isAnonymous",
                                            "multiValueHandling": "SORTED_ARRAY",
                                            "createBitmapIndex": true
                                        },
                                        {
                                            "type": "string",
                                            "name": "isMinor",
                                            "multiValueHandling": "SORTED_ARRAY",
                                            "createBitmapIndex": true
                                        },
                                        {
                                            "type": "string",
                                            "name": "isNew",
                                            "multiValueHandling": "SORTED_ARRAY",
                                            "createBitmapIndex": true
                                        },
                                        {
                                            "type": "string",
                                            "name": "isRobot",
                                            "multiValueHandling": "SORTED_ARRAY",
                                            "createBitmapIndex": true
                                        },
                                        {
                                            "type": "string",
                                            "name": "isUnpatrolled",
                                            "multiValueHandling": "SORTED_ARRAY",
                                            "createBitmapIndex": true
                                        },
                                        {
                                            "type": "string",
                                            "name": "metroCode",
                                            "multiValueHandling": "SORTED_ARRAY",
                                            "createBitmapIndex": true
                                        },
                                        {
                                            "type": "string",
                                            "name": "namespace",
                                            "multiValueHandling": "SORTED_ARRAY",
                                            "createBitmapIndex": true
                                        },
                                        {
                                            "type": "string",
                                            "name": "page",
                                            "multiValueHandling": "SORTED_ARRAY",
                                            "createBitmapIndex": true
                                        },
                                        {
                                            "type": "string",
                                            "name": "regionIsoCode",
                                            "multiValueHandling": "SORTED_ARRAY",
                                            "createBitmapIndex": true
                                        },
                                        {
                                            "type": "string",
                                            "name": "regionName",
                                            "multiValueHandling": "SORTED_ARRAY",
                                            "createBitmapIndex": true
                                        },
                                        {
                                            "type": "string",
                                            "name": "timestamp",
                                            "multiValueHandling": "SORTED_ARRAY",
                                            "createBitmapIndex": true
                                        },
                                        {
                                            "type": "string",
                                            "name": "user",
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
                                    "type": "arbitrary",
                                    "queryGranularity": {
                                        "type": "none"
                                    },
                                    "rollup": false,
                                    "intervals": [
                                        "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z"
                                    ]
                                },
                                "transformSpec": {
                                    "filter": null,
                                    "transforms": []
                                }
                            },
                            "columnMappings": [
                                {
                                    "queryColumn": "v0",
                                    "outputColumn": "__time"
                                },
                                {
                                    "queryColumn": "added",
                                    "outputColumn": "added"
                                },
                                {
                                    "queryColumn": "channel",
                                    "outputColumn": "channel"
                                },
                                {
                                    "queryColumn": "cityName",
                                    "outputColumn": "cityName"
                                },
                                {
                                    "queryColumn": "comment",
                                    "outputColumn": "comment"
                                },
                                {
                                    "queryColumn": "commentLength",
                                    "outputColumn": "commentLength"
                                },
                                {
                                    "queryColumn": "countryIsoCode",
                                    "outputColumn": "countryIsoCode"
                                },
                                {
                                    "queryColumn": "countryName",
                                    "outputColumn": "countryName"
                                },
                                {
                                    "queryColumn": "deleted",
                                    "outputColumn": "deleted"
                                },
                                {
                                    "queryColumn": "delta",
                                    "outputColumn": "delta"
                                },
                                {
                                    "queryColumn": "deltaBucket",
                                    "outputColumn": "deltaBucket"
                                },
                                {
                                    "queryColumn": "diffUrl",
                                    "outputColumn": "diffUrl"
                                },
                                {
                                    "queryColumn": "flags",
                                    "outputColumn": "flags"
                                },
                                {
                                    "queryColumn": "isAnonymous",
                                    "outputColumn": "isAnonymous"
                                },
                                {
                                    "queryColumn": "isMinor",
                                    "outputColumn": "isMinor"
                                },
                                {
                                    "queryColumn": "isNew",
                                    "outputColumn": "isNew"
                                },
                                {
                                    "queryColumn": "isRobot",
                                    "outputColumn": "isRobot"
                                },
                                {
                                    "queryColumn": "isUnpatrolled",
                                    "outputColumn": "isUnpatrolled"
                                },
                                {
                                    "queryColumn": "metroCode",
                                    "outputColumn": "metroCode"
                                },
                                {
                                    "queryColumn": "namespace",
                                    "outputColumn": "namespace"
                                },
                                {
                                    "queryColumn": "page",
                                    "outputColumn": "page"
                                },
                                {
                                    "queryColumn": "regionIsoCode",
                                    "outputColumn": "regionIsoCode"
                                },
                                {
                                    "queryColumn": "regionName",
                                    "outputColumn": "regionName"
                                },
                                {
                                    "queryColumn": "timestamp",
                                    "outputColumn": "timestamp"
                                },
                                {
                                    "queryColumn": "user",
                                    "outputColumn": "user"
                                }
                            ],
                            "tuningConfig": {
                                "maxNumWorkers": 2,
                                "maxRowsInMemory": 100000,
                                "rowsPerSegment": 3000000
                            }
                        },
                        "signature": [],
                        "maxWorkerCount": 2,
                        "maxInputBytesPerWorker": 10737418240
                    },
                    "phase": "FINISHED",
                    "workerCount": 1,
                    "partitionCount": 1,
                    "startTime": "2023-06-22T22:55:08.368Z",
                    "duration": 1373
                }
            ],
            "counters": {
                "0": {
                    "0": {
                        "input0": {
                            "type": "channel",
                            "rows": [
                                24433
                            ],
                            "bytes": [
                                11956576
                            ],
                            "files": [
                                1
                            ],
                            "totalFiles": [
                                1
                            ]
                        },
                        "output": {
                            "type": "channel",
                            "rows": [
                                24433
                            ],
                            "bytes": [
                                11936116
                            ],
                            "frames": [
                                2
                            ]
                        },
                        "shuffle": {
                            "type": "channel",
                            "rows": [
                                24433
                            ],
                            "bytes": [
                                11839098
                            ],
                            "frames": [
                                23
                            ]
                        },
                        "sortProgress": {
                            "type": "sortProgress",
                            "totalMergingLevels": 3,
                            "levelToTotalBatches": {
                                "0": 1,
                                "1": 1,
                                "2": 1
                            },
                            "levelToMergedBatches": {
                                "0": 1,
                                "1": 1,
                                "2": 1
                            },
                            "totalMergersForUltimateLevel": 1,
                            "progressDigest": 1.0
                        }
                    }
                },
                "1": {
                    "0": {
                        "input0": {
                            "type": "channel",
                            "rows": [
                                24433
                            ],
                            "bytes": [
                                11839098
                            ],
                            "frames": [
                                23
                            ]
                        },
                        "segmentGenerationProgress": {
                            "type": "segmentGenerationProgress",
                            "rowsProcessed": 24433,
                            "rowsPersisted": 24433,
                            "rowsMerged": 24433,
                            "rowsPushed": 24433
                        }
                    }
                }
            }
        }
    }
}</code></pre>
</details>

### Submit a task

#### URL

<code class="postAPI">POST</code> `/druid/indexer/v1/task`

Endpoint for submitting tasks and supervisor specs to the Overlord. Returns the taskId of the submitted task.

#### Request body
<details>
  <summary>Toggle to show sample request body</summary>
  <pre><code>{
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
}</code></pre>
</details>

#### Responses
<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully submitted task* 
<!--400 BAD REQUEST-->
<br/>
*Missing information in query* 
<!--415 UNSUPPORTED MEDIA TYPE-->
<br/>
*Incorrect request body media type* 
<!--500 Server Error-->
<br/>
*Unexpected token or characters in request body* 
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample request

The following request is an example of submitting a task of type `index_parallel`.

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->
```shell
curl "{domain}/druid/indexer/v1/task" \
--header "Content-Type: application/json" \
--data "{
  \"type\" : \"index_parallel\",
  \"spec\" : {
    \"dataSchema\" : {
      \"dataSource\" : \"wikipedia_auto\",
      \"timestampSpec\": {
        \"column\": \"time\",
        \"format\": \"iso\"
      },
      \"dimensionsSpec\" : {
        \"useSchemaDiscovery\": true
      },
      \"metricsSpec\" : [],
      \"granularitySpec\" : {
        \"type\" : \"uniform\",
        \"segmentGranularity\" : \"day\",
        \"queryGranularity\" : \"none\",
        \"intervals\" : [\"2015-09-12/2015-09-13\"],
        \"rollup\" : false
      }
    },
    \"ioConfig\" : {
      \"type\" : \"index_parallel\",
      \"inputSource\" : {
        \"type\" : \"local\",
        \"baseDir\" : \"quickstart/tutorial/\",
        \"filter\" : \"wikiticker-2015-09-12-sampled.json.gz\"
      },
      \"inputFormat\" : {
        \"type\" : \"json\"
      },
      \"appendToExisting\" : false
    },
    \"tuningConfig\" : {
      \"type\" : \"index_parallel\",
      \"maxRowsPerSegment\" : 5000000,
      \"maxRowsInMemory\" : 25000
    }
  }
}"
```
<!--HTTP-->
```HTTP
POST /druid/indexer/v1/task HTTP/1.1
Host: {domain}
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
  <summary>Toggle to show sample response</summary>
  <pre><code>{
    "task": "index_parallel_wikipedia_odofhkle_2023-06-23T21:07:28.226Z"
}</code></pre>
</details>

### Shut down a task

#### URL
<code class="postAPI">POST</code> `/druid/indexer/v1/task/{taskId}/shutdown`

Shuts down a task if it not already complete. Returns a JSON object with the id of the task that was shutdown successfully.

#### Responses
<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully shut down task* 
<!--404 NOT FOUND-->
<br/>
*Cannot find task with id* 
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample request

The following request is an example of shutting down task of id `query-52as 8aafe-7265-4427-89fe-dc51275cc470`.

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->
```shell
curl --request POST "{domain}/druid/indexer/v1/task/query-52as 8aafe-7265-4427-89fe-dc51275cc470/shutdown"
```
<!--HTTP-->
```HTTP
POST /druid/indexer/v1/task/query-52as 8aafe-7265-4427-89fe-dc51275cc470/shutdown HTTP/1.1
Host: (domain)
```

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response
<details>
  <summary>Toggle to show sample response</summary>
  <pre><code>{'task': 'query-577a83dd-a14e-4380-bd01-c942b781236b'}</code></pre>
</details>

### Shut down all tasks for a datasource

#### URL
<code class="postAPI">POST</code> `/druid/indexer/v1/datasources/{dataSource}/shutdownAllTasks`

Shuts down all tasks for a dataSource.

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully shut down tasks* 
<!--404 NOT FOUND-->
<br/>
*Error or datasource does not have a running task* 
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample request

The following request is an example of shutting down all tasks for datasource `wikipedia_auto`.

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->
```shell
curl --request POST "{domain}/druid/indexer/v1/datasources/wikipedia_auto/shutdownAllTasks"
```
<!--HTTP-->
```HTTP
POST /druid/indexer/v1/datasources/wikipedia_auto/shutdownAllTasks HTTP/1.1
Host: {domain}
```

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response
<details>
  <summary>Toggle to show sample response</summary>
  <pre><code>{
    "dataSource": "wikipedia_api"
}</code></pre>
</details>


### Retrieve status objects for tasks

#### URL
<code class="postAPI">POST</code> `/druid/indexer/v1/taskStatus`

Retrieve list of task status objects for list of task id strings in request body.

#### Request body

A JSON array of task id strings.

<details>
  <summary>Toggle to show sample request body</summary>
  <pre><code>["query-52a8aafe-7265-4427-89fe-dc51275cc470"]</code></pre>
</details>

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved status objects* 
<!--415 UNSUPPORTED MEDIA TYPE-->
<br/>
*Missing request body or incorrect request body type* 
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample request

The following request is an example of retrieving status objects for task id `query-52a8aafe-7265-4427-89fe-dc51275cc470`.

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->
```shell
curl "{domain}/druid/indexer/v1/taskStatus" \
--header "Content-Type: application/json" \
--data "[\"query-52a8aafe-7265-4427-89fe-dc51275cc470\"]"
```
<!--HTTP-->
```HTTP
POST /druid/indexer/v1/taskStatus HTTP/1.1
Host: {domain}
Content-Type: application/json
Content-Length: 46

["query-52a8aafe-7265-4427-89fe-dc51275cc470"]
```
<!--END_DOCUSAURUS_CODE_TABS-->


#### Sample response
<details>
  <summary>Toggle to show sample response</summary>
  <pre><code>{
    "query-52a8aafe-7265-4427-89fe-dc51275cc470": {
        "id": "query-52a8aafe-7265-4427-89fe-dc51275cc470",
        "status": "SUCCESS",
        "duration": 106244,
        "errorMsg": null,
        "location": {
            "host": "localhost",
            "port": 8100,
            "tlsPort": -1
        }
    }
}</code></pre>
</details>

### Clean up pending segments for a data source.

#### URL

<code class="deleteAPI">DELETE</code> `/druid/indexer/v1/pendingSegments/{dataSource}`

Manually clean up pending segments table in metadata storage for `datasource`. Returns a JSON object response with
`numDeleted` and count of rows deleted from the pending segments table. This API is used by the
`druid.coordinator.kill.pendingSegments.on` [coordinator setting](../configuration/index.md#coordinator-operation)
which automates this operation to perform periodically.

#### Responses
<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully deleted pending segments* 

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample request

The following request is an example of cleaning up pending segments for `wikipedia_api` datasource.

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->
```shell
curl --request DELETE "{domain}/druid/indexer/v1/pendingSegments/wikipedia_api"
```
<!--HTTP-->
```HTTP
DELETE /druid/indexer/v1/pendingSegments/wikipedia_api HTTP/1.1
Host: {domain}
```

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response
<details>
  <summary>Toggle to show sample response</summary>
  <pre><code>{
    "numDeleted": 2
}</code></pre>
</details>