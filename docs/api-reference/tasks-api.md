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

In this document, `{domain}` is a placeholder for the server address of deployment. For example, on the quickstart configuration, replace `{domain}` with `http://localhost:8888`.

For query parameters that take an interval, provide ISO 8601 strings delimited by `_` instead of `/`. For example, `2023-06-27_2023-06-28`.

## Task information and retrieval 

### Get an array of tasks

#### URL
<code class="getAPI">GET</code> `/druid/indexer/v1/tasks`

Retrieves an array of all tasks in the Druid cluster. Each task object includes information on its ID, status, associated datasource, and other metadata. 

#### Query parameters

The endpoint supports a set of optional query parameters to filter results. 

|Parameter|Type|Description|
|---|---|---|
|`state`|String|Filter list of tasks by task state, valid options are `running`, `complete`, `waiting`, and `pending`.|
| `datasource`|String| Return tasks filtered by Druid datasource.|
| `createdTimeInterval`|String (ISO-8601)| Return tasks created within the specified interval. |
| `max`|Integer|Maximum number of `complete` tasks to return. Only applies when `state` is set to `complete`.|
| `type`|String|Filter tasks by task type. See [task documentation](../ingestion/tasks.md) for more details.|

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved list of tasks* 
<!--400 BAD REQUEST-->
<br/>
*Invalid `state` query parameter value* 
<!--500 SERVER ERROR-->
<br/>
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

#### URL
<code class="getAPI">GET</code> `/druid/indexer/v1/completeTasks`

Retrieves an array of completed tasks in the Druid cluster. This is functionally equivalent to `/druid/indexer/v1/tasks?state=complete`.

#### Query parameters

The endpoint supports a set of optional query parameters to filter results. 

|Parameter|Type|Description|
|---|---|---|
| `datasource`|String| Return tasks filtered by Druid datasource.|
| `createdTimeInterval`|String (ISO-8601)| Return tasks created within the specified interval. |
| `max`|Integer|Maximum number of `complete` tasks to return. Only applies when `state` is set to `complete`.|
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

---

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

#### URL
<code class="getAPI">GET</code> `/druid/indexer/v1/runningTasks`

Retrieves an array of running task objects in the Druid cluster. It is functionally equivalent to `/druid/indexer/v1/tasks?state=running`.

#### Query parameters

The endpoint supports a set of optional query parameters to filter results. 

|Parameter|Type|Description|
|---|---|---|
| `datasource`|String| Return tasks filtered by Druid datasource.|
| `createdTimeInterval`|String (ISO-8601)| Return tasks created within the specified interval. |
| `max`|Integer|Maximum number of `complete` tasks to return. Only applies when `state` is set to `complete`.|
| `type`|String|Filter tasks by task type. See [task documentation](../ingestion/tasks.md) for more details.|

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved list of running tasks* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

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

#### URL
<code class="getAPI">GET</code> `/druid/indexer/v1/waitingTasks`

Retrieves an array of waiting tasks in the Druid cluster. It is functionally equivalent to `/druid/indexer/v1/tasks?state=waiting`.

#### Query parameters

The endpoint supports a set of optional query parameters to filter results. 

|Parameter|Type|Description|
|---|---|---|
| `datasource`|String| Return tasks filtered by Druid datasource.|
| `createdTimeInterval`|String (ISO-8601)| Return tasks created within the specified interval. |
| `max`|Integer|Maximum number of `complete` tasks to return. Only applies when `state` is set to `complete`.|
| `type`|String|Filter tasks by task type. See [task documentation](../ingestion/tasks.md) for more details.|

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved list of waiting tasks* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

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

#### URL

<code class="getAPI">GET</code> `/druid/indexer/v1/pendingTasks`

Retrieves an array of pending tasks in the Druid cluster. It is functionally equivalent to `/druid/indexer/v1/tasks?state=pending`. 

#### Query parameters

The endpoint supports a set of optional query parameters to filter results. 

|Parameter|Type|Description|
|---|---|---|
| `datasource`|String| Return tasks filtered by Druid datasource.|
| `createdTimeInterval`|String (ISO-8601)| Return tasks created within the specified interval. |
| `max`|Integer|Maximum number of `complete` tasks to return. Only applies when `state` is set to `complete`.|
| `type`|String|Filter tasks by task type. See [task documentation](../ingestion/tasks.md) for more details.|

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved list of pending tasks* 
<!--END_DOCUSAURUS_CODE_TABS-->

---

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

#### URL
<code class="getAPI">GET</code> `/druid/indexer/v1/task/{taskId}`

Retrieves the payload of a task given the task ID. It returns a JSON object with the task ID and payload that includes task configuration details and relevant specifications associated with the execution of the task.

#### Responses 
<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved payload of task* 
<!--404 NOT FOUND-->
<br/>
*Cannot find task with ID* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

The following examples shows how to retrieve the task payload of a task with the specified ID `query-32663269-ead9-405a-8eb6-0817a952ef47`.

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
  <summary>Click to show sample response</summary>
  
  ```json
  {
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
  }
  ```

</details>

### Get task status

#### URL
<code class="getAPI">GET</code> `/druid/indexer/v1/task/{taskId}/status`

Retrieves the status of a task given the task ID. It returns a JSON object with the task's current state, task type, datasource, and other relevant metadata.

#### Responses 
<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved task status* 
<!--404 NOT FOUND-->
<br/>
*Cannot find task with ID* 
<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

The following examples shows how to retrieve the status of a task with the specified ID `query-223549f8-b993-4483-b028-1b0d54713cad`.

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

#### URL

<code class="getAPI">GET</code> `/druid/indexer/v1/task/{taskId}/segments`

> This API is deprecated and will be removed in future releases.

Retrieves information about segments generated by the task given the task ID. To hit this endpoint, make sure to enable the audit log config on the Overlord with `druid.indexer.auditLog.enabled = true`.

In addition to enabling audit logs, configure a cleanup strategy to prevent overloading the metadata store with old audit logs which may cause performance issues. To enable automated cleanup of audit logs on the Coordinator, set `druid.coordinator.kill.audit.on`. You may also manually export the audit logs to external storage. For more information, see [Audit records](../operations/clean-metadata-store.md#audit-records).

#### Responses
<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved task segments* 
<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

The following examples shows how to retrieve the task segment of the task with the specified ID `query-52a8aafe-7265-4427-89fe-dc51275cc470`.

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

Retrieves a [task completion report](../ingestion/tasks.md#task-reports) for a task. It returns a JSON object with information about the number of rows ingested, and any parse exceptions that Druid raised.

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved task report* 
<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

The following examples shows how to retrieve the completion report of a task with the specified ID `query-52a8aafe-7265-4427-89fe-dc51275cc470`.

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

#### URL

<code class="postAPI">POST</code> `/druid/indexer/v1/task`

Submits a task or supervisor spec to the Overlord. It returns the task ID of the submitted task.

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

---

#### Sample request

The following request is an example of submitting a task to create a datasource named `"wikipedia auto"`.

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
  <summary>Click to show sample response</summary>

  ```json
  {
      "task": "index_parallel_wikipedia_odofhkle_2023-06-23T21:07:28.226Z"
  }
  ```

</details>

### Shut down a task

#### URL
<code class="postAPI">POST</code> `/druid/indexer/v1/task/{taskId}/shutdown`

Shuts down a task if it not already complete. Returns a JSON object with the ID of the task that was shut down successfully.

#### Responses
<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully shut down task* 
<!--404 NOT FOUND-->
<br/>
*Cannot find task with ID* 
<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

The following request shows how to shut down a task with the ID `query-52as 8aafe-7265-4427-89fe-dc51275cc470`.

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
  <summary>Click to show sample response</summary>

  ```json
  {
    'task': 'query-577a83dd-a14e-4380-bd01-c942b781236b'
  }
  ```

</details>

### Shut down all tasks for a datasource

#### URL
<code class="postAPI">POST</code> `/druid/indexer/v1/datasources/{dataSource}/shutdownAllTasks`

Shuts down all tasks for a specified datasource. If successful, it returns a JSON object with the name of the datasource whose tasks are shut down.

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully shut down tasks* 
<!--404 NOT FOUND-->
<br/>
*Error or datasource does not have a running task* 
<!--END_DOCUSAURUS_CODE_TABS-->

---

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
  <summary>Click to show sample response</summary>
  
  ```json
  {
    "dataSource": "wikipedia_api"
  }
  ```

</details>

## Task management

### Retrieve status objects for tasks

#### URL
<code class="postAPI">POST</code> `/druid/indexer/v1/taskStatus`

Retrieves list of task status objects for list of task ID strings in request body. It returns a set of JSON objects with the status, duration, location of each task, and any error messages.

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved status objects* 
<!--415 UNSUPPORTED MEDIA TYPE-->
<br/>
*Missing request body or incorrect request body type* 
<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

The following request is an example of retrieving status objects for task ID `index_parallel_wikipedia_auto_jndhkpbo_2023-06-26T17:23:05.308Z` and `index_parallel_wikipedia_auto_jbgiianh_2023-06-26T23:17:56.769Z` .

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->
```shell
curl "{domain}/druid/indexer/v1/taskStatus" \
--header "Content-Type: application/json" \
--data "[\"index_parallel_wikipedia_auto_jndhkpbo_2023-06-26T17:23:05.308Z\", \"index_parallel_wikipedia_auto_jbgiianh_2023-06-26T23:17:56.769Z\"]"
```
<!--HTTP-->
```HTTP
POST /druid/indexer/v1/taskStatus HTTP/1.1
Host: {domain}
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

#### URL

<code class="deleteAPI">DELETE</code> `/druid/indexer/v1/pendingSegments/{dataSource}`

Manually clean up pending segments table in metadata storage for `datasource`. It returns a JSON object response with
`numDeleted` for the number of rows deleted from the pending segments table. This API is used by the
`druid.coordinator.kill.pendingSegments.on` [Coordinator setting](../configuration/index.md#coordinator-operation)
which automates this operation to perform periodically.

#### Responses
<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully deleted pending segments* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

The following request is an example of cleaning up pending segments for the `wikipedia_api` datasource.

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
  <summary>Click to show sample response</summary>
  
  ```json
  {
    "numDeleted": 2
  }
  ```

</details>