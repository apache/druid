---
id: sql-ingestion-api
title: SQL-based ingestion API
sidebar_label: SQL-based ingestion
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

:::info
 This page describes SQL-based batch ingestion using the [`druid-multi-stage-query`](../multi-stage-query/index.md)
 extension, new in Druid 24.0. Refer to the [ingestion methods](../ingestion/index.md#batch) table to determine which
 ingestion method is right for you.
:::

The **Query** view in the web console provides a friendly experience for the multi-stage query task engine (MSQ task engine) and multi-stage query architecture. We recommend using the web console if you don't need a programmatic interface.

When using the API for the MSQ task engine, the action you want to take determines the endpoint you use:

- `/druid/v2/sql/task`: Submit a query for ingestion.
- `/druid/indexer/v1/task`: Interact with a query, including getting its status or details, or canceling the query. This page describes a few of the Overlord Task APIs that you can use with the MSQ task engine. For information about Druid APIs, see the [API reference for Druid](../ingestion/tasks.md).

In this topic, `http://ROUTER_IP:ROUTER_PORT` is a placeholder for your Router service address and port. Replace it with the information for your deployment. For example, use `http://localhost:8888` for quickstart deployments.

## Submit a query

Submits queries to the MSQ task engine.

The `/druid/v2/sql/task` endpoint accepts the following:

- [SQL requests in the JSON-over-HTTP form](sql-api.md#request-body) using the
`query`, `context`, and `parameters` fields. The endpoint ignores the `resultFormat`, `header`, `typesHeader`, and `sqlTypesHeader` fields.
- [INSERT](../multi-stage-query/reference.md#insert) and [REPLACE](../multi-stage-query/reference.md#replace) statements.
- SELECT queries (experimental feature). SELECT query results are collected from workers by the controller, and written into the [task report](#get-the-report-for-a-query-task) as an array of arrays. The behavior and result format of plain SELECT queries (without INSERT or REPLACE) is subject to change.

### URL

`POST` `/druid/v2/sql/task`

### Responses

<Tabs>

<TabItem value="1" label="200 SUCCESS">


*Successfully submitted query*

</TabItem>
<TabItem value="2" label="400 BAD REQUEST">


*Error thrown due to bad query. Returns a JSON object detailing the error with the following format:*

```json
{
    "error": "A well-defined error code.",
    "errorMessage": "A message with additional details about the error.",
    "errorClass": "Class of exception that caused this error.",
    "host": "The host on which the error occurred."
}
```
</TabItem>
<TabItem value="3" label="500 INTERNAL SERVER ERROR">


*Request not sent due to unexpected conditions. Returns a JSON object detailing the error with the following format:*

```json
{
    "error": "A well-defined error code.",
    "errorMessage": "A message with additional details about the error.",
    "errorClass": "Class of exception that caused this error.",
    "host": "The host on which the error occurred."
}
```

</TabItem>
</Tabs>

---

### Sample request

The following example shows a query that fetches data from an external JSON source and inserts it into a table named `wikipedia`.

<Tabs>

<TabItem value="4" label="HTTP">


```HTTP
POST /druid/v2/sql/task HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
Content-Type: application/json

{
  "query": "INSERT INTO wikipedia\nSELECT\n  TIME_PARSE(\"timestamp\") AS __time,\n  *\nFROM TABLE(\n  EXTERN(\n    '{\"type\": \"http\", \"uris\": [\"https://druid.apache.org/data/wikipedia.json.gz\"]}',\n    '{\"type\": \"json\"}',\n    '[{\"name\": \"added\", \"type\": \"long\"}, {\"name\": \"channel\", \"type\": \"string\"}, {\"name\": \"cityName\", \"type\": \"string\"}, {\"name\": \"comment\", \"type\": \"string\"}, {\"name\": \"commentLength\", \"type\": \"long\"}, {\"name\": \"countryIsoCode\", \"type\": \"string\"}, {\"name\": \"countryName\", \"type\": \"string\"}, {\"name\": \"deleted\", \"type\": \"long\"}, {\"name\": \"delta\", \"type\": \"long\"}, {\"name\": \"deltaBucket\", \"type\": \"string\"}, {\"name\": \"diffUrl\", \"type\": \"string\"}, {\"name\": \"flags\", \"type\": \"string\"}, {\"name\": \"isAnonymous\", \"type\": \"string\"}, {\"name\": \"isMinor\", \"type\": \"string\"}, {\"name\": \"isNew\", \"type\": \"string\"}, {\"name\": \"isRobot\", \"type\": \"string\"}, {\"name\": \"isUnpatrolled\", \"type\": \"string\"}, {\"name\": \"metroCode\", \"type\": \"string\"}, {\"name\": \"namespace\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"regionIsoCode\", \"type\": \"string\"}, {\"name\": \"regionName\", \"type\": \"string\"}, {\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\n  )\n)\nPARTITIONED BY DAY",
  "context": {
    "maxNumTasks": 3
  }
}
```

</TabItem>

<TabItem value="5" label="cURL">


```shell
curl --location --request POST 'http://ROUTER_IP:ROUTER_PORT/druid/v2/sql/task' \
  --header 'Content-Type: application/json' \
  --data-raw '{
    "query": "INSERT INTO wikipedia\nSELECT\n  TIME_PARSE(\"timestamp\") AS __time,\n  *\nFROM TABLE(\n  EXTERN(\n    '\''{\"type\": \"http\", \"uris\": [\"https://druid.apache.org/data/wikipedia.json.gz\"]}'\'',\n    '\''{\"type\": \"json\"}'\'',\n    '\''[{\"name\": \"added\", \"type\": \"long\"}, {\"name\": \"channel\", \"type\": \"string\"}, {\"name\": \"cityName\", \"type\": \"string\"}, {\"name\": \"comment\", \"type\": \"string\"}, {\"name\": \"commentLength\", \"type\": \"long\"}, {\"name\": \"countryIsoCode\", \"type\": \"string\"}, {\"name\": \"countryName\", \"type\": \"string\"}, {\"name\": \"deleted\", \"type\": \"long\"}, {\"name\": \"delta\", \"type\": \"long\"}, {\"name\": \"deltaBucket\", \"type\": \"string\"}, {\"name\": \"diffUrl\", \"type\": \"string\"}, {\"name\": \"flags\", \"type\": \"string\"}, {\"name\": \"isAnonymous\", \"type\": \"string\"}, {\"name\": \"isMinor\", \"type\": \"string\"}, {\"name\": \"isNew\", \"type\": \"string\"}, {\"name\": \"isRobot\", \"type\": \"string\"}, {\"name\": \"isUnpatrolled\", \"type\": \"string\"}, {\"name\": \"metroCode\", \"type\": \"string\"}, {\"name\": \"namespace\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"regionIsoCode\", \"type\": \"string\"}, {\"name\": \"regionName\", \"type\": \"string\"}, {\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\''\n  )\n)\nPARTITIONED BY DAY",
    "context": {
        "maxNumTasks": 3
    }
  }'
```

</TabItem>

<TabItem value="6" label="Python">


```python
import json
import requests

url = "http://ROUTER_IP:ROUTER_PORT/druid/v2/sql/task"

payload = json.dumps({
  "query": "INSERT INTO wikipedia\nSELECT\n  TIME_PARSE(\"timestamp\") AS __time,\n  *\nFROM TABLE(\n  EXTERN(\n    '{\"type\": \"http\", \"uris\": [\"https://druid.apache.org/data/wikipedia.json.gz\"]}',\n    '{\"type\": \"json\"}',\n    '[{\"name\": \"added\", \"type\": \"long\"}, {\"name\": \"channel\", \"type\": \"string\"}, {\"name\": \"cityName\", \"type\": \"string\"}, {\"name\": \"comment\", \"type\": \"string\"}, {\"name\": \"commentLength\", \"type\": \"long\"}, {\"name\": \"countryIsoCode\", \"type\": \"string\"}, {\"name\": \"countryName\", \"type\": \"string\"}, {\"name\": \"deleted\", \"type\": \"long\"}, {\"name\": \"delta\", \"type\": \"long\"}, {\"name\": \"deltaBucket\", \"type\": \"string\"}, {\"name\": \"diffUrl\", \"type\": \"string\"}, {\"name\": \"flags\", \"type\": \"string\"}, {\"name\": \"isAnonymous\", \"type\": \"string\"}, {\"name\": \"isMinor\", \"type\": \"string\"}, {\"name\": \"isNew\", \"type\": \"string\"}, {\"name\": \"isRobot\", \"type\": \"string\"}, {\"name\": \"isUnpatrolled\", \"type\": \"string\"}, {\"name\": \"metroCode\", \"type\": \"string\"}, {\"name\": \"namespace\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"regionIsoCode\", \"type\": \"string\"}, {\"name\": \"regionName\", \"type\": \"string\"}, {\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\n  )\n)\nPARTITIONED BY DAY",
  "context": {
    "maxNumTasks": 3
  }
})
headers = {
  'Content-Type': 'application/json'
}

response = requests.post(url, headers=headers, data=payload)

print(response.text)

```

</TabItem>

</Tabs>

### Sample response

<details>
  <summary>View the response</summary>

```json
{
  "taskId": "query-f795a235-4dc7-4fef-abac-3ae3f9686b79",
  "state": "RUNNING",
}
```
</details>

**Response fields**

| Field | Description |
|---|---|
| `taskId` | Controller task ID. You can use Druid's standard [Tasks API](./tasks-api.md) to interact with this controller task. |
| `state` | Initial state for the query. |

## Get the status for a query task

Retrieves the status of a query task. It returns a JSON object with the task's status code, runner status, task type, datasource, and other relevant metadata.

### URL

`GET` `/druid/indexer/v1/task/{taskId}/status`

### Responses

<Tabs>

<TabItem value="7" label="200 SUCCESS">


<br/>

*Successfully retrieved task status*

</TabItem>
<TabItem value="8" label="404 NOT FOUND">


<br/>

*Cannot find task with ID*

</TabItem>
</Tabs>

---

### Sample request

The following example shows how to retrieve the status of a task with the ID `query-3dc0c45d-34d7-4b15-86c9-cdb2d3ebfc4e`.

<Tabs>

<TabItem value="9" label="HTTP">

```HTTP
GET /druid/indexer/v1/task/query-3dc0c45d-34d7-4b15-86c9-cdb2d3ebfc4e/status HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

</TabItem>

<TabItem value="10" label="cURL">


```shell
curl --location --request GET 'http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/task/query-3dc0c45d-34d7-4b15-86c9-cdb2d3ebfc4e/status'
```

</TabItem>

<TabItem value="11" label="Python">


```python
import requests

url = "http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/task/query-3dc0c45d-34d7-4b15-86c9-cdb2d3ebfc4e/status"

payload={}
headers = {}

response = requests.post(url, headers=headers, data=payload)

print(response.text)
print(response.text)
```

</TabItem>

</Tabs>

### Sample response

<details>
  <summary>View the response</summary>

```json
{
  "task": "query-3dc0c45d-34d7-4b15-86c9-cdb2d3ebfc4e",
  "status": {
    "id": "query-3dc0c45d-34d7-4b15-86c9-cdb2d3ebfc4e",
    "groupId": "query-3dc0c45d-34d7-4b15-86c9-cdb2d3ebfc4e",
    "type": "query_controller",
    "createdTime": "2022-09-14T22:12:00.183Z",
    "queueInsertionTime": "1970-01-01T00:00:00.000Z",
    "statusCode": "RUNNING",
    "status": "RUNNING",
    "runnerStatusCode": "RUNNING",
    "duration": -1,
    "location": {
      "host": "localhost",
      "port": 8100,
      "tlsPort": -1
    },
    "dataSource": "kttm_simple",
    "errorMsg": null
  }
}
```
</details>

## Get the report for a query task

Retrieves the task report for a query.
The report provides detailed information about the query task, including things like the stages, warnings, and errors.

Keep the following in mind when using the task API to view reports:

- The task report for an entire job is associated with the `query_controller` task. The `query_worker` tasks don't have their own reports; their information is incorporated into the controller report.
- The task report API may report `404 Not Found` temporarily while the task is in the process of starting up.
- As an experimental feature, the MSQ task engine supports running SELECT queries. SELECT query results are written into
the `multiStageQuery.payload.results.results` task report key as an array of arrays. The behavior and result format of plain
SELECT queries (without INSERT or REPLACE) is subject to change.
- `multiStageQuery.payload.results.resultsTruncated` denotes whether the results of the report have been truncated to prevent the reports from blowing up.

For an explanation of the fields in a report, see [Report response fields](#report-response-fields).

### URL


`GET` `/druid/indexer/v1/task/{taskId}/reports`

### Responses

<Tabs>

<TabItem value="12" label="200 SUCCESS">


<br/>

*Successfully retrieved task report*

</TabItem>
</Tabs>

---

### Sample request

The following example shows how to retrieve the report for a query with the task ID `query-3dc0c45d-34d7-4b15-86c9-cdb2d3ebfc4e`.

<Tabs>

<TabItem value="13" label="HTTP">

```HTTP
GET /druid/indexer/v1/task/query-3dc0c45d-34d7-4b15-86c9-cdb2d3ebfc4e/reports HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

</TabItem>

<TabItem value="14" label="cURL">


```shell
curl --location --request GET 'http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/task/query-3dc0c45d-34d7-4b15-86c9-cdb2d3ebfc4e/reports'
```

</TabItem>

<TabItem value="15" label="Python">


```python
import requests

url = "http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/task/query-3dc0c45d-34d7-4b15-86c9-cdb2d3ebfc4e/reports"

headers = {}

response = requests.post(url, headers=headers, data=payload)

print(response.text)
print(response.text)
```

</TabItem>

</Tabs>

### Sample response

The response shows an example report for a query.

<details><summary>View the response</summary>

```json
{
  "multiStageQuery": {
    "type": "multiStageQuery",
    "taskId": "query-3dc0c45d-34d7-4b15-86c9-cdb2d3ebfc4e",
    "payload": {
      "status": {
        "status": "SUCCESS",
        "startTime": "2022-09-14T22:12:09.266Z",
        "durationMs": 28227,
        "workers": {
          "0": [
            {
              "workerId": "query-3dc0c45d-34d7-4b15-86c9-cdb2d3ebfc4e-worker0_0",
              "state": "SUCCESS",
              "durationMs": 15511,
              "pendingMs": 137
            }
          ]
        },
        "pendingTasks": 0,
        "runningTasks": 2,
        "segmentLoadWaiterStatus": {
          "state": "SUCCESS",
          "dataSource": "kttm_simple",
          "startTime": "2022-09-14T23:12:09.266Z",
          "duration": 15,
          "totalSegments": 1,
          "usedSegments": 1,
          "precachedSegments": 0,
          "onDemandSegments": 0,
          "pendingSegments": 0,
          "unknownSegments": 0
        },
        "segmentReport": {
          "shardSpec": "NumberedShardSpec",
          "details": "Cannot use RangeShardSpec, RangedShardSpec only supports string CLUSTER BY keys. Using NumberedShardSpec instead."
        }
      },
      "stages": [
        {
          "stageNumber": 0,
          "definition": {
            "id": "71ecb11e-09d7-42f8-9225-1662c8e7e121_0",
            "input": [
              {
                "type": "external",
                "inputSource": {
                  "type": "http",
                  "uris": [
                    "https://static.imply.io/example-data/kttm-v2/kttm-v2-2019-08-25.json.gz"
                  ],
                  "httpAuthenticationUsername": null,
                  "httpAuthenticationPassword": null
                },
                "inputFormat": {
                  "type": "json",
                  "flattenSpec": null,
                  "featureSpec": {},
                  "keepNullColumns": false
                },
                "signature": [
                  {
                    "name": "timestamp",
                    "type": "STRING"
                  },
                  {
                    "name": "agent_category",
                    "type": "STRING"
                  },
                  {
                    "name": "agent_type",
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
                "resultFormat": "compactedList",
                "columns": [
                  "agent_category",
                  "agent_type",
                  "timestamp"
                ],
                "context": {
                  "finalize": false,
                  "finalizeAggregations": false,
                  "groupByEnableMultiValueUnnesting": false,
                  "scanSignature": "[{\"name\":\"agent_category\",\"type\":\"STRING\"},{\"name\":\"agent_type\",\"type\":\"STRING\"},{\"name\":\"timestamp\",\"type\":\"STRING\"}]",
                  "sqlInsertSegmentGranularity": "{\"type\":\"all\"}",
                  "sqlQueryId": "3dc0c45d-34d7-4b15-86c9-cdb2d3ebfc4e",
                  "sqlReplaceTimeChunks": "all"
                },
                "granularity": {
                  "type": "all"
                }
              }
            },
            "signature": [
              {
                "name": "__boost",
                "type": "LONG"
              },
              {
                "name": "agent_category",
                "type": "STRING"
              },
              {
                "name": "agent_type",
                "type": "STRING"
              },
              {
                "name": "timestamp",
                "type": "STRING"
              }
            ],
            "shuffleSpec": {
              "type": "targetSize",
              "clusterBy": {
                "columns": [
                  {
                    "columnName": "__boost"
                  }
                ]
              },
              "targetSize": 3000000
            },
            "maxWorkerCount": 1,
            "shuffleCheckHasMultipleValues": true
          },
          "phase": "FINISHED",
          "workerCount": 1,
          "partitionCount": 1,
          "startTime": "2022-09-14T22:12:11.663Z",
          "duration": 19965,
          "sort": true
        },
        {
          "stageNumber": 1,
          "definition": {
            "id": "71ecb11e-09d7-42f8-9225-1662c8e7e121_1",
            "input": [
              {
                "type": "stage",
                "stage": 0
              }
            ],
            "processor": {
              "type": "segmentGenerator",
              "dataSchema": {
                "dataSource": "kttm_simple",
                "timestampSpec": {
                  "column": "__time",
                  "format": "millis",
                  "missingValue": null
                },
                "dimensionsSpec": {
                  "dimensions": [
                    {
                      "type": "string",
                      "name": "timestamp",
                      "multiValueHandling": "SORTED_ARRAY",
                      "createBitmapIndex": true
                    },
                    {
                      "type": "string",
                      "name": "agent_category",
                      "multiValueHandling": "SORTED_ARRAY",
                      "createBitmapIndex": true
                    },
                    {
                      "type": "string",
                      "name": "agent_type",
                      "multiValueHandling": "SORTED_ARRAY",
                      "createBitmapIndex": true
                    }
                  ],
                  "dimensionExclusions": [
                    "__time"
                  ],
                  "includeAllDimensions": false
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
                  "queryColumn": "timestamp",
                  "outputColumn": "timestamp"
                },
                {
                  "queryColumn": "agent_category",
                  "outputColumn": "agent_category"
                },
                {
                  "queryColumn": "agent_type",
                  "outputColumn": "agent_type"
                }
              ],
              "tuningConfig": {
                "maxNumWorkers": 1,
                "maxRowsInMemory": 100000,
                "rowsPerSegment": 3000000
              }
            },
            "signature": [],
            "maxWorkerCount": 1
          },
          "phase": "FINISHED",
          "workerCount": 1,
          "partitionCount": 1,
          "startTime": "2022-09-14T22:12:31.602Z",
          "duration": 5891
        }
      ],
      "counters": {
        "0": {
          "0": {
            "input0": {
              "type": "channel",
              "rows": [
                465346
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
                465346
              ],
              "bytes": [
                43694447
              ],
              "frames": [
                7
              ]
            },
            "shuffle": {
              "type": "channel",
              "rows": [
                465346
              ],
              "bytes": [
                41835307
              ],
              "frames": [
                73
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
              "progressDigest": 1
            }
          }
        },
        "1": {
          "0": {
            "input0": {
              "type": "channel",
              "rows": [
                465346
              ],
              "bytes": [
                41835307
              ],
              "frames": [
                73
              ]
            },
            "segmentGenerationProgress": {
              "type": "segmentGenerationProgress",
              "rowsProcessed": 465346,
              "rowsPersisted": 465346,
              "rowsMerged": 465346
            }
          }
        }
      }
    }
  }
}
```

</details>

<a name="report-response-fields"></a>

The following table describes the response fields when you retrieve a report for a MSQ task engine using the `/druid/indexer/v1/task/{taskId}/reports` endpoint:

| Field | Description |
|---|---|
| `multiStageQuery.taskId` | Controller task ID. |
| `multiStageQuery.payload.status` | Query status container. |
| `multiStageQuery.payload.status.status` | RUNNING, SUCCESS, or FAILED. |
| `multiStageQuery.payload.status.startTime` | Start time of the query in ISO format. Only present if the query has started running. |
| `multiStageQuery.payload.status.durationMs` | Milliseconds elapsed after the query has started running. -1 denotes that the query hasn't started running yet. |
| `multiStageQuery.payload.status.workers` | Workers for the controller task.|
| `multiStageQuery.payload.status.workers.<workerNumber>` | Array of worker tasks including retries. |
| `multiStageQuery.payload.status.workers.<workerNumber>[].workerId` | Id of the worker task.| |
| `multiStageQuery.payload.status.workers.<workerNumber>[].status` | RUNNING, SUCCESS, or FAILED.|
| `multiStageQuery.payload.status.workers.<workerNumber>[].durationMs` | Milliseconds elapsed between when the worker task was first requested and when it finished. It is -1 for worker tasks with status RUNNING.|
| `multiStageQuery.payload.status.workers.<workerNumber>[].pendingMs` | Milliseconds elapsed between when the worker task was first requested and when it fully started RUNNING. Actual work time can be calculated using `actualWorkTimeMS = durationMs - pendingMs`.|
| `multiStageQuery.payload.status.pendingTasks` | Number of tasks that are not fully started. -1 denotes that the number is currently unknown. |
| `multiStageQuery.payload.status.runningTasks` | Number of currently running tasks. Should be at least 1 since the controller is included. |
| `multiStageQuery.payload.status.segmentLoadStatus` | Segment loading container. Only present after the segments have been published. |
| `multiStageQuery.payload.status.segmentLoadStatus.state` | Either INIT, WAITING, SUCCESS, FAILED or TIMED_OUT. |
| `multiStageQuery.payload.status.segmentLoadStatus.startTime` | Time since which the controller has been waiting for the segments to finish loading. |
| `multiStageQuery.payload.status.segmentLoadStatus.duration` | The duration in milliseconds that the controller has been waiting for the segments to load. |
| `multiStageQuery.payload.status.segmentLoadStatus.totalSegments` | The total number of segments generated by the job. This includes tombstone segments (if any). |
| `multiStageQuery.payload.status.segmentLoadStatus.usedSegments` | The number of segments which are marked as used based on the load rules. Unused segments can be cleaned up at any time. |
| `multiStageQuery.payload.status.segmentLoadStatus.precachedSegments` | The number of segments which are marked as precached and served by historicals, as per the load rules. |
| `multiStageQuery.payload.status.segmentLoadStatus.onDemandSegments` | The number of segments which are not loaded on any historical, as per the load rules. |
| `multiStageQuery.payload.status.segmentLoadStatus.pendingSegments` | The number of segments remaining to be loaded. |
| `multiStageQuery.payload.status.segmentLoadStatus.unknownSegments` | The number of segments whose status is unknown. |
| `multiStageQuery.payload.status.segmentReport` | Segment report. Only present if the query is an ingestion. |
| `multiStageQuery.payload.status.segmentReport.shardSpec` | Contains the shard spec chosen. |
| `multiStageQuery.payload.status.segmentReport.details` | Contains further reasoning about the shard spec chosen. |
| `multiStageQuery.payload.status.errorReport` | Error object. Only present if there was an error. |
| `multiStageQuery.payload.status.errorReport.taskId` | The task that reported the error, if known. May be a controller task or a worker task. |
| `multiStageQuery.payload.status.errorReport.host` | The hostname and port of the task that reported the error, if known. |
| `multiStageQuery.payload.status.errorReport.stageNumber` | The stage number that reported the error, if it happened during execution of a specific stage. |
| `multiStageQuery.payload.status.errorReport.error` | Error object. Contains `errorCode` at a minimum, and may contain other fields as described in the [error code table](../multi-stage-query/reference.md#error-codes). Always present if there is an error. |
| `multiStageQuery.payload.status.errorReport.error.errorCode` | One of the error codes from the [error code table](../multi-stage-query/reference.md#error-codes). Always present if there is an error. |
| `multiStageQuery.payload.status.errorReport.error.errorMessage` | User-friendly error message. Not always present, even if there is an error. |
| `multiStageQuery.payload.status.errorReport.exceptionStackTrace` | Java stack trace in string form, if the error was due to a server-side exception. |
| `multiStageQuery.payload.stages` | Array of query stages. |
| `multiStageQuery.payload.stages[].stageNumber` | Each stage has a number that differentiates it from other stages. |
| `multiStageQuery.payload.stages[].phase` | Either NEW, READING_INPUT, POST_READING, RESULTS_COMPLETE, or FAILED. Only present if the stage has started. |
| `multiStageQuery.payload.stages[].workerCount` | Number of parallel tasks that this stage is running on. Only present if the stage has started. |
| `multiStageQuery.payload.stages[].partitionCount` | Number of output partitions generated by this stage. Only present if the stage has started and has computed its number of output partitions. |
| `multiStageQuery.payload.stages[].startTime` | Start time of this stage. Only present if the stage has started. |
| `multiStageQuery.payload.stages[].duration` | The number of milliseconds that the stage has been running. Only present if the stage has started. |
| `multiStageQuery.payload.stages[].sort` | A boolean that is set to `true` if the stage does a sort as part of its execution. |
| `multiStageQuery.payload.stages[].definition` | The object defining what the stage does. |
| `multiStageQuery.payload.stages[].definition.id` | The unique identifier of the stage. |
| `multiStageQuery.payload.stages[].definition.input` | Array of inputs that the stage has. |
| `multiStageQuery.payload.stages[].definition.broadcast` | Array of input indexes that get broadcasted. Only present if there are inputs that get broadcasted. |
| `multiStageQuery.payload.stages[].definition.processor` | An object defining the processor logic. |
| `multiStageQuery.payload.stages[].definition.signature` | The output signature of the stage. |

## Cancel a query task

Cancels a query task.
Returns a JSON object with the ID of the task that was canceled successfully.

### URL

`POST` `/druid/indexer/v1/task/{taskId}/shutdown`

### Responses

<Tabs>

<TabItem value="16" label="200 SUCCESS">


<br/>

*Successfully shut down task*

</TabItem>
<TabItem value="17" label="404 NOT FOUND">


<br/>

*Cannot find task with ID or task is no longer running*

</TabItem>
</Tabs>

---

### Sample request

The following example shows how to cancel a query task with the ID `query-655efe33-781a-4c50-ae84-c2911b42d63c`.

<Tabs>

<TabItem value="18" label="HTTP">


```HTTP
POST /druid/indexer/v1/task/query-655efe33-781a-4c50-ae84-c2911b42d63c/shutdown HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

</TabItem>

<TabItem value="19" label="cURL">


```shell
curl --location --request POST 'http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/task/query-655efe33-781a-4c50-ae84-c2911b42d63c/shutdown'
```

</TabItem>

<TabItem value="20" label="Python">


```python
import requests

url = "http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/task/query-655efe33-781a-4c50-ae84-c2911b42d63c/shutdown"

payload = {}
headers = {}

response = requests.post(url, headers=headers, data=payload)

print(response.text)
print(response.text)
```

</TabItem>

</Tabs>

### Sample response

The response shows the ID of the task that was canceled.

```json
{
    "task": "query-655efe33-781a-4c50-ae84-c2911b42d63c"
}
```