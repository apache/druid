---
id: api
title: SQL-based ingestion and multi-stage query task API
sidebar_label: API
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

> This page describes SQL-based batch ingestion using the [`druid-multi-stage-query`](../multi-stage-query/index.md)
> extension, new in Druid 24.0. Refer to the [ingestion methods](../ingestion/index.md#batch) table to determine which
> ingestion method is right for you.

The **Query** view in the web console provides a friendly experience for the multi-stage query task engine (MSQ task
engine) and multi-stage query architecture. We recommend using the web console if you do not need a programmatic
interface.

When using the API for the MSQ task engine, the action you want to take determines the endpoint you use:

- `/druid/v2/sql/task` endpoint: Submit a query for ingestion.
- `/druid/indexer/v1/task` endpoint: Interact with a query, including getting its status, getting its details, or canceling it. This page describes a few of the Overlord Task APIs that you can use with the MSQ task engine. For information about Druid APIs, see the [API reference for Druid](../operations/api-reference.md#tasks).

## Submit a query

You submit queries to the MSQ task engine using the `POST /druid/v2/sql/task/` endpoint.

#### Request

The SQL task endpoint accepts [SQL requests in the JSON-over-HTTP form](../querying/sql-api.md#request-body) using the
`query`, `context`, and `parameters` fields, but ignoring the `resultFormat`, `header`, `typesHeader`, and
`sqlTypesHeader` fields.

This endpoint accepts [INSERT](reference.md#insert) and [REPLACE](reference.md#replace) statements.

As an experimental feature, this endpoint also accepts SELECT queries. SELECT query results are collected from workers
by the controller, and written into the [task report](#get-the-report-for-a-query-task) as an array of arrays. The
behavior and result format of plain SELECT queries (without INSERT or REPLACE) is subject to change.

<!--DOCUSAURUS_CODE_TABS-->

<!--HTTP-->

```
POST /druid/v2/sql/task
```

```json
{
  "query": "INSERT INTO wikipedia\nSELECT\n  TIME_PARSE(\"timestamp\") AS __time,\n  *\nFROM TABLE(\n  EXTERN(\n    '{\"type\": \"http\", \"uris\": [\"https://druid.apache.org/data/wikipedia.json.gz\"]}',\n    '{\"type\": \"json\"}',\n    '[{\"name\": \"added\", \"type\": \"long\"}, {\"name\": \"channel\", \"type\": \"string\"}, {\"name\": \"cityName\", \"type\": \"string\"}, {\"name\": \"comment\", \"type\": \"string\"}, {\"name\": \"commentLength\", \"type\": \"long\"}, {\"name\": \"countryIsoCode\", \"type\": \"string\"}, {\"name\": \"countryName\", \"type\": \"string\"}, {\"name\": \"deleted\", \"type\": \"long\"}, {\"name\": \"delta\", \"type\": \"long\"}, {\"name\": \"deltaBucket\", \"type\": \"string\"}, {\"name\": \"diffUrl\", \"type\": \"string\"}, {\"name\": \"flags\", \"type\": \"string\"}, {\"name\": \"isAnonymous\", \"type\": \"string\"}, {\"name\": \"isMinor\", \"type\": \"string\"}, {\"name\": \"isNew\", \"type\": \"string\"}, {\"name\": \"isRobot\", \"type\": \"string\"}, {\"name\": \"isUnpatrolled\", \"type\": \"string\"}, {\"name\": \"metroCode\", \"type\": \"string\"}, {\"name\": \"namespace\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"regionIsoCode\", \"type\": \"string\"}, {\"name\": \"regionName\", \"type\": \"string\"}, {\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\n  )\n)\nPARTITIONED BY DAY",
  "context": {
    "maxNumTasks": 3
  }
}
```

<!--curl-->

```bash
# Make sure you replace `username`, `password`, `your-instance`, and `port` with the values for your deployment.
curl --location --request POST 'https://<username>:<password>@<your-instance>:<port>/druid/v2/sql/task/' \
  --header 'Content-Type: application/json' \
  --data-raw '{
    "query": "INSERT INTO wikipedia\nSELECT\n  TIME_PARSE(\"timestamp\") AS __time,\n  *\nFROM TABLE(\n  EXTERN(\n    '\''{\"type\": \"http\", \"uris\": [\"https://druid.apache.org/data/wikipedia.json.gz\"]}'\'',\n    '\''{\"type\": \"json\"}'\'',\n    '\''[{\"name\": \"added\", \"type\": \"long\"}, {\"name\": \"channel\", \"type\": \"string\"}, {\"name\": \"cityName\", \"type\": \"string\"}, {\"name\": \"comment\", \"type\": \"string\"}, {\"name\": \"commentLength\", \"type\": \"long\"}, {\"name\": \"countryIsoCode\", \"type\": \"string\"}, {\"name\": \"countryName\", \"type\": \"string\"}, {\"name\": \"deleted\", \"type\": \"long\"}, {\"name\": \"delta\", \"type\": \"long\"}, {\"name\": \"deltaBucket\", \"type\": \"string\"}, {\"name\": \"diffUrl\", \"type\": \"string\"}, {\"name\": \"flags\", \"type\": \"string\"}, {\"name\": \"isAnonymous\", \"type\": \"string\"}, {\"name\": \"isMinor\", \"type\": \"string\"}, {\"name\": \"isNew\", \"type\": \"string\"}, {\"name\": \"isRobot\", \"type\": \"string\"}, {\"name\": \"isUnpatrolled\", \"type\": \"string\"}, {\"name\": \"metroCode\", \"type\": \"string\"}, {\"name\": \"namespace\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"regionIsoCode\", \"type\": \"string\"}, {\"name\": \"regionName\", \"type\": \"string\"}, {\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\''\n  )\n)\nPARTITIONED BY DAY",
    "context": {
        "maxNumTasks": 3
    }
  }'
```

<!--Python-->

```python
import json
import requests

# Make sure you replace `username`, `password`, `your-instance`, and `port` with the values for your deployment.
url = "https://<username>:<password>@<your-instance>:<port>/druid/v2/sql/task/"

payload = json.dumps({
  "query": "INSERT INTO wikipedia\nSELECT\n  TIME_PARSE(\"timestamp\") AS __time,\n  *\nFROM TABLE(\n  EXTERN(\n    '{\"type\": \"http\", \"uris\": [\"https://druid.apache.org/data/wikipedia.json.gz\"]}',\n    '{\"type\": \"json\"}',\n    '[{\"name\": \"added\", \"type\": \"long\"}, {\"name\": \"channel\", \"type\": \"string\"}, {\"name\": \"cityName\", \"type\": \"string\"}, {\"name\": \"comment\", \"type\": \"string\"}, {\"name\": \"commentLength\", \"type\": \"long\"}, {\"name\": \"countryIsoCode\", \"type\": \"string\"}, {\"name\": \"countryName\", \"type\": \"string\"}, {\"name\": \"deleted\", \"type\": \"long\"}, {\"name\": \"delta\", \"type\": \"long\"}, {\"name\": \"deltaBucket\", \"type\": \"string\"}, {\"name\": \"diffUrl\", \"type\": \"string\"}, {\"name\": \"flags\", \"type\": \"string\"}, {\"name\": \"isAnonymous\", \"type\": \"string\"}, {\"name\": \"isMinor\", \"type\": \"string\"}, {\"name\": \"isNew\", \"type\": \"string\"}, {\"name\": \"isRobot\", \"type\": \"string\"}, {\"name\": \"isUnpatrolled\", \"type\": \"string\"}, {\"name\": \"metroCode\", \"type\": \"string\"}, {\"name\": \"namespace\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"regionIsoCode\", \"type\": \"string\"}, {\"name\": \"regionName\", \"type\": \"string\"}, {\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\n  )\n)\nPARTITIONED BY DAY",
  "context": {
    "maxNumTasks": 3
  }
})
headers = {
  'Content-Type': 'application/json'
}

response = requests.request("POST", url, headers=headers, data=payload)

print(response.text)

```

<!--END_DOCUSAURUS_CODE_TABS-->


#### Response

```json
{
  "taskId": "query-f795a235-4dc7-4fef-abac-3ae3f9686b79",
  "state": "RUNNING",
}
```

**Response fields**

|Field|Description|
|-----|-----------|
| taskId | Controller task ID. You can use Druid's standard [task APIs](../operations/api-reference.md#overlord) to interact with this controller task.|
| state | Initial state for the query, which is "RUNNING".|


## Get the status for a query task

You can retrieve status of a query to see if it is still running, completed successfully, failed, or got canceled. 

#### Request

<!--DOCUSAURUS_CODE_TABS-->

<!--HTTP-->

```
GET /druid/indexer/v1/task/<taskId>
```

<!--curl-->

```bash
# Make sure you replace `username`, `password`, `your-instance`, `port`, and `taskId` with the values for your deployment.
curl --location --request GET 'https://<username>:<password>@<hostname>:<port>/druid/indexer/v1/task/<taskId>/status'
```

<!--Python-->

```python
import requests

# Make sure you replace `username`, `password`, `your-instance`, `port`, and `taskId` with the values for your deployment.
url = "https://<username>:<password>@<hostname>:<port>/druid/indexer/v1/task/<taskId>/status"

payload={}
headers = {}

response = requests.request("GET", url, headers=headers, data=payload)

print(response.text)
```

<!--END_DOCUSAURUS_CODE_TABS-->

#### Response

```
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

## Get the report for a query task

A report provides detailed information about a query task, including things like the stages, warnings, and errors.

Keep the following in mind when using the task API to view reports:

- The task report for an entire job is associated with the `query_controller` task. The `query_worker` tasks do not have
  their own reports; their information is incorporated into the controller report.
- The task report API may report `404 Not Found` temporarily while the task is in the process of starting up.
- As an experimental feature, the MSQ task engine supports running SELECT queries. SELECT query results are written into
the `multiStageQuery.payload.results.results` task report key as an array of arrays. The behavior and result format of plain
SELECT queries (without INSERT or REPLACE) is subject to change.

For an explanation of the fields in a report, see [Report response fields](#report-response-fields).

#### Request

<!--DOCUSAURUS_CODE_TABS-->

<!--HTTP-->

```
GET /druid/indexer/v1/task/<taskId>/reports
```

<!--curl-->

```bash
# Make sure you replace `username`, `password`, `your-instance`, `port`, and `taskId` with the values for your deployment.
curl --location --request GET 'https://<username>:<password>@<hostname>:<port>/druid/indexer/v1/task/<taskId>/reports'
```

<!--Python-->

```python
import requests

# Make sure you replace `username`, `password`, `your-instance`, `port`, and `taskId` with the values for your deployment.
url = "https://<username>:<password>@<hostname>:<port>/druid/indexer/v1/task/<taskId>/reports"

headers = {}
response = requests.request("GET", url, headers=headers)
print(response.text)
```


<!--END_DOCUSAURUS_CODE_TABS-->

#### Response

The response shows an example report for a query.

<details><summary>Show the response</summary>

```json
{
  "multiStageQuery": {
    "type": "multiStageQuery",
    "taskId": "query-3dc0c45d-34d7-4b15-86c9-cdb2d3ebfc4e",
    "payload": {
      "status": {
        "status": "SUCCESS",
        "startTime": "2022-09-14T22:12:09.266Z",
        "durationMs": 28227
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
                "legacy": false,
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

The following table describes the response fields when you retrieve a report for a MSQ task engine using the `/druid/indexer/v1/task/<taskId>/reports` endpoint:

|Field|Description|
|-----|-----------|
|multiStageQuery.taskId|Controller task ID.|
|multiStageQuery.payload.status|Query status container.|
|multiStageQuery.payload.status.status|RUNNING, SUCCESS, or FAILED.|
|multiStageQuery.payload.status.startTime|Start time of the query in ISO format. Only present if the query has started running.|
|multiStageQuery.payload.status.durationMs|Milliseconds elapsed after the query has started running. -1 denotes that the query hasn't started running yet.|
|multiStageQuery.payload.status.errorReport|Error object. Only present if there was an error.|
|multiStageQuery.payload.status.errorReport.taskId|The task that reported the error, if known. May be a controller task or a worker task.|
|multiStageQuery.payload.status.errorReport.host|The hostname and port of the task that reported the error, if known.|
|multiStageQuery.payload.status.errorReport.stageNumber|The stage number that reported the error, if it happened during execution of a specific stage.|
|multiStageQuery.payload.status.errorReport.error|Error object. Contains `errorCode` at a minimum, and may contain other fields as described in the [error code table](./reference.md#error-codes). Always present if there is an error.|
|multiStageQuery.payload.status.errorReport.error.errorCode|One of the error codes from the [error code table](./reference.md#error-codes). Always present if there is an error.|
|multiStageQuery.payload.status.errorReport.error.errorMessage|User-friendly error message. Not always present, even if there is an error.|
|multiStageQuery.payload.status.errorReport.exceptionStackTrace|Java stack trace in string form, if the error was due to a server-side exception.|
|multiStageQuery.payload.stages|Array of query stages.|
|multiStageQuery.payload.stages[].stageNumber|Each stage has a number that differentiates it from other stages.|
|multiStageQuery.payload.stages[].phase|Either NEW, READING_INPUT, POST_READING, RESULTS_COMPLETE, or FAILED. Only present if the stage has started.|
|multiStageQuery.payload.stages[].workerCount|Number of parallel tasks that this stage is running on. Only present if the stage has started.|
|multiStageQuery.payload.stages[].partitionCount|Number of output partitions generated by this stage. Only present if the stage has started and has computed its number of output partitions.|
|multiStageQuery.payload.stages[].startTime|Start time of this stage. Only present if the stage has started.|
|multiStageQuery.payload.stages[].duration|The number of milliseconds that the stage has been running. Only present if the stage has started.|
|multiStageQuery.payload.stages[].sort|A boolean that is set to `true` if the stage does a sort as part of its execution.|
|multiStageQuery.payload.stages[].definition|The object defining what the stage does.|
|multiStageQuery.payload.stages[].definition.id|The unique identifier of the stage.|
|multiStageQuery.payload.stages[].definition.input|Array of inputs that the stage has.|
|multiStageQuery.payload.stages[].definition.broadcast|Array of input indexes that get broadcasted. Only present if there are inputs that get broadcasted.|
|multiStageQuery.payload.stages[].definition.processor|An object defining the processor logic.|
|multiStageQuery.payload.stages[].definition.signature|The output signature of the stage.|

## Cancel a query task

#### Request

<!--DOCUSAURUS_CODE_TABS-->

<!--HTTP-->

```
POST /druid/indexer/v1/task/<taskId>/shutdown
```

<!--curl-->

```bash
# Make sure you replace `username`, `password`, `your-instance`, `port`, and `taskId` with the values for your deployment.
curl --location --request POST 'https://<username>:<password>@<your-instance>:<port>/druid/indexer/v1/task/<taskId>/shutdown'
```

<!--Python-->

```
import requests

# Make sure you replace `username`, `password`, `your-instance`, `port`, and `taskId` with the values for your deployment.
url = "https://<username>:<password>@<your-instance>:<port>/druid/indexer/v1/task/<taskId>/shutdown"

payload={}
headers = {}

response = requests.request("POST", url, headers=headers, data=payload)

print(response.text)
```

<!--END_DOCUSAURUS_CODE_TABS-->

#### Response

```
{
    "task": "query-655efe33-781a-4c50-ae84-c2911b42d63c"
}
```
