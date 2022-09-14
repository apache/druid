---
id: api
title: SQL-based ingestion APIs
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

> SQL-based ingestion using the multi-stage query task engine is our recommended solution starting in Druid 24.0. Alternative ingestion solutions, such as native batch and Hadoop-based ingestion systems, will still be supported. We recommend you read all [known issues](./msq-known-issues.md) and test the feature in a development environment before rolling it out in production. Using the multi-stage query task engine with `SELECT` statements that do not write to a datasource is experimental.

The **Query** view in the Druid console provides the most stable experience for the multi-stage query task engine (MSQ task engine) and multi-stage query architecture. Use the UI if you do not need a programmatic interface.

When using the API for the MSQ task engine, the action you want to take determines the endpoint you use:

- `/druid/v2/sql/task` endpoint: Submit a query for ingestion.
- `/druid/indexer/v1/task` endpoint: Interact with a query, including getting its status, getting its details, or canceling it. This page describes a few of the Overlord Task APIs that you can use with the MSQ task engine. For information about Druid APIs, see the [API reference for Druid](../operations/api-reference.md#tasks).

## Submit a query

You submit queries to the MSQ task engine using the `POST /druid/v2/sql/task/` endpoint.

### Request

Currently, the MSQ task engine ignores the provided values of `resultFormat`, `header`,
`typesHeader`, and `sqlTypesHeader`. SQL SELECT queries write out their results into the task report (in the `multiStageQuery.payload.results.results` key) formatted as if `resultFormat` is an `array`.

For task queries similar to the [example queries](./msq-example-queries.md), you need to escape characters such as quotation marks (") if you use something like `curl`. 
You don't need to escape characters if you use a method that can parse JSON seamlessly, such as Python.
The Python example in this topic escapes quotation marks although it's not required.

The following example is the same query that you submit when you complete [Convert a JSON ingestion spec](./msq-tutorial-convert-ingest-spec.md) where you insert data into a table named `wikipedia`. 

<!--DOCUSAURUS_CODE_TABS-->

<!--HTTP-->

```
POST /druid/v2/sql/task
```

```json
{
  "query": "INSERT INTO wikipedia\nSELECT\n  TIME_PARSE(\"timestamp\") AS __time,\n  *\nFROM TABLE(\n  EXTERN(\n    '{\"type\": \"http\", \"uris\": [\"https://static.imply.io/data/wikipedia.json.gz\"]}',\n    '{\"type\": \"json\"}',\n    '[{\"name\": \"added\", \"type\": \"long\"}, {\"name\": \"channel\", \"type\": \"string\"}, {\"name\": \"cityName\", \"type\": \"string\"}, {\"name\": \"comment\", \"type\": \"string\"}, {\"name\": \"commentLength\", \"type\": \"long\"}, {\"name\": \"countryIsoCode\", \"type\": \"string\"}, {\"name\": \"countryName\", \"type\": \"string\"}, {\"name\": \"deleted\", \"type\": \"long\"}, {\"name\": \"delta\", \"type\": \"long\"}, {\"name\": \"deltaBucket\", \"type\": \"string\"}, {\"name\": \"diffUrl\", \"type\": \"string\"}, {\"name\": \"flags\", \"type\": \"string\"}, {\"name\": \"isAnonymous\", \"type\": \"string\"}, {\"name\": \"isMinor\", \"type\": \"string\"}, {\"name\": \"isNew\", \"type\": \"string\"}, {\"name\": \"isRobot\", \"type\": \"string\"}, {\"name\": \"isUnpatrolled\", \"type\": \"string\"}, {\"name\": \"metroCode\", \"type\": \"string\"}, {\"name\": \"namespace\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"regionIsoCode\", \"type\": \"string\"}, {\"name\": \"regionName\", \"type\": \"string\"}, {\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\n  )\n)\nPARTITIONED BY DAY",
  "context": {
      "maxNumTasks": 3
  }
}
```

<!--curl-->

Make sure you replace `username`, `password`, `your-instance`, and `port` with the values for your deployment.

```bash
curl --location --request POST 'https://<username>:<password>@<your-instance>:<port>/druid/v2/sql/task/' \
--header 'Content-Type: application/json' \
--data-raw '{
    "query": "INSERT INTO wikipedia\nSELECT\n  TIME_PARSE(\"timestamp\") AS __time,\n  *\nFROM TABLE(\n  EXTERN(\n    '\''{\"type\": \"http\", \"uris\": [\"https://static.imply.io/data/wikipedia.json.gz\"]}'\'',\n    '\''{\"type\": \"json\"}'\'',\n    '\''[{\"name\": \"added\", \"type\": \"long\"}, {\"name\": \"channel\", \"type\": \"string\"}, {\"name\": \"cityName\", \"type\": \"string\"}, {\"name\": \"comment\", \"type\": \"string\"}, {\"name\": \"commentLength\", \"type\": \"long\"}, {\"name\": \"countryIsoCode\", \"type\": \"string\"}, {\"name\": \"countryName\", \"type\": \"string\"}, {\"name\": \"deleted\", \"type\": \"long\"}, {\"name\": \"delta\", \"type\": \"long\"}, {\"name\": \"deltaBucket\", \"type\": \"string\"}, {\"name\": \"diffUrl\", \"type\": \"string\"}, {\"name\": \"flags\", \"type\": \"string\"}, {\"name\": \"isAnonymous\", \"type\": \"string\"}, {\"name\": \"isMinor\", \"type\": \"string\"}, {\"name\": \"isNew\", \"type\": \"string\"}, {\"name\": \"isRobot\", \"type\": \"string\"}, {\"name\": \"isUnpatrolled\", \"type\": \"string\"}, {\"name\": \"metroCode\", \"type\": \"string\"}, {\"name\": \"namespace\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"regionIsoCode\", \"type\": \"string\"}, {\"name\": \"regionName\", \"type\": \"string\"}, {\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\''\n  )\n)\nPARTITIONED BY DAY",
    "context": {
        "maxNumTasks": 3
    }
```

<!--Python-->
Make sure you replace `username`, `password`, `your-instance`, and `port` with the values for your deployment.

```python
import json
import requests

url = "https://<username>:<password>@<your-instance>:<port>/druid/v2/sql/task/"

payload = json.dumps({
  "query": "INSERT INTO wikipedia\nSELECT\n  TIME_PARSE(\"timestamp\") AS __time,\n  *\nFROM TABLE(\n  EXTERN(\n    '{\"type\": \"http\", \"uris\": [\"https://static.imply.io/data/wikipedia.json.gz\"]}',\n    '{\"type\": \"json\"}',\n    '[{\"name\": \"added\", \"type\": \"long\"}, {\"name\": \"channel\", \"type\": \"string\"}, {\"name\": \"cityName\", \"type\": \"string\"}, {\"name\": \"comment\", \"type\": \"string\"}, {\"name\": \"commentLength\", \"type\": \"long\"}, {\"name\": \"countryIsoCode\", \"type\": \"string\"}, {\"name\": \"countryName\", \"type\": \"string\"}, {\"name\": \"deleted\", \"type\": \"long\"}, {\"name\": \"delta\", \"type\": \"long\"}, {\"name\": \"deltaBucket\", \"type\": \"string\"}, {\"name\": \"diffUrl\", \"type\": \"string\"}, {\"name\": \"flags\", \"type\": \"string\"}, {\"name\": \"isAnonymous\", \"type\": \"string\"}, {\"name\": \"isMinor\", \"type\": \"string\"}, {\"name\": \"isNew\", \"type\": \"string\"}, {\"name\": \"isRobot\", \"type\": \"string\"}, {\"name\": \"isUnpatrolled\", \"type\": \"string\"}, {\"name\": \"metroCode\", \"type\": \"string\"}, {\"name\": \"namespace\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"regionIsoCode\", \"type\": \"string\"}, {\"name\": \"regionName\", \"type\": \"string\"}, {\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\n  )\n)\nPARTITIONED BY DAY",
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


### Response

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


## Get the payload for a query task

You can retrieve basic information about a query task, such as the SQL query and context parameters that were submitted. 

### Request

<!--DOCUSAURUS_CODE_TABS-->

<!--HTTP-->

```
GET /druid/indexer/v1/task/<taskId>
```

<!--curl-->

Make sure you replace `username`, `password`, `your-instance`, `port`, and `taskId` with the values for your deployment.

```bash
curl --location --request GET 'https://<username>:<password>@<your-instance>:<port>/druid/indexer/v1/task/<taskId>'
```

<!--Python-->
Make sure you replace `username`, `password`, `your-instance`, `port`, and `taskId` with the values for your deployment.

```python
import requests

url = "<username>:<password>@<your-instance>:<port>/druid/indexer/v1/task/<taskId>"

payload={}
headers = {}

response = requests.request("GET", url, headers=headers, data=payload)

print(response.text)

```

<!--END_DOCUSAURUS_CODE_TABS-->

### Response

<details><summary>Show the response</summary>

```
{
    "task": "query-a6b65442-f77e-44e4-af28-ab3b711a27ac",
    "payload": {
        "type": "query_controller",
        "id": "query-a6b65442-f77e-44e4-af28-ab3b711a27ac",
        "spec": {
            "query": {
                "queryType": "scan",
                "dataSource": {
                    "type": "external",
                    "inputSource": {
                        "type": "http",
                        "uris": [
                            "https://static.imply.io/data/kttm/kttm-v2-2019-08-25.json.gz"
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
                        },
                        {
                            "name": "browser",
                            "type": "STRING"
                        },
                        {
                            "name": "browser_version",
                            "type": "STRING"
                        },
                        {
                            "name": "city",
                            "type": "STRING"
                        },
                        {
                            "name": "continent",
                            "type": "STRING"
                        },
                        {
                            "name": "country",
                            "type": "STRING"
                        },
                        {
                            "name": "version",
                            "type": "STRING"
                        },
                        {
                            "name": "event_type",
                            "type": "STRING"
                        },
                        {
                            "name": "event_subtype",
                            "type": "STRING"
                        },
                        {
                            "name": "loaded_image",
                            "type": "STRING"
                        },
                        {
                            "name": "adblock_list",
                            "type": "STRING"
                        },
                        {
                            "name": "forwarded_for",
                            "type": "STRING"
                        },
                        {
                            "name": "language",
                            "type": "STRING"
                        },
                        {
                            "name": "number",
                            "type": "LONG"
                        },
                        {
                            "name": "os",
                            "type": "STRING"
                        },
                        {
                            "name": "path",
                            "type": "STRING"
                        },
                        {
                            "name": "platform",
                            "type": "STRING"
                        },
                        {
                            "name": "referrer",
                            "type": "STRING"
                        },
                        {
                            "name": "referrer_host",
                            "type": "STRING"
                        },
                        {
                            "name": "region",
                            "type": "STRING"
                        },
                        {
                            "name": "remote_address",
                            "type": "STRING"
                        },
                        {
                            "name": "screen",
                            "type": "STRING"
                        },
                        {
                            "name": "session",
                            "type": "STRING"
                        },
                        {
                            "name": "session_length",
                            "type": "LONG"
                        },
                        {
                            "name": "timezone",
                            "type": "STRING"
                        },
                        {
                            "name": "timezone_offset",
                            "type": "LONG"
                        },
                        {
                            "name": "window",
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
                "resultFormat": "compactedList",
                "columns": [
                    "adblock_list",
                    "agent_category",
                    "agent_type",
                    "browser",
                    "browser_version",
                    "city",
                    "continent",
                    "country",
                    "event_subtype",
                    "event_type",
                    "forwarded_for",
                    "language",
                    "loaded_image",
                    "number",
                    "os",
                    "path",
                    "platform",
                    "referrer",
                    "referrer_host",
                    "region",
                    "remote_address",
                    "screen",
                    "session",
                    "session_length",
                    "timestamp",
                    "timezone",
                    "timezone_offset",
                    "version",
                    "window"
                ],
                "legacy": false,
                "context": {
                    "finalize": true,
                    "msqMaxNumTasks": 3,
                    "msqSignature": "[{\"name\":\"adblock_list\",\"type\":\"STRING\"},{\"name\":\"agent_category\",\"type\":\"STRING\"},{\"name\":\"agent_type\",\"type\":\"STRING\"},{\"name\":\"browser\",\"type\":\"STRING\"},{\"name\":\"browser_version\",\"type\":\"STRING\"},{\"name\":\"city\",\"type\":\"STRING\"},{\"name\":\"continent\",\"type\":\"STRING\"},{\"name\":\"country\",\"type\":\"STRING\"},{\"name\":\"event_subtype\",\"type\":\"STRING\"},{\"name\":\"event_type\",\"type\":\"STRING\"},{\"name\":\"forwarded_for\",\"type\":\"STRING\"},{\"name\":\"language\",\"type\":\"STRING\"},{\"name\":\"loaded_image\",\"type\":\"STRING\"},{\"name\":\"number\",\"type\":\"LONG\"},{\"name\":\"os\",\"type\":\"STRING\"},{\"name\":\"path\",\"type\":\"STRING\"},{\"name\":\"platform\",\"type\":\"STRING\"},{\"name\":\"referrer\",\"type\":\"STRING\"},{\"name\":\"referrer_host\",\"type\":\"STRING\"},{\"name\":\"region\",\"type\":\"STRING\"},{\"name\":\"remote_address\",\"type\":\"STRING\"},{\"name\":\"screen\",\"type\":\"STRING\"},{\"name\":\"session\",\"type\":\"STRING\"},{\"name\":\"session_length\",\"type\":\"LONG\"},{\"name\":\"timestamp\",\"type\":\"STRING\"},{\"name\":\"timezone\",\"type\":\"STRING\"},{\"name\":\"timezone_offset\",\"type\":\"LONG\"},{\"name\":\"version\",\"type\":\"STRING\"},{\"name\":\"window\",\"type\":\"STRING\"}]",
                    "multiStageQuery": true,
                    "sqlInsertSegmentGranularity": "{\"type\":\"all\"}",
                    "sqlQueryId": "a6b65442-f77e-44e4-af28-ab3b711a27ac",
                    "sqlReplaceTimeChunks": "all"
                },
                "granularity": {
                    "type": "all"
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
                },
                {
                    "queryColumn": "browser",
                    "outputColumn": "browser"
                },
                {
                    "queryColumn": "browser_version",
                    "outputColumn": "browser_version"
                },
                {
                    "queryColumn": "city",
                    "outputColumn": "city"
                },
                {
                    "queryColumn": "continent",
                    "outputColumn": "continent"
                },
                {
                    "queryColumn": "country",
                    "outputColumn": "country"
                },
                {
                    "queryColumn": "version",
                    "outputColumn": "version"
                },
                {
                    "queryColumn": "event_type",
                    "outputColumn": "event_type"
                },
                {
                    "queryColumn": "event_subtype",
                    "outputColumn": "event_subtype"
                },
                {
                    "queryColumn": "loaded_image",
                    "outputColumn": "loaded_image"
                },
                {
                    "queryColumn": "adblock_list",
                    "outputColumn": "adblock_list"
                },
                {
                    "queryColumn": "forwarded_for",
                    "outputColumn": "forwarded_for"
                },
                {
                    "queryColumn": "language",
                    "outputColumn": "language"
                },
                {
                    "queryColumn": "number",
                    "outputColumn": "number"
                },
                {
                    "queryColumn": "os",
                    "outputColumn": "os"
                },
                {
                    "queryColumn": "path",
                    "outputColumn": "path"
                },
                {
                    "queryColumn": "platform",
                    "outputColumn": "platform"
                },
                {
                    "queryColumn": "referrer",
                    "outputColumn": "referrer"
                },
                {
                    "queryColumn": "referrer_host",
                    "outputColumn": "referrer_host"
                },
                {
                    "queryColumn": "region",
                    "outputColumn": "region"
                },
                {
                    "queryColumn": "remote_address",
                    "outputColumn": "remote_address"
                },
                {
                    "queryColumn": "screen",
                    "outputColumn": "screen"
                },
                {
                    "queryColumn": "session",
                    "outputColumn": "session"
                },
                {
                    "queryColumn": "session_length",
                    "outputColumn": "session_length"
                },
                {
                    "queryColumn": "timezone",
                    "outputColumn": "timezone"
                },
                {
                    "queryColumn": "timezone_offset",
                    "outputColumn": "timezone_offset"
                },
                {
                    "queryColumn": "window",
                    "outputColumn": "window"
                }
            ],
            "destination": {
                "type": "dataSource",
                "dataSource": "kttm_simple",
                "segmentGranularity": {
                    "type": "all"
                },
                "replaceTimeChunks": [
                    "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z"
                ]
            },
            "assignmentStrategy": "max",
            "tuningConfig": {
                "type": "index_parallel",
                "maxRowsPerSegment": 3000000,
                "appendableIndexSpec": {
                    "type": "onheap",
                    "preserveExistingMetrics": false
                },
                "maxRowsInMemory": 100000,
                "maxBytesInMemory": 0,
                "skipBytesInMemoryOverheadCheck": false,
                "maxTotalRows": null,
                "numShards": null,
                "splitHintSpec": null,
                "partitionsSpec": {
                    "type": "dynamic",
                    "maxRowsPerSegment": 3000000,
                    "maxTotalRows": null
                },
                "indexSpec": {
                    "bitmap": {
                        "type": "roaring",
                        "compressRunOnSerialization": true
                    },
                    "dimensionCompression": "lz4",
                    "metricCompression": "lz4",
                    "longEncoding": "longs",
                    "segmentLoader": null
                },
                "indexSpecForIntermediatePersists": {
                    "bitmap": {
                        "type": "roaring",
                        "compressRunOnSerialization": true
                    },
                    "dimensionCompression": "lz4",
                    "metricCompression": "lz4",
                    "longEncoding": "longs",
                    "segmentLoader": null
                },
                "maxPendingPersists": 0,
                "forceGuaranteedRollup": false,
                "reportParseExceptions": false,
                "pushTimeout": 0,
                "segmentWriteOutMediumFactory": null,
                "maxNumConcurrentSubTasks": 2,
                "maxRetry": 1,
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
        "sqlQuery": "REPLACE INTO \"kttm_simple\" OVERWRITE ALL\nSELECT *\nFROM TABLE(\n  EXTERN(\n    '{\"type\":\"http\",\"uris\":[\"https://static.imply.io/data/kttm/kttm-v2-2019-08-25.json.gz\"]}',\n    '{\"type\":\"json\"}',\n    '[{\"name\":\"timestamp\",\"type\":\"string\"},{\"name\":\"agent_category\",\"type\":\"string\"},{\"name\":\"agent_type\",\"type\":\"string\"},{\"name\":\"browser\",\"type\":\"string\"},{\"name\":\"browser_version\",\"type\":\"string\"},{\"name\":\"city\",\"type\":\"string\"},{\"name\":\"continent\",\"type\":\"string\"},{\"name\":\"country\",\"type\":\"string\"},{\"name\":\"version\",\"type\":\"string\"},{\"name\":\"event_type\",\"type\":\"string\"},{\"name\":\"event_subtype\",\"type\":\"string\"},{\"name\":\"loaded_image\",\"type\":\"string\"},{\"name\":\"adblock_list\",\"type\":\"string\"},{\"name\":\"forwarded_for\",\"type\":\"string\"},{\"name\":\"language\",\"type\":\"string\"},{\"name\":\"number\",\"type\":\"long\"},{\"name\":\"os\",\"type\":\"string\"},{\"name\":\"path\",\"type\":\"string\"},{\"name\":\"platform\",\"type\":\"string\"},{\"name\":\"referrer\",\"type\":\"string\"},{\"name\":\"referrer_host\",\"type\":\"string\"},{\"name\":\"region\",\"type\":\"string\"},{\"name\":\"remote_address\",\"type\":\"string\"},{\"name\":\"screen\",\"type\":\"string\"},{\"name\":\"session\",\"type\":\"string\"},{\"name\":\"session_length\",\"type\":\"long\"},{\"name\":\"timezone\",\"type\":\"string\"},{\"name\":\"timezone_offset\",\"type\":\"long\"},{\"name\":\"window\",\"type\":\"string\"}]'\n  )\n)\nPARTITIONED BY ALL TIME",
        "sqlQueryContext": {
            "parseExceptions": 0,
            "maxNumTasks": 3,
            "signature": "[{\"name\":\"adblock_list\",\"type\":\"STRING\"},{\"name\":\"agent_category\",\"type\":\"STRING\"},{\"name\":\"agent_type\",\"type\":\"STRING\"},{\"name\":\"browser\",\"type\":\"STRING\"},{\"name\":\"browser_version\",\"type\":\"STRING\"},{\"name\":\"city\",\"type\":\"STRING\"},{\"name\":\"continent\",\"type\":\"STRING\"},{\"name\":\"country\",\"type\":\"STRING\"},{\"name\":\"event_subtype\",\"type\":\"STRING\"},{\"name\":\"event_type\",\"type\":\"STRING\"},{\"name\":\"forwarded_for\",\"type\":\"STRING\"},{\"name\":\"language\",\"type\":\"STRING\"},{\"name\":\"loaded_image\",\"type\":\"STRING\"},{\"name\":\"number\",\"type\":\"LONG\"},{\"name\":\"os\",\"type\":\"STRING\"},{\"name\":\"path\",\"type\":\"STRING\"},{\"name\":\"platform\",\"type\":\"STRING\"},{\"name\":\"referrer\",\"type\":\"STRING\"},{\"name\":\"referrer_host\",\"type\":\"STRING\"},{\"name\":\"region\",\"type\":\"STRING\"},{\"name\":\"remote_address\",\"type\":\"STRING\"},{\"name\":\"screen\",\"type\":\"STRING\"},{\"name\":\"session\",\"type\":\"STRING\"},{\"name\":\"session_length\",\"type\":\"LONG\"},{\"name\":\"timestamp\",\"type\":\"STRING\"},{\"name\":\"timezone\",\"type\":\"STRING\"},{\"name\":\"timezone_offset\",\"type\":\"LONG\"},{\"name\":\"version\",\"type\":\"STRING\"},{\"name\":\"window\",\"type\":\"STRING\"}]",
            "multiStageQuery": true,
            "sqlInsertSegmentGranularity": "{\"type\":\"all\"}",
            "sqlQueryId": "a6b65442-f77e-44e4-af28-ab3b711a27ac",
            "sqlReplaceTimeChunks": "all"
        },
        "sqlTypeNames": [
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
            "VARCHAR",
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
            "BIGINT",
            "VARCHAR",
            "BIGINT",
            "VARCHAR"
        ],
        "context": {
            "forceTimeChunkLock": true,
            "useLineageBasedSegmentAllocation": true
        },
        "groupId": "query-a6b65442-f77e-44e4-af28-ab3b711a27ac",
        "dataSource": "kttm_simple",
        "resource": {
            "availabilityGroup": "query-a6b65442-f77e-44e4-af28-ab3b711a27ac",
            "requiredCapacity": 1
        }
    }
}
```

</details>

## Get the status for a query task

You can retrieve status of a query to see if it is still running, completed successfully, failed, or got canceled. 

### Request

<!--DOCUSAURUS_CODE_TABS-->

<!--HTTP-->

```
GET /druid/indexer/v1/task/<taskId>
```

<!--curl-->
Make sure you replace `username`, `password`, `your-instance`, `port`, and `taskId` with the values for your deployment.

```bash
curl --location --request GET 'https://<username>:<password>@<hostname>:<port>/druid/indexer/v1/task/<taskId>/status'
```

<!--Python-->
Make sure you replace `username`, `password`, `your-instance`, `port`, and `taskId` with the values for your deployment.

```python
import requests

url = "https://<username>:<password>@<hostname>:<port>/druid/indexer/v1/task/<taskId>/status"

payload={}
headers = {}

response = requests.request("GET", url, headers=headers, data=payload)

print(response.text)
```

<!--END_DOCUSAURUS_CODE_TABS-->

### Response

```
{
    "task": "query-a6b65442-f77e-44e4-af28-ab3b711a27ac",
    "status": {
        "id": "query-a6b65442-f77e-44e4-af28-ab3b711a27ac",
        "groupId": "query-a6b65442-f77e-44e4-af28-ab3b711a27ac",
        "type": "query_controller",
        "createdTime": "2022-07-27T20:09:23.551Z",
        "queueInsertionTime": "1970-01-01T00:00:00.000Z",
        "statusCode": "SUCCESS",
        "status": "SUCCESS",
        "runnerStatusCode": "WAITING",
        "duration": 136636,
        "location": {
            "host": "ip-10-201-5-81.ec2.internal",
            "port": -1,
            "tlsPort": 8100
        },
        "dataSource": "kttm_simple",
        "errorMsg": null
    }
}
```

## Get the report for a query task

A report provides detailed information about a query task, including things like the stages, warnings, and errors.

Keep the following in mind when using the task API to view reports:

- For SELECT queries, the report includes the results. At this time, if you want to view results for SELECT queries, you need to retrieve them as a generic map from the report and extract the results.
- The task report stores query details for controller tasks.
- If you encounter `500 Server Error` or `404 Not Found` errors, the task may be in the process of starting up or shutting down.

For an explanation of the fields in a report, see [Report response fields](#report-response-fields).

### Request

<!--DOCUSAURUS_CODE_TABS-->

<!--HTTP-->

```
GET /druid/indexer/v1/task/<taskId>/report
```

<!--curl-->
Make sure you replace `username`, `password`, `your-instance`, `port`, and `taskId` with the values for your deployment.

```bash
curl --location --request GET 'https://<username>:<password>@<hostname>:<port>/druid/indexer/v1/task/<taskId>/report'
```

<!--Python-->

Make sure you replace `username`, `password`, `your-instance`, `port`, and `taskId` with the values for your deployment.

```python
import requests

url = "https://<username>:<password>@<hostname>:<port>/druid/indexer/v1/task/<taskId>/reports"

payload={}
headers = {}

response = requests.request("GET", url, headers=headers, data=payload)

print(response.text)
```


<!--END_DOCUSAURUS_CODE_TABS-->

### Response

The response shows an example report for a query.

<details><summary>Show the response</summary>

```json
{
    "multiStageQuery": {
        "taskId": "query-a6b65442-f77e-44e4-af28-ab3b711a27ac",
        "payload": {
            "status": {
                "status": "SUCCESS",
                "startTime": "2022-07-27T20:09:39.915Z",
                "durationMs": 116516,
                "warningReports": []
            },
            "stages": [
                {
                    "stageNumber": 0,
                    "definition": {
                        "id": "f224410f-1cad-4ee7-b10d-f10ddf8bb517_0",
                        "input": [
                            {
                                "type": "external",
                                "inputSource": {
                                    "type": "http",
                                    "uris": [
                                        "https://static.imply.io/data/kttm/kttm-v2-2019-08-25.json.gz"
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
                                    },
                                    {
                                        "name": "browser",
                                        "type": "STRING"
                                    },
                                    {
                                        "name": "browser_version",
                                        "type": "STRING"
                                    },
                                    {
                                        "name": "city",
                                        "type": "STRING"
                                    },
                                    {
                                        "name": "continent",
                                        "type": "STRING"
                                    },
                                    {
                                        "name": "country",
                                        "type": "STRING"
                                    },
                                    {
                                        "name": "version",
                                        "type": "STRING"
                                    },
                                    {
                                        "name": "event_type",
                                        "type": "STRING"
                                    },
                                    {
                                        "name": "event_subtype",
                                        "type": "STRING"
                                    },
                                    {
                                        "name": "loaded_image",
                                        "type": "STRING"
                                    },
                                    {
                                        "name": "adblock_list",
                                        "type": "STRING"
                                    },
                                    {
                                        "name": "forwarded_for",
                                        "type": "STRING"
                                    },
                                    {
                                        "name": "language",
                                        "type": "STRING"
                                    },
                                    {
                                        "name": "number",
                                        "type": "LONG"
                                    },
                                    {
                                        "name": "os",
                                        "type": "STRING"
                                    },
                                    {
                                        "name": "path",
                                        "type": "STRING"
                                    },
                                    {
                                        "name": "platform",
                                        "type": "STRING"
                                    },
                                    {
                                        "name": "referrer",
                                        "type": "STRING"
                                    },
                                    {
                                        "name": "referrer_host",
                                        "type": "STRING"
                                    },
                                    {
                                        "name": "region",
                                        "type": "STRING"
                                    },
                                    {
                                        "name": "remote_address",
                                        "type": "STRING"
                                    },
                                    {
                                        "name": "screen",
                                        "type": "STRING"
                                    },
                                    {
                                        "name": "session",
                                        "type": "STRING"
                                    },
                                    {
                                        "name": "session_length",
                                        "type": "LONG"
                                    },
                                    {
                                        "name": "timezone",
                                        "type": "STRING"
                                    },
                                    {
                                        "name": "timezone_offset",
                                        "type": "LONG"
                                    },
                                    {
                                        "name": "window",
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
                                    "adblock_list",
                                    "agent_category",
                                    "agent_type",
                                    "browser",
                                    "browser_version",
                                    "city",
                                    "continent",
                                    "country",
                                    "event_subtype",
                                    "event_type",
                                    "forwarded_for",
                                    "language",
                                    "loaded_image",
                                    "number",
                                    "os",
                                    "path",
                                    "platform",
                                    "referrer",
                                    "referrer_host",
                                    "region",
                                    "remote_address",
                                    "screen",
                                    "session",
                                    "session_length",
                                    "timestamp",
                                    "timezone",
                                    "timezone_offset",
                                    "version",
                                    "window"
                                ],
                                "legacy": false,
                                "context": {
                                    "finalize": true,
                                    "msqMaxNumTasks": 3,
                                    "msqSignature": "[{\"name\":\"adblock_list\",\"type\":\"STRING\"},{\"name\":\"agent_category\",\"type\":\"STRING\"},{\"name\":\"agent_type\",\"type\":\"STRING\"},{\"name\":\"browser\",\"type\":\"STRING\"},{\"name\":\"browser_version\",\"type\":\"STRING\"},{\"name\":\"city\",\"type\":\"STRING\"},{\"name\":\"continent\",\"type\":\"STRING\"},{\"name\":\"country\",\"type\":\"STRING\"},{\"name\":\"event_subtype\",\"type\":\"STRING\"},{\"name\":\"event_type\",\"type\":\"STRING\"},{\"name\":\"forwarded_for\",\"type\":\"STRING\"},{\"name\":\"language\",\"type\":\"STRING\"},{\"name\":\"loaded_image\",\"type\":\"STRING\"},{\"name\":\"number\",\"type\":\"LONG\"},{\"name\":\"os\",\"type\":\"STRING\"},{\"name\":\"path\",\"type\":\"STRING\"},{\"name\":\"platform\",\"type\":\"STRING\"},{\"name\":\"referrer\",\"type\":\"STRING\"},{\"name\":\"referrer_host\",\"type\":\"STRING\"},{\"name\":\"region\",\"type\":\"STRING\"},{\"name\":\"remote_address\",\"type\":\"STRING\"},{\"name\":\"screen\",\"type\":\"STRING\"},{\"name\":\"session\",\"type\":\"STRING\"},{\"name\":\"session_length\",\"type\":\"LONG\"},{\"name\":\"timestamp\",\"type\":\"STRING\"},{\"name\":\"timezone\",\"type\":\"STRING\"},{\"name\":\"timezone_offset\",\"type\":\"LONG\"},{\"name\":\"version\",\"type\":\"STRING\"},{\"name\":\"window\",\"type\":\"STRING\"}]",
                                    "multiStageQuery": true,
                                    "sqlInsertSegmentGranularity": "{\"type\":\"all\"}",
                                    "sqlQueryId": "a6b65442-f77e-44e4-af28-ab3b711a27ac",
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
                                "name": "adblock_list",
                                "type": "STRING"
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
                                "name": "browser",
                                "type": "STRING"
                            },
                            {
                                "name": "browser_version",
                                "type": "STRING"
                            },
                            {
                                "name": "city",
                                "type": "STRING"
                            },
                            {
                                "name": "continent",
                                "type": "STRING"
                            },
                            {
                                "name": "country",
                                "type": "STRING"
                            },
                            {
                                "name": "event_subtype",
                                "type": "STRING"
                            },
                            {
                                "name": "event_type",
                                "type": "STRING"
                            },
                            {
                                "name": "forwarded_for",
                                "type": "STRING"
                            },
                            {
                                "name": "language",
                                "type": "STRING"
                            },
                            {
                                "name": "loaded_image",
                                "type": "STRING"
                            },
                            {
                                "name": "number",
                                "type": "LONG"
                            },
                            {
                                "name": "os",
                                "type": "STRING"
                            },
                            {
                                "name": "path",
                                "type": "STRING"
                            },
                            {
                                "name": "platform",
                                "type": "STRING"
                            },
                            {
                                "name": "referrer",
                                "type": "STRING"
                            },
                            {
                                "name": "referrer_host",
                                "type": "STRING"
                            },
                            {
                                "name": "region",
                                "type": "STRING"
                            },
                            {
                                "name": "remote_address",
                                "type": "STRING"
                            },
                            {
                                "name": "screen",
                                "type": "STRING"
                            },
                            {
                                "name": "session",
                                "type": "STRING"
                            },
                            {
                                "name": "session_length",
                                "type": "LONG"
                            },
                            {
                                "name": "timestamp",
                                "type": "STRING"
                            },
                            {
                                "name": "timezone",
                                "type": "STRING"
                            },
                            {
                                "name": "timezone_offset",
                                "type": "LONG"
                            },
                            {
                                "name": "version",
                                "type": "STRING"
                            },
                            {
                                "name": "window",
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
                            "targetSize": 3000000,
                            "aggregate": false
                        },
                        "maxWorkerCount": 2,
                        "shuffleCheckHasMultipleValues": true
                    },
                    "phase": "FINISHED",
                    "workerCount": 1,
                    "partitionCount": 1,
                    "startTime": "2022-07-27T20:09:43.168Z",
                    "duration": 62837,
                    "sort": true
                },
                {
                    "stageNumber": 1,
                    "definition": {
                        "id": "f224410f-1cad-4ee7-b10d-f10ddf8bb517_1",
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
                                        },
                                        {
                                            "type": "string",
                                            "name": "browser",
                                            "multiValueHandling": "SORTED_ARRAY",
                                            "createBitmapIndex": true
                                        },
                                        {
                                            "type": "string",
                                            "name": "browser_version",
                                            "multiValueHandling": "SORTED_ARRAY",
                                            "createBitmapIndex": true
                                        },
                                        {
                                            "type": "string",
                                            "name": "city",
                                            "multiValueHandling": "SORTED_ARRAY",
                                            "createBitmapIndex": true
                                        },
                                        {
                                            "type": "string",
                                            "name": "continent",
                                            "multiValueHandling": "SORTED_ARRAY",
                                            "createBitmapIndex": true
                                        },
                                        {
                                            "type": "string",
                                            "name": "country",
                                            "multiValueHandling": "SORTED_ARRAY",
                                            "createBitmapIndex": true
                                        },
                                        {
                                            "type": "string",
                                            "name": "version",
                                            "multiValueHandling": "SORTED_ARRAY",
                                            "createBitmapIndex": true
                                        },
                                        {
                                            "type": "string",
                                            "name": "event_type",
                                            "multiValueHandling": "SORTED_ARRAY",
                                            "createBitmapIndex": true
                                        },
                                        {
                                            "type": "string",
                                            "name": "event_subtype",
                                            "multiValueHandling": "SORTED_ARRAY",
                                            "createBitmapIndex": true
                                        },
                                        {
                                            "type": "string",
                                            "name": "loaded_image",
                                            "multiValueHandling": "SORTED_ARRAY",
                                            "createBitmapIndex": true
                                        },
                                        {
                                            "type": "string",
                                            "name": "adblock_list",
                                            "multiValueHandling": "SORTED_ARRAY",
                                            "createBitmapIndex": true
                                        },
                                        {
                                            "type": "string",
                                            "name": "forwarded_for",
                                            "multiValueHandling": "SORTED_ARRAY",
                                            "createBitmapIndex": true
                                        },
                                        {
                                            "type": "string",
                                            "name": "language",
                                            "multiValueHandling": "SORTED_ARRAY",
                                            "createBitmapIndex": true
                                        },
                                        {
                                            "type": "long",
                                            "name": "number",
                                            "multiValueHandling": "SORTED_ARRAY",
                                            "createBitmapIndex": false
                                        },
                                        {
                                            "type": "string",
                                            "name": "os",
                                            "multiValueHandling": "SORTED_ARRAY",
                                            "createBitmapIndex": true
                                        },
                                        {
                                            "type": "string",
                                            "name": "path",
                                            "multiValueHandling": "SORTED_ARRAY",
                                            "createBitmapIndex": true
                                        },
                                        {
                                            "type": "string",
                                            "name": "platform",
                                            "multiValueHandling": "SORTED_ARRAY",
                                            "createBitmapIndex": true
                                        },
                                        {
                                            "type": "string",
                                            "name": "referrer",
                                            "multiValueHandling": "SORTED_ARRAY",
                                            "createBitmapIndex": true
                                        },
                                        {
                                            "type": "string",
                                            "name": "referrer_host",
                                            "multiValueHandling": "SORTED_ARRAY",
                                            "createBitmapIndex": true
                                        },
                                        {
                                            "type": "string",
                                            "name": "region",
                                            "multiValueHandling": "SORTED_ARRAY",
                                            "createBitmapIndex": true
                                        },
                                        {
                                            "type": "string",
                                            "name": "remote_address",
                                            "multiValueHandling": "SORTED_ARRAY",
                                            "createBitmapIndex": true
                                        },
                                        {
                                            "type": "string",
                                            "name": "screen",
                                            "multiValueHandling": "SORTED_ARRAY",
                                            "createBitmapIndex": true
                                        },
                                        {
                                            "type": "string",
                                            "name": "session",
                                            "multiValueHandling": "SORTED_ARRAY",
                                            "createBitmapIndex": true
                                        },
                                        {
                                            "type": "long",
                                            "name": "session_length",
                                            "multiValueHandling": "SORTED_ARRAY",
                                            "createBitmapIndex": false
                                        },
                                        {
                                            "type": "string",
                                            "name": "timezone",
                                            "multiValueHandling": "SORTED_ARRAY",
                                            "createBitmapIndex": true
                                        },
                                        {
                                            "type": "long",
                                            "name": "timezone_offset",
                                            "multiValueHandling": "SORTED_ARRAY",
                                            "createBitmapIndex": false
                                        },
                                        {
                                            "type": "string",
                                            "name": "window",
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
                                },
                                {
                                    "queryColumn": "browser",
                                    "outputColumn": "browser"
                                },
                                {
                                    "queryColumn": "browser_version",
                                    "outputColumn": "browser_version"
                                },
                                {
                                    "queryColumn": "city",
                                    "outputColumn": "city"
                                },
                                {
                                    "queryColumn": "continent",
                                    "outputColumn": "continent"
                                },
                                {
                                    "queryColumn": "country",
                                    "outputColumn": "country"
                                },
                                {
                                    "queryColumn": "version",
                                    "outputColumn": "version"
                                },
                                {
                                    "queryColumn": "event_type",
                                    "outputColumn": "event_type"
                                },
                                {
                                    "queryColumn": "event_subtype",
                                    "outputColumn": "event_subtype"
                                },
                                {
                                    "queryColumn": "loaded_image",
                                    "outputColumn": "loaded_image"
                                },
                                {
                                    "queryColumn": "adblock_list",
                                    "outputColumn": "adblock_list"
                                },
                                {
                                    "queryColumn": "forwarded_for",
                                    "outputColumn": "forwarded_for"
                                },
                                {
                                    "queryColumn": "language",
                                    "outputColumn": "language"
                                },
                                {
                                    "queryColumn": "number",
                                    "outputColumn": "number"
                                },
                                {
                                    "queryColumn": "os",
                                    "outputColumn": "os"
                                },
                                {
                                    "queryColumn": "path",
                                    "outputColumn": "path"
                                },
                                {
                                    "queryColumn": "platform",
                                    "outputColumn": "platform"
                                },
                                {
                                    "queryColumn": "referrer",
                                    "outputColumn": "referrer"
                                },
                                {
                                    "queryColumn": "referrer_host",
                                    "outputColumn": "referrer_host"
                                },
                                {
                                    "queryColumn": "region",
                                    "outputColumn": "region"
                                },
                                {
                                    "queryColumn": "remote_address",
                                    "outputColumn": "remote_address"
                                },
                                {
                                    "queryColumn": "screen",
                                    "outputColumn": "screen"
                                },
                                {
                                    "queryColumn": "session",
                                    "outputColumn": "session"
                                },
                                {
                                    "queryColumn": "session_length",
                                    "outputColumn": "session_length"
                                },
                                {
                                    "queryColumn": "timezone",
                                    "outputColumn": "timezone"
                                },
                                {
                                    "queryColumn": "timezone_offset",
                                    "outputColumn": "timezone_offset"
                                },
                                {
                                    "queryColumn": "window",
                                    "outputColumn": "window"
                                }
                            ],
                            "tuningConfig": {
                                "type": "index_parallel",
                                "maxRowsPerSegment": 3000000,
                                "appendableIndexSpec": {
                                    "type": "onheap",
                                    "preserveExistingMetrics": false
                                },
                                "maxRowsInMemory": 100000,
                                "maxBytesInMemory": 0,
                                "skipBytesInMemoryOverheadCheck": false,
                                "maxTotalRows": null,
                                "numShards": null,
                                "splitHintSpec": null,
                                "partitionsSpec": {
                                    "type": "dynamic",
                                    "maxRowsPerSegment": 3000000,
                                    "maxTotalRows": null
                                },
                                "indexSpec": {
                                    "bitmap": {
                                        "type": "roaring",
                                        "compressRunOnSerialization": true
                                    },
                                    "dimensionCompression": "lz4",
                                    "metricCompression": "lz4",
                                    "longEncoding": "longs",
                                    "segmentLoader": null
                                },
                                "indexSpecForIntermediatePersists": {
                                    "bitmap": {
                                        "type": "roaring",
                                        "compressRunOnSerialization": true
                                    },
                                    "dimensionCompression": "lz4",
                                    "metricCompression": "lz4",
                                    "longEncoding": "longs",
                                    "segmentLoader": null
                                },
                                "maxPendingPersists": 0,
                                "forceGuaranteedRollup": false,
                                "reportParseExceptions": false,
                                "pushTimeout": 0,
                                "segmentWriteOutMediumFactory": null,
                                "maxNumConcurrentSubTasks": 2,
                                "maxRetry": 1,
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
                        "signature": [],
                        "maxWorkerCount": 2
                    },
                    "phase": "FINISHED",
                    "workerCount": 1,
                    "partitionCount": 1,
                    "startTime": "2022-07-27T20:10:45.840Z",
                    "duration": 50590
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
                                267146161
                            ],
                            "frames": [
                                42
                            ]
                        },
                        "sort": {
                            "type": "channel",
                            "rows": [
                                465346
                            ],
                            "bytes": [
                                265300383
                            ],
                            "frames": [
                                501
                            ]
                        },
                        "sortProgress": {
                            "type": "sortProgress",
                            "totalMergingLevels": 3,
                            "levelToTotalBatches": {
                                "0": 9,
                                "1": 2,
                                "2": 1
                            },
                            "levelToMergedBatches": {
                                "0": 9,
                                "1": 2,
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
                                465346
                            ],
                            "bytes": [
                                265300383
                            ],
                            "frames": [
                                501
                            ]
                        }
                    }
                }
            }
        }
    }
}
```

### Report response fields

The following table describes the response fields when you retrieve a report for a MSQ task engine using the `/druid/indexer/v1/task/<taskId>/report` endpoint:

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
|multiStageQuery.payload.status.errorReport.error|Error object. Contains `errorCode` at a minimum, and may contain other fields as described in the [error code table](./msq-concepts.md#error-codes). Always present if there is an error.|
|multiStageQuery.payload.status.errorReport.error.errorCode|One of the error codes from the [error code table](./msq-concepts.md#error-codes). Always present if there is an error.|
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

### Request

<!--DOCUSAURUS_CODE_TABS-->

<!--HTTP-->

```
POST /druid/indexer/v1/task/<taskId>/shutdown
```

<!--curl-->

Make sure you replace `username`, `password`, `your-instance`, `port`, and `taskId` with the values for your deployment.

```bash
curl --location --request POST 'https://<username>:<password>@<your-instance>:<port>/druid/indexer/v1/task/<taskId>/shutdown'
```

<!--Python-->

Make sure you replace `username`, `password`, `your-instance`, `port`, and `taskId` with the values for your deployment.

```
import requests

url = "https://<username>:<password>@<your-instance>:<port>/druid/indexer/v1/task/<taskId>/shutdown"

payload={}
headers = {}

response = requests.request("POST", url, headers=headers, data=payload)

print(response.text)
```

<!--END_DOCUSAURUS_CODE_TABS-->

### Response

```
{
    "task": "query-655efe33-781a-4c50-ae84-c2911b42d63c"
}
```
