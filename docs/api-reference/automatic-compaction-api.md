---
id: automatic-compaction-api
title: Automatic compaction API
sidebar_label: Automatic compaction
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

This document describes status and configuration API endpoints for [automatic compaction](../data-management/automatic-compaction.md) in Apache Druid. Automatic compaction can be configured in the Druid web console or API. 

In this topic, `http://ROUTER_IP:ROUTER_PORT` is a placeholder for your Router service address and port. Replace it with the information for your deployment. For example, use `http://localhost:8888` for quickstart deployments.

## Manage automatic compaction

### Update capacity for compaction tasks

Update the capacity for compaction tasks. Maximum number of compaction tasks is limited by `ratio` and `max`.
`ratio` is ratio of the total task slots to the compaction task slots and the `max` is the number of task slots for compaction tasks.

The max and min number of compaction tasks is derived from this equation `clamp(floor(compaction_task_slot_ratio * total_task_slots), 1, 2147483652)`, where the minimum number of compaction tasks is 1 and maximum is 2147483652.

#### URL

<code class="postAPI">POST</code> <code>/druid/coordinator/v1/config/compaction/taskslots</code>

#### Query parameters
* `ratio` (optional)
  * Type: Float
  * Default: 0.1
  * Limit the ratio of the total task slots to compaction task slots.
* `max` (optional)
  * Type: Int
  * Default: 2147483647
  * Limit the maximum number of task slots for compaction tasks.

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->

*Successfully updated compaction configuration* 

<!--404 NOT FOUND-->

*Invalid `max` value* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->

```shell
curl --request POST "http://ROUTER_IP:ROUTER_PORT/druid/coordinator/v1/config/compaction/taskslots?ratio=0.2&max=250000"
```

<!--HTTP-->

```HTTP
POST /druid/coordinator/v1/config/compaction/taskslots?ratio=0.2&max=250000 HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

A successful request returns an HTTP `200 OK` and an empty response body.

### Create or update datasource automatic compaction configuration

Creates or updates the automatic compaction config for a datasource. The automatic compaction can be submitted as a JSON object in the request body. 

Automatic compaction configuration require only the `dataSource` property. All others will be filled with default values if not specified. See [Automatic compaction dynamic configuration](../configuration/index.md#automatic-compaction-dynamic-configuration) for configuration details.

Note that this endpoint will return an HTTP `200 OK` even if the datasource name does not exist.

#### URL 

<code class="postAPI">POST</code> <code>/druid/coordinator/v1/config/compaction</code>

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->

*Successfully submitted auto compaction configuration* 

<!--END_DOCUSAURUS_CODE_TABS-->

---
#### Sample request

The following example creates an automatic compaction configuration for datasource `wikipedia_hour`. This automatic compaction configuration performs compaction on `wikipedia_hour`, resulting in compacted segments that represent a day interval of data.

* `wikipedia_hour` is a datasource with `HOUR` segment granularity.
* `skipOffsetFromLatest` is set to `PT0S`, meaning that no data is skipped. 
* `partitionsSpec` is set to the default `dynamic`, allowing Druid to dynamically determine the optimal partitioning strategy.
* `type` is set to `index_parallel`, meaning that parallel indexing will be used.
* `segmentGranularity` is set to `DAY`, meaning that each compacted segment will be a day of data.

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->

```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/coordinator/v1/config/compaction"\
--header 'Content-Type: application/json' \
--data '{
    "dataSource": "wikipedia_hour",
    "skipOffsetFromLatest": "PT0S",
    "tuningConfig": {
        "partitionsSpec": {
            "type": "dynamic"
        },
        "type": "index_parallel"
    },
    "granularitySpec": {
        "segmentGranularity": "DAY"
    }
}'
```

<!--HTTP-->

```HTTP
POST /druid/coordinator/v1/config/compaction HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
Content-Type: application/json
Content-Length: 281

{
    "dataSource": "wikipedia_hour",
    "skipOffsetFromLatest": "PT0S",
    "tuningConfig": {
        "partitionsSpec": {
            "type": "dynamic"
        },
        "type": "index_parallel"
    },
    "granularitySpec": {
        "segmentGranularity": "DAY"
    }
}
```

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

A successful request returns an HTTP `200 OK` and an empty response body.


### Remove datasource automatic compaction configuration

Removes the automatic compaction configuration for a datasource. This updates the compaction status of the datasource to "Not enabled." 

#### URL

<code class="deleteAPI">DELETE</code> <code>/druid/coordinator/v1/config/compaction/:dataSource</code>

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->

*Successfully deleted automatic compaction configuration* 

<!--404 NOT FOUND-->

*Datasource does not have automatic compaction or invalid datasource name* 

<!--END_DOCUSAURUS_CODE_TABS-->

---


#### Sample request

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->

```shell
curl --request DELETE "http://ROUTER_IP:ROUTER_PORT/druid/coordinator/v1/config/compaction/wikipedia_hour"
```

<!--HTTP-->

```HTTP
DELETE /druid/coordinator/v1/config/compaction/wikipedia_hour HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

A successful request returns an HTTP `200 OK` and an empty response body.

## Automatic compaction configuration

### Get all automatic compaction configuration

Retrieves all automatic compaction configurations. Returns a `compactionConfigs` object containing the active automatic compaction configuration of all datasources.

You can use this endpoint to retrieve `compactionTaskSlotRatio` and `maxCompactionTaskSlots` values for managing resource allocation of compaction tasks.

#### URL

<code class="getAPI">GET</code> <code>/druid/coordinator/v1/config/compaction</code>

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->

*Successfully retrieved automatic compaction configurations* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->

```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/coordinator/v1/config/compaction"
```

<!--HTTP-->

```HTTP
GET /druid/coordinator/v1/config/compaction HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

<details>
  <summary>Click to show sample response</summary>

```json
{
    "compactionConfigs": [
        {
            "dataSource": "wikipedia_hour",
            "taskPriority": 25,
            "inputSegmentSizeBytes": 100000000000000,
            "maxRowsPerSegment": null,
            "skipOffsetFromLatest": "PT0S",
            "tuningConfig": {
                "maxRowsInMemory": null,
                "appendableIndexSpec": null,
                "maxBytesInMemory": null,
                "maxTotalRows": null,
                "splitHintSpec": null,
                "partitionsSpec": {
                    "type": "dynamic",
                    "maxRowsPerSegment": 5000000,
                    "maxTotalRows": null
                },
                "indexSpec": null,
                "indexSpecForIntermediatePersists": null,
                "maxPendingPersists": null,
                "pushTimeout": null,
                "segmentWriteOutMediumFactory": null,
                "maxNumConcurrentSubTasks": null,
                "maxRetry": null,
                "taskStatusCheckPeriodMs": null,
                "chatHandlerTimeout": null,
                "chatHandlerNumRetries": null,
                "maxNumSegmentsToMerge": null,
                "totalNumMergeTasks": null,
                "maxColumnsToMerge": null,
                "type": "index_parallel",
                "forceGuaranteedRollup": false
            },
            "granularitySpec": {
                "segmentGranularity": "DAY",
                "queryGranularity": null,
                "rollup": null
            },
            "dimensionsSpec": null,
            "metricsSpec": null,
            "transformSpec": null,
            "ioConfig": null,
            "taskContext": null
        },
        {
            "dataSource": "wikipedia",
            "taskPriority": 25,
            "inputSegmentSizeBytes": 100000000000000,
            "maxRowsPerSegment": null,
            "skipOffsetFromLatest": "PT0S",
            "tuningConfig": {
                "maxRowsInMemory": null,
                "appendableIndexSpec": null,
                "maxBytesInMemory": null,
                "maxTotalRows": null,
                "splitHintSpec": null,
                "partitionsSpec": {
                    "type": "dynamic",
                    "maxRowsPerSegment": 5000000,
                    "maxTotalRows": null
                },
                "indexSpec": null,
                "indexSpecForIntermediatePersists": null,
                "maxPendingPersists": null,
                "pushTimeout": null,
                "segmentWriteOutMediumFactory": null,
                "maxNumConcurrentSubTasks": null,
                "maxRetry": null,
                "taskStatusCheckPeriodMs": null,
                "chatHandlerTimeout": null,
                "chatHandlerNumRetries": null,
                "maxNumSegmentsToMerge": null,
                "totalNumMergeTasks": null,
                "maxColumnsToMerge": null,
                "type": "index_parallel",
                "forceGuaranteedRollup": false
            },
            "granularitySpec": {
                "segmentGranularity": "DAY",
                "queryGranularity": null,
                "rollup": null
            },
            "dimensionsSpec": null,
            "metricsSpec": null,
            "transformSpec": null,
            "ioConfig": null,
            "taskContext": null
        }
    ],
    "compactionTaskSlotRatio": 0.1,
    "maxCompactionTaskSlots": 2147483647,
    "useAutoScaleSlots": false
}
```
</details>

### Get datasource automatic compaction configuration

Retrieves the automatic compaction configuration of a datasource.

#### URL

<code class="getAPI">GET</code> <code>/druid/coordinator/v1/config/compaction/:dataSource</code>

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->

*Successfully retrieved configuration for datasource* 

<!--404 NOT FOUND-->

*Invalid datasource or datasource does not have automatic compaction enabled*

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

The following example retrieves the automatic compaction configuration for datasource `wikipedia_hour`.

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->

```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/coordinator/v1/config/compaction/wikipedia_hour"
```

<!--HTTP-->

```HTTP
GET /druid/coordinator/v1/config/compaction/wikipedia_hour HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

<details>
  <summary>Click to show sample response</summary>

```json
{
    "dataSource": "wikipedia_hour",
    "taskPriority": 25,
    "inputSegmentSizeBytes": 100000000000000,
    "maxRowsPerSegment": null,
    "skipOffsetFromLatest": "PT0S",
    "tuningConfig": {
        "maxRowsInMemory": null,
        "appendableIndexSpec": null,
        "maxBytesInMemory": null,
        "maxTotalRows": null,
        "splitHintSpec": null,
        "partitionsSpec": {
            "type": "dynamic",
            "maxRowsPerSegment": 5000000,
            "maxTotalRows": null
        },
        "indexSpec": null,
        "indexSpecForIntermediatePersists": null,
        "maxPendingPersists": null,
        "pushTimeout": null,
        "segmentWriteOutMediumFactory": null,
        "maxNumConcurrentSubTasks": null,
        "maxRetry": null,
        "taskStatusCheckPeriodMs": null,
        "chatHandlerTimeout": null,
        "chatHandlerNumRetries": null,
        "maxNumSegmentsToMerge": null,
        "totalNumMergeTasks": null,
        "maxColumnsToMerge": null,
        "type": "index_parallel",
        "forceGuaranteedRollup": false
    },
    "granularitySpec": {
        "segmentGranularity": "DAY",
        "queryGranularity": null,
        "rollup": null
    },
    "dimensionsSpec": null,
    "metricsSpec": null,
    "transformSpec": null,
    "ioConfig": null,
    "taskContext": null
}
```
</details>

### Get datasource automatic compaction configuration history

Retrieves the history of the automatic compaction config for a datasource. Returns an empty list if the  datasource does not exist or there is no compaction history for the datasource.

The response contains a list of objects with the following keys:
* `globalConfig`: A JSON object containing automatic compaction config that applies to the entire cluster. 
* `compactionConfig`: A JSON object containing the automatic compaction config for the datasource.
* `auditInfo`: A JSON object that contains information about the change made - like `author`, `comment` and `ip`.
* `auditTime`: The date and time when the change was made.

#### URL

<code class="getAPI">GET</code> <code>/druid/coordinator/v1/config/compaction/:dataSource/history</code>

#### Query parameters
* `interval` (optional)
  * Type: ISO-8601
  * Limit the results within a specified interval. Use `/` as the delimiter for the interval string. 
* `count` (optional)
  * Type: Int
  * Limit the number of results.

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->

*Successfully retrieved configuration history* 

<!--400 BAD REQUEST-->

*Invalid `count` value* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->

```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/coordinator/v1/config/compaction/wikipedia_hour/history"
```

<!--HTTP-->

```HTTP
GET /druid/coordinator/v1/config/compaction/wikipedia_hour/history HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

<details>
  <summary>Click to show sample response</summary>

```json
[
    {
        "globalConfig": {
            "compactionTaskSlotRatio": 0.1,
            "maxCompactionTaskSlots": 2147483647,
            "useAutoScaleSlots": false
        },
        "compactionConfig": {
            "dataSource": "wikipedia_hour",
            "taskPriority": 25,
            "inputSegmentSizeBytes": 100000000000000,
            "maxRowsPerSegment": null,
            "skipOffsetFromLatest": "P1D",
            "tuningConfig": null,
            "granularitySpec": {
                "segmentGranularity": "DAY",
                "queryGranularity": null,
                "rollup": null
            },
            "dimensionsSpec": null,
            "metricsSpec": null,
            "transformSpec": null,
            "ioConfig": null,
            "taskContext": null
        },
        "auditInfo": {
            "author": "",
            "comment": "",
            "ip": "127.0.0.1"
        },
        "auditTime": "2023-07-31T18:15:19.302Z"
    },
    {
        "globalConfig": {
            "compactionTaskSlotRatio": 0.1,
            "maxCompactionTaskSlots": 2147483647,
            "useAutoScaleSlots": false
        },
        "compactionConfig": {
            "dataSource": "wikipedia_hour",
            "taskPriority": 25,
            "inputSegmentSizeBytes": 100000000000000,
            "maxRowsPerSegment": null,
            "skipOffsetFromLatest": "PT0S",
            "tuningConfig": {
                "maxRowsInMemory": null,
                "appendableIndexSpec": null,
                "maxBytesInMemory": null,
                "maxTotalRows": null,
                "splitHintSpec": null,
                "partitionsSpec": {
                    "type": "dynamic",
                    "maxRowsPerSegment": 5000000,
                    "maxTotalRows": null
                },
                "indexSpec": null,
                "indexSpecForIntermediatePersists": null,
                "maxPendingPersists": null,
                "pushTimeout": null,
                "segmentWriteOutMediumFactory": null,
                "maxNumConcurrentSubTasks": null,
                "maxRetry": null,
                "taskStatusCheckPeriodMs": null,
                "chatHandlerTimeout": null,
                "chatHandlerNumRetries": null,
                "maxNumSegmentsToMerge": null,
                "totalNumMergeTasks": null,
                "maxColumnsToMerge": null,
                "type": "index_parallel",
                "forceGuaranteedRollup": false
            },
            "granularitySpec": {
                "segmentGranularity": "DAY",
                "queryGranularity": null,
                "rollup": null
            },
            "dimensionsSpec": null,
            "metricsSpec": null,
            "transformSpec": null,
            "ioConfig": null,
            "taskContext": null
        },
        "auditInfo": {
            "author": "",
            "comment": "",
            "ip": "127.0.0.1"
        },
        "auditTime": "2023-07-31T18:16:16.362Z"
    }
]
```
</details>

## Automatic compaction status

### Get segments awaiting compaction

Returns the total size of segments awaiting compaction for the given datasource. The specified datasource must have automatic compaction enabled.

#### URL

<code class="getAPI">GET</code> <code>/druid/coordinator/v1/compaction/progress?dataSource=:dataSource</code>

#### Query parameter
* `dataSource` (required)
  * Type: String
  * Name of the datasource for this status information

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->

*Successfully retrieved segment size awaiting compaction* 

<!--404 NOT FOUND-->

*Unknown datasource name or datasource does not have automatic compaction enabled* 

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

The following example retrieves the remaining segments to be compacted for datasource `wikipedia_hour`.

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->

```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/coordinator/v1/compaction/progress?dataSource=wikipedia_hour"
```

<!--HTTP-->

```HTTP
GET /druid/coordinator/v1/compaction/progress?dataSource=wikipedia_hour HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

<details>
  <summary>Click to show sample response</summary>

```json
{
    "remainingSegmentSize": 7615837
}
```
</details>


### Get compaction status and statistics

Retrieves the status and statistics from the automatic compaction run of all datasources which have automatic compaction enabled in the latest run. Returns a list of `latestStatus` objects. Each `latestStatus` represents the status for a datasource with automatic compaction enabled.

The `latestStatus` object has the following keys:
* `dataSource`: Name of the datasource for this status information.
* `scheduleStatus`: Automatic compaction scheduling status. Possible values are `NOT_ENABLED` and `RUNNING`. Returns `RUNNING ` if the dataSource has an active automatic compaction config submitted. Otherwise, returns `NOT_ENABLED`.
* `bytesAwaitingCompaction`: Total bytes of this datasource waiting to be compacted by the automatic compaction (only consider intervals/segments that are eligible for automatic compaction).
* `bytesCompacted`: Total bytes of this datasource that are already compacted with the spec set in the automatic compaction config.
* `bytesSkipped`: Total bytes of this datasource that are skipped (not eligible for automatic compaction) by the automatic compaction.
* `segmentCountAwaitingCompaction`: Total number of segments of this datasource waiting to be compacted by the automatic compaction (only consider intervals/segments that are eligible for automatic compaction).
* `segmentCountCompacted`: Total number of segments of this datasource that are already compacted with the spec set in the automatic compaction config.
* `segmentCountSkipped`: Total number of segments of this datasource that are skipped (not eligible for automatic compaction) by the automatic compaction.
* `intervalCountAwaitingCompaction`: Total number of intervals of this datasource waiting to be compacted by the automatic compaction (only consider intervals/segments that are eligible for automatic compaction).
* `intervalCountCompacted`: Total number of intervals of this datasource that are already compacted with the spec set in the automatic compaction config.
* `intervalCountSkipped`: Total number of intervals of this datasource that are skipped (not eligible for automatic compaction) by the automatic compaction.

#### URL

<code class="getAPI">GET</code> <code>/druid/coordinator/v1/compaction/status</code>

#### Query parameters
* `dataSource` (optional)
  * Type: String
  * Filter the result by name of specific datasource.

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->

*Successfully retrieved `latestStatus` object* 

<!--END_DOCUSAURUS_CODE_TABS-->

---
#### Sample request

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->

```shell
curl "hhttp://ROUTER_IP:ROUTER_PORT/druid/coordinator/v1/compaction/status"
```

<!--HTTP-->

```HTTP
GET /druid/coordinator/v1/compaction/status HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response

<details>
  <summary>Click to show sample response</summary>

```json
{
    "latestStatus": [
        {
            "dataSource": "wikipedia_api",
            "scheduleStatus": "RUNNING",
            "bytesAwaitingCompaction": 0,
            "bytesCompacted": 0,
            "bytesSkipped": 64133616,
            "segmentCountAwaitingCompaction": 0,
            "segmentCountCompacted": 0,
            "segmentCountSkipped": 8,
            "intervalCountAwaitingCompaction": 0,
            "intervalCountCompacted": 0,
            "intervalCountSkipped": 1
        },
        {
            "dataSource": "wikipedia_hour",
            "scheduleStatus": "RUNNING",
            "bytesAwaitingCompaction": 0,
            "bytesCompacted": 5998634,
            "bytesSkipped": 0,
            "segmentCountAwaitingCompaction": 0,
            "segmentCountCompacted": 1,
            "segmentCountSkipped": 0,
            "intervalCountAwaitingCompaction": 0,
            "intervalCountCompacted": 1,
            "intervalCountSkipped": 0
        }
    ]
}
```
</details>
