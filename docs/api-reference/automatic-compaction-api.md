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

In this topic, `http://ROUTER_IP:ROUTER_PORT` is a place holder for your Router service address and port. Replace it with the information for your deployment. For example, use `http://localhost:8888` for quickstart deployments.

## Automatic compaction status

### Get segments awaiting compaction

Returns the total size of segments awaiting compaction for the given datasource. The specified datasource must have automatic compaction enabled.

#### URL

<code class="getAPI">GET</code> <code>/druid/coordinator/v1/compaction/progress?dataSource=:datasource</code>

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
    "remainingSegmentSize": 0
}
```
</details>


### Get compaction status and statistics

Returns the status and statistics from the auto-compaction run of all datasources which have auto-compaction enabled in the latest run. The response payload includes a list of `latestStatus` objects. Each `latestStatus` represents the status for a datasource (which has/had auto-compaction enabled).

The `latestStatus` object has the following keys:
* `dataSource`: name of the datasource for this status information
* `scheduleStatus`: auto-compaction scheduling status. Possible values are `NOT_ENABLED` and `RUNNING`. Returns `RUNNING ` if the dataSource has an active auto-compaction config submitted. Otherwise, returns `NOT_ENABLED`.
* `bytesAwaitingCompaction`: total bytes of this datasource waiting to be compacted by the auto-compaction (only consider intervals/segments that are eligible for auto-compaction)
* `bytesCompacted`: total bytes of this datasource that are already compacted with the spec set in the auto-compaction config
* `bytesSkipped`: total bytes of this datasource that are skipped (not eligible for auto-compaction) by the auto-compaction
* `segmentCountAwaitingCompaction`: total number of segments of this datasource waiting to be compacted by the auto-compaction (only consider intervals/segments that are eligible for auto-compaction)
* `segmentCountCompacted`: total number of segments of this datasource that are already compacted with the spec set in the auto-compaction config
* `segmentCountSkipped`: total number of segments of this datasource that are skipped (not eligible for auto-compaction) by the auto-compaction
* `intervalCountAwaitingCompaction`: total number of intervals of this datasource waiting to be compacted by the auto-compaction (only consider intervals/segments that are eligible for auto-compaction)
* `intervalCountCompacted`: total number of intervals of this datasource that are already compacted with the spec set in the auto-compaction config
* `intervalCountSkipped`: total number of intervals of this datasource that are skipped (not eligible for auto-compaction) by the auto-compaction

#### URL

<code class="getAPI">GET</code> <code>/druid/coordinator/v1/compaction/status</code>

#### Query parameters
* `dataSource` (optional)
  * Type: String
  * Filter the result by name of specific datasource

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


## Automatic compaction configuration

### Get all automatic compaction configuration

Retrieves all automatic compaction configurations.

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

<code class="getAPI">GET</code> <code>/druid/coordinator/v1/config/compaction/:datasource</code>

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
* `globalConfig`: A json object containing automatic compaction config that applies to the entire cluster. 
* `compactionConfig`: A json object containing the automatic compaction config for the datasource.
* `auditInfo`: A json object that contains information about the change made - like `author`, `comment` and `ip`.
* `auditTime`: The date and time when the change was made.

#### URL

<code class="getAPI">GET</code> <code>/druid/coordinator/v1/config/compaction/:datasource/history</code>

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

### Update the capacity for compaction tasks

#### URL

<code class="postAPI">POST</code> <code>/druid/coordinator/v1/config/compaction/taskslots?ratio={someRatio}&max={someMaxSlots}</code>

Update the capacity for compaction tasks. `ratio` and `max` are used to limit the max number of compaction tasks.
They mean the ratio of the total task slots to the compaction task slots and the maximum number of task slots for compaction tasks, respectively. The actual max number of compaction tasks is `min(max, ratio * total task slots)`.
Note that `ratio` and `max` are optional and can be omitted. If they are omitted, default values (0.1 and unbounded)
will be set for them.

### Create or update datasource automatic compaction configuration

#### URL 

<code class="postAPI">POST</code> <code>/druid/coordinator/v1/config/compaction</code>

Creates or updates the [automatic compaction](../data-management/automatic-compaction.md) config for a dataSource. See [Automatic compaction dynamic configuration](../configuration/index.md#automatic-compaction-dynamic-configuration) for configuration details.

`DELETE /druid/coordinator/v1/config/compaction/{dataSource}`

Removes the automatic compaction config for a dataSource.
