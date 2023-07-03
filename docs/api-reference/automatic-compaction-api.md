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

This document describes status and configuration API endpoints for [automatic compaction](../data-management/automatic-compaction.md) in Apache Druid.

## Automatic compaction status

`GET /druid/coordinator/v1/compaction/progress?dataSource={dataSource}`

Returns the total size of segments awaiting compaction for the given dataSource. The specified dataSource must have [automatic compaction](../data-management/automatic-compaction.md) enabled.

`GET /druid/coordinator/v1/compaction/status`

Returns the status and statistics from the auto-compaction run of all dataSources which have auto-compaction enabled in the latest run. The response payload includes a list of `latestStatus` objects. Each `latestStatus` represents the status for a dataSource (which has/had auto-compaction enabled).

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

`GET /druid/coordinator/v1/compaction/status?dataSource={dataSource}`

Similar to the API `/druid/coordinator/v1/compaction/status` above but filters response to only return information for the dataSource given.
The dataSource must have auto-compaction enabled.

## Automatic compaction configuration

`GET /druid/coordinator/v1/config/compaction`

Returns all automatic compaction configs.

`GET /druid/coordinator/v1/config/compaction/{dataSource}`

Returns an automatic compaction config of a dataSource.

`GET /druid/coordinator/v1/config/compaction/{dataSource}/history?interval={interval}&count={count}`

Returns the history of the automatic compaction config for a dataSource. Optionally accepts `interval` and  `count`
query string parameters to filter by interval and limit the number of results respectively. If the dataSource does not
exist or there is no compaction history for the dataSource, an empty list is returned.

The response contains a list of objects with the following keys:
* `globalConfig`: A json object containing automatic compaction config that applies to the entire cluster. 
* `compactionConfig`: A json object containing the automatic compaction config for the datasource.
* `auditInfo`: A json object that contains information about the change made - like `author`, `comment` and `ip`.
* `auditTime`: The date and time when the change was made.

`POST /druid/coordinator/v1/config/compaction/taskslots?ratio={someRatio}&max={someMaxSlots}`

Update the capacity for compaction tasks. `ratio` and `max` are used to limit the max number of compaction tasks.
They mean the ratio of the total task slots to the compaction task slots and the maximum number of task slots for compaction tasks, respectively. The actual max number of compaction tasks is `min(max, ratio * total task slots)`.
Note that `ratio` and `max` are optional and can be omitted. If they are omitted, default values (0.1 and unbounded)
will be set for them.

`POST /druid/coordinator/v1/config/compaction`

Creates or updates the [automatic compaction](../data-management/automatic-compaction.md) config for a dataSource. See [Automatic compaction dynamic configuration](../configuration/index.md#automatic-compaction-dynamic-configuration) for configuration details.

`DELETE /druid/coordinator/v1/config/compaction/{dataSource}`

Removes the automatic compaction config for a dataSource.
