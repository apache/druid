---
id: data-management-api
title: Data management API
sidebar_label: Data management
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

This document describes the data management API endpoints for Apache Druid. This includes information on how to mark segments as `used` or `unused` and delete them from Druid.

## Note for Coordinator's POST and DELETE APIs

While segments may be enabled by issuing POST requests for the datasources, the Coordinator may again disable segments if they match any configured [drop rules](../operations/rule-configuration.md#drop-rules). Even if segments are enabled by these APIs, you must configure a [load rule](../operations/rule-configuration.md#load-rules) to load them onto Historical processes. If an indexing or kill task runs at the same time these APIs are invoked, the behavior is undefined. Some segments might be killed and others might be enabled. It's also possible that all segments might be disabled, but the indexing task can still read data from those segments and succeed.

:::info
 Avoid using indexing or kill tasks and these APIs at the same time for the same datasource and time chunk.
:::

`POST /druid/coordinator/v1/datasources/{dataSourceName}`

Marks as used all segments belonging to a datasource. Returns a JSON object of the form
`{"numChangedSegments": <number>}` with the number of segments in the database whose state has been changed (that is,
the segments were marked as used) as the result of this API call.

`POST /druid/coordinator/v1/datasources/{dataSourceName}/segments/{segmentId}`

Marks as used a segment of a datasource. Returns a JSON object of the form `{"segmentStateChanged": <boolean>}` with
the boolean indicating if the state of the segment has been changed (that is, the segment was marked as used) as the
result of this API call.

`POST /druid/coordinator/v1/datasources/{dataSourceName}/markUsed`

`POST /druid/coordinator/v1/datasources/{dataSourceName}/markUnused`

Marks segments (un)used for a datasource by interval or set of segment Ids. When marking used only segments that are not overshadowed will be updated.

The request payload contains the interval or set of segment IDs to be marked unused.
Either interval or segment IDs should be provided, if both or none are provided in the payload, the API would throw an error (400 BAD REQUEST).

Interval specifies the start and end times as IS0 8601 strings. `interval=(start/end)` where start and end both are inclusive and only the segments completely contained within the specified interval will be disabled, partially overlapping segments will not be affected.

JSON Request Payload:

 |Key|Description|Example|
|----------|-------------|---------|
|`interval`|The interval for which to mark segments unused|`"2015-09-12T03:00:00.000Z/2015-09-12T05:00:00.000Z"`|
|`segmentIds`|Set of segment IDs to be marked unused|`["segmentId1", "segmentId2"]`|

`DELETE /druid/coordinator/v1/datasources/{dataSourceName}`

Marks as unused all segments belonging to a datasource. Returns a JSON object of the form
`{"numChangedSegments": <number>}` with the number of segments in the database whose state has been changed (that is,
the segments were marked as unused) as the result of this API call.

`DELETE /druid/coordinator/v1/datasources/{dataSourceName}/intervals/{interval}`
`@Deprecated. /druid/coordinator/v1/datasources/{dataSourceName}?kill=true&interval={myInterval}`

Runs a [Kill task](../ingestion/tasks.md) for a given interval and datasource.

`DELETE /druid/coordinator/v1/datasources/{dataSourceName}/segments/{segmentId}`

Marks as unused a segment of a datasource. Returns a JSON object of the form `{"segmentStateChanged": <boolean>}` with
the boolean indicating if the state of the segment has been changed (that is, the segment was marked as unused) as the
result of this API call.