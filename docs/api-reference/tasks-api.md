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

`GET /druid/indexer/v1/tasks`

Retrieve list of tasks. Accepts query string parameters `state`, `datasource`, `createdTimeInterval`, `max`, and `type`.

|Query Parameter |Description |
|---|---|
|`state`|filter list of tasks by task state, valid options are `running`, `complete`, `waiting`, and `pending`.|
| `datasource`| return tasks filtered by Druid datasource.|
| `createdTimeInterval`| return tasks created within the specified interval. |
| `max`| maximum number of `"complete"` tasks to return. Only applies when `state` is set to `"complete"`.|
| `type`| filter tasks by task type. See [task documentation](../ingestion/tasks.md) for more details.|


`GET /druid/indexer/v1/completeTasks`

Retrieve list of complete tasks. Equivalent to `/druid/indexer/v1/tasks?state=complete`.

`GET /druid/indexer/v1/runningTasks`

Retrieve list of running tasks. Equivalent to `/druid/indexer/v1/tasks?state=running`.

`GET /druid/indexer/v1/waitingTasks`

Retrieve list of waiting tasks. Equivalent to `/druid/indexer/v1/tasks?state=waiting`.

`GET /druid/indexer/v1/pendingTasks`

Retrieve list of pending tasks. Equivalent to `/druid/indexer/v1/tasks?state=pending`.

`GET /druid/indexer/v1/task/{taskId}`

Retrieve the 'payload' of a task.

`GET /druid/indexer/v1/task/{taskId}/status`

Retrieve the status of a task.

`GET /druid/indexer/v1/task/{taskId}/segments`

> This API is deprecated and will be removed in future releases.

Retrieve information about the segments of a task.

`GET /druid/indexer/v1/task/{taskId}/reports`

Retrieve a [task completion report](../ingestion/tasks.md#task-reports) for a task. Only works for completed tasks.

`POST /druid/indexer/v1/task`

Endpoint for submitting tasks and supervisor specs to the Overlord. Returns the taskId of the submitted task.

`POST /druid/indexer/v1/task/{taskId}/shutdown`

Shuts down a task.

`POST /druid/indexer/v1/datasources/{dataSource}/shutdownAllTasks`

Shuts down all tasks for a dataSource.

`POST /druid/indexer/v1/taskStatus`

Retrieve list of task status objects for list of task id strings in request body.

`DELETE /druid/indexer/v1/pendingSegments/{dataSource}`

Manually clean up pending segments table in metadata storage for `datasource`. Returns a JSON object response with
`numDeleted` and count of rows deleted from the pending segments table. This API is used by the
`druid.coordinator.kill.pendingSegments.on` [coordinator setting](../configuration/index.md#coordinator-operation)
which automates this operation to perform periodically.
