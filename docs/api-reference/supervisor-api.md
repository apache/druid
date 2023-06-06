---
id: supervisor-api
title: Supervisor API
sidebar_label: Supervisor
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

This document describes the API endpoints to manage and monitor supervisors for Apache Druid.

## Supervisors

`GET /druid/indexer/v1/supervisor`

Returns a list of strings of the currently active supervisor ids.

`GET /druid/indexer/v1/supervisor?full`

Returns a list of objects of the currently active supervisors.

|Field|Type|Description|
|---|---|---|
|`id`|String|supervisor unique identifier|
|`state`|String|basic state of the supervisor. Available states:`UNHEALTHY_SUPERVISOR`, `UNHEALTHY_TASKS`, `PENDING`, `RUNNING`, `SUSPENDED`, `STOPPING`. Check [Kafka Docs](../development/extensions-core/kafka-supervisor-operations.md) for details.|
|`detailedState`|String|supervisor specific state. See documentation of specific supervisor for details: [Kafka](../development/extensions-core/kafka-ingestion.md) or [Kinesis](../development/extensions-core/kinesis-ingestion.md)|
|`healthy`|Boolean|true or false indicator of overall supervisor health|
|`spec`|SupervisorSpec|JSON specification of supervisor|

`GET /druid/indexer/v1/supervisor?state=true`

Returns a list of objects of the currently active supervisors and their current state.

|Field|Type|Description|
|---|---|---|
|`id`|String|supervisor unique identifier|
|`state`|String|basic state of the supervisor. Available states: `UNHEALTHY_SUPERVISOR`, `UNHEALTHY_TASKS`, `PENDING`, `RUNNING`, `SUSPENDED`, `STOPPING`. Check [Kafka Docs](../development/extensions-core/kafka-supervisor-operations.md) for details.|
|`detailedState`|String|supervisor specific state. See documentation of the specific supervisor for details: [Kafka](../development/extensions-core/kafka-ingestion.md) or [Kinesis](../development/extensions-core/kinesis-ingestion.md)|
|`healthy`|Boolean|true or false indicator of overall supervisor health|
|`suspended`|Boolean|true or false indicator of whether the supervisor is in suspended state|

`GET /druid/indexer/v1/supervisor/<supervisorId>`

Returns the current spec for the supervisor with the provided ID.

`GET /druid/indexer/v1/supervisor/<supervisorId>/status`

Returns the current status of the supervisor with the provided ID.

`GET/druid/indexer/v1/supervisor/history`

Returns an audit history of specs for all supervisors (current and past).

`GET /druid/indexer/v1/supervisor/<supervisorId>/history`

Returns an audit history of specs for the supervisor with the provided ID.

`POST /druid/indexer/v1/supervisor`

Create a new supervisor or update an existing one.

`POST /druid/indexer/v1/supervisor/<supervisorId>/suspend`

Suspend the current running supervisor of the provided ID. Responds with updated SupervisorSpec.

`POST /druid/indexer/v1/supervisor/suspendAll`

Suspend all supervisors at once.

`POST /druid/indexer/v1/supervisor/<supervisorId>/resume`

Resume indexing tasks for a supervisor. Responds with updated SupervisorSpec.

`POST /druid/indexer/v1/supervisor/resumeAll`

Resume all supervisors at once.

`POST /druid/indexer/v1/supervisor/<supervisorId>/reset`

Reset the specified supervisor.

`POST /druid/indexer/v1/supervisor/<supervisorId>/terminate`

Terminate a supervisor of the provided ID.

`POST /druid/indexer/v1/supervisor/terminateAll`

Terminate all supervisors at once.

`POST /druid/indexer/v1/supervisor/<supervisorId>/shutdown`

> This API is deprecated and will be removed in future releases.
> Please use the equivalent `terminate` instead.

Shutdown a supervisor.