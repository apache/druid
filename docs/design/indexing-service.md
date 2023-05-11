---
id: indexing-service
title: "Indexing Service"
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


The Apache Druid indexing service is a highly-available, distributed service that runs indexing related tasks.

Indexing [tasks](../ingestion/tasks.md) are responsible for creating and [killing](../ingestion/tasks.md#kill) Druid [segments](../design/segments.md).

The indexing service is composed of three main components: [Peons](../design/peons.md) that can run a single task, [MiddleManagers](../design/middlemanager.md) that manage Peons, and an [Overlord](../design/overlord.md) that manages task distribution to MiddleManagers.
Overlords and MiddleManagers may run on the same process or across multiple processes, while MiddleManagers and Peons always run on the same process.

Tasks are managed using API endpoints on the Overlord service. See [Overlord Task API](../operations/api-reference.md#tasks) for more information.

![Indexing Service](../assets/indexing_service.png "Indexing Service")

## Overlord

See [Overlord](../design/overlord.md).

## Middle Managers

See [Middle Manager](../design/middlemanager.md).

## Peons

See [Peon](../design/peons.md).

## Tasks

See [Tasks](../ingestion/tasks.md).
