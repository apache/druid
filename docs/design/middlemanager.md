---
id: middlemanager
title: "MiddleManager service"
sidebar_label: "MiddleManager"
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

The MiddleManager service is a worker service that executes submitted tasks. MiddleManagers forward tasks to [Peons](../design/peons.md) that run in separate JVMs.
Druid uses separate JVMs for tasks to isolate resources and logs. Each Peon is capable of running only one task at a time, wheres a MiddleManager may have multiple Peons.

## Configuration

For Apache Druid MiddleManager service configuration, see [MiddleManager and Peons](../configuration/index.md#middlemanager-and-peons).

For basic tuning guidance for the MiddleManager service, see [Basic cluster tuning](../operations/basic-cluster-tuning.md#middlemanager).

## HTTP endpoints

For a list of API endpoints supported by the MiddleManager, see the [Service status API reference](../api-reference/service-status-api.md#middlemanager).

## Running

```
org.apache.druid.cli.Main server middleManager
```
