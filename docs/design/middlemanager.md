---
id: middlemanager
title: "Middle Manager service"
sidebar_label: "Middle Manager"
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

The Middle Manager service is a worker service that executes submitted tasks. Middle Managers forward tasks to [Peons](../design/peons.md) that run in separate JVMs.
Druid uses separate JVMs for tasks to isolate resources and logs. Each Peon is capable of running only one task at a time, whereas a Middle Manager may have multiple Peons.

## Configuration

For Apache Druid Middle Manager service configuration, see [Middle Manager and Peons](../configuration/index.md#middle-manager-and-peon).

For basic tuning guidance for the Middle Manager service, see [Basic cluster tuning](../operations/basic-cluster-tuning.md#middle-manager).

## HTTP endpoints

For a list of API endpoints supported by the Middle Manager, see the [Service status API reference](../api-reference/service-status-api.md#middle-manager).

## Running

```
org.apache.druid.cli.Main server middleManager
```
