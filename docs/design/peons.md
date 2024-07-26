---
id: peons
title: "Peon service"
sidebar_label: "Peon"
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

The Peon service is a task execution engine spawned by the MiddleManager. Each Peon runs a separate JVM and is responsible for executing a single task. Peons always run on the same host as the MiddleManager that spawned them.

## Configuration

For Apache Druid Peon configuration, see [Peon Query Configuration](../configuration/index.md#peon-query-configuration) and [Additional Peon Configuration](../configuration/index.md#additional-peon-configuration).

For basic tuning guidance for MiddleManager tasks, see [Basic cluster tuning](../operations/basic-cluster-tuning.md#task-configurations).

## HTTP endpoints

Peons run a single task in a single JVM. The MiddleManager is responsible for creating Peons for running tasks.
Peons should rarely run on their own.

## Running

The Peon should seldom run separately from the MiddleManager, except for development purposes.

```
org.apache.druid.cli.Main internal peon <task_file> <status_file>
```

The task file contains the task JSON object.
The status file indicates where the task status will be output.
