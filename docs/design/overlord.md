---
id: overlord
title: "Overlord service"
sidebar_label: "Overlord"
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


The Overlord service is responsible for accepting tasks, coordinating task distribution, creating locks around tasks, and returning statuses to callers. The Overlord can be configured to run in one of two modes - local or remote (local being default).
In local mode, the Overlord is also responsible for creating Peons for executing tasks. When running the Overlord in local mode, all MiddleManager and Peon configurations must be provided as well.
Local mode is typically used for simple workflows. In remote mode, the Overlord and MiddleManager are run in separate services and you can run each on a different server.
This mode is recommended if you intend to use the indexing service as the single endpoint for all Druid indexing.

## Configuration

For Apache Druid Overlord service configuration, see [Overlord Configuration](../configuration/index.md#overlord).

For basic tuning guidance for the Overlord service, see [Basic cluster tuning](../operations/basic-cluster-tuning.md#overlord).

## HTTP endpoints

For a list of API endpoints supported by the Overlord, please see the [Service status API reference](../api-reference/service-status-api.md#overlord).

## Blacklisted workers

If a MiddleManager has task failures above a threshold, the Overlord will blacklist these MiddleManagers. No more than 20% of the MiddleManagers can be blacklisted. Blacklisted MiddleManagers will be periodically whitelisted.

The following variables can be used to set the threshold and blacklist timeouts.

```
druid.indexer.runner.maxRetriesBeforeBlacklist
druid.indexer.runner.workerBlackListBackoffTime
druid.indexer.runner.workerBlackListCleanupPeriod
druid.indexer.runner.maxPercentageBlacklistWorkers
```

## Autoscaling

The autoscaling mechanisms currently in place are tightly coupled with our deployment infrastructure but the framework should be in place for other implementations. We are highly open to new implementations or extensions of the existing mechanisms. In our own deployments, MiddleManager services are Amazon AWS EC2 nodes and they are provisioned to register themselves in a [galaxy](https://github.com/ning/galaxy) environment.

If autoscaling is enabled, new MiddleManagers may be added when a task has been in pending state for too long. MiddleManagers may be terminated if they have not run any tasks for a period of time.
