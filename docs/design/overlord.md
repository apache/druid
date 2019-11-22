---
id: overlord
title: "Overlord Process"
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


### Configuration

For Apache Druid (incubating) Overlord Process Configuration, see [Overlord Configuration](../configuration/index.html#overlord).

### HTTP endpoints

For a list of API endpoints supported by the Overlord, please see the [API reference](../operations/api-reference.html#overlord).

### Overview

The Overlord process is responsible for accepting tasks, coordinating task distribution, creating locks around tasks, and returning statuses to callers. Overlord can be configured to run in one of two modes - local or remote (local being default).
In local mode Overlord is also responsible for creating Peons for executing tasks. When running the Overlord in local mode, all MiddleManager and Peon configurations must be provided as well.
Local mode is typically used for simple workflows.  In remote mode, the Overlord and MiddleManager are run in separate processes and you can run each on a different server.
This mode is recommended if you intend to use the indexing service as the single endpoint for all Druid indexing.

### Overlord console

The Overlord provides a UI for managing tasks and workers. For more details, please see [overlord console](../operations/management-uis.html#overlord-console).

### Blacklisted workers

If a MiddleManager has task failures above a threshold, the Overlord will blacklist these MiddleManagers. No more than 20% of the MiddleManagers can be blacklisted. Blacklisted MiddleManagers will be periodically whitelisted.

The following variables can be used to set the threshold and blacklist timeouts.

```
druid.indexer.runner.maxRetriesBeforeBlacklist
druid.indexer.runner.workerBlackListBackoffTime
druid.indexer.runner.workerBlackListCleanupPeriod
druid.indexer.runner.maxPercentageBlacklistWorkers
```

### Autoscaling

The Autoscaling mechanisms currently in place are tightly coupled with our deployment infrastructure but the framework should be in place for other implementations. We are highly open to new implementations or extensions of the existing mechanisms. In our own deployments, MiddleManager processes are Amazon AWS EC2 nodes and they are provisioned to register themselves in a [galaxy](https://github.com/ning/galaxy) environment.

If autoscaling is enabled, new MiddleManagers may be added when a task has been in pending state for too long. MiddleManagers may be terminated if they have not run any tasks for a period of time.
