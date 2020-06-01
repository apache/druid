---
id: rolling-updates
title: "Rolling updates"
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


For rolling Apache Druid cluster updates with no downtime, we recommend updating Druid processes in the
following order:

1. Historical
2. \*Overlord (if any)
3. \*Middle Manager/Indexers (if any)
4. Standalone Real-time (if any)
5. Broker
6. Coordinator ( or merged Coordinator+Overlord )

\* In 0.12.0, there are protocol changes between the Kafka supervisor and Kafka Indexing task and also some changes to the metadata formats persisted on disk. Therefore, to support rolling upgrade, all the Middle Managers will need to be upgraded first before the Overlord. Note that this ordering is different from the standard order of upgrade, also note that this ordering is only necessary when using the Kafka Indexing Service. If one is not using Kafka Indexing Service or can handle down time for Kafka Supervisor then one can upgrade in any order.

## Historical

Historical processes can be updated one at a time. Each Historical process has a startup time to memory map
all the segments it was serving before the update. The startup time typically takes a few seconds to
a few minutes, depending on the hardware of the host. As long as each Historical process is updated
with a sufficient delay (greater than the time required to start a single process), you can rolling
update the entire Historical cluster.

## Overlord

Overlord processes can be updated one at a time in a rolling fashion.

## Middle Managers/Indexers

Middle Managers or Indexer nodes run both batch and real-time indexing tasks. Generally you want to update Middle
Managers in such a way that real-time indexing tasks do not fail. There are three strategies for
doing that.

### Rolling restart (restore-based)

Middle Managers can be updated one at a time in a rolling fashion when you set
`druid.indexer.task.restoreTasksOnRestart=true`. In this case, indexing tasks that support restoring
will restore their state on Middle Manager restart, and will not fail.

Currently, only realtime tasks support restoring, so non-realtime indexing tasks will fail and will
need to be resubmitted.

### Rolling restart (graceful-termination-based)

Middle Managers can be gracefully terminated using the "disable" API. This works for all task types,
even tasks that are not restorable.

To prepare a Middle Manager for update, send a POST request to
`<MiddleManager_IP:PORT>/druid/worker/v1/disable`. The Overlord will now no longer send tasks to
this Middle Manager. Tasks that have already started will run to completion. Current state can be checked
using `<MiddleManager_IP:PORT>/druid/worker/v1/enabled` .

To view all existing tasks, send a GET request to `<MiddleManager_IP:PORT>/druid/worker/v1/tasks`.
When this list is empty, you can safely update the Middle Manager. After the Middle Manager starts
back up, it is automatically enabled again. You can also manually enable Middle Managers by POSTing
to `<MiddleManager_IP:PORT>/druid/worker/v1/enable`.

### Autoscaling-based replacement

If autoscaling is enabled on your Overlord, then Overlord processes can launch new Middle Manager processes
en masse and then gracefully terminate old ones as their tasks finish. This process is configured by
setting `druid.indexer.runner.minWorkerVersion=#{VERSION}`. Each time you update your Overlord process,
the `VERSION` value should be increased, which will trigger a mass launch of new Middle Managers.

The config `druid.indexer.autoscale.workerVersion=#{VERSION}` also needs to be set.

## Standalone Real-time

Standalone real-time processes can be updated one at a time in a rolling fashion.

## Broker

Broker processes can be updated one at a time in a rolling fashion. There needs to be some delay between
updating each process as Brokers must load the entire state of the cluster before they return valid
results.

## Coordinator

Coordinator processes can be updated one at a time in a rolling fashion.
