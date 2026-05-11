---
id: zookeeper
title: "ZooKeeper"
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


Apache Druid uses [Apache ZooKeeper](http://zookeeper.apache.org/) (ZK) for management of current cluster state.

## Minimum ZooKeeper versions

Apache Druid supports all stable versions of ZooKeeper. For information about ZooKeeper's stable version, see [ZooKeeper releases](https://zookeeper.apache.org/releases.html).

## ZooKeeper operations

The operations that happen over ZK are:

1. [Coordinator](../design/coordinator.md) leader election
2. [Overlord](../design/overlord.md) leader election
3. Service (node) announcement and discovery — services announce their presence so other services can find them
4. [Overlord](../design/overlord.md) and [Middle Manager](../design/middlemanager.md) task management

Segment loading, dropping, and discovery no longer use ZooKeeper — they are served over HTTP.

## Coordinator leader election

Druid uses the Curator [LeaderLatch](https://curator.apache.org/docs/recipes-leader-latch) recipe to perform leader election at path

```
${druid.zk.paths.coordinatorPath}/_COORDINATOR
```

## Overlord leader election

Druid uses the same [LeaderLatch](https://curator.apache.org/docs/recipes-leader-latch) recipe for Overlord leader election at path

```
${druid.zk.paths.overlordPath}/_OVERLORD
```

## Service announcement and discovery

Each Druid service announces a `DruidNode` record (host, port, role, services) under the internal-discovery path so that other services can enumerate cluster members by role:

```
${druid.zk.paths.base}/internal-discovery/${nodeRole}/${druid.host}
```

Brokers and Coordinators use this path to find Historicals, Peons, and Indexers. They then poll each discovered service's HTTP `/druid-internal/v1/segments` endpoint to get its current set of served segments.
