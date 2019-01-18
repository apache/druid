---
layout: doc_page
title: "Process Colocation"
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

# Process Colocation

Druid processes can be colocated based on the Master/Data/Query server organization as
described in the [architecture overview](../design/index.html). This organization generally results in better utilization of
hardware resources for most clusters.

For very large scale clusters, however, it can be desirable to split the Druid processes
such that they run on individual servers to avoid resource contention.

This page describes guidelines and configuration parameters related to process colocation.

## Coordinators and Overlords

The workload on the coordinator process tends to increase with the number of segments in the cluster. The overlord's workload also increases based on the number of segments in the cluster, but to a lesser degree than the coordinator.

In clusters with very high segment counts, it can make sense to separate the coordinator and overlord processes to provide more resources for the coordinator's segment balancing workload.

### Unified Process

The coordinator and overlord processes can be run as a single combined process by setting the `druid.coordinator.asOverlord.enabled` property.

Please see [Coordinator Configuration: Operation](../configuration/index.html#coordinator-operation) for details.

## Historicals and MiddleManagers

With higher levels of ingestion or query load, it can make sense to deploy the Historical and MiddleManager processes on separate nodes to to avoid CPU and memory contention. 

The historical also benefits from having free memory for memory mapped segments, which can be another reason to deploy the Data Server processes separately.