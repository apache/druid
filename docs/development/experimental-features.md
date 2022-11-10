---
id: experimental-features
title: "Experimental features"
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

The following features are marked [experimental](./experimental.md) in the Druid docs.

This document includes each page that mentions an experimental feature. To graduate a feature, remove all mentions of its experimental status on all relevant pages.

## SQL-based ingestion

[SQL-based ingestion](../multi-stage-query/index.md)

- As an experimental feature, the task engine also supports running SELECT queries as batch tasks.

[SQL-based ingestion concepts](../multi-stage-query/concepts.md)

- As an experimental feature, the MSQ task engine also supports running SELECT queries as batch tasks. The behavior and result format of plain SELECT (without INSERT or REPLACE) is subject to change.

[SQL-based ingestion and multi-stage query task API](../multi-stage-query/api.md)

- As an experimental feature, the `/druid/v2/sql/task/` endpoint also accepts SELECT queries.
- As an experimental feature, the MSQ task engine supports running SELECT queries.

## Nested columns

[Nested columns](../querying/nested-columns.md)

- Nested columns is an experimental feature available starting in Apache Druid 24.0.

## Indexer process

[Indexer process](../design/indexer.md)

- The Indexer is an optional and experimental feature. Its memory management system is still under development and will be significantly enhanced in later releases.

[Configuration reference](../configuration/index.md)

- Data Server: Configuration options for the experimental Indexer process are also provided here.

[Processes and servers](../design/processes.md)

- Indexer process (optional): The Indexer is designed to be easier to configure and deploy compared to the MiddleManager + Peon system and to better enable resource sharing across tasks. The Indexer is a newer feature and is currently designated experimental due to the fact that its memory management system is still under development. It will continue to mature in future versions of Druid.

## Kubernetes

[Kubernetes](../development/extensions-core/kubernetes.md)

- Consider this an EXPERIMENTAL feature mostly because it has not been tested yet on a wide variety of long running Druid clusters.

## Segment locking

[Configuration reference](../configuration/index.md)

- Overlord operations property `druid.indexer.tasklock.forceTimeChunkLock`
   <br>Setting this to false is still experimental. If set, all tasks are enforced to use time chunk lock. If not set, each task automatically chooses a lock type to use. This configuration can be overwritten by setting `forceTimeChunkLock` in the task context.

[Task reference](../ingestion/tasks.md)

- Locking: The segment locking is still experimental. It could have unknown bugs which potentially lead to incorrect query results.
- Context parameter `forceTimeChunkLock`: Setting this to false is still experimental.

[Design](../design/architecture.md)

- Druid also supports an experimental segment locking mode that is activated by setting `forceTimeChunkLock` to false in the context of an ingestion task.

## Materialized view

[Materialized view](../development/extensions-contrib/materialized-view.md)

- Note that Materialized View is currently designated as experimental. Please make sure the time of all processes are the same and increase monotonically. Otherwise, some unexpected errors may happen on query results.

## Moments sketch

[Aggregations](../querying/aggregations.md)

- Moments Sketch (Experimental): The Moments Sketch extension-provided aggregator is an experimental aggregator that provides quantile estimates using the Moments Sketch. 
- The Moments Sketch aggregator is provided as an experimental option. It is optimized for merging speed and it can have higher aggregation performance compared to the DataSketches quantiles aggregator. However, the accuracy of the Moments Sketch is distribution-dependent, so users will need to empirically verify that the aggregator is suitable for their input data.
- As a general guideline for experimentation, the Moments Sketch paper points out that this algorithm works better on inputs with high entropy. In particular, the algorithm is not a good fit when the input data consists of a small number of clustered discrete values.

## Other configuration properties

[Configuration reference](../configuration/index.md)

- Task runner `httpRemote`
   <br>Overlord operations property `druid.indexer.runner.type`: Choices "local" or "remote". Indicates whether tasks should be run locally or in a distributed environment. Experimental task runner "httpRemote" is also available which is same as "remote" but uses HTTP to interact with Middle Managers instead of Zookeeper.<br><br>
- `CLOSED_SEGMENTS_SINKS` mode
   <br>Peon config `druid.indexer.task.batchProcessingMode`: `CLOSED_SEGMENTS_SINKS` mode isn't as well tested as other modes so is currently considered experimental.<br><br>
- Expression processing configuration `druid.expressions.allowNestedArrays`
   <br>If enabled, Druid array expressions can create nested arrays. This is experimental and should be used with caution.
