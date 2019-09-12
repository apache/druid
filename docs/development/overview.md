---
id: overview
title: "Developing on Apache Druid"
sidebar_label: "Developing on Druid"
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


Druid's codebase consists of several major components. For developers interested in learning the code, this document provides
a high level overview of the main components that make up Druid and the relevant classes to start from to learn the code.

## Storage format

Data in Druid is stored in a custom column format known as a [segment](../design/segments.md). Segments are composed of
different types of columns. `Column.java` and the classes that extend it is a great place to looking into the storage format.

## Segment creation

Raw data is ingested in `IncrementalIndex.java`, and segments are created in `IndexMerger.java`.

## Storage engine

Druid segments are memory mapped in `IndexIO.java` to be exposed for querying.

## Query engine

Most of the logic related to Druid queries can be found in the Query* classes. Druid leverages query runners to run queries.
Query runners often embed other query runners and each query runner adds on a layer of logic. A good starting point to trace
the query logic is to start from `QueryResource.java`.

## Coordination

Most of the coordination logic for Historical processes is on the Druid Coordinator. The starting point here is `DruidCoordinator.java`.
Most of the coordination logic for (real-time) ingestion is in the Druid indexing service. The starting point here is `OverlordResource.java`.

## Real-time Ingestion

Druid loads data through `FirehoseFactory.java` classes. Firehoses often wrap other firehoses, where, similar to the design of the
query runners, each firehose adds a layer of logic, and the persist and hand-off logic is in `RealtimePlumber.java`.

## Hadoop-based Batch Ingestion

The two main Hadoop indexing classes are `HadoopDruidDetermineConfigurationJob.java` for the job to determine how many Druid
segments to create, and `HadoopDruidIndexerJob.java`, which creates Druid segments.

At some point in the future, we may move the Hadoop ingestion code out of core Druid.

## Internal UIs

Druid currently has two internal UIs. One is for the Coordinator and one is for the Overlord.

At some point in the future, we will likely move the internal UI code out of core Druid.

## Client libraries

We welcome contributions for new client libraries to interact with Druid. See the
[Community and third-party libraries](https://druid.apache.org/libraries.html) page for links to existing client
libraries.
