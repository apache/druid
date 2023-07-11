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

# `druid-multi-stage-query` developer notes

This document provides developer notes for the major packages of the `druid-multi-stage-query` extension. It does not
discuss future plans; these are discussed on the list or in GitHub issues.

## Model

Multi-stage queries are modeled as a directed acyclic graph (DAG) of stages. Each stage has some inputs (or, possibly,
zero inputs, if the stage generates data out of thin air). Those inputs can be Druid tables, external data, or
the outputs of other stages of the same query. There is one final stage that produces the query result. Stage outputs,
whether they are inputs to other stages or results of the query itself, are optionally shuffled.

SQL-based ingestion jobs run as multi-stage query tasks. In this case, the result of the query is inserted into the
target table.

Package `org.apache.druid.msq.kernel` and `org.apache.druid.msq.input` contain the model classes for multi-stage
queries.

Main classes:

- [QueryDefinition](src/main/java/org/apache/druid/msq/kernel/QueryDefinition.java) represents a multi-stage query.
- [StageDefinition](src/main/java/org/apache/druid/msq/kernel/StageDefinition.java) represents an individual stage of
  a multi-stage query.
- [InputSpec](src/main/java/org/apache/druid/msq/input/InputSpec.java) represents an input to a stage. Links between
  stages are represented by [StageInputSpec](src/main/java/org/apache/druid/msq/input/stage/StageInputSpec.java).
- [ShuffleSpec](src/main/java/org/apache/druid/msq/input/ShuffleSpec.java) represents the shuffle that happens as part
  of stage output.

## Indexing service

Package `org.apache.druid.msq.indexing` contains code related to integrating with the indexing service. This allows
multi-stage queries to run as indexing service tasks.

Main classes:

- [MSQControllerTask](src/main/java/org/apache/druid/msq/indexing/MSQControllerTask.java) is a `query_controller` task.
  Each query has one controller task. The controller task launches worker tasks to do the actual work.
- [MSQWorkerTask](src/main/java/org/apache/druid/msq/indexing/MSQWorkerTask.java) is a `query_worker` task. These stick
  around for the lifetime of the query. Each task may do work for multiple stages. It has a specific worker number
  that is retained across all stages that the task may work on.

## Planning

Multi-stage queries, when run as SQL via query tasks, are planned in three phases:

1. The SQL planner generates a native query corresponding to the user's SQL query.
2. The `query_controller` task generates a multi-stage QueryDefinition corresponding to the native query, using
   QueryKit.
3. The `query_controller` task determines how many workers will run and generates WorkOrders for each worker.

Once all three of these phases are complete, `query_worker` tasks are launched, sent their WorkOrders, and query
execution begins.

Packages `org.apache.druid.msq.querykit`, `org.apache.druid.msq.input`, and `org.apache.druid.msq.kernel` contain code
related to query planning.

Main classes:

- [QueryKit](src/main/java/org/apache/druid/msq/querykit/QueryKit.java) implementations produce QueryDefinition
  instances from native Druid queries.
- [InputSlice](src/main/java/org/apache/druid/msq/input/InputSlice.java) represents a slice of stage input assigned to
  a particular worker.
- [WorkerAssignmentStrategy](src/main/java/org/apache/druid/msq/kernel/WorkerAssignmentStrategy.java) drives the splitting
  of input specs into input slices, and is therefore responsible for assigning work to workers.
- [WorkOrder](src/main/java/org/apache/druid/msq/kernel/WorkOrder.java) represents the work assigned to a particular
  worker in a particular stage.

## Execution

Package `org.apache.druid.msq.exec` and `org.apache.druid.msq.kernel` contain code related to driving query execution.

Main classes:

- [ControllerQueryKernel](src/main/java/org/apache/druid/msq/kernel/controller/ControllerQueryKernel.java) is the state
  machine that drives execution on the controller.
- [WorkerStageKernel](src/main/java/org/apache/druid/msq/kernel/worker/WorkerStageKernel.java) is the state machine
  that drives execution on workers.
- [ControllerImpl](src/main/java/org/apache/druid/msq/kernel/exec/ControllerImpl.java) embeds a ControllerQueryKernel
  and handles controller-side execution beyond the state machine, including query planning, RPC, counters, and so on.
- [WorkerImpl](src/main/java/org/apache/druid/msq/kernel/exec/WorkerImpl.java) embeds a WorkerStageKernel and handles
  worker-side execution beyond the state machine, including setup of processors, channels, counters, and so on.

## Statistics

Package `org.apache.druid.msq.statistics` contains code related to determining partition boundaries as part of
doing a range-based shuffle. During a stage that intends to do range-based shuffle, workers gather statistics
using a ClusterByStatisticsCollector, which are then merged on the controller and used to generate partition
boundaries.

- [ClusterByStatisticsCollector](src/main/java/org/apache/druid/msq/statistics/ClusterByStatisticsCollector.java)
  is the interface to statistics collection.
- [ClusterByStatisticsCollectorImpl](src/main/java/org/apache/druid/msq/statistics/ClusterByStatisticsCollectorImpl.java)
  is the main implementation of the statistics collection interface.

## Counters

Package `org.apache.druid.msq.counters` contains code related to tracking and reporting query execution metrics.

Main classes:

- [CounterTracker](src/main/java/org/apache/druid/msq/counters/CounterTracker.java) is used by workers to keep track of
  named counters of various types.
- [CounterSnapshots](src/main/java/org/apache/druid/msq/counters/CounterSnapshots.java) are periodically reported from
  workers to the controller.
- [CounterSnapshotsTree](src/main/java/org/apache/druid/msq/counters/CounterSnapshotsTree.java) is used by the
  controller to store worker snapshots. It is also included in task reports, which enables live metrics, and also
  allows query counters to be reviewed after the query has been completed.

## SQL

Package `org.apache.druid.msq.sql` contains code related to integration with Druid SQL APIs.

Main classes:

- [SqlTaskResource](src/main/java/org/apache/druid/msq/counters/CounterTracker.java) offers the endpoint
  `/druid/v2/sql/task`, where SQL queries are executed as multi-stage query tasks.
- [MSQTaskSqlEngine](src/main/java/org/apache/druid/msq/sql/MSQTaskSqlEngine.java) is a SqlEngine implementation that
  executes SQL queries as multi-stage query tasks. It is injected into the SqlTaskResource.

## References

- Multi-stage distributed query proposal: https://github.com/apache/druid/issues/12262
