---
id: supervisor
title: Supervisor
sidebar_label: Supervisor
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

A supervisor manages streaming ingestion from external streaming sources into Apache Druid.
Supervisors oversee the state of indexing tasks to coordinate handoffs, manage failures, and ensure that the scalability and replication requirements are maintained.

## Supervisor spec

You use a JSON specification, often referred to as the supervisor spec, to define streaming ingestion tasks.
The supervisor spec specifies how Druid should consume, process, and index streaming data.

Druid starts a new supervisor for a datasource when you create a supervisor spec.
You can create and manage supervisor specs using the data loader in the Druid web console or by calling the [Supervisor API](../api-reference/supervisor-api.md).
Once started, the supervisor persists in the configured metadata database. There can only be one supervisor per datasource, and submitting a second supervisor spec for the same datasource overwrites the previous one.

When an Overlord gains leadership, either by being started or as a result of another Overlord failing, it spawns a supervisor for each supervisor spec in the metadata database. The supervisor then discovers running indexing tasks and attempts to adopt them if they are compatible with the supervisor's configuration. If they are not compatible, the tasks are terminated and the supervisor creates a new set of tasks. This way, the supervised tasks persist across Overlord restarts and failovers.

### Schema and configuration changes

Schema and configuration changes are handled by submitting the new supervisor spec. The Overlord initiates a graceful shutdown of the existing supervisor. The running supervisor signals its tasks to stop reading and begin publishing, exiting itself. Druid then uses the provided configuration to create a new supervisor. Druid submits a new schema while retaining existing publishing tasks and starts new tasks at the previous task offsets.
This way, configuration changes can be applied without requiring any pause in ingestion.

## Status report

The supervisor status report contains the state of the supervisor tasks and an array of recently thrown exceptions reported as `recentErrors`. 
To retrieve the current status report for a single supervisor, send a `GET` request to the `/druid/indexer/v1/supervisor/:supervisorId/status` endpoint.
You can control the maximum size of the exceptions using the `druid.supervisor.maxStoredExceptionEvents` configuration.

The two properties related to the supervisor's state are `state` and `detailedState`. The `state` property contains a small number of generic states that apply to any type of supervisor, while the `detailedState` property contains a more descriptive, implementation-specific state that may provide more insight into the supervisor's activities.

Possible state values are `PENDING`, `RUNNING`, `SUSPENDED`, `STOPPING`, `UNHEALTHY_SUPERVISOR`, and `UNHEALTHY_TASKS`.

The following table lists `detailedState` values and their corresponding `state` mapping:

|Detailed state|Corresponding state|Description|
|--------------|-------------------|-----------|
|`UNHEALTHY_SUPERVISOR`|`UNHEALTHY_SUPERVISOR`|The supervisor encountered errors on previous `druid.supervisor.unhealthinessThreshold` iterations.|
|`UNHEALTHY_TASKS`|`UNHEALTHY_TASKS`|The last `druid.supervisor.taskUnhealthinessThreshold` tasks all failed.|
|`UNABLE_TO_CONNECT_TO_STREAM`|`UNHEALTHY_SUPERVISOR`|The supervisor is encountering connectivity issues with the stream and has not successfully connected in the past.|
|`LOST_CONTACT_WITH_STREAM`|`UNHEALTHY_SUPERVISOR`|The supervisor is encountering connectivity issues with the stream but has successfully connected in the past.|
|`PENDING` (first iteration only)|`PENDING`|The supervisor has been initialized but hasn't started connecting to the stream.|
|`CONNECTING_TO_STREAM` (first iteration only)|`RUNNING`|The supervisor is trying to connect to the stream and update partition data.|
|`DISCOVERING_INITIAL_TASKS` (first iteration only)|`RUNNING`|The supervisor is discovering already-running tasks.|
|`CREATING_TASKS` (first iteration only)|`RUNNING`|The supervisor is creating tasks and discovering state.|
|`RUNNING`|`RUNNING`|The supervisor has started tasks and is waiting for `taskDuration` to elapse.|
|`IDLE`|`IDLE`|The supervisor is not creating tasks since the input stream has not received any new data and all the existing data is read.|
|`SUSPENDED`|`SUSPENDED`|The supervisor is suspended.|
|`STOPPING`|`STOPPING`|The supervisor is stopping.|

On each iteration of the supervisor's run loop, the supervisor completes the following tasks in sequence:

1. Fetch the list of units of parallelism, such as Kinesis shards or Kafka partitions, and determine the starting sequence number or offset for each unit (either based on the last processed sequence number or offset if continuing, or starting from the beginning or ending of the stream if this is a new stream).
2. Discover any running indexing tasks that are writing to the supervisor's datasource and adopt them if they match the supervisor's configuration, else signal them to stop.
3. Send a status request to each supervised task to update the view of the state of the tasks under supervision.
4. Handle tasks that have exceeded `taskDuration` and should transition from the reading to publishing state.
5. Handle tasks that have finished publishing and signal redundant replica tasks to stop.
6. Handle tasks that have failed and clean up the supervisor's internal state.
7. Compare the list of healthy tasks to the requested `taskCount` and `replicas` configurations and create additional tasks if required.

The `detailedState` property shows additional values (marked with "first iteration only" in the preceding table) the first time the
supervisor executes this run loop after startup or after resuming from a suspension. This is intended to surface
initialization-type issues, where the supervisor is unable to reach a stable state. For example, if the supervisor cannot connect to
the stream, if it's unable to read from the stream, or cannot communicate with existing tasks. Once the supervisor is stable;
that is, once it has completed a full execution without encountering any issues, `detailedState` will show a `RUNNING`
state until it is stopped, suspended, or hits a failure threshold and transitions to an unhealthy state.

:::info
For Kafka indexing service, the consumer lag per partition may be reported as negative values if the supervisor hasn't received the latest offset response from Kafka. The aggregate lag value will always be >= 0.
:::

## Capacity planning

Indexing tasks run on MiddleManagers and are limited by the resources available in the MiddleManager cluster. In particular, you should make sure that you have sufficient worker capacity, configured using the
`druid.worker.capacity` property, to handle the configuration in the supervisor spec. Note that worker capacity is
shared across all types of indexing tasks, so you should plan your worker capacity to handle your total indexing load, such as batch processing, streaming tasks, and merging tasks. If your workers run out of capacity, indexing tasks queue and wait for the next available worker. This may cause queries to return partial results but will not result in data loss, assuming the tasks run before the stream purges those sequence numbers.

A running task can be in one of two states: reading or publishing. A task remains in reading state for the period defined in `taskDuration`, at which point it transitions to publishing state. A task remains in publishing state for as long as it takes to generate segments, push segments to deep storage, and have them loaded and served by a Historical service or until `completionTimeout` elapses.

The number of reading tasks is controlled by `replicas` and `taskCount`. In general, there are `replicas * taskCount` reading tasks. An exception occurs if `taskCount` is over the number of shards in Kinesis or partitions in Kafka, in which case Druid uses the number of shards or partitions. When `taskDuration` elapses, these tasks transition to publishing state and `replicas * taskCount` new reading tasks are created. To allow for reading tasks and publishing tasks to run concurrently, there should be a minimum capacity of:

```text
workerCapacity = 2 * replicas * taskCount
```

This value is for the ideal situation in which there is at most one set of tasks publishing while another set is reading.
In some circumstances, it is possible to have multiple sets of tasks publishing simultaneously. This would happen if the
time-to-publish (generate segment, push to deep storage, load on Historical) is greater than `taskDuration`. This is a valid and correct scenario but requires additional worker capacity to support. In general, it is a good idea to have `taskDuration` be large enough that the previous set of tasks finishes publishing before the current set begins.

## Learn more

See the following topics for more information:

* [Supervisor API](../api-reference/supervisor-api.md) for how to manage and monitor supervisors using the API.
* [Apache Kafka ingestion](../ingestion/kafka-ingestion.md) to learn about ingesting data from an Apache Kafka stream.
* [Amazon Kinesis ingestion](../ingestion/kinesis-ingestion.md) to learn about ingesting data from an Amazon Kinesis stream.