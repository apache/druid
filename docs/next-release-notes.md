---
title: "WIP release notes for 25.0"
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

## Highlights

### Multi-stage query 

The multi-stage query (MSQ) task engine used for SQL-based ingestion is now production ready. Use it for any supported workloads. For more information, see the following pages:

- [Ingestion](https://druid.apache.org/docs/latest/ingestion/index.html)
- [SQL-based ingestion](https://druid.apache.org/docs/latest/multi-stage-query/index.html)

### String dictionary compression (experimental)

> Any segment written using string dictionary compression is not readable by older versions of Druid.

Added support for front coded string dictionaries for smaller string columns, leading to reduced segment sizes with only minor performance penalties for most Druid queries.

This functionality can be utilized by a new property to `IndexSpec.stringDictionaryEncoding`, which can be set to {"type":"frontCoded", "bucketSize": 4}, {"type":"frontCoded", "bucketSize": 16}, or any power of 2 that is 128 or lower. This property instructs indexing tasks to write segments with the compressed dictionaries with the specific bucket size specified.  (`{"type":"utf8"}` is the default).

For more information, see [Front coding](https://druid.apache.org/docs/latest/ingestion/ingestion-spec.html#front-coding).

https://github.com/apache/druid/pull/12277

### Kubernetes native tasks

Druid can now use Kubernetes to launch and manage tasks, eliminating the need for MiddleManagers.

To use this feature, enable the [`druid-kubernetes-overlord-extensions`]((../extensions.md#loading-extensions) in the extensions load list for your Overlord process.

https://github.com/apache/druid/pull/13156

## Behavior changes

### Memory estimates

The task context flag `useMaxMemoryEstimates` is now set to false by default to improve memory usage estimation.

https://github.com/apache/druid/pull/13178

### HLL and quantiles sketches

The aggregation functions for HLL and quantiles sketches returned sketches or numbers when they are finalized depending on where they were in the native query plan. 

Druid no longer finalizes aggregators in the following two cases:

  - aggregators appear in the outer level of a query
  - aggregators are used as input to an expression or finalizing-field-access post-aggregator

This change aligns the behavior of HLL and quantiles sketches with theta sketches.

To provide backwards compatibility, you can use the `sqlFinalizeOuterSketches` query context parameter that restores the old behavior. 
    
https://github.com/apache/druid/pull/13247

### Kill tasks do not include markAsUnuseddone

When you kill a task, Druid no longer automatically marks segments as unused. You must explicitly mark them as unused with `POST /druid/coordinator/v1/datasources/{dataSourceName}/markUnused`. 
For more information, see the [API reference](https://druid.apache.org/docs/latest/operations/api-reference.html#coordinator)

https://github.com/apache/druid/pull/13104

### Segment discovery

The default segment discovery method now uses HTTP instead of ZooKeeper.

This update changes the defaults for the following properties:

| Property | New default | Previous default | 
| - | - | - |
| `druid.serverview.type` for segment management | http | batch |
| `druid.coordinator.loadqueuepeon.type` for segment management | http | curator |
| `druid.indexer.runner.type` for the Overlord | httpRemote | local |

To use ZooKeeper instead of HTTP, change the values for the properties back to the previous defaults.

https://github.com/apache/druid/pull/13092

## Multi-stage query task engine

### CLUSTERED BY limit

When using the MSQ task engine to ingest data, there is now a 1,500 column limit to the number of columns that can be passed in the CLUSTERED BY clause.

https://github.com/apache/druid/pull/13352

### Metrics used to downsample bucket

Changed the way the MSQ task engine determines whether or not to downsample data, to improve accuracy. The task engine now uses the number of bytes instead of number of keys.

https://github.com/apache/druid/pull/12998

### MSQ heap footprint

When determining partition boundaries, the heap footprint of the sketches that MSQ uses is capped at 10% of available memory or 300 MB, whichever is lower. Previously, the cap was strictly 300 MB.

https://github.com/apache/druid/pull/13274

### MSQ Docker improvement

Enabled MSQ task query engine for Docker by default.

https://github.com/apache/druid/pull/13069


### Improved MSQ warnings

For disallowed MSQ warnings of certain types, the warning is now surfaced as the error.

https://github.com/apache/druid/pull/13198

### Added support for indexSpec

The MSQ task engine now supports the `indexSpec` context parameter. This context parameter can also be configured through the web console.

https://github.com/apache/druid/pull/13275

### Added task start status to the worker report

Added `pendingTasks` and `runningTasks` fields to the worker report for the MSQ task engine.
See [Query task status information](#query-task-status-information) for related web console changes.

https://github.com/apache/druid/pull/13263

### Improved handling of secrets

When MSQ submits tasks containing SQL with sensitive keys, the keys can get logged in the file.
Druid now masks the sensitive keys in the log files using regular expressions.

https://github.com/apache/druid/pull/13231

### Query history

Multi-stage queries no longer show up in the Query history dialog. They are still available in the **Recent query tasks** panel.

## Query engine

### Use worker number to communicate between tasks

Changed the way WorkerClient communicates between the worker tasks, to abstract away the complexity of resolving the `workerNumber` to the `taskId` from the callers.
Once the WorkerClient writes it's outputs to the durable storage, it adds a file with `__success` in the `workerNumber` output directory for that stage and with its `taskId`. This allows you to determine the worker, which has successfully written its outputs to the durable storage, and differentiate from the partial outputs by orphan or failed worker tasks.

https://github.com/apache/druid/pull/13062

### Sketch merging mode

When a query requires key statistics to generate partition boundaries, key statistics are gathered by the workers while reading rows from the datasource.You can now configure whether the MSQ task engine does this task in parallel or sequentially. Configure the behavior using `clusterStatisticsMergeMode` context parameter. For more information, see [Sketch merging mode](https://druid.apache.org/docs/latest/multi-stage-query/reference.html#sketch-merging-mode).

https://github.com/apache/druid/pull/13205 

## Querying

### HTTP response headers

Exposed HTTP response headers for SQL queries. 

https://github.com/apache/druid/pull/13052

### Enabled async reads for JDBC

Prevented JDBC timeouts on long queries by returning empty batches when a batch fetch takes too long. Uses an async model to run the result fetch concurrently with JDBC requests.

https://github.com/apache/druid/pull/13196

### Enabled composite approach for checking in-filter values set in column dictionary

To accommodate large value sets arising from large in-filters or from joins pushed down as in-filters, Druid now uses sorted merge algorithm for merging the set and dictionary for larger values.

https://github.com/apache/druid/pull/13133

### Added new configuration keys to query context security model

Added the following configuration properties that refine the query context security model controlled by `druid.auth.authorizeQueryContextParams`:

* `druid.auth.unsecuredContextKeys`: A JSON list of query context keys that do not require a security check.
* `druid.auth.securedContextKeys`: A JSON list of query context keys that do require a security check.

If both are set, `unsecuredContextKeys` acts as exceptions to `securedContextKeys`..

https://github.com/apache/druid/pull/13071

## Metrics

### Improved metric reporting

Improved global-cached-lookups metric reporting.

https://github.com/apache/druid/pull/13219


### New metric for segment handoff

`segment/handoff/time` captures the total time taken for handoff for a given set of published segments.

https://github.com/apache/druid/pull/13238 

### New metrics for segment allocation 

Segment allocation can now be performed in batches, which can improve performance and decrease ingestion lag. For more information about this change, see [Segment batch allocation](#segment-batch-allocation). The following metrics correspond to those changes.

Metrics for batch segment allocation (dims: dataSource, taskActionType=segmentAllocate):

- `task/action/batch/runTime`: Milliseconds taken to execute a batch of task actions. Currently only being emitted for [batched `segmentAllocate` actions
- task/action/batch/queueTime: Milliseconds spent by a batch of task actions in queue. Currently only being emitted for [batched `segmentAllocate` actions](#segment-batch-allocation).
- `task/action/batch/size`: Number of task actions in a batch that was executed during the emission period. Currently only being emitted for [batched `segmentAllocate` actions](#segment-batch-allocation).
- `task/action/batch/attempts`: Number of execution attempts for a single batch of task actions. Currently only being emitted for [batched `segmentAllocate` actions](#segment-batch-allocation). 

Metrics for a request (dims: taskId, taskType, dataSource, taskActionType=segmentAllocate):

- `task/action/failed/count`: Number of task actions that failed during the emission period. Currently only being emitted for [batched `segmentAllocate` actions](#segment-batch-allocation).
- `task/action/success/count`: Number of task actions that were executed successfully during the emission period. Currently only being emitted for [batched `segmentAllocate` actions](#segment-batch-allocation).

https://github.com/apache/druid/pull/13369
https://github.com/apache/druid/pull/13503

### New metrics for streaming ingestion

The following metrics related to streaming ingestion have been added:

- `ingest/kafka/partitionLag`: Partition-wise lag between the offsets consumed by the Kafka indexing tasks and latest offsets in Kafka brokers. 
- `ingest/kinesis/partitionLag/time`: Partition-wise lag time in milliseconds between the current message sequence number consumed by the Kinesis indexing tasks and latest sequence number in Kinesis.
- `ingest/pause/time`: Milliseconds spent by a task in a paused state without ingesting.|dataSource, taskId| < 10 seconds.|

https://github.com/apache/druid/pull/13331
https://github.com/apache/druid/pull/13313

### taskActionType dimension for task/action/run/time metric

The `task/action/run/time` metric for the Indexing service now includes the `taskActionType` dimension.

https://github.com/apache/druid/pull/13333


## Nested columns

### Nested columns performance improvement

Improved `NestedDataColumnSerializer` to no longer explicitly write null values to the field writers for the missing values of every row. Instead, passing the row counter is moved to the field writers so that they can backfill null values in bulk.

https://github.com/apache/druid/pull/13101

### Support for more formats

Druid nested columns and the associated JSON transform functions now support Avro, ORC, and Parquet.

https://github.com/apache/druid/pull/13325 

https://github.com/apache/druid/pull/13375 

### Refactored a datasource before unnest 

When data requires "flattening" during processing, the operator now takes in an array and then flattens the array into N (N=number of elements in the array) rows where each row has one of the values from the array.

https://github.com/apache/druid/pull/13085

## Ingestion

### Improved filtering for cloud objects

You can now stop at arbitrary subfolders using glob syntax in the `ioConfig.inputSource.filter` field for native batch ingestion from cloud storage, such as S3. 

https://github.com/apache/druid/pull/13027


### Async task client for streaming ingestion

You can now use asynchronous communication with indexing tasks by setting `chatAsync` to true in the `tuningConfig`. Enabling asynchronous communication means that the `chatThreads` property is ignored.

https://github.com/apache/druid/pull/13354 

### Improved control for how Druid reads JSON data for streaming ingestion

You can now better control how Druid reads JSON data for streaming ingestion by setting the following fields in the input format specification:

* `assumedNewlineDelimited` to parse lines of JSON independently.
* `useJsonNodeReader` to retain valid JSON events when parsing multi-line JSON events when a parsing exception occurs.

The web console has been updated to include these options.

https://github.com/apache/druid/pull/13089

### When a Kafka stream becomes inactive, prevent Supervisor from creating new indexing tasks

Added Idle feature to `SeekableStreamSupervisor` for inactive stream.

https://github.com/apache/druid/pull/13144

### Kafka Consumer improvement

You can now configure the Kafka Consumer's custom deserializer after its instantiation.

https://github.com/apache/druid/pull/13097

### Kafka supervisor logging

Kafka supervisor logs are now less noisy. The supervisors now log events at the DEBUG level instead of INFO. 

https://github.com/apache/druid/pull/13392

### Fixed Overlord leader election

Fixed a problem where Overlord leader election failed due to lock reacquisition issues. Druid now fails these tasks and clears all locks so that the Overlord leader election isn't blocked.

https://github.com/apache/druid/pull/13172

### Support for inline protobuf descriptor

Added a new `inline` type `protoBytesDecoder` that allows a user to pass inline the contents of a Protobuf descriptor file, encoded as a Base64 string.

https://github.com/apache/druid/pull/13192

### Duplicate notices

For streaming ingestion, notices that are the same as one already in queue won't be enqueued. This will help reduce notice queue size. 

https://github.com/apache/druid/pull/13334


### Sampling from stream input now respects the configured timeout

Fixed a problem where sampling from a stream input, such as Kafka or Kinesis, failed to respect the configured timeout when the stream had no records available. You can now set the maximum amount of time in which the entry iterator will return results.

https://github.com/apache/druid/pull/13296

### Streaming tasks resume on Overlord switch

Fixed a problem where streaming ingestion tasks continued to run until their duration elapsed after the Overlord leader had issued a pause to the tasks. Now, when the Overlord switch occurs right after it has issued a pause to the task, the task remains in a paused state even after the Overlord re-election.

https://github.com/apache/druid/pull/13223

### Fixed Parquet list conversion

Fixes an issue with Parquet list conversion, where lists of complex objects could unexpectedly be wrapped in an extra object, appearing as `[{"element":<actual_list_element>},{"element":<another_one>}...]` instead of the direct list. This changes the behavior of the parquet reader for lists of structured objects to be consistent with other parquet logical list conversions. The data is now fetched directly, more closely matching its expected structure.

https://github.com/apache/druid/pull/13294

### Introduced a tree type to flattenSpec

Introduced a `tree` type to `flattenSpec`. In the event that a simple hierarchical lookup is required, the `tree` type allows for faster JSON parsing than `jq` and `path` parsing types.

https://github.com/apache/druid/pull/12177

## Operations

### Deploying Druid

There is now a Python installation script available for Druid that simplifies deployments that don't fit the parameters of the singel-server profiles, such as the nano or micro-quickstart profiles. This `start-druid` script lets you override Druid settings to fit your needs. For more information, see [Single server deployment](https://druid.apache.org/docs/latest/operations/single-server.html).

### Compaction

Compaction behavior has changed to improve the amount of time it takes and disk space it takes:

- When segments need to be fetched, download them one at a time and delete them when Druid is done with them. This still takes time but minimizes the required disk space.
- Don't fetch segments on the main compact task when they aren't needed. If the user provides a full `granularitySpec`, `dimensionsSpec`, and `metricsSpec`, Druid skips fetching segments.

For more information, see the documentation on [Compaction](https://druid.apache.org/docs/latest/data-management/compaction.html) and [Automatic compaction](https://druid.apache.org/docs/latest/data-management/automatic-compaction.html).

https://github.com/apache/druid/pull/13280

### Idle configs for the Supervisor

You can now set the Supervisor to idle, which is useful in cases where freeing up slots so that autoscaling can be more effective.

To configure the idle behavior, use the following properties:

| Property | Description | Default |
| - | - | -|
|`druid.supervisor.idleConfig.enabled`| (Cluster wide) If `true`, supervisor can become idle if there is no data on input stream/topic for some time.|false|
|`druid.supervisor.idleConfig.inactiveAfterMillis`| (Cluster wide) Supervisor is marked as idle if all existing data has been read from input topic and no new data has been published for `inactiveAfterMillis` milliseconds.|`600_000`|
| `inactiveAfterMillis` | (Individual Supervisor) Supervisor is marked as idle if all existing data has been read from input topic and no new data has been published for `inactiveAfterMillis` milliseconds. | no (default == `600_000`) |

https://github.com/apache/druid/pull/13311

### Improved supervisor termination

Fixed issues with delayed supervisor termination during certain transient states.

https://github.com/apache/druid/pull/13072


### Backoff for HttpPostEmitter

The `HttpPostEmitter` option now has a backoff. This means that there should be less noise in the logs and lower CPU usage if you use this option for logging. 

https://github.com/apache/druid/pull/12102

### DumpSegment tool for nested columns

The DumpSegment tool can now be used on nested columns with the `--dump nested` option. 

For more information, see [dump-segment tool](https://druid.apache.org/docs/latest/operations/dump-segment).

https://github.com/apache/druid/pull/13356

### Segment loading and balancing

#### Segment batch allocation

Segment allocation on the Overlord can take some time to finish, which can cause ingestion lag while a task waits for segments to be allocated.  Performing segment allocation in batches can help improve performance.

There are two new properties that affect how Druid performs segment allocation:

| Property | Description | Default | 
| - | - | - |
|`druid.indexer.tasklock.batchSegmentAllocation`| If set to true, Druid performs segment allocate actions in batches to improve throughput and reduce the average `task/action/run/time`. See [batching `segmentAllocate` actions](https://druid.apache.org/docs/latest/ingestion/tasks.html#batching-segmentallocate-actions) for details.|false|
|`druid.indexer.tasklock.batchAllocationWaitTime`|Number of milliseconds after Druid adds the first segment allocate action to a batch, until it executes the batch. Allows the batch to add more requests and improve the average segment allocation run time. This configuration takes effect only if `batchSegmentAllocation` is enabled.|500|

In addition to these properties, there are new metrics to track batch segment allocation. For more information, see [New metrics for segment  allocation](#new-metrics-for-segment-allocation).

For more information, see the following:
- [Overlord operations](https://druid.apache.org/docs/latest/configuration/index.html#overlord-operations)
- [Task actions and Batching `segmentAllocate` actions](https://druid.apache.org/docs/latest/ingestion/tasks.html#task-actions)

https://github.com/apache/druid/pull/13369
https://github.com/apache/druid/pull/13503

#### cachingCost balancer strategy

The `cachingCost` balancer strategy now behaves more similarly to cost strategy. When computing the cost of moving a segment to a server, the following calculations are performed:

- Subtract the self cost of a segment if it is being served by the target server
- Subtract the cost of segments that are marked to be dropped

https://github.com/apache/druid/pull/13321

#### Segment assignment

You can now use a round-robin segment strategy to speed up initial segment assignments.

Set `useRoundRobinSegmentAssigment` to `true` in the Coordinator dynamic config to enable this feature.

https://github.com/apache/druid/pull/13367

#### Segment load queue peon

Batch sampling is now the default method for sampling segments during balancing as it performs significantly better than the alternative when there is a large number of used segments in the cluster.

As part of this change, the following have been deprecated and will be removed in future releases:

- coordinator dynamic config `useBatchedSegmentSampler`
- coordinator dynamic config `percentOfSegmentsToConsiderPerMove`
- non-batch method of sampling segments used by coordinator duty `BalanceSegments`

The unused coordinator property `druid.coordinator.loadqueuepeon.repeatDelay` has been removed.

Use only `druid.coordinator.loadqueuepeon.http.repeatDelay` to configure repeat delay for the HTTP-based segment loading queue.

https://github.com/apache/druid/pull/13391

#### Segment replication

Improved the process of checking server inventory to prevent over-replication of segments during segment balancing.

https://github.com/apache/druid/pull/13114

### Provided service specific log4j overrides in containerized deployments

Provided an option to override log4j configs setup at the service level directories so that it works with Druid-operator based deployments.

https://github.com/apache/druid/pull/13020

### Various Docker improvements

* Updated Docker to run with JRE 11 by default.
* Updated Docker to use [`gcr.io/distroless/java11-debian11`](https://github.com/GoogleContainerTools/distroless) image as base by default.
* Enabled Docker buildkit cache to speed up building.
* Downloaded [`bash-static`](https://github.com/robxu9/bash-static) to the Docker image so that scripts that require bash can be executed.
* Bumped builder image from `3.8.4-jdk-11-slim` to `3.8.6-jdk-11-slim`.
* Switched busybox from `amd64/busybox:1.30.0-glibc` to `busybox:1.35.0-glibc`.
* Added support to build arm64-based image.

https://github.com/apache/druid/pull/13059



### Enabled cleaner JSON for various input sources and formats

Added `JsonInclude` to various properties, to avoid population of default values in serialized JSON.

https://github.com/apache/druid/pull/13064

### Improved direct memory check on startup

Improved direct memory check on startup by providing better support for Java 9+ in `RuntimeInfo`, and clearer log messages where validation fails.

https://github.com/apache/druid/pull/13207


### Improved the run time of the MarkAsUnusedOvershadowedSegments duty

Improved the run time of the `MarkAsUnusedOvershadowedSegments` duty by iterating over all overshadowed segments and marking segments as unused in batches.

https://github.com/apache/druid/pull/13287


## Web console

### Delete an interval

You can now pick an interval to delete from a dropdown in the kill task dialog.  

https://github.com/apache/druid/pull/13431

### Removed the old query view

The old query view is removed. Use the new query view with tabs.
For more information, see [Web console](https://druid.apache.org/docs/latest/operations/web-console.html#query).

https://github.com/apache/druid/pull/13169

### Filter column values in query results

The web console now allows you to add to existing filters for a selected column.

https://github.com/apache/druid/pull/13169

### Ability to add issue comments

You can now add an issue comment in SQL, for example `--:ISSUE: this is an issue` that is rendered in red and prevents the SQL from running. The comments are used by the spec-to-SQL converter to indicate that something could not be converted.

https://github.com/apache/druid/pull/13136

### Support for Kafka lookups

Added support for Kafka-based lookups rendering and input in the web console.

https://github.com/apache/druid/pull/13098

### Improved array detection

Added better detection for arrays containing objects.

https://github.com/apache/druid/pull/13077

### Updated Druid Query Toolkit version

[Druid Query Toolkit](https://www.npmjs.com/package/druid-query-toolkit) version 0.16.1 adds quotes to references in auto generated queries by default.

https://github.com/apache/druid/pull/13243

### Query task status information

The web console now exposes a textual indication about running and pending tasks when a query is stuck due to lack of task slots.

https://github.com/apache/druid/pull/13291

## Extensions

### BIG_SUM SQL

#### BIG_SUM SQL function

Added SQL function `BIG_SUM` that uses the [Compressed Big Decimal](https://github.com/apache/druid/pull/10705) Druid extension.

https://github.com/apache/druid/pull/13102

#### Added Compressed Big Decimal min and max functions

Added min and max functions for Compressed Big Decimal and exposed these functions via SQL: BIG_MIN and BIG_MAX.

https://github.com/apache/druid/pull/13141


### Extension optimization

Optimized the `compareTo` function in `CompressedBigDecimal`.

https://github.com/apache/druid/pull/13086

### CompressedBigDecimal cleanup and extension

Removed unnecessary generic type from CompressedBigDecimal, added support for number input types, added support for reading aggregator input types directly (uningested data), and fixed scaling bug in buffer aggregator.

https://github.com/apache/druid/pull/13048


### Support for Kubernetes discovery

Added `POD_NAME` and `POD_NAMESPACE` env variables to all Kubernetes Deployments and StatefulSets.
Helm deployment is now compatible with `druid-kubernetes-extension`.

https://github.com/apache/druid/pull/13262

## Dependency updates

### Updated Kafka version

Updated the Apache Kafka core dependency to version 3.3.1.

https://github.com/apache/druid/pull/13176

### Docker improvements

Updated dependencies for the Druid image for Docker, including JRE 11. Docker BuildKit cache is enabled to speed up building.

https://github.com/apache/druid/pull/13059