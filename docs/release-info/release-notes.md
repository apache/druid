---
id: release-notes
title: "Release notes"
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

<!--Replace {{DRUIDVERSION}} with the correct Druid version.-->

Apache Druid \{\{DRUIDVERSION}} contains over $NUMBER_FEATURES new features, bug fixes, performance enhancements, documentation improvements, and additional test coverage from $NUMBER_OF_CONTRIBUTORS contributors.

<!--
Replace {{MILESTONE}} with the correct milestone number. For example: https://github.com/apache/druid/issues?q=is%3Aclosed+milestone%3A28.0+sort%3Aupdated-desc+
-->

See the [complete set of changes](https://github.com/apache/druid/issues?q=is%3Aclosed+milestone%3A{{MILESTONE}}+sort%3Aupdated-desc+) for additional details, including bug fixes.

Review the [upgrade notes](#upgrade-notes) and [incompatible changes](#incompatible-changes) before you upgrade to Druid \{\{DRUIDVERSION}}.
If you are upgrading across multiple versions, see the [Upgrade notes](upgrade-notes.md) page, which lists upgrade notes for the most recent Druid versions.

<!-- 
This file is a collaborative work in process. Adding a release note to this file doesn't guarantee its presence in the next release until the release branch is cut and the release notes are finalized.

This file contains the following sections:
- Important features, changes, and deprecations
- Functional area and related changes
- Upgrade notes and incompatible changes

Please add your release note to the appropriate section and include the following:
- Detailed title
- Summary of the changes (a couple of sentences) aimed at Druid users
- Link to the associated PR

If your release note contains images, put the images in the release-info/assets folder.

For tips about how to write a good release note, see [Release notes](https://github.com/apache/druid/blob/master/CONTRIBUTING.md#release-notes).
-->

## Important features, changes, and deprecations

This section contains important information about new and existing features.

## Functional area and related changes

This section contains detailed release notes separated by areas.

#### Druid operator

Druid Operator is a Kubernetes controller that manages the lifecycle of your Druid clusters. The operator simplifies the management of Druid clusters with its custom logic that is configurable through
Kubernetes CRDs.

[#18435](https://github.com/apache/druid/pull/18435)

#### Cost-based autoscaling for streaming ingestion

Druid now supports cost-based autoscaling for streaming ingestion that optimizes task count by balancing lag reduction against resource efficiency.. This autoscaling strategy uses the following formula:

```
totalCost = lagWeight × lagRecoveryTime + idleWeight × idlenessCost
```

which accounts for the time to clear the backlog and compute time:

```
lagRecoveryTime = aggregateLag / (taskCount × avgProcessingRate) — time to clear backlog
idlenessCost = taskCount × taskDuration × predictedIdleRatio — wasted compute time
```

[#18819](https://github.com/apache/druid/pull/18819)

#### Kubernetes client mode (experimental)

THe new experimental Kubernetes client mode uses the `fabric8` `SharedInformers` to cache k8s metadata. This greatly reduces API traffic between the Overlord and k8s control plane. You can try out this feature using the following config:

```
druid.indexer.runner.useK8sSharedInformers=true
```

[#18599](https://github.com/apache/druid/pull/18599)

#### cgroup v2 support

cgroup v2 is now supported, and all cgroup metrics now emit `cgroupversion` to identify which version is being used.

The following metrics automatically switch to v2 if v2 is detected: `CgroupCpuMonitor` , `CgroupCpuSetMonitor`, `CgroupDiskMonitor`,`MemoryMonitor`. `CpuAcctDeltaMonitor` fails gracefully if v2 is detected.

Additionally, `CgroupV2CpuMonitor` now also emits  `cgroup/cpu/shares` and `cgroup/cpu/cores_quota`.

[#18705](https://github.com/apache/druid/pull/18705)

#### Query reports for Dart

Dart now supports query reports for running and recently completed queries. that can be fetched from the `/druid/v2/sql/queries/<sqlQueryId>/reports` endpoint.

The format of the response is a JSON object with two keys, "query" and "report". The "query" key is the same info that is available from the existing `/druid/v2/sql/queries` endpoint. The "report" key is a report map including an MSQ report.

You can control the retention behavior for reports using the following configs:

* `druid.msq.dart.controller.maxRetainedReportCount`: Max number of reports that are retained. The default is 0, meaning no reports are retained
* `druid.msq.dart.controller.maxRetainedReportDuration`: How long reports are retained in ISO 8601 duration format. The default is PT0S, meaning time-based expiration is turned off

[#18886](https://github.com/apache/druid/pull/18886)

#### New segment format

The new v10 segment format improves upon v9. v10 supports partial segment downloads, a feature provided by the experimental virtual storage fabric feature. To streamline partial fetches, the base segment contents are combined into a single file, `druid.segment.`

Set `druid.indexer.task.buildV10=true` to make segments in the new format.

You can use the `bin/dump-segment` tool to view segment metadata. The tool outputs serialized JSON.

[#18880](https://github.com/apache/druid/pull/18880) [#18901](https://github.com/apache/druid/pull/18901) 

### Web console

#### New info available in the web console

The web console now includes information about the number of available processors and the total memory (in binary bytes).

This information is also available through the `sys.servers` table.

[#18613](https://github.com/apache/druid/pull/18613)

#### Other web console improvements

* Added tracking for inactive workers for MSQ execution stages [#18768](https://github.com/apache/druid/pull/18768)
* Added a refresh button for JSON views and stage viewers [#18768](https://github.com/apache/druid/pull/18768)
* You can now define `ARRAY` type parameters in the query view [#18586](https://github.com/apache/druid/pull/18586)
* Changed system table queries to now automatically use the native engine [#18857](https://github.com/apache/druid/pull/18857)
* Improved time charts to support multiple measures [#18701](https://github.com/apache/druid/pull/18701)

### Ingestion

* Added support for AWS `InternalError` code retries [#18720](https://github.com/apache/druid/pull/18720)
* Improved ingestion to be more resilient. Ingestion tasks no longer fail if the task log upload fails with an exception [#18748](https://github.com/apache/druid/pull/18748)
* Improved how Druid handles situations where data doesn't match the expected type [#18878](https://github.com/apache/druid/pull/18878)
* Improved JSON ingestion so that Druid can compute JSON values directly from dictionary or index structures, allowing ingestion to skip persisting raw JSON data entirely. This reduces on-disk storage size [#18589](https://github.com/apache/druid/pull/18589)
* You can now choose between full dictionary-based indexing and nulls-only indexing for long/double fields in a nested column [#18722](https://github.com/apache/druid/pull/18722)

#### SQL-based ingestion

##### Additional ingestion configurations

You can now use the following configs to control how your data gets ingested and stored:

* `maxInputFilesPerWorker`: Controls the maximum number of input files or segments per worker.
* `maxPartitions`: Controls the maximum number of output partitions for any single stage, which affects how many segments are generated during ingestion.

[#18826](https://github.com/apache/druid/pull/18826)

##### Other SQL-based ingestion improvements

* Added `maxRowsInMemory` to replace `rowsInMemory`. `rowsInMemory` now functions as an alternate way to provide that config and is ignored if `maxRowsInMemory` is specified. Previously, only `rowsInMemory` existed [#18832](https://github.com/apache/druid/pull/18832)
* Improved the parallelism for sort-merge joins [#18765](https://github.com/apache/druid/pull/18765)

#### Streaming ingestion

##### Record offset and partition

You can now ingest the record offset (`offsetColumnName`) and partition (`partitionColumnName`) using the `KafkaInputFormat`. Their default names are `kafka.offset` and `kafka.partition` respectively .

[#18757](https://github.com/apache/druid/pull/18757)

##### Other streaming ingestion improvements

* Improved supervisors so that they can't kill tasks while the supervisor is stopping [#18767](https://github.com/apache/druid/pull/18767)
* Improved the lag-based autoscaler for streaming ingestion [#18745](https://github.com/apache/druid/pull/18745)
* Improved the `SeekableStream` supervisor autoscaler to wait for tasks to complete before attempting subsequent scale operations. This helps prevent duplicate supervisor history entries [#18715](https://github.com/apache/druid/pull/18715)

### Querying

#### Other querying improvements

* Improved the user experience for invalid `regex_exp` queries. An error gets returned now [#18762](https://github.com/apache/druid/pull/18762)

### Cluster management

#### Dynamic capacity for Kubernetes-based deployments

Druid can now dynamically tune the task runner capacity.

Include the `capacity` field in a POST API call to `/druid/indexer/v1/k8s/taskrunner/executionconfig`. Setting a value this way overrides `druid.indexer.runner.capacity`.

[#18591](https://github.com/apache/druid/pull/18591)

#### Server properties table

The `server_properties` table exposes the runtime properties configured for each Druid server. Each row represents a single property key-value pair associated with a specific server.

[#18692](https://github.com/apache/druid/pull/18692)

#### Other cluster management improvements

* Added quality of service filtering for the Overlord so that health check threads don't get blocked [#18033](https://github.com/apache/druid/pull/18033)

### Data management

#### Other data management improvements

* Added the `mostFragmentedFirst` compaction policy that prioritizes intervals with the most small uncompacted segments [#18802](https://github.com/apache/druid/pull/18802)
* Improved how segment files get deleted to prevent partial segment files from remaining in the event of a failure during a delete operation [#18696](https://github.com/apache/druid/pull/18696)
* Improved compaction so that it identifies multi-value dimensions for dimension schemas that can  produce them [#18760](https://github.com/apache/druid/pull/18760)

### Metrics and monitoring

#### Task metrics

All task metrics now emit the following dimensions: `taskId`, `dataSource`, `taskType`, `groupId`, and `id`. Note that `id` is emitted for backwards compatibility. It will be removed in favor of the `taskId` dimension in a future release.

[#18876](https://github.com/apache/druid/pull/18876)

#### Ingestion metrics

The following metrics for streaming and batch tasks now emit the actual values instead of 0: `ingest/merge/time`, `ingest/merge/cpu`, and `ingest/persists/cpu`.

[#18866](https://github.com/apache/druid/pull/18866)

##### statsd metrics

The following metrics have been added to the default list for statsd:

* `task/action/run/time`
* `task/status/queue/count`
* `task/status/updated/count`
* `ingest/handoff/time`

[#18846](https://github.com/apache/druid/pull/18846)

#### Other metrics and monitoring improvements

* Added logging for all handlers for a stage before they start or stop, which can help you understand execution order [#18662](https://github.com/apache/druid/pull/18662)
* Added new Jetty thread pool metrics to capture request-serving thread statistics: `jetty/threadPool/utilized`, `jetty/threadPool/ready` and `jetty/threadPool/utilizationRate` [#18883](https://github.com/apache/druid/pull/18883)
* Added `tier` and `priority` dimensions to the `segments/max` metric [#18890](https://github.com/apache/druid/pull/18890)
* Added `GroupByStatsMonitor`, which includes `dataSource` and `taskId` dimensions for metrics emitted on peons [#18711](https://github.com/apache/druid/pull/18711)
* Added `task/waiting/time` metric, which measures the time it takes for a task to be placed onto the task runner for scheduling and running [#18735](https://github.com/apache/druid/pull/18735)
* Added the `supervisorId` to streaming task metrics to help clarify situations where multiple supervisors ingest data into a single datasource [#18803](https://github.com/apache/druid/pull/18803)
* Added `StorageMonitor` to `druid.monitoring.monitors` to measure storage and virtual storage usage by segment cache [#18742](https://github.com/apache/druid/pull/18742)
* Druid now logs the following: 
  * total bytes gathered when the max scatter-gather bytes limit is reached [#18841](https://github.com/apache/druid/pull/18841)
  * `query/bytes` metric for even for failed requests [#18842](https://github.com/apache/druid/pull/18842)
* Changed Prometheus emitter TTL tracking to consider all label value combinations instead of just the metric name. Labels aren't tracked when the TTL isn't set [#18718](https://github.com/apache/druid/pull/18718) [#18689](https://github.com/apache/druid/pull/18689)
* Changed lifecycle `stop()` to be logged at the `info` level to match `start()` [#18640](https://github.com/apache/druid/pull/18640)
* Improved the metrics emitter so that it emits metrics for all task completions [#18766](https://github.com/apache/druid/pull/18766)

### Extensions

#### SpectatorHistogram extension

* Added `SPECTATOR_COUNT` and `SPECTATOR_PERCENTILE` SQL functions [#18885](https://github.com/apache/druid/pull/18885)
* Improved the performance of the SpectatorHistogram extension through vectorization [#18813](https://github.com/apache/druid/pull/18813)

## Upgrade notes and incompatible changes

### Upgrade notes

#### Deprecated metrics

Monitors on peons that previously emitted the `id` dimension from `JettyMonitor`, `OshiSysMonitor`, `JvmMonitor`, `JvmCpuMonitor`, `JvmThreadsMonitor` and `SysMonitor` to represent the task ID are deprecated and will be removed in a future release. Use the `taskId` dimension instead.

[#18709](https://github.com/apache/druid/pull/18709)

#### Removed metrics

The following obsolete metrics have been removed:

* `segment/cost/raw` [#18846](https://github.com/apache/druid/pull/18846)
* `segment/cost/normalized` [#18846](https://github.com/apache/druid/pull/18846)
* `segment/cost/normalization` [#18846](https://github.com/apache/druid/pull/18846)
* `task/action/log/time` [#18649](https://github.com/apache/druid/pull/18649)

### Developer notes

#### Segment file interfaces

New `SegmentFileBuilder` and `SegmentFileMapper` interfaces have been defined to replace direct usages of `FileSmoosher` and `SmooshedFileMapper` to abstract the segment building and reading process.

The main developer visible changes for extension writers with custom column implementations is that the `Serializer` interface has changed the `writeTo` method:

* It now accepts a `SegmentFileBuilder` instead of a `FileSmoosher`
* The `ColumnBuilder` method `getFileMapper` now returns a `SegmentFileMapper` instead of `SmooshedFileMapper`. 

Extensions which do not provide custom column implementations should not be impacted by these changes.

[#18608](https://github.com/apache/druid/pull/18608)

#### Other developer improvements

* Added the ability to override the default Kafka image for testing [#18739](https://github.com/apache/druid/pull/18739)
* Extensions can now provide query kit implementations [#18875](https://github.com/apache/druid/pull/18875)
* Removed version overrides in individual `pom` files. For a full list, see the pull request [#18708](https://github.com/apache/druid/pull/18708)

#### Dependency updates

The following dependencies have had their versions bumped:

* `org.apache.logging.log4j:log4j-core` from `2.22.1` to `2.25.3` [#18874](https://github.com/apache/druid/pull/18874)
* `org.mozilla:rhino` from `1.7.14` to `1.7.14.1` [#18868](https://github.com/apache/druid/pull/18868)
* `net.java.dev.jna` and `net.java.dev.jna` versions from `5.13.0` to `5.18.1` for Oshi monitor [#18848](https://github.com/apache/druid/pull/18848)
* `com.github.oshi:oshi-core from` from `6.4.4` to `6.9.1` [#18839](https://github.com/apache/druid/pull/18839)
* `bcpkix-jdk18on` from `1.78.1` to `1.79` [#18834](https://github.com/apache/druid/pull/18834)
* `org.eclipse.jetty` from `12.0.25`to `12.0.30` [#18773](https://github.com/apache/druid/pull/18773)
* `hamcrest` from `1.3` to `2.2` [#18708](https://github.com/apache/druid/pull/18708)
* `org.apache.commons:commons-lang3` from `3.18.0` to `3.19.0` [#18695](https://github.com/apache/druid/pull/18695)
* `org.apache.maven.plugins:maven-shade-plugin` from `3.5.0` to `3.6.1`
* `com.netflix.spectator` from `1.7.0` to `1.9.0` [#18887](https://github.com/apache/druid/pull/18887)