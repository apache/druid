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

### DDL statements for the catalog (experimental)

Druid now supports CREATE and ALTER statements to manage metadata in the catalog. Use them to declare a table's columns, time partitioning, clustering, aggregate projections, and physical layout, instead of setting these in each ingestion spec.

This feature is off by default. To enable it, set `druid.sql.planner.enableCatalogDdl` to `true`.

For more information, see [Catalog](../development/extensions-core/catalog.md).

[#19830](https://github.com/apache/druid/pull/19830) [#20377](https://github.com/apache/druid/pull/20377) [#20452](https://github.com/apache/druid/pull/20452) [#19770](https://github.com/apache/druid/pull/19770) [#19711](https://github.com/apache/druid/pull/19711)

### Pre-ingestion header filtering for Kafka

### Kafka header-based filtering

Kafka supervisors can now filter records by their headers. This reduces ingestion CPU and memory use when you only need some of a topic's records. 

To use the filter, add `headerBasedFilterConfig` to the supervisor's `ioConfig`. Use an `in` filter where `dimension` is the header name and `values` lists the header values to ingest. Records whose header value isn't in the list are dropped, and offsets still advance.

Filtering is permissive: records are ingested if they don't have the header, if the header value is null, empty, or can't be decoded, or if evaluation fails.

The new `ingest/events/filtered` metric reports how many records header filtering dropped.

[#18525](https://github.com/apache/druid/pull/18525)

### Druid JDBC driver

Druid now provides its own JDBC driver that uses the `/druid/v2/sql/` endpoint. We recommend that you switch from the Avatica driver to this driver. The new driver provides the following benefits: driver-side connection management that doesn't require sticky load balancing, streaming results, support for SET statements, and support for non-native SQL engines.

[#19951](https://github.com/apache/druid/pull/19951)

### Netty 4.2

Druid now uses Netty 4.2 for its HTTP client. Previously, Druid used Netty 3. This is a substantive change in memory characteristics:

- Direct memory: The HTTP client now consumes some direct memory. Operators running close to their `-XX:MaxDirectMemorySize` budget may need to increase it.
- Pooled buffers: Buffers are now pooled, so resident memory may appear higher than under Netty 3, even when idle. This is normal for a pooled allocator.

There are new configs that control the behavior of the HTTP client: `druid.global.http.connectTimeout`, `druid.global.http.allocator`, `druid.broker.http.connectTimeout`, and `druid.broker.http.allocator`. You can also set some Netty configs directly.

[#19567](https://github.com/apache/druid/pull/19567)

### Quality of service filtering for Coordinator

You can now enable QoS filtering for the Coordinator to prevent a burst of slow API requests from exhausting the thread pool and blocking health checks.

Use `druid.coordinator.server.maxConcurrentRequests` to configure this feature (default: max(1, max(serverHttpNumThreads - 4, serverHttpNumThreads * 0.8))). Set this property to `0` to turn it off.

Leader-status endpoints are excluded from the cap so load balancers/monitors stay unaffected.

[#19271](https://github.com/apache/druid/pull/19271)

## Functional area and related changes

This section contains detailed release notes separated by areas.

### Web console

#### Other web console improvements

### Ingestion

- Added support for RE2/J regex for the `regex` input format [#19514](https://github.com/apache/druid/pull/19514)
- Changed the S3 client to use the AWS-recommended standard retry policy. You can customize the retry strategy using `druid.s3.retryMode` and `druid.s3.maxAttempts` [#19892](https://github.com/apache/druid/pull/19892)

#### SQL-based ingestion

##### Other SQL-based ingestion improvements

#### Streaming ingestion

##### Ingesting from selected partitions

Kafka supervisors now accept `ioConfig.partitionIds`, which lets you ingest only selected partitions of a topic. For example, you can use this for independent diagnostic ingestion.

[#20474](https://github.com/apache/druid/pull/20474)

##### Simulate cost-based autoscaler

You can now simulate the cost-based autoscaler for Kafka to help determine the optimal task count for different amounts of lag.

[#19687](https://github.com/apache/druid/pull/19687)

##### Cost-based autoscaler critical lag

You can now set a critical lag threshold for the cost-based autoscaler. When lag reaches 75% of the threshold, the autoscaler skips the lag cost factor. When lag reaches 100%, the autoscaler scales up to the maximum task count.

[#19655](https://github.com/apache/druid/pull/19655)

##### Other streaming ingestion improvements

### Querying

#### Query planning time

You can now limit how long the Broker spends planning a SQL query with the new `druid.sql.planner.maxPlanningTimeMs` runtime property. You can also set it per query using the `maxPlanningTimeMs` query context parameter. When planning exceeds the configured budget, the query fails with an HTTP 504 Query timeout error instead of tying up a Broker thread. The default value is `0`, which disables the timeout.

[#20314](https://github.com/apache/druid/pull/20314)

#### Other querying improvements

- You can now use Dart to query metadata tables such as those in the `sys` schema [#19952](https://github.com/apache/druid/pull/19952)
- `CAST(x AS LONG)` is now accepted as an alias for `CAST(x AS BIGINT)` in Druid SQL [#20402](https://github.com/apache/druid/pull/20402)
- Added `datasource` filter pushdown to `sys.segments` table [#19718](https://github.com/apache/druid/pull/19718)
- Added framing support to the FIRST_VALUE and LAST_VALUE window functions [#19301](https://github.com/apache/druid/pull/19301)
- Improved the performance of timeseries, timeBoundary, and scan queries that run in descending order [#19947](https://github.com/apache/druid/pull/19947)
- Improved the performance of COALESCE [#18400](https://github.com/apache/druid/pull/18400)
- Improved S3 API usage efficiency for MSQ tasks using durable storage and MSQ export [#19893](https://github.com/apache/druid/pull/19893)
- Improved validation for catalog types and parsing for native type strings [#19769](https://github.com/apache/druid/pull/19769)
- Upgraded to Calcite 1.42.0 [#19976](https://github.com/apache/druid/pull/19976) [#19370](https://github.com/apache/druid/pull/19370)

### Cluster management

#### Kubernetes node labels

The `druid-kubernetes-extensions` extension now support a `k8sNodeLabel` dynamic config provider, which resolves configuration values from the labels of the Kubernetes node a Druid process runs on.

[#20404](https://github.com/apache/druid/pull/20404)

#### Peon pod template

You can now retrieve the Peon pod template names currently configured on the Overlord using the new `GET` `/druid/indexer/v1/k8s/taskrunner/podTemplates` endpoint. It returns each configured template's name and full pod spec. The endpoint requires CONFIG read permission and returns 404 when the Overlord is not using the custom template pod adapter.

[#19712](https://github.com/apache/druid/pull/19712)

#### Environment variables

The `environment` dynamic config provider now omits variables that are not set in the environment instead of producing a null value that fails consumers such as the Kafka supervisor with a `NullPointerException`. A skipped variable is logged at info level.

[#20381](https://github.com/apache/druid/pull/20381)

#### Connection pools

You can now configure how the Druid connection pool tracks connection demand using `druid.global.http.poolImplementation`.

The default is `retaining`. With `retaining`, the pool keeps every connection it opens and replaces each stale or broken connection with a new one, so the pool stays at its high-water mark.

[#20542](https://github.com/apache/druid/pull/20542) [#20273](https://github.com/apache/druid/pull/20273)

#### HTTP response headers

Druid can now optionally return `X-Druid-Server`, `X-Druid-Service`, and `X-Druid-Version` on HTTP responses, allowing operators to identify the process and version that served a direct or Router-proxied request. These headers expose cluster topology and version information, so only enable them if all clients are permitted to receive it. To enable them, set `druid.server.http.enableResponseIdentityHeaders` to `true`.

[#20268](https://github.com/apache/druid/pull/20268)

#### Improved LDAP support

Added support for LDAP group-based authorization on servers that do not return the `memberOf` attribute (such as OpenLDAP). Configure `groupBaseDn` and `groupSearch` on the LDAP credentials validator to enable a reverse group lookup that resolves group memberships automatically.

[#19432](https://github.com/apache/druid/pull/19432)

#### Table visibility

Users who query tables they don't have READ permission for now see a "not found" error instead of a "forbidden" error.

To restore the previous behavior, set `druid.sql.planner.authorizeTableVisibility` to `false`.

Additionally, when set to `true`, INSERT and REPLACE require both READ and WRITE access to the target table. If this property is set to `false`, they only require WRITE permission.Regardless of the value of this property, tables only appear in INFORMATION_SCHEMA if the user has READ access to them.

This is a breaking change that affects the permissions you need. For more information, see [permissions changes for SQL-based ingestion](#permissions-changes-for-sql-based-ingestion).

[#20075](https://github.com/apache/druid/pull/20075)

#### JAVA_HOME

Middle Managers now honor JAVA_HOME when using the `bin/java` script, which should be present in the working directory.

Druid uses the following logic

- If you specified a command (such as if `config.getJavaCommand` is not null), use that.
- If `bin/run-java` exists, use that
- Use java

[#19709](https://github.com/apache/druid/pull/19709)

#### Other cluster management improvements

- Added a `KILL` task lock type that can be used by embedded kill tasks so that the `UnusedSegmentsKiller` does not skip intervals with active ingestions [#19921](https://github.com/apache/druid/pull/19921)
- Added support for PostgreSQL to the `export-metadata` tool [#19698](https://github.com/apache/druid/pull/19698)
- Peons running tasks that do not serve queries (for example, batch, compaction, and MSQ tasks) now ignore `druid.server.http.unannouncePropagationDelay` and shut down without waiting. Real-time streaming tasks still honor the delay [#20444](https://github.com/apache/druid/pull/20444)
- Improved port binding. Druid nodes can now advertise a different plaintext port for peer discovery while binding Jetty to the actual service port [#19598](https://github.com/apache/druid/pull/19598)
- Reduced the heap footprint of loaded segments [#20467](https://github.com/apache/druid/pull/20467)

### Data management

#### Skip intervals during compaction

Auto-compaction now supports a `skipIntervals` setting in the datasource compaction config, allowing specific intervals to be excluded from compaction in addition to the existing `skipOffsetFromLatest` behavior. This is useful, for example, to exclude an interval that a backfill job is actively writing to, or one that consistently fails compaction.

[#19792](https://github.com/apache/druid/pull/19792)

#### Better visibility for large segment files

Druid now reports when your range or hash partitioning settings produce overly large segment files (hot partitions) in a time chunk. Native batch ingestion with hash or range partitioning now logs a warning and emits the `ingest/segments/oversized` metric. The metric counts published segments with more than double `maxRowsPerSegment` rows, or more than triple `targetRowsPerSegment` for range partitioning. The task completion report also includes this count as `oversizedSegments`.

[#20173](https://github.com/apache/druid/pull/20173)

#### Faster segment searches

You can now speed up segment searches in the in-memory timeline by enabling fast interval search. This feature uses an [interval tree](https://en.wikipedia.org/wiki/Interval_tree) index to organize the timeline in memory for faster loading and searching of segments.

To use this feature, set `druid.segment.timeline.fastIntervalSearch` to `true`.

[#19138](https://github.com/apache/druid/pull/19138) [#19861](https://github.com/apache/druid/pull/19861)

#### Partial segment loading

Druid now supports partial segment loading for virtual storage (on by default). To turn it off, set either of the following to `false`:

- the runtime property `druid.indexer.task.virtualStoragePartialDownloadsEnabled`
- the query context parameter `virtualStoragePartialDownloadsEnabled` 

[#19739](https://github.com/apache/druid/pull/19739)

#### Other data management improvements

- Added support for MD5 request checksums for S3-compatible storage. To enable this feature, set `druid.s3.enableLegacyMd5` to `true` [#20055](https://github.com/apache/druid/pull/20055)
- Added support for pushing, pulling, and killing segments in Azure and Google Cloud deep storage without zip compression [#20454](https://github.com/apache/druid/pull/20454) [#20327](https://github.com/apache/druid/pull/20327) [#20382](https://github.com/apache/druid/pull/20382) [#20380](https://github.com/apache/druid/pull/20380)
- Improved the loading algorithm to better disperse requests across Historical tiers when using virtual storage [#20155](https://github.com/apache/druid/pull/20155)
- Improved the efficiency of embedded kill tasks [#19772](https://github.com/apache/druid/pull/19772)

### Metrics and monitoring

#### Tier storage metrics

The tier storage metrics now provide better observability for Historicals, with or without virtual storage. Two metrics, `tier/required/capacity` and `tier/total/capacity`, are deprecated and will be removed in a future release. Use `tier/storage/required` and `tier/storage/capacity` instead.

[#20199](https://github.com/apache/druid/pull/20199)

#### `task/run/time` metric

The `statsd` and `prometheus` emitters now emit the `taskStatus` dimension on `task/run/time`. This is a breaking change. For more information, see [alerts on `task/run/time`](#alerts-on-taskruntime).

[#20122](https://github.com/apache/druid/pull/20122)

#### Other metrics and monitoring improvements

- Peon task metrics for clusters that use the Kubernetes Overlord extension now include a `podTemplate` dimension. This makes it easier to monitor clusters that map different tasks to different pod templates [#20001](https://github.com/apache/druid/pull/20001)
- Added `mergeBuffer/maxSpillProximity` which measures how close any single groupBy query came to spilling during the emission period, as a fraction in `[0.0, 1.0]` [#19627](https://github.com/apache/druid/pull/19627)
- Added `taskType` to the various task count metrics, so you can filter them by task type [#20123](https://github.com/apache/druid/pull/20123)

### Contributor extensions

#### Iceberg

Apache Iceberg ingestion now supports an opt-in vectorized reader backed by `iceberg-arrow`. Enable it with `"useArrowReader": true`; tune batching with `arrowBatchSize` (default 1024). The Arrow path supports Parquet data files, dictionary-encoded values, schema evolution, time-travel snapshots, column projection, and predicate pushdown. Snapshots containing positional or equality delete files aren't supported, and Druid rejects them with an error; use the standard delete-aware reader for those snapshots. The Arrow path is not splittable, so keep it disabled where parallel batch throughput is required. The default reader remains unchanged, and both paths coexist under the same Iceberg input-source type.

[#19510](https://github.com/apache/druid/pull/19510)

#### Open Policy Agent

Added the `druid-opa-authorizer` contributor extension, which enables authorization through Open Policy Agent (OPA). You can write Rego policies to control access to Druid resources based on user identity, action, and resource type. The extension supports both basic authentication and LDAP authentication workflows.

[#19670](https://github.com/apache/druid/pull/19670)

### Documentation improvements

## Upgrade notes and incompatible changes

### Upgrade notes

#### Dart workers

Dart workers on Historicals are now announced and discovered by the Broker with a new `DartWorkerService` mechanism. If you've enabled Dart, follow the standard rolling upgrade order: during an upgrade, restart Historicals before the Broker; during a downgrade, restart the Broker before Historicals.

[#20508](https://github.com/apache/druid/pull/20508)

### Incompatible changes

#### Permissions changes for SQL-based ingestion

The [table visibility](#table-visibility) changes affect the permissions required for SQL-based ingestion. INSERT and REPLACE now require READ access (in addition to WRITE) on the target table.

To restore the previous behavior, set `druid.sql.planner.authorizeTableVisibility` to `false`.

[#20075](https://github.com/apache/druid/pull/20075)

#### Alerts on `task/run/time`

The `statsd` and `prometheus` emitters now emit the `taskStatus` dimension on `task/run/time`. This is a breaking change for dashboards and alerts on `task/run/time`:

- `statsd` (non-DogStatsD mode): Dimension values are joined into the dotted metric name, so `druid.overlord.task.run.time.DATASOURCE.TASKTYPE` becomes `druid.overlord.task.run.time.DATASOURCE.TASKSTATUS.TASKTYPE`. Queries against the old name stop receiving data.
- `statsd` (DogStatsD mode): A `taskStatus` tag is added. Existing queries keep working and aggregate over it.
- `prometheus`: A `taskStatus` label is added. Update any queries that select on an exact label set and any recording rules that assume the old label set.

To keep the previous behavior, pin the old dimension list with `druid.emitter.statsd.dimensionMapPath` or `druid.emitter.prometheus.dimensionMapPath`.

[#20122](https://github.com/apache/druid/pull/20122)

#### `SysMonitor` removed

`SysMonitor` has been removed after being deprecated in Druid 27. Use `OshiSysMonitor` instead.

[#20200](https://github.com/apache/druid/pull/20200)

### Developer notes

#### Web console code editor

The web console's code editors moved from Ace (`ace-builds` and `react-ace`) to CodeMirror 6. All editors now share a single `CodeEditor` component, including the workbench, the explore view's SQL inputs, JSON inputs, spec and value dialogs, history, and explain. Two Lezer grammars, one for DruidSQL and one for Hjson, drive the highlighting and the completions.

[#20507](https://github.com/apache/druid/pull/20507)

#### Dependency updates

Druid 39.0.0 includes a variety of updates to dependencies such as Calcite. For a full list, see the [Area - Dependencies label](https://github.com/apache/druid/milestone/69?closed=1&q=state%3Aclosed+milestone-number%3A69+archived%3Afalse+sort%3Amilestone_prio-desc+label%3A%22Area+-+Dependencies%22) in the milestone.