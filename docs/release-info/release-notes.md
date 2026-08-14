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

Apache Druid 38.0.0 contains over $NUMBER_FEATURES new features, bug fixes, performance enhancements, documentation improvements, and additional test coverage from $NUMBER_OF_CONTRIBUTORS contributors.

<!--
Replace {{MILESTONE}} with the correct milestone number. For example: https://github.com/apache/druid/issues?q=is%3Aclosed+milestone%3A28.0+sort%3Aupdated-desc+
-->

See the [complete set of changes](https://github.com/apache/druid/milestone/68?closed=1) for additional details, including bug fixes.

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

#### Java

Druid now supports Java 25. While Druid 21 is still supported, we recommend you upgrade to Java 25.

Support for Java 17 has been dropped.

[#19304](https://github.com/apache/druid/pull/19304) [#19336](https://github.com/apache/druid/pull/19336)

#### Historical tier aliases

You can now use the `historicalTierAliases` Coordinator dynamic configuration to map a virtual tier name to a set of real Historical tiers so that a group of Historical tiers has a single identifier. When a load/drop rule references the alias, the Coordinator replaces it with the actual tiers. For example, if you map the Historical tiers `hot_1` and `hot_2` to the alias `hot`, the rule `{"hot": 2}` loads 2 replicas of each onto `hot_1` and `hot_2`.

[#19204](https://github.com/apache/druid/pull/19204) [#19667](https://github.com/apache/druid/pull/19667)

#### New load rule types

Adds a new family of retention rules, `loadPartialByPeriod`, `loadPartialByInterval`, `loadPartialForever`, laying the groundwork for partial loading of version 10 segment projections on Historicals. 

[#19374](https://github.com/apache/druid/pull/19374)

#### Realtime segments query context

The `realtimeSegmentsOnly` query context parameter has been deprecated and replaced with `realtimeSegmentsMode`. 

You can set `realtimeSegmentsMode` to one of the following:

- `include` (default): query all segments, including realtime segments
- `exclude`: skip realtime segments for the query
- `exclusive`: query only realtime segments 

This is useful when performing things like blue/green deployments and you only want to query new Historical replica autoscaling groups and not touch any "live" nodes (neither realtime nor historical).

[#19486](https://github.com/apache/druid/pull/19486)

#### Faster segment metadata search

You can set `druid.segment.timeline.fastIntervalSearch` to `true` so that Druid uses an index based on interval trees to store that metadata in memory for faster identification and retrieval.

This feature is off by default.

[#19138](https://github.com/apache/druid/issues/19138) [#19850](https://github.com/apache/druid/issues/19850)

#### Fetching files in the background

The MSQ task engine now supports workers fetching input files from cloud storage asynchronously. This overlaps downloading with processing, which generally improves throughput when reading larger numbers of files. Otherwise, workers stream each file directly from cloud storage while processing it.

This feature is on by default and is controlled by the `backgroundFetchExternalFiles` setting.

[#19539](https://github.com/apache/druid/pull/19539)

#### Segment prefetching for Dart

Dart now supports the runtime property `druid.msq.dart.worker.segmentLoadAheadCount`, which controls the number of segments that Dart prefetches. If configured to be greater than 0 for a worker, this becomes the default `segmentLoadAheadCount` value for the worker. If a query includes the `segmentLoadAheadCount` query context parameter, the query context takes precedence.

[#19559](https://github.com/apache/druid/pull/19559)

#### Partial segment loading

[#19620](https://github.com/apache/druid/pull/19620)
[#19535](https://github.com/apache/druid/pull/19535)

#### Clustered segments

[#19579](https://github.com/apache/druid/pull/19579)
[#19597](https://github.com/apache/druid/pull/19597) [#19460](https://github.com/apache/druid/pull/19460)

#### Improved Convert to SQL in web console

The web console now supports converting streaming supervisors to SQL-based ingestion queries. This makes it easier for you to run a streaming task as a one-time batch ingestion.

Select the **Convert supervisor to SQL** option from the **...** menu in the Query view. You can select from existing supervisors or provide JSON for a new one.

[#19547](https://github.com/apache/druid/pull/19547)

## Functional area and related changes

This section contains detailed release notes separated by areas.

### Web console

#### Other web console improvements

- Added the following status details to the **Services** view for Historical services: cloning from another Historical, in turbo loading mode, in decommissioning mode [#19253](https://github.com/apache/druid/pull/19253)
- Added support for resetting a supervisor to the latest offsets and backfilling [#19533](https://github.com/apache/druid/pull/19533)
- Improved how new tabs are handled [#19483](https://github.com/apache/druid/pull/19483)
- Improved the Home view's **Services** card. It now reports Overlord, Coordinator, Router, Broker, and Indexer counts on clusters where the web console talks to the Coordinator without SQL access [#19481](https://github.com/apache/druid/pull/19481)  

### Ingestion

* You can now use the expression aggregator at ingestion time for expressions that produce a LONG or DOUBLE for both fold and combine expressions [#19508](https://github.com/apache/druid/pull/19508)
* Added `now()` expression function that returns the current system timestamp in milliseconds since epoch. Useful at ingestion time for troubleshooting pipeline delays (e.g., `now() - __time`). Note: `now()` is non-deterministic as it evaluates for every row, so it can break idempotency. This can be added to any besides `__time` [#19386](https://github.com/apache/druid/pull/19386)
* Improved resiliency when ingesting from S3. Druid now retries on `SSLException` and transient credential errors instead of failing [#19617](https://github.com/apache/druid/pull/19617) [#19558](https://github.com/apache/druid/pull/19558)
* Improved S3 performance [#19394](https://github.com/apache/druid/pull/19394)
* Updated the default S3 connection pool size so that it's computed based on the number of available processors [#19536](https://github.com/apache/druid/pull/19536)

#### SQL-based ingestion

##### EXTERN for S3 now supports role ARN

You can now include the role ARN when running an INSERT INTO EXTERN query. For example:

```
INSERT INTO
EXTERN(
  s3(bucket => 'test2', prefix => 'export', assumeRoleArn => 'arn:aws:iam::00000:role/test-20260520'))
AS CSV
SELECT ...
```

[#19317](https://github.com/apache/druid/pull/19317)

##### Other SQL-based ingestion improvements

- Added storage counters for the amount of bytes and files written to local and durable storage as well as the state of the local `ByteTracker`. Per-worker storage counters have also been added to the web console [#19316](https://github.com/apache/druid/pull/19316)

#### Streaming ingestion

##### Scaling cool down

You can now configure different cool downs for scaling up and scaling down streaming task autoscalers.

[#19286](https://github.com/apache/druid/pull/19286)

##### Improved supervisor restarts

Supervisors no longer restart for all changes. Based on the type of change, one of the following can occur:

- The updated spec is persisted without a restart
- The supervisor is restarted but running tasks aren't impacted
- The supervisor is restarted and its tasks are terminated (the default behavior prior to this change)

For example, cosmetic changes to a supervisor spec no longer trigger a restart.

[#19700](https://github.com/apache/druid/pull/19700) [#19720](https://github.com/apache/druid/pull/19720)

Additionally, the algorithm for determining a change in the spec has been improved. For example, changes to `ioConfig.taskCount` don't trigger a supervisor restart if auto-scaling is enabled.

[#19541](https://github.com/apache/druid/pull/19541)

###### Latest offset and backfill

For Kafka and RabbitMQ, you can now reset a supervisor to the latest offset and start a new bounded backfill supervisor to ingest data from the skipped range. This is a useful feature for operating Druid clusters where the most recent data is the most important, such as for alerting.

Note the following requirements:

- The supervisor's `useEarliestSequenceNumber` property must be `false`.
- The supervisor context must have `useConcurrentLocks` set to `true` to allow the backfill supervisor's tasks to write concurrently with the main supervisor's tasks.
- The supervisor must be in a `RUNNING` state.

Use `POST` `/druid/indexer/v1/supervisor/{supervisorId}/resetToLatestAndBackfill` or the web console to perform this action.

[#19477](https://github.com/apache/druid/pull/19477)

##### Prunable shard specs for streaming published segments

Kafka ingestion can now publish segments that the Broker prunes at query time without waiting for compaction. Set `tuningConfig.streamingPartitionsSpec.partitionDimensions` to a list of low-to-medium cardinality dimensions; each task records
the distinct values it observes per dimension and stamps them onto a new `dim_value_set` shard spec. Queries that filter on a declared dimension then skip segments whose values can't match. 

The feature is opt-in, Kafka-only, and disabled by
default; when unset, behavior is unchanged.

`dim_value_set` is a new core shard spec type with no fallback, so it is not forward-compatible. Upgrade all services before enabling `streamingPartitionsSpec`. Once `dim_value_set` segments are published, downgrade is unsupported
until they are compacted away or `streamingPartitionsSpec` is removed.

Added `maxValuesPerDimension` (optional) to `streamingPartitionsSpec`.
  
[#19571](https://github.com/apache/druid/pull/19571) [#19596](https://github.com/apache/druid/pull/19596)

##### Other streaming ingestion improvements

- Added a property called `boundedStreamConfig` to the `SeekableStreamSupervisorIOConfig`, which allows operators to spin up a Supervisor that consumes only a specified offset range [#19372](https://github.com/apache/druid/pull/19372)
- Improved the cost-based autoscaler for better throughput [#19646](https://github.com/apache/druid/pull/19646)

### Querying

#### New query laning strategy

The `weighted` query laning strategy scores queries by how many thresholds they breach (segment count, interval duration, data age, segment range) and assigns them to configurable graduated lanes with different capacity limits, providing more nuanced lane assignment than the existing binary high/low strategy.

The weighted query laning strategy supports optional per-threshold cost weights:

- `periodWeight`
- `durationWeight`
- `segmentCountWeight`
- `segmentRangeWeight`

These threshold weights default to 1.

[#19225](https://github.com/apache/druid/pull/19225) [#19665](https://github.com/apache/druid/pull/19665) [#19696](https://github.com/apache/druid/pull/19696)

#### Other querying improvements

- Added nullable `minTime/maxTime` Long fields to `ProjectionMetadata` [#19398](https://github.com/apache/druid/pull/19398)
- Added `getDimensionRangeSet` support to `LikeDimFilter` for equality and prefix cases [#19524](https://github.com/apache/druid/pull/19524)
- Added support for aggregate projections with clustered segments [#19599](https://github.com/apache/druid/pull/19599)
- Optimized performance of aggregators for groupBy queries [#19423](https://github.com/apache/druid/pull/19423)

### Cluster management

#### Improved `diskNormalized` balancer strategy

The `diskNormalized` strategy is now more tunable. The primary changes are making the `utilizationThreshold` more intuitive: increasing the threshold increases the "tolerance" of the strategy while decreasing pushes nodes' disk utilization closer together.

[#19663](https://github.com/apache/druid/pull/19663)

#### MiddleManager and Indexer restarts

MiddleManagers and Indexers now persist their enabled or disabled state across restarts. If you prefer the old behavior, where the server re-enables itself after a restart, set `druid.worker.startAlwaysEnabled = true`.

[#19373](https://github.com/apache/druid/pull/19373)

#### Kafka idle signal

Improved the cost-based auto scaler for Kafka. The `poll-idle ratio` only reflected the time spent polling, whether there is spare processing capacity. You can now configure the autoscaler to use a utilization ratio instead: 

```
1 - (avgProcessingRate / maxObservedRate)
```

Set `useUtilizationRatio` to `true` to use this new ratio for autoscaling. 

[#19622](https://github.com/apache/druid/pull/19622)


#### Other cluster management improvements

- Added `datasource` filter pushdown to `sys.segments` table [#19718](https://github.com/apache/druid/pull/19718) [#19731](https://github.com/apache/druid/pull/19731)
- Added a `restarted` boolean field to the supervisor POST endpoint response to indicate whether the supervisor was actually restarted [#19349](https://github.com/apache/druid/pull/19349)
- Added `error_message` column to `sys.server_properties` table and made the table resilient to unreachable servers. Previously, the entire query would fail if any server was unreachable; now a row is returned with `error_message` populated. The table also now supports filter and projection pushdown [#19459](https://github.com/apache/druid/pull/19459)
- Added `druid.expressions.useVectorApi` config to support the incubating JDK Vector API. To use the API, set the config to `true` and start Druid with the `--add-modules=jdk.incubator.vector` flag [#19512](https://github.com/apache/druid/pull/19512)
- Added debug logging at the INFO level for projections if the debug flag is set [#19613](https://github.com/apache/druid/pull/19613)
- Changed MSQ task engine logging. It now logs the full stack trace when `debug` is set in the context [#19361](https://github.com/apache/druid/pull/19361)
- Improved the cost-based autoscaler so that it scales down over-provisioned supervisors running above the ideal idle ratio with low lag [#19562](https://github.com/apache/druid/pull/19562)
- Improved how Druid handles Java. MiddleManagers now honor `JAVA_HOME` [#19709](https://github.com/apache/druid/pull/19709)

### Data management

#### Other data management improvements

* Improved how compaction supervisor specs start up. They no longer attempt to create tasks with invalid configs [#19223](https://github.com/apache/druid/pull/19223)
* Changed Historical tiers so that they can only be associated with one tier alias [#19595](https://github.com/apache/druid/pull/19595)
* Sped up segment metadata cache syncs [#19672](https://github.com/apache/druid/pull/19672)

### Metrics and monitoring

#### New metadata cache metrics

Added the following metrics for the segment metadata cache:

- `segment/metadataCache/unused/count`
- `segment/metadataCache/fetchIds/time`
- `segment/metadataCache/fetchPayloads/time`
- `segment/metadataCache/fetchPending/time`
- `segment/metadataCache/fetchSchemas/time`
- `segment/metadataCache/fetchIndexingStates/time`
- `segment/metadataCache/updateIds/time`
- `segment/metadataCache/updateSnapshot/time`
- `segment/metadataCache/schema/skipped`
- `segment/metadataCache/indexingState/added`
- `segment/metadataCache/indexingState/deleted`

[#19672](https://github.com/apache/druid/pull/19672)

#### Changed metrics for cost-based autoscaling

Removed the following metrics:

- `task/autoScaler/costBased/lagCost`
- `task/autoScaler/costBased/idleCost`

Added the following metrics:

- `task/autoScaler/costBased/avgProcessingRate`
- `task/autoScaler/costBased/avgPollIdleRatio`
- `task/autoScaler/costBased/lagWeight`
- `task/autoScaler/costBased/costWeight`

[#19631](https://github.com/apache/druid/pull/19631)

#### Auth metrics

You can now configure Druid to emit metrics for authorization events.

The `auth/forbidden` and `auth/exception` metrics have the following dimensions to support precise alerting on security events:

- `identity`
- `authorizerName`
- `resourceName`
- `resourceType`
- `action`
- `errorMessage` where applicable


To enable this functionality, set `druid.auth.emitAuthMetrics` to `true`.

[#19552](https://github.com/apache/druid/pull/19552)

#### Storage metrics

The `storage/load/bytes` and `storage/virtual/load/bytes` metrics now measure once the load is complete. Previously, they measured when the load starts.

Additionally, `storage/load/begin/bytes` and `storage/virtual/load/begin/bytes` have been introduced and have the previous function of `storage/load/bytes` and `storage/virtual/load/bytes`.

The `count` metrics have also been updated to reflect this.

[#19451](https://github.com/apache/druid/pull/19451)

#### Concurrent append and replace

Added the following metrics for concurrent append and replace in realtime ingestion tasks:

- `ingest/segmentUpgrade/count`
- `ingest/segmentUpgrade/notified`
- `ingest/segmentUpgrade/unmatched`
- `ingest/segmentUpgrade/sendFailed`
- `ingest/segmentUpgrade/announced`
- `ingest/segmentUpgrade/skipped`

[#19651](https://github.com/apache/druid/pull/19651)

#### Other metrics and monitoring improvements

* Added the metric `segment/allocated/count` to track IDs of allocated pending segments [#19674](https://github.com/apache/druid/pull/19674)
* Added `remoteAddress` dimension for JDBC/Avatica queries to the following metrics: `sqlQuery/time`, `sqlQuery/bytes`, and `sqlQuery/planningTimeMs` [#19231](https://github.com/apache/druid/pull/19231)
* Added `identity` dimension on `query/time` metric for the Router [#19342](https://github.com/apache/druid/pull/19342)
* Added `kafka/consumer/pollIdleRatio`, which corresponds to the Kafka consumer `poll-idle-ratio-avg` [#19366](https://github.com/apache/druid/pull/19366)
* Added `supervisorId` to Kafka consumer metrics [#19525](https://github.com/apache/druid/pull/19525)
* Added `tierAlias` dimension to some tiered metrics, making it easier to aggregate across aliases for the monitoring and alerting [#19595](https://github.com/apache/druid/pull/19595)
* Added the `query/segments/count` metric to data nodes [#19624](https://github.com/apache/druid/pull/19624)
* Added the following metrics for virtual storage: `storage/virtual/read/count`,  `storage/virtual/read/bytes`, `storage/virtual/read/time ` [#19632](https://github.com/apache/druid/pull/19632)
* Changed `query/node/{bytes/time}` and backpressure metrics to emit even on query failure to data nodes [#19453](https://github.com/apache/druid/pull/19453)
* Improved metrics for partial segment loading [#19632](https://github.com/apache/druid/pull/19632)
* Improved router logging to always include `statusCode` dimension [#19668](https://github.com/apache/druid/pull/19668)

### Extensions

#### Redis

You can now enable TLS support for Redis connections.

[#19666](https://github.com/apache/druid/pull/19666)

#### `pac4j` OIDC authentication

Users of the `druid-pac4j` OIDC authentication extension can now explicitly configure their preferred client authentication method using the new optional `clientAuthenticationMethod` parameter. This resolves compatibility issues introduced with `pac4j` `5.7.3` where OIDC providers advertising `private_key_jwt` would cause authentication failures when the asymmetric JWT method was not configured.

Supported values include: `client_secret_basic`, `client_secret_post`, `client_secret_jwt`, `private_key_jwt`, and `none`. If not specified, `pac4j` will continue to use its auto-detection behavior.

[#19020](https://github.com/apache/druid/pull/19020)

#### Kubernetes

- Added `sys` metrics for k8s peons [#19305](https://github.com/apache/druid/pull/19305)
- Added experimental support for running Kubernetes indexing tasks across multiple Kubernetes clusters. Set `druid.indexer.runner.type=multik8s` and configure `druid.indexer.runner.clusters` to schedule tasks across multiple Kubernetes clusters from a single Overlord [#19433](https://github.com/apache/druid/pull/19433)
- `jvm.config` ConfigMaps are now honored by Peons [#19364](https://github.com/apache/druid/pull/19364)
- You can now set `podTemplateSelectionKey` in a task's context to pick a specific configured pod template (e.g. `druid.indexer.runner.k8s.podTemplate.<selectionKey>`) without configuring a `selectorBased` strategy. The override takes precedence over both the default and `selectorBased` strategies. The feature is controlled by the new runtime property `druid.indexer.runner.allowTaskPodTemplateSelection`, which defaults to `false`. If the named template isn't configured, the task fails to launch. [#19419](https://github.com/apache/druid/pull/19419)

#### T-digest sketch

You can now pin the maximum available compression on T-digest operators to bind resources using the `druid.tdigest.maxCompression` parameter.

[#19310](https://github.com/apache/druid/pull/19310)

#### OpenLineage

Added `extensions-contrib/openlineage-emitter` as a community extension. It uses the `RequestLogger` to transform and send lineage information to any OpenLineage-compatible API.

[#19107](https://github.com/apache/druid/pull/19107)

## Upgrade notes and incompatible changes

### Upgrade notes

#### Java

Druid now supports Java 25. While Java 21 is still supported, we recommend you upgrade to Java 25.

Support for Java 17 has been dropped.

[#19304](https://github.com/apache/druid/pull/19304) [#19336](https://github.com/apache/druid/pull/19336)

#### JAVA_HOME

When Druid uses the bundled `bin/run-java` script during startup, it honors the `DRUID_JAVA_HOME`/`JAVA_HOME` environment variables. Otherwise, Druid falls back to `java` on the `PATH`.

[#19709](https://github.com/apache/druid/pull/19709)

#### ZooKeeper-based task runner

The ZooKeeper-based `RemoteTaskRunner` (`druid.indexer.runner.type=remote`) has been removed. The HTTP-based `httpRemote` runner has been the default since Druid 25.0.0 and is now the only supported distributed task runner. `local` (in-process) remains supported for single-process testing, and the `k8s` runner from the Kubernetes extension is unaffected.

If your configuration sets `druid.indexer.runner.type=remote`, startup fails. Remove the property or set it to `httpRemote` (which is the default) to proceed.

The following configuration properties are no longer recognized and should be removed from `common.runtime.properties`:

- `druid.indexer.runner.maxZnodeBytes`
- `druid.indexer.runner.taskShutdownLinkTimeout`
- `druid.indexer.runner.compressZnodes`
- `druid.zk.paths.indexer.base`
- `druid.zk.paths.indexer.announcementsPath`
- `druid.zk.paths.indexer.tasksPath`
- `druid.zk.paths.indexer.statusPath`

ZooKeeper is still used for Coordinator/Overlord leader election and service (node) announcement and discovery.

[#19500](https://github.com/apache/druid/pull/19500)

#### ZooKeeper-based segment announcement and discovery removed

The ZooKeeper-based segment announcement and inventory view, which have been deprecated and off by default for several releases, have been removed. The HTTP-based path (the default for `druid.serverview.type=http`) is now the only supported option.

If your configuration sets `druid.serverview.type` to anything other than `http`, startup now fails with a clear error message. Remove the property (or set it to `http`, which is the default) to proceed.

The following configuration properties are no longer recognized and should be removed from `common.runtime.properties`:

- `druid.announcer.segmentsPerNode`
- `druid.announcer.maxBytesPerNode`
- `druid.announcer.skipLoadSpec`
- `druid.announcer.skipDimensionsAndMetrics`
- `druid.announcer.skipSegmentAnnouncementOnZk`
- `druid.zk.paths.announcementsPath`
- `druid.zk.paths.liveSegmentsPath`
- `druid.zk.paths.propertiesPath`
- `druid.zk.paths.connectorPath`

ZooKeeper is still used for leader election, service (node) announcement and discovery, and Overlord-to-MiddleManager task management.

[#19377](https://github.com/apache/druid/pull/19377)

#### Jackson update

If external code has both `@JacksonInject` and `@JsonProperty` on the same parameter and relies on the JSON value winning when supplied, add the explicit `useInput = OptBoolean.TRUE` to the annotation (or stay on Jackson `2.20.x`). All such sites in Druid itself have been updated.

[#19528](https://github.com/apache/druid/pull/19528)

### Incompatible changes

#### Java 17

Support for Java 17 has been dropped. [#19304](https://github.com/apache/druid/pull/19304)

### Developer notes

* Changed the shard spec collector to an interface to make extensibility easier [#19744](https://github.com/apache/druid/pull/19744)
* Changed `iceberg.core.version` in root pom and aligned embedded tests to use `1.10.0`
* Updated tests in `druid-processing` to use JUnit 5 [#19601](https://github.com/apache/druid/pull/19601)
* Updated the `apache/druid` and `apache/druid-website-src` repositories to use Docusaurus 3.10 when building the website [#19522](https://github.com/apache/druid/pull/19522)
* Updated IntelliJ settings to match Druid's supported Java versions [#19661](https://github.com/apache/druid/pull/19661)

#### Dependency updates

The following dependencies have had their versions bumped:

* `org.apache.logging.log4j` from `2.25.4` to `2.26.0` [#19629](https://github.com/apache/druid/pull/19629)
* `com.fasterxml.jackson` from `2.21.3` to `2.21.4` [#19618](https://github.com/apache/druid/pull/19618)
* `io.netty` from `4.2.12.Final` to `4.2.15.Final` [#19566](https://github.com/apache/druid/pull/19566)
* `caffeine` from `2.9.3` to `3.2.4` [#19527](https://github.com/apache/druid/pull/19527)
* `errorprone` from `2.41.0` to `2.49.0` [#19527](https://github.com/apache/druid/pull/19527)
* `jackson` from `2.20.2` to `2.21.3` [#19528](https://github.com/apache/druid/pull/19528)
* `derby` from `10.14.2.0` to `10.17.1.0` [#19492](https://github.com/apache/druid/pull/19492)
* `postgres` from `42.7.2` to `42.7.11` [#19474](https://github.com/apache/druid/pull/19474)
* `org.bouncycastle` from `1.82` to `1.84` [#19473](https://github.com/apache/druid/pull/19473)
- Apache Kafka client and broker dependencies from `3.9.2` to `4.2.3` [#19441](https://github.com/apache/druid/pull/19441) [#19584](https://github.com/apache/druid/pull/19584)
- `axios` from 1.15.0 to 1.15.2 [#19430](https://github.com/apache/druid/pull/19430)
- `pac4j` from `5.7.3` to `5.7.10` [#19388](https://github.com/apache/druid/pull/19388)
- `jose.jwt` from `9.37.2` to `9.37.3` [#19388](https://github.com/apache/druid/pull/19388)
- `log4j` from `2.25.3` to `2.25.4` [#19388](https://github.com/apache/druid/pull/19388)
- `RoaringBitmap` from `1.6.13` to `1.6.14` [#19688](https://github.com/apache/druid/pull/19688)