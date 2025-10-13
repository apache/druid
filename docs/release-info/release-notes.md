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

### Java 11 support removed

Upgrade to Java 17 or 21. Note that some versions of Java 21 encountered issues during test, specifically Java 21.05-21.07. If possible, avoid these versions.

[#18424](https://github.com/apache/druid/pull/18424)

### Projections (beta)

[#18342](https://github.com/apache/druid/pull/18342)
[#18535](https://github.com/apache/druid/pull/18535)

### Virtual storage (experimental)

[#18176](https://github.com/apache/druid/pull/18176)

### New input format

Druid now supports a `lines` input format. Druid reads each line from an input as UTF-8 text and creates a single column named `line` that contains the entire line as a string. Use this for reading line-oriented data in a simple form for later processing.

[#18433](https://github.com/apache/druid/pull/18433)

### Multi-stage query task engine

The MSQ task engine is now a core capability of Druid rather than an extension. It has been in the default extension load list for several releases. 

Remove `druid-multi-stage-query` from `druid.extensions.loadList` in `common.runtimes.properties` before you upgrade.

Druid 35.0.0 will ignore the extension if it's in the load list. Future versions of Druid will fail to start since it can't locate the extension.

[#18394](https://github.com/apache/druid/pull/18394)

### Improved monitor loading

You can now specify monitors in `common.runtime.properties`. Previously, you needed to define monitors in the specific `runtime.properties` file for the service a monitor is meant for.

[#18321](https://github.com/apache/druid/pull/18321)

### Exact count extension

A new contributor extension (`druid-exact-count-bitmap`) adds support for exact cardinality counting using Roaring Bitmap over a Long column.

[#18021](https://github.com/apache/druid/pull/18021)


### Customize JSON and auto column physical storage 

NEED TO WRITE

[#17762](https://github.com/apache/druid/pull/17762)

## Functional area and related changes

This section contains detailed release notes separated by areas.

### Web console

#### Other web console improvements

- Added better support for MSQ task engine-based compaction tasks. They now use the stages pane to render the compaction report instead of showing the JSON [#18545](https://github.com/apache/druid/pull/18545)
- Added a version column to the **Services** tab so that you can see what version a service is running. This is helpful during rolling upgrades to verify the state of the cluster and upgrade [#18542](https://github.com/apache/druid/pull/18542)
- Improved the resiliency of the web console when the supervisor history is extensive [#18416](https://github.com/apache/druid/pull/18416) 


### Ingestion

- Improved task performance. Druid now polls from memory before fetching task information from the metadata database [#18448](https://github.com/apache/druid/pull/18448)
- Improved task execution so that they can successfully complete even if there are problems pushing logs and reports to deep storage [#18210](https://github.com/apache/druid/pull/18210)

#### SQL-based ingestion

##### Other SQL-based ingestion improvements

- Added the ability to configure the maximum frazme size. Generally, you don't need to change this unless you have very large rows [#18442](https://github.com/apache/druid/pull/18442)
- Added logging for when segment processing fails [#18378](https://github.com/apache/druid/pull/18378)
- Improved logging to store the cause of invalid field exceptions [#18517](https://github.com/apache/druid/pull/18517) [#18517](https://github.com/apache/druid/pull/18517)


#### Streaming ingestion

##### Other streaming ingestion improvements

- Added a count parameter to the supervisor history API [#18416](https://github.com/apache/druid/pull/18416)

### Querying

#### Caching scan query results

Druid now supports result-level caching for scan queries.

By default, this behavior is turned off. To enable, override the `druid.*.cache.unCacheable` property. 

[#18568](https://github.com/apache/druid/pull/18568)

#### New expressions for sketches

Druid now supports the following expressions:

- `HLL_SKETCH_ESTIMATE_WITH_ERROR_BOUNDS` 
- `THETA_SKETCH_ESTIMATE_WITH_ERROR_BOUNDS`

These estimates work on a sketch column and have the same behavior as the post aggregators.

[#18426](https://github.com/apache/druid/pull/18426)

#### New multi-value SQL functions

Druid now supports the following multi-value functions:

- `MV_FILTER_REGEX`: filters a multi-value expression to include only values matching the specified regular expression pattern
- `MV_FILTER_PREFIX`: filters a multi-value expression to include only values that start with the specified prefix

[#18281](https://github.com/apache/druid/pull/18281)

#### Exception handling

You can now  write exceptions that Druid encounters as a row and then verify whether the exception is expected. To use this feature, set the `writeExceptionBodyAsResponseRow` query context parameter to `true`.

[#18571](https://github.com/apache/druid/pull/18571)

#### Other querying improvements

- Added stricter validation for `GREATEST` and `LEAST` [#18562](https://github.com/apache/druid/pull/18562)
- Added a query context option called `realtimeSegmentsOnly` that returns results from realtime segments only when set to `true` [#18329](https://github.com/apache/druid/pull/18329)
- Improved the performance of the following query types by vectorizing them: 
  - `CASE_SEARCHED` and `CASE_SIMPLE` queries [#18512](https://github.com/apache/druid/pull/18512)
  - `timestamp_ceil` and `timestamp_extract` queries [#18517](https://github.com/apache/druid/pull/18517)
  - `IF` expressions where the `THEN` and `ELSE` expressions [#18507](https://github.com/apache/druid/pull/18507)
- Improved projections so that the granularty in queries can match UTC time zones [#18403](https://github.com/apache/druid/pull/18403)
- Improved the deserialization time for intervals by more than 40% [#18477](https://github.com/apache/druid/pull/18477)
- Improved the performance of `AND`/`OR` as well as `NOT`/`IS TRUE`/`IS FALSE` [#18491](https://github.com/apache/druid/pull/18491) [#18488](https://github.com/apache/druid/pull/18488)
- Improved the performance of scan queries [#18441](https://github.com/apache/druid/pull/18441)
- Improved the performance of metadata queries by parallelizing them at the data-node level [#18592](https://github.com/apache/druid/pull/18592)

### Cluster management

#### Error message strategy

You can now configure Druid to log an error message and return an error ID for non-user targeted messages. This can be configured by setting `druid.server.http.errorResponseTransform.strategy` to `persona`.

[#18487](https://github.com/apache/druid/pull/18487)

#### Kerberos authentication

The `druid.auth.authenticator.kerberos.cookieSignatureSecret` config is now mandatory. 

[#18368](https://github.com/apache/druid/pull/18368)

#### Other cluster management improvements

- Added a `version` column `sys.server`. This is useful during rolling upgrades to verify the state of the cluster [#18542](https://github.com/apache/druid/pull/18542)
- Added support for short, unique index names in metadata stores [#18515](https://github.com/apache/druid/pull/18515)
- Added support for proportional `stopTaskCount` to the task count autoscaler 
- Added headers to the user agent for services, improving traceability between services [#18505](https://github.com/apache/druid/pull/18505)
- Added a new Kafka emitter config: `druid.emitter.kafka.producer.shutdownTimeout`. It controls how long the Kafka producer waits for pending requests to finish before shutting down [#18427](https://github.com/apache/druid/pull/18427)
- Changed how Netty worker threads get calculated, lowering the number of threads used at small processor counts [#18493](https://github.com/apache/druid/pull/18493)
- Changed the response for the `/status/properties` to be alphabetical. The response is more readable now and related keys are grouped together [#18506](https://github.com/apache/druid/pull/18506) 
- Improved segment loading during startup in certain situations by optimizing concurrent writing [#18470](https://github.com/apache/druid/pull/18470)

### Data management

#### Other data management improvements

- Storing segments in S3 now supports not using zip compression, like local storage [#18544](https://github.com/apache/druid/pull/18544)

### Metrics and monitoring

#### Other metrics and monitoring improvements

- Added `segment/schemaCache/refreshSkipped/count` metric that shows the number of segments that skipped schema refresh [#18561](https://github.com/apache/druid/pull/18561)
- Added a metric for when auto-scaling /gets skipped when the pubishing task set is not empty [#18536](https://github.com/apache/druid/pull/18536)
- Added Kafka consumer metrics for indexing tasks in the `prometheus-emitter` default metrics config [#18458](https://github.com/apache/druid/pull/18458)
- Added an event when a datasource gets dropped from the Coordinator: `segment/schemaCache/datasource/dropped/count` [#18497](https://github.com/apache/druid/pull/18497)
- Added the `taskType` dimension to the following Prometheus metrics to distinguish between realtime and compaction tasks: `ingest/count`, `ingest/segments/count`, and `ingest/tombstones/count` [#18452](https://github.com/apache/druid/pull/18452)
- Added a server dimension to segment assignment skip metrics like `segment/assignSkipped/count` [#18313](https://github.com/apache/druid/pull/18313)
- Changed `CgroupUtil` to be less noisy [#18472](https://github.com/apache/druid/pull/18472)
- Reduced the clutter of output logs when there's a mismatch in Kafka and task partition set [#18234](https://github.com/apache/druid/pull/18234)

### Extensions

#### Iceberg

You can now filter Iceberg data based on a configurable time window.

[#18531](https://github.com/apache/druid/pull/18531)

#### Kubernetes

##### Task logs

For Middle Manager-less ingestion, persisting task logs no longer have the potential to block indefinitely. Instead, there is a time limit for persisting logs, that if breached, results in giving up on the persist. The default timeout is 5 minutes, but can be configured by overriding `druid.indexer.runner.logSaveTimeout` with a duration, such as `PT60S`.

Additionally, task log storage for the Kubernetes runner is now optional. 

[#18444](https://github.com/apache/druid/pull/18444) [#18341](https://github.com/apache/druid/pull/18341)

##### Improved fault tolerance

The Kubernetes extension is now more resilient against certain transient faults. 

[#18406](https://github.com/apache/druid/pull/18406)

##### Faster task failure

Task pods will now output Kubernetes job events when failing during the pod creation phase, and K8s tasks will fail fast under unrecoverable conditions.

[#18159](https://github.com/apache/druid/pull/18159)

#### Mount additional configs

Previously, you could specify additional configuration to be mounted to `_common` via an environment variable, but this only applied to the Druid process. You can now also apply the same configuration to ingestion tasks submitted as Kubernetes jobs (MiddleManager-less mode), ensuring consistency across all task types. 

[#18447](https://github.com/apache/druid/pull/18447) 

### Documentation improvements

## Upgrade notes and incompatible changes

### Upgrade notes

#### Fallback vectorization on by default

The `druid.expressions.allowVectorizeFallback` now defaults to `true`. Additionally, `SAFE_DIVIDE` can now vectorize as a fallback.

[#18549](https://github.com/apache/druid/pull/18549)

#### Java 11 support removed

Upgrade to Java 17 or 21. Note that some versions of Java 21 encountered issues during test, specifically Java 21.05-21.07. If possible, avoid these versions.

[#18424](https://github.com/apache/druid/pull/18424)

#### Jetty 12 

A new server configuration option has been added: `druid.server.http.uriCompliance`. Jetty 12 by default has strict enforcement of RFC3986 URI format. This is a change from Jetty 9. To retain compatibility with legacy Druid, this config defaults to `LEGACY`, which uses the more permissive URI format enforcement that Jetty 9 used. If the cluster you operate does not require legacy compatibility, we recommend you use the upstream Jetty default of `RFC3986` in your Druid deployment. See the jetty documentation for more info.

[#18424](https://github.com/apache/druid/pull/18424)

#### Kerberos authentication

The `druid.auth.authenticator.kerberos.cookieSignatureSecret` config is now mandatory. 

[#18368](https://github.com/apache/druid/pull/18368)

#### Multi-stage query task engine

The MSQ task engine is now a core capability of Druid rather than an extension. It has been in the default extension load list for several releases. 

Remove `druid-multi-stage-query` from `druid.extensions.loadList` in `common.runtimes.properties` before you upgrade.

Druid 35.0.0 will ignore the extension if it's in the load list. Future versions of Druid will fail to start since it cannot locate the extension.

[#18394](https://github.com/apache/druid/pull/18394)

#### pac4j extension

Due to the upgrade from `pac4j` 4.x to 5.x, session serialization has changed from `pac4j`’s JavaSerializer to standard Java serialization. As a result, clients of clusters using the `pac4j` extension may be logged out during rolling upgrades and need to re‑authenticate.

#### Front-coded dictionaries

<!--Carry this forward until 32. Then move it to incompatible changes -->

In Druid 32.0.0, the front coded dictionaries feature will be turned on by default. Front-coded dictionaries reduce storage and improve performance by optimizing for strings where the front part looks similar.

Once this feature is on, you cannot easily downgrade to an earlier version that does not support the feature. 

For more information, see [Migration guide: front-coded dictionaries](./migr-front-coded-dict.md).

If you're already using this feature, you don't need to take any action. 


### Incompatible changes


### Developer notes

#### Specialized virtual columns for JSON

The `ColumnHolder` returned by `ColumnSelector` now returns a `SelectableColumn` rather than `BaseColumn` from `getColumn()`. The `SelectableColumn` may be a `BaseColumn` or may be an implementation provided by a virtual column. Extensions that need to retrieve physical columns from a `ColumnSelector` can check if the column is a `BaseColumn` using `instanceof`. For convenience, the return type of `getColumnHolder` on `QueryableIndex` has been specialized to `BaseColumnHolder`, which always returns `BaseColumn` from `getColumn()`. Extensions that are calling this method on `QueryableIndex` can store the result in a variable of type `BaseColumnHolder` to avoid needing to cast the column.

[#18521](https://github.com/apache/druid/pull/18521)

#### `reset-cluster` tool removed

The `reset-cluster` dev and testing environment oriented tool has been removed. It was outdated and would fail to completely clear out the metadata store (missing `upgradeSegments` and `segmentSchemas` tables added in Druid 28 and 30 respectively). This tool was not intended for production use cases, so there should be no impact for cluster operators.

[#18246](https://github.com/apache/druid/pull/18246)

#### Breaking change for SQL metadata extensions

The `MetadataStorageActionHandler` extension point now has simpler APIs.
This is a breaking change for any SQL metadata extensions in the wild.
The changes for PostgreSQL, MySQL and SQLServer are already complete. [#18479](https://github.com/apache/druid/pull/18479)

#### `DriudLeaderClient` removal

`DruidLeaderClient` has been removed from extensions and replaced with a `ServiceClient`-based implmentation. Additionally, `CoordinatorServiceClient` for querying non-core APIs exposed by the Coordinator is available through `druid-server`.

[#18297](https://github.com/apache/druid/pull/18297) [#18326](https://github.com/apache/druid/pull/18326)

#### Dependency updates

The following dependencies have had their versions bumped:

- `pac4j` version 4.5.7 to 5.7.3
- `nimbus-jose-jwt` version 8.22.1 to 9.37.2
- `oauth2-oidc-sdk` version 8.22 to 10.8
- `avactica` version 1.25.0 to 1.26.0
- `protobuf-dynamic` library replaced with a built-in protobuf method
- `com.github.spotbugs:spotbugs-maven-plugin` version 4.8.6.6 to 4.9.3.2
- `com.google.code.gson` version 2.10.1 to 2.12.0
- `org.apache.commons` version 3.17.0 to 3.18.0
- `fabric8` version 7.2.0 to 7.4
- `jackson` version 2.18.4 to 2.19.2