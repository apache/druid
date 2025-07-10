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

Apache Druid 34.0.0 contains over $NUMBER_FEATURES new features, bug fixes, performance enhancements, documentation improvements, and additional test coverage from $NUMBER_OF_CONTRIBUTORS contributors.

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

#### Improved HTTP endpoints

You can now use raw SQL in the HTTP body for `/druid/v2/sql` endpoints. You can set `Content-Type` to `text/plain` instead of `application/json`, so you can provide raw text that isn't escaped. 

[#17937](https://github.com/apache/druid/pull/17937)

Additionally, SQL requests can now include multiple SET statements to build up context for the final statement. For example, the following query results in a statement that includes the `timeout`, `useCache`, `populateCache`, and `vectorize` query context parameters: 

```sql
SET timeout = 20000;
SET useCache = false;
SET populateCache = false;
SET vectorize = 'force';
SELECT "channel", "page", sum("added") from "wikipedia" GROUP BY 1, 2
```

This improvement also works for INSERT and REPLACE queries using the MSQ task engine. Note that JDBC isn't supported.

[#17974](https://github.com/apache/druid/pull/17974)
### Cloning Historicals

You can now configure clones for Historicals using the dynamic Coordinator configuration `cloneServers`. Cloned Historicals are useful for situations such as rolling updates where you want to launch a new Historical as a replacement for an existing one.

Set the config to a map from the target Historical server to the source Historical:

```
  "cloneServers": {"historicalClone":"historicalOriginal"}
```

The clone doesn't participate in regular segment assignment or balancing. Instead, the Coordinator mirrors any segment assignment made to the original Historical onto the clone, so that the clone becomes an exact copy of the source. Segments on the clone Historical do not count towards replica counts either. If the original Historical disappears, the clone remains in the last known state of the source server until removed from the `cloneServers` config.

When you query your data using the native query engine, you can prefer (`preferClones`), exclude (`excludeClones`), or include (`includeClones`) clones by setting the query context parameter `cloneQueryMode`. By default, clones are excluded.

As part of this change, new Coordinator APIs are available. For more information, see [Coordinator APIs for clones](#coordinator-apis-for-clones).

[#17863](https://github.com/apache/druid/pull/17863) [#17899](https://github.com/apache/druid/pull/17899) [#17956](https://github.com/apache/druid/pull/17956) 
### Overlord kill tasks

You can now run kill tasks directly on the Overlord itself. Running kill tasks on the Overlord provides the following benefits:

- Unused segments are killed as soon as they're eligible and are killed faster
- Doesn't require a task slot
- Locked intervals are automatically skipped
- Configuration is simpler
- A large number of unused segments doesn't cause issues for them

This feature is controlled by the following configs:

- `druid.manager.segments.killUnused.enabled` - Whether the feature is enabled or not
- `druid.manager.segments.killUnused.bufferPeriod` - The amount of time that a segment must be unused before it is able to be permanently removed from metadata and deep storage. This can serve as a buffer period to prevent data loss if data ends up being needed after being marked unused.

As part of this feature, [new metrics](#overlord-kill-task-metrics) have been added.

[#18028](https://github.com/apache/druid/pull/18028)

### Dart improvements

Dart specific endpoints have been removed and folded into SqlResource. [#18003](https://github.com/apache/druid/pull/18003)
Added a new engine QueryContext parameter. The value can be native or msq-dart. The value determines the engine used to run the query. The default value is native. [#18003](https://github.com/apache/druid/pull/18003)

MSQ Dart is now able to query real-time tasks by setting the query context parameter includeSegmentSource to realtime, in a similar way to MSQ tasks. [#18076](https://github.com/apache/druid/pull/18076)

### `SegmentMetadataCache` on the Coordinator

[#17996](https://github.com/apache/druid/pull/17996) [#17935](https://github.com/apache/druid/pull/17935)

## Functional area and related changes

This section contains detailed release notes separated by areas.

### Web console

#### SET statements

The web console supports using SET statements to specify query context parameters. For example, if you include `SET timeout = 20000;` in your query, the timeout query context parameter is set. 

[#17966](https://github.com/apache/druid/pull/17966)

#### Other web console improvements

- You can now assign tiered replications to tiers that aren't currently online [#18050](https://github.com/apache/druid/pull/18050)
- You can now filter tasks by the error in the Task view [#18057](https://github.com/apache/druid/pull/18057)

### Ingestion

- Improved concurrency for batch and streaming ingestion tasks [#17828](https://github.com/apache/druid/pull/17828)
- Removed the `useMaxMemoryEstimates` config. When set to false, Druid used a much more accurate memory estimate that was introduced in Druid 0.23.0. That more accurate method is the only available method now. The config has defaulted to false for several releases [#17936](https://github.com/apache/druid/pull/17936)

#### SQL-based ingestion

##### Other SQL-based ingestion improvements

#### Streaming ingestion

##### Multi-stream supervisors (experimental)

You can now use more than one supervisor to ingest data into the same datasource. Include the `spec.dataSchema.dataSource` field to help identify the supervisor.

When using this feature, make sure you set `useConcurrentLocks` to `true` for the `context` field in the supervisor spec.

[#18149](https://github.com/apache/druid/pull/18149) [#18082](https://github.com/apache/druid/pull/18082)

##### Supervisors and the underlying input stream

Seekable stream supervisors (Kafka, Kinesis, and Rabbit) can no longer update the underlying input stream (such as a topic for Kafka) that is persisted for it. This action was previously allowed by the API, but it isn't fully supported by the underlying system. Going forward, a request to make such a change results in a 400 error from the Supervisor API that explains why it isn't allowed. 

[#17955](https://github.com/apache/druid/pull/17955) [#17975](https://github.com/apache/druid/pull/17975)

##### Other streaming ingestion improvements

- Improved streaming ingestion so that it automatically determine the maximum number of columns to merge [#17917](https://github.com/apache/druid/pull/17917)


### Querying

#### Metadata query for segments

You can use a segment metadata query to find the list of projections attached to a segment.

[#18119](https://github.com/apache/druid/pull/18119)


#### Other querying improvements

- Changed `MV_OVERLAP` and `MV_CONTAINS` functions now aligns more closely with the native `inType` filter [#18084](https://github.com/apache/druid/pull/18084)
- Improved query handling when some segments are missing on Historicals. Druid no longer incorrectly returns partial results [#18025](https://github.com/apache/druid/pull/18025)

### Cluster management

#### Configurable timeout for subtasks

You can now configure a timeout for `index_parallel` and `compact` type tasks. Set the context parameter `subTaskTimeoutMillis` to the maximum time in milliseconds you want to wait before a subtask gets canceled. By default, there's no timeout.

Using this config helps parent tasks fail sooner instead of getting stuck and can free up tasks slots from zombie tasks.

[#18039](https://github.com/apache/druid/pull/18039)

#### Coordinator APIs for clones

The following Coordinator APIs are now available:

- `/druid/coordinator/v1/cloneStatus` to get information about ongoing cloning operations.
- `/druid/coordinator/v1/brokerConfigurationStatus` which returns the broker sync status for coordinator dynamic configs.

[#17899](https://github.com/apache/druid/pull/17899)

#### Other cluster management improvements

- Added the optional `taskCountStart`  property to the lag based auto scaler. Use it to specify the initial task count for the supervisor to be submitted with [#17900](https://github.com/apache/druid/pull/17900)
- Added audit logs for the following `BasicAuthorizerResource` update methods: `authorizerUserUpdateListener`, `authorizerGroupMappingUpdateListener`, `authorizerUpdateListener` (deprecated) [#17916](https://github.com/apache/druid/pull/17916)

### Data management

#### Other data management improvements

### Metrics and monitoring

#### Metrics for Historical cloning

The following metrics for Historical cloning have been added: 

- `config/brokerSync/time`
- `config/brokerSync/total/time`
- `config/brokerSync/error`


#### Real-time ingestion metrics

The following metrics for streaming ingestion have been added: 

|Metric|Description|Dimensions|
|------|------------|-----------|
|`ingest/events/maxMessageGap`|Maximum seen time gap in milliseconds between each ingested event timestamp and the current system timestamp of metrics emission. This metric is reset every emission period.|`dataSource`, `taskId`, `taskType`, `groupId`, `tags`|Greater than 0, depends on the time carried in event.|
|`ingest/events/minMessageGap`|Minimum seen time gap in milliseconds between each ingested event timestamp and the current system timestamp of metrics emission. This metric is reset every emission period.|`dataSource`, `taskId`, `taskType`, `groupId`, `tags`|Greater than 0, depends on the time carried in event.|
|`ingest/events/avgMessageGap`|Average time gap in milliseconds between each ingested event timestamp and the current system timestamp of metrics emission. This metric is reset every emission period.|`dataSource`, `taskId`, `taskType`, `groupId`, `tags`|Greater than 0, depends on the time carried in event.|

[#17847](https://github.com/apache/druid/pull/17847)

#### Kafka consumer metrics

The following metrics that correspond to Kafka metrics have been added:

| Kafka metric           | Druid metric                     |
|----------------------------|----------------------------------------|
| `bytes-consumed-total`     | `kafka/consumer/bytesConsumed`         |
| `records-consumed-total`   | `kafka/consumer/recordsConsumed`       |
| `fetch-total`              | `kafka/consumer/fetch`                 |
| `fetch-rate`               | `kafka/consumer/fetchRate`             |
| `fetch-latency-avg`        | `kafka/consumer/fetchLatencyAvg`       |
| `fetch-latency-max`        | `kafka/consumer/fetchLatencyMax`       |
| `fetch-size-avg`           | `kafka/consumer/fetchSizeAvg`          |
| `fetch-size-max`           | `kafka/consumer/fetchSizeMax`          |
| `records-lag`              | `kafka/consumer/recordsLag`            |
| `records-per-request-avg`  | `kafka/consumer/recordsPerRequestAvg`  |
| `outgoing-byte-total`      | `kafka/consumer/outgoingBytes`         |
| `incoming-byte-total`      | `kafka/consumer/incomingBytes`         |

[#17919](https://github.com/apache/druid/pull/17919)

#### Overlord kill task metrics

|Metric|Description|Dimensions|
|------|------------|-----------|
|`segment/killed/metadataStore/count`|Number of segments permanently deleted from metadata store|`dataSource`, `taskId`, `taskType`, `groupId`, `tags`|
|`segment/killed/deepStorage/count`|Number of segments permanently deleted from deep storage|`dataSource`, `taskId`, `taskType`, `groupId`, `tags`|
|`segment/kill/queueReset/time`|Time taken to reset the kill queue on the Overlord. This metric is emitted only if `druid.manager.segments.killUnused.enabled` is true.||
|`segment/kill/queueProcess/time`|Time taken to fully process all the jobs in the kill queue on the Overlord. This metric is emitted only if `druid.manager.segments.killUnused.enabled` is true.||
|`segment/kill/jobsProcessed/count`|Number of jobs processed from the kill queue on the Overlord. This metric is emitted only if `druid.manager.segments.killUnused.enabled` is true.||
|`segment/kill/skippedIntervals/count`|Number of intervals skipped from kill due to being already locked. This metric is emitted only if `druid.manager.segments.killUnused.enabled` is true.|`dataSource`, `taskId`|

[#18028](https://github.com/apache/druid/pull/18028)

#### Other metrics and monitoring improvements

- Added the `description` dimension for the `task/run/time` metric 
- Added a metric for how long it takes to complete an autoscale action: `task/autoScaler/scaleActionTime` [#17971](https://github.com/apache/druid/pull/17971)
- Added a `taskType` dimension to Overlord-emitted task count metrics  [#18032](https://github.com/apache/druid/pull/18032)
- Added the following groupBy metrics to the Prometheus emitter: `mergeBuffer/used`, `mergeBuffer/acquisitionTimeNs`, `mergeBuffer/acquisition`, `groupBy/spilledQueries`, `groupBy/spilledBytes`, and `groupBy/mergeDictionarySize` [#17929](https://github.com/apache/druid/pull/17929)
- Changed the logging level for query cancelations from `warn` to `info` to reduce noise [#18046](https://github.com/apache/druid/pull/18046)
- Changed query logging so that SQL queries that can't be3 parsed are no longer logged and don't emit metrics [#18102](https://github.com/apache/druid/pull/18102)
- Changed the logging level for lifecycle from `debug` to `info` [#17884](https://github.com/apache/druid/pull/17884)
- Added  `groupId` and `tasks` to Overlord logs [#17980](https://github.com/apache/druid/pull/17980)
- You can now use the `druid.request.logging.rollPeriod` to configure the log rotation period (default 1 day) [#17976](https://github.com/apache/druid/pull/17976)

### Extensions

#### Kubernetes

- The task runner log now includes the task id for a job and includes a log before a job is created [#18105](https://github.com/apache/druid/pull/18105)

### Documentation improvements

## Upgrade notes and incompatible changes

### Upgrade notes

#### `groupBy` and `topN` queries

Druid now uses the `groupBy` native query type, rather than `topN`, for SQL queries that group
by and order by the same column, have `LIMIT`, and don't have `HAVING`. This speeds up execution
of such queries since `groupBy` is vectorized while `topN` is not. 

You can restore the previous behavior by setting the query context parameter `useLexicographicTopN` to `true`. Behavior for `useApproximateTopN` is unchanged, and the default remains `true`.

#### `IS_INCREMENTAL_HANDOFF_SUPPORTED` config removed

Removed the `IS_INCREMENTAL_HANDOFF_SUPPORTED` context reference from supervisors, as incremental publishing has been the default behavior since version 0.16.0. This context was originally introduced to support rollback to LegacyKafkaIndexTaskRunner in versions earlier than 0.16.0, which has since been removed.

#### `useMaxMemoryEstimates` config removed 

Removed the `useMaxMemoryEstimates` config. When set to false, Druid used a much more accurate memory estimate that was introduced in Druid 0.23.0. That more accurate method is the only available method now. The config has defaulted to false for several releases. 

[#17936](https://github.com/apache/druid/pull/17936)

### Incompatible changes

### Developer notes

- Some maven plugins no longer use hardcoded version numbers. Instead, they now pull from the Apache parent [#18138](https://github.com/apache/druid/pull/18138)

#### Dependency updates

The following dependencies have had their versions bumped:

- `aws.sdk` for Java from `1.12.638` to  `1.12.784` [#18068](https://github.com/apache/druid/pull/18068)
- `fabric8` from `6.7.2` to `6.13.1`. The updated `fabric8` version uses `Vert.x` as an HTTP client instead of  `OkHttp` [#17913](https://github.com/apache/druid/pull/17913)
- Curator from `5.5.0` to `5.8.0` [#17857](https://github.com/apache/druid/pull/17857)
- `com.fasterxml.jackson.core` from `2.12.7.1` to `2.18.4` [#18013](https://github.com/apache/druid/pull/18013)
- `fabric8` from `6.13.1` to `7.2.0`
- `org.apache.parquet:parquet-avro` from `1.15.1` to `1.15.2`  [#18131](https://github.com/apache/druid/pull/18131)
- `commons-beanutils:commons-beanutils` from `1.9.4` to `1.11.0` [#18132](https://github.com/apache/druid/pull/18132)
- 