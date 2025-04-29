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

Apache Druid 33.0.0 contains over 190 new features, bug fixes, performance enhancements, documentation improvements, and additional test coverage from 44 contributors.

<!--
Replace {{MILESTONE}} with the correct milestone number. For example: https://github.com/apache/druid/issues?q=is%3Aclosed+milestone%3A28.0+sort%3Aupdated-desc+
-->

See the [complete set of changes](https://github.com/apache/druid/issues?q=is%3Aclosed+milestone%3A{{MILESTONE}}+sort%3Aupdated-desc+) for additional details, including bug fixes.

Review the [upgrade notes](#upgrade-notes) before you upgrade to Druid 33.0.0.
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

### Increase segment load speed

You can now increase the speed at which segments get loaded on a Historical by providing a list of servers for the Coordinator dynamic config `turboLoadingNodes`. For these servers, the Coordinator ignores `druid.coordinator.loadqueuepeon.http.batchSize` and uses the value of the respective `numLoadingThreads` instead. Please note that putting a Historical in turbo-loading mode might affect query performance since more resources would be used by the segment loading threads.

[#17775](https://github.com/apache/druid/pull/17775)

### Overlord APIs for compaction (experimental)

You can use the following Overlord compaction APIs to manage compaction status and configs. These APIs work seamlessly irrespective of whether compaction supervisors are enabled or not.

For more information, see [Compaction APIs](#compaction-apis)

[#17834](https://github.com/apache/druid/pull/17834)

### Scheduled batch ingestion (experimental)

You can now schedule batch ingestions with the MSQ task engine by using the scheduled batch supervisor. You can specify the schedule using either the standard Unix cron syntax or Quartz cron syntax by setting the `type` field to either `unix` or `quartz`. Unix also supports macro expressions such as `@daily` and others.

Submit your supervisor spec to the `/druid/v2/sql/task/` endpoint.

The following example scheduled batch supervisor spec submits a REPLACE query every 5 minutes:

```json
{
    "type": "scheduled_batch",
    "schedulerConfig": {
        "type": "unix",
        "schedule": "*/5 * * * *"
    },
    "spec": {
        "query": "REPLACE INTO foo OVERWRITE ALL SELECT * FROM bar PARTITIONED BY DAY"
    },
    "suspended": false
}
```

[#17353](https://github.com/apache/druid/pull/17353)

### Improved S3 upload 

Druid can now use AWS S3 Transfer Manager for S3 uploads, which can significantly reduce segment upload time. This feature is on by default and controlled with the following configs in `common.runtime.properties`: 

```
    druid.storage.transfer.useTransferManager=true
    druid.storage.transfer.minimumUploadPartSize=20971520
    druid.storage.transfer.multipartUploadThreshold=20971520
```

[#17674](https://github.com/apache/druid/pull/17674)

## Functional area and related changes

This section contains detailed release notes separated by areas.

### Web console

#### MERGE INTO

The MERGE INTO keyword is now highlighted in the web console and the query gets treated as an insert query. 

[#17679](https://github.com/apache/druid/pull/17679)

#### Other web console improvements

- Added the ability to multi-select in table filters and added suggestions to the **Status** field for tasks and supervisors as well as service type [#17765](https://github.com/apache/druid/pull/17765)
- The Explore view now supports timezones [#17650](https://github.com/apache/druid/pull/17650)
- Data exported from the web console is now normalized to how Druid exports data. Additionally, you can now export results as Markdown tables [#17845](https://github.com/apache/druid/pull/17845)


### Ingestion

#### SQL-based ingestion

##### Other SQL-based ingestion improvements

#### Streaming ingestion

#### Query parameter for restarts

You can now use an optional query parameter called `skipRestartIfUnmodified` for the `/druid/indexer/v1/supervisor` endpoint. You can set `skipRestartIfUnmodified=true` to not restart the supervisor if the spec is unchanged.

For example:

```bash
curl -X POST --header "Content-Type: application/json" -d @supervisor.json localhost:8888/druid/indexer/v1/supervisor?skipRestartIfUnmodified=true
```

[#17707](https://github.com/apache/druid/pull/17707)

##### Other streaming ingestion improvements

- Improved the efficiency of streaming ingestion by fetching active tasks from memory. This reduces the number of calls to the metadata store for active datasource task payloads [#16098](https://github.com/apache/druid/pull/16098)

### Querying

#### Improved the query results API 

The query results API (`GET /druid/v2/sql/statements/{queryId}/results`) now supports an optional `filename` parameter. When provided, the response instructs web browsers to save the results as a file instead of showing them inline (via the `Content-Disposition` header).

[#17840](https://github.com/apache/druid/pull/17840)

#### GROUP BY and ORDER BY for nulls

SQL queries now support GROUP BY and ORDER BY for null types.

[#16252](https://github.com/apache/druid/pull/16252)

#### Other querying improvements

- Queries that include functions with a large number of arguments, such as CASE statements, now run faster [#17613](https://github.com/apache/druid/pull/17613)

### Cluster management

#### Controller task management

You can now control how many task slots are available for MSQ task engine controller tasks by using the following configs:

| Property   | Description    | Default value |
|-------|--------------|--------|
| `druid.indexer.queue.controllerTaskSlotRatio` | (Optional) The proportion of available task slots that can be allocated to MSQ task engine controller tasks. This is a floating-point value between 0 and 1                                                            | null          |
| `druid.indexer.queue.maxControllerTaskSlots`  | (Optional) The maximum number of task slots that can be allocated to controller tasks. This is an integer value that defines a hard limit on the number of task slots available for MSQ task engine controller tasks. | null         |

[#16889](https://github.com/apache/druid/pull/16889)

#### Other cluster management improvements

- Improved logging for permissions issues [#17754](https://github.com/apache/druid/pull/17754)
- Improved query distribution when `druid.broker.balancer.type` is set to `connectionCount` [#17764](https://github.com/apache/druid/pull/17764)

### Data management

#### Compaction supervisors (experimental)

You now configure compaction supervisors with the following Coordinator compaction config:

- `useSupervisors` - Enable compaction to run as a supervisor on the Overlord instead of as a Coordinator duty
- `engine` - Choose between `native` and `msq` to run compaction tasks. The `msq` setting uses the MSQ task engine and can be used only when `useSupervisors` is true.

Previously, you used runtime properties for the Overlord. Support for these has been removed.

[#17782](https://github.com/apache/druid/pull/17782)

#### Compaction APIs

You can use the following Overlord APIs to manage compaction:

|Method|Path|Description|Required Permission|
|--------|--------------------------------------------|------------|--------------------|
|GET|`/druid/indexer/v1/compaction/config/cluster`|Get the cluster-level compaction config|Read configs|
|POST|`/druid/indexer/v1/compaction/config/cluster`|Update the cluster-level compaction config|Write configs|
|GET|`/druid/indexer/v1/compaction/config/datasources`|Get the compaction configs for all datasources|Read datasource|
|GET|`/druid/indexer/v1/compaction/config/datasources/{dataSource}`|Get the compaction config of a single datasource|Read datasource|
|POST|`/druid/indexer/v1/compaction/config/datasources/{dataSource}`|Update the compaction config of a single datasource|Write datasource|
|GET|`/druid/indexer/v1/compaction/config/datasources/{dataSource}/history`|Get the compaction config history of a single datasource|Read datasource|
|GET|`/druid/indexer/v1/compaction/status/datasources`|Get the compaction status of all datasources|Read datasource|
|GET|`/druid/indexer/v1/compaction/status/datasources/{dataSource}`|Get the compaction status of a single datasource|Read datasource|

#### Faster segment metadata operations

Enable segment metadata caching on the Overlord with the runtime property `druid.manager.segments.useCache`. This feature is off by default.  

You can set the property to the following values: 

- `never`: Cache is disabled (default)
- `always`: Reads are always done from the cache. Service start-up will be blocked until the cache has synced with the metadata store at least once. Transactions are blocked until the cache has synced with the metadata store at least once after becoming leader. 
- `ifSynced`: Reads are done from the cache only if it has already synced with the metadata store. This mode does not block service start-up or transactions unlike the `always` setting.

As part of this change, additional metrics have been introduced. For more information about these metrics, see [Segment metadata cache metrics](#segment-metadata-cache-metrics).

[#17653](https://github.com/apache/druid/pull/17653) [#17824](https://github.com/apache/druid/pull/17824)

#### Automatic kill task interval

The Coordinator can optionally issue kill tasks for cleaning up unused segments. Starting with this release, individual kill tasks are limited to processing 30 days or fewer worth of segments per task by default. This improves performance of the individual kill tasks.

The previous behavior (no limit on interval per kill task) can be restored by setting `druid.coordinator.kill.maxInterval = P0D.`

[#17680](https://github.com/apache/druid/pull/17680)

#### Other data management improvements

- Metadata queries now return `maxIngestedEventTime`, which is the timestamp of the latest ingested event for the datasource. For realtime datasources, this may be later than `MAX(__time)` if `queryGranularity` is being used. For non-realtime datasources, this is equivalent to `MAX(__time)` [#17686](https://github.com/apache/druid/pull/17686)
- Metadata kill queries are now more efficient. They consider a maximum end time since the last segment was killed [#17770](https://github.com/apache/druid/pull/17770)
- Newly added segments are loaded more quickly [#17732](https://github.com/apache/druid/pull/17732)

### Metrics and monitoring

#### Custom Histogram buckets for Prometheus

You can now configure custom Histogram buckets for `timer` metrics from the Prometheus emitter using the `histogramBuckets` parameter. 

If no custom buckets are provided, the following default buckets are used: `[0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0, 30.0, 60.0, 120.0, 300.0]`. If the user does not specify their own JSON file, a default mapping is used.

[#17689](https://github.com/apache/druid/pull/17689)

#### Segment metadata cache metrics

The following metrics have been added:

The following metrics have been introduced as part of the segment metadata cache performance improvement.

- `segment/metadataCache/sync/time`
- `segment/metadataCache/transactions/readOnly`
- `segment/metadataCache/transactions/writeOnly`
- `segment/metadataCache/transactions/readWrite`

For more information about the segment metadata cache, see [Faster segment metadata operations](#faster-segment-metadata-operations).

[#17653](https://github.com/apache/druid/pull/17653)

#### Streaming ingestion lag metrics

The Kafka supervisor now includes additional lag metrics for how many minutes of data Druid is behind:

|Metric|Description|Default value|
|-|-|
|`ingest/kafka/updateOffsets/time`|Total time (in milliseconds) taken to fetch the latest offsets from Kafka stream and the ingestion tasks.|`dataSource`, `taskId`, `taskType`, `groupId`, `tags`|Generally a few seconds at most.|
|`ingest/kafka/lag/time`|Total lag time in milliseconds between the current message sequence number consumed by the Kafka indexing tasks and latest sequence number in Kafka across all shards. Minimum emission period for this metric is a minute. Enabled only when `pusblishLagTime` is set to true on supervisor config.|`dataSource`, `stream`, `tags`|Greater than 0, up to max kafka retention period in milliseconds. |
|`ingest/kafka/maxLag/time`|Max lag time in milliseconds between the current message sequence number consumed by the Kafka indexing tasks and latest sequence number in Kafka across all shards. Minimum emission period for this metric is a minute. Enabled only when `pusblishLagTime` is set to true on supervisor config.|`dataSource`, `stream`, `tags`|Greater than 0, up to max kafka retention period in milliseconds. |
|`ingest/kafka/avgLag/time`|Average lag time in milliseconds between the current message sequence number consumed by the Kafka indexing tasks and latest sequence number in Kafka across all shards. Minimum emission period for this metric is a minute. Enabled only when `pusblishLagTime` is set to true on supervisor config.|`dataSource`, `stream`, `tags`|Greater than 0, up to max kafka retention period in milliseconds. |
|`ingest/kinesis/updateOffsets/time`|Total time (in milliseconds) taken to fetch the latest offsets from Kafka stream and the ingestion tasks.|`dataSource`, `taskId`, `taskType`, `groupId`, `tags`|Generally a few seconds at most.|

[#17735](https://github.com/apache/druid/pull/17735)

#### Other metrics and monitoring changes

- Added the `ingest/processed/bytes` metric that tracks the total number of bytes processed during ingestion tasks for JSON-based batch, SQL-based batch, and streaming ingestion tasks [#17581](https://github.com/apache/druid/pull/17581)

### Extensions

#### Kubernetes 

- You can now ingest payloads larger than 128 KiB when using HDFS as deep storage for Middle Manager-less ingestion [#17742](https://github.com/apache/druid/pull/17742)
- You can now run task pods in a namespace different from the rest of the cluster [#17738](https://github.com/apache/druid/pull/17738)
- You can now name your K8s job names using `druid.indexer.runner.k8sTaskPodNamePrefix` [#17749](https://github.com/apache/druid/pull/17749)
- The logging level is now set to info. Previously, it was set to debug [#17752](https://github.com/apache/druid/pull/17752)
- Druid now supports lazy loading of pod templates so that any config changes you make are deployed more quickly [#17701](https://github.com/apache/druid/pull/17701)
- Removed startup probe so that peon tasks can start up properly without being killed by Kubernetes [#17784](https://github.com/apache/druid/pull/17784)

### Documentation improvements

## Upgrade notes and incompatible changes

### Upgrade notes

#### `useMaxMemoryEstimates`

`useMaxMemoryEstimates` is now set to false for MSQ task engine tasks. Additionally, the property has been deprecated and will be removed in a future release. Setting this to false allows for better on-heap memory estimation.

[#17792](https://github.com/apache/druid/pull/17792)

#### Automatic kill tasks interval

Automatic kill tasks are now limited to 30 days or fewer worth of segments per task.

The previous behavior (no limit on interval per kill task) can be restored by setting `druid.coordinator.kill.maxInterval = P0D`.

[#17680](https://github.com/apache/druid/pull/17680)

#### Kubernetes deployments

By default, the Docker image now uses the canonical hostname to register services in ZooKeeper for internal communication if you're running Druid in Kubernetes. Otherwise, it uses the IP address. [#17697](https://github.com/apache/druid/pull/17697). 

You can set the environment variable `DRUID_SET_HOST_IP` to `1` to restore old behavior.

#### Updated configs

Various configs were deprecated in a previous release and have now been removed. The following table lists the removed configs and their replacements:

| Removed config | Replacement config|
|-|-|
|`druid.processing.merge.task.initialYieldNumRows `|`druid.processing.merge.initialYieldNumRows`|
|`druid.processing.merge.task.targetRunTimeMillis`|`druid.processing.merge.targetRunTimeMillis`|
|`druid.processing.merge.task.smallBatchNumRows`|`druid.processing.merge.smallBatchNumRows`|
|`druid.processing.merge.pool.awaitShutdownMillis`|
|`druid.processing.merge.awaitShutdownMillis`|
|`druid.processing.merge.pool.parallelism`|`druid.processing.merge.parallelism`|
|`druid.processing.merge.pool.defaultMaxQueryParallelism`|`druid.processing.merge.defaultMaxQueryParallelism`|

[#17776](https://github.com/apache/druid/pull/17776)

#### Segment metadata cache configs

If you need to downgrade to a version where Druid doesn't support the segment metadata cache, you must set the `druid.manager.segments.useCache` config to false or remove it prior to the upgrade.

This feature is introduced in Druid 33.0.

[#17653](https://github.com/apache/druid/pull/17653)

### Developer notes

#### Dependency updates

- Added `GraalJS` to enable Javascript-backed selector strategies [#17843](https://github.com/apache/druid/pull/17843)


The following dependencies have had their versions bumped:

- `netty4` to version 4.1.118 
- `async-http-client` to version 3.0.1 [#17646](https://github.com/apache/druid/pull/17646)
- `jetty` to version 9.4.57.v20241219 [#17879](https://github.com/apache/druid/pull/17879)
- `parquet-avro` to version 1.15.1 [#17874](https://github.com/apache/druid/pull/17874)