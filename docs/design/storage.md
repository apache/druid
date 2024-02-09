---
id: storage
title: "Storage overview"
sidebar_label: "Storage"
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


Druid stores data in datasources, which are similar to tables in a traditional RDBMS. Each datasource is partitioned by time and, optionally, further partitioned by other attributes. Each time range is called a chunk (for example, a single day, if your datasource is partitioned by day). Within a chunk, data is partitioned into one or more [segments](../design/segments.md). Each segment is a single file, typically comprising up to a few million rows of data. Since segments are organized into time chunks, it's sometimes helpful to think of segments as living on a timeline like the following:

![Segment timeline](../assets/druid-timeline.png)

A datasource may have anywhere from just a few segments, up to hundreds of thousands and even millions of segments. Each segment is created by a MiddleManager as mutable and uncommitted. Data is queryable as soon as it is added to an uncommitted segment. The segment building process accelerates later queries by producing a data file that is compact and indexed:

- Conversion to columnar format
- Indexing with bitmap indexes
- Compression
    - Dictionary encoding with id storage minimization for String columns
    - Bitmap compression for bitmap indexes
    - Type-aware compression for all columns

Periodically, segments are committed and published to [deep storage](deep-storage.md), become immutable, and move from MiddleManagers to the Historical services. An entry about the segment is also written to the [metadata store](metadata-storage.md). This entry is a self-describing bit of metadata about the segment, including things like the schema of the segment, its size, and its location on deep storage. These entries tell the Coordinator what data is available on the cluster.

For details on the segment file format, see [segment files](segments.md).

For details on modeling your data in Druid, see [schema design](../ingestion/schema-design.md).

## Indexing and handoff

Indexing is the mechanism by which new segments are created, and handoff is the mechanism by which they are published and served by Historical services. 

On the indexing side:

1. An indexing task starts running and building a new segment. It must determine the identifier of the segment before it starts building it. For a task that is appending (like a Kafka task, or an index task in append mode) this is done by calling an "allocate" API on the Overlord to potentially add a new partition to an existing set of segments. For
a task that is overwriting (like a Hadoop task, or an index task not in append mode) this is done by locking an interval and creating a new version number and new set of segments.
2. If the indexing task is a realtime task (like a Kafka task) then the segment is immediately queryable at this point. It's available, but unpublished.
3. When the indexing task has finished reading data for the segment, it pushes it to deep storage and then publishes it by writing a record into the metadata store.
4. If the indexing task is a realtime task, then to ensure data is continuously available for queries, it waits for a Historical service to load the segment. If the indexing task is not a realtime task, it exits immediately.

On the Coordinator / Historical side:

1. The Coordinator polls the metadata store periodically (by default, every 1 minute) for newly published segments.
2. When the Coordinator finds a segment that is published and used, but unavailable, it chooses a Historical service to load that segment and instructs that Historical to do so.
3. The Historical loads the segment and begins serving it.
4. At this point, if the indexing task was waiting for handoff, it will exit.

## Segment identifiers

Segments all have a four-part identifier with the following components:

- Datasource name.
- Time interval (for the time chunk containing the segment; this corresponds to the `segmentGranularity` specified at ingestion time).
- Version number (generally an ISO8601 timestamp corresponding to when the segment set was first started).
- Partition number (an integer, unique within a datasource+interval+version; may not necessarily be contiguous).

For example, this is the identifier for a segment in datasource `clarity-cloud0`, time chunk
`2018-05-21T16:00:00.000Z/2018-05-21T17:00:00.000Z`, version `2018-05-21T15:56:09.909Z`, and partition number 1:

```
clarity-cloud0_2018-05-21T16:00:00.000Z_2018-05-21T17:00:00.000Z_2018-05-21T15:56:09.909Z_1
```

Segments with partition number 0 (the first partition in a chunk) omit the partition number, like the following example, which is a segment in the same time chunk as the previous one, but with partition number 0 instead of 1:

```
clarity-cloud0_2018-05-21T16:00:00.000Z_2018-05-21T17:00:00.000Z_2018-05-21T15:56:09.909Z
```

## Segment versioning

The version number provides a form of [multi-version concurrency control](https://en.wikipedia.org/wiki/Multiversion_concurrency_control) (MVCC) to support batch-mode overwriting. If all you ever do is append data, then there will be just a single version for each time chunk. But when you overwrite data, Druid will seamlessly switch from querying the old version to instead query the new, updated versions. Specifically, a new set of segments is created with the same datasource, same time interval, but a higher version number. This is a signal to the rest of the Druid system that the older version should be removed from the cluster, and the new version should replace it.

The switch appears to happen instantaneously to a user, because Druid handles this by first loading the new data (but not allowing it to be queried), and then, as soon as the new data is all loaded, switching all new queries to use those new segments. Then it drops the old segments a few minutes later.

## Segment lifecycle

Each segment has a lifecycle that involves the following three major areas:

1. **Metadata store:** Segment metadata (a small JSON payload generally no more than a few KB) is stored in the [metadata store](metadata-storage.md) once a segment is done being constructed. The act of inserting a record for a segment into the metadata store is called publishing. These metadata records have a boolean flag named `used`, which controls whether the segment is intended to be queryable or not. Segments created by realtime tasks will be
available before they are published, since they are only published when the segment is complete and will not accept any additional rows of data.
2. **Deep storage:** Segment data files are pushed to deep storage once a segment is done being constructed. This happens immediately before publishing metadata to the metadata store.
3. **Availability for querying:** Segments are available for querying on some Druid data server, like a realtime task, directly from deep storage, or a Historical service.

You can inspect the state of currently active segments using the Druid SQL
[`sys.segments` table](../querying/sql-metadata-tables.md#segments-table). It includes the following flags:

- `is_published`: True if segment metadata has been published to the metadata store and `used` is true.
- `is_available`: True if the segment is currently available for querying, either on a realtime task or Historical service.
- `is_realtime`: True if the segment is only available on realtime tasks. For datasources that use realtime ingestion, this will generally start off `true` and then become `false` as the segment is published and handed off.
- `is_overshadowed`: True if the segment is published (with `used` set to true) and is fully overshadowed by some other published segments. Generally this is a transient state, and segments in this state will soon have their `used` flag automatically set to false.

## Availability and consistency

Druid has an architectural separation between ingestion and querying, as described above in
[Indexing and handoff](#indexing-and-handoff). This means that when understanding Druid's availability and consistency properties, we must look at each function separately.

On the ingestion side, Druid's primary [ingestion methods](../ingestion/index.md#ingestion-methods) are all pull-based and offer transactional guarantees. This means that you are guaranteed that ingestion using these methods will publish in an all-or-nothing manner:

- Supervised "seekable-stream" ingestion methods like [Kafka](../ingestion/kafka-ingestion.md) and [Kinesis](../ingestion/kinesis-ingestion.md). With these methods, Druid commits stream offsets to its [metadata store](metadata-storage.md) alongside segment metadata, in the same transaction. Note that ingestion of data that has not yet been published can be rolled back if ingestion tasks fail. In this case, partially-ingested data is
discarded, and Druid will resume ingestion from the last committed set of stream offsets. This ensures exactly-once publishing behavior.
- [Hadoop-based batch ingestion](../ingestion/hadoop.md). Each task publishes all segment metadata in a single transaction.
- [Native batch ingestion](../ingestion/native-batch.md). In parallel mode, the supervisor task publishes all segment metadata in a single transaction after the subtasks are finished. In simple (single-task) mode, the single task publishes all segment metadata in a single transaction after it is complete.

Additionally, some ingestion methods offer an _idempotency_ guarantee. This means that repeated executions of the same ingestion will not cause duplicate data to be ingested:

- Supervised "seekable-stream" ingestion methods like [Kafka](../ingestion/kafka-ingestion.md) and [Kinesis](../ingestion/kinesis-ingestion.md) are idempotent due to the fact that stream offsets and segment metadata are stored together and updated in lock-step.
- [Hadoop-based batch ingestion](../ingestion/hadoop.md) is idempotent unless one of your input sources is the same Druid datasource that you are ingesting into. In this case, running the same task twice is non-idempotent, because you are adding to existing data instead of overwriting it.
- [Native batch ingestion](../ingestion/native-batch.md) is idempotent unless
[`appendToExisting`](../ingestion/native-batch.md) is true, or one of your input sources is the same Druid datasource that you are ingesting into. In either of these two cases, running the same task twice is non-idempotent, because you are adding to existing data instead of overwriting it.

On the query side, the Druid Broker is responsible for ensuring that a consistent set of segments is involved in a given query. It selects the appropriate set of segment versions to use when the query starts based on what is currently available. This is supported by atomic replacement, a feature that ensures that from a user's perspective, queries flip instantaneously from an older version of data to a newer set of data, with no consistency or performance impact.
This is used for Hadoop-based batch ingestion, native batch ingestion when `appendToExisting` is false, and compaction.

Note that atomic replacement happens for each time chunk individually. If a batch ingestion task or compaction involves multiple time chunks, then each time chunk will undergo atomic replacement soon after the task finishes, but the replacements will not all happen simultaneously.

Typically, atomic replacement in Druid is based on a core set concept that works in conjunction with segment versions.
When a time chunk is overwritten, a new core set of segments is created with a higher version number. The core set must all be available before the Broker will use them instead of the older set. There can also only be one core set per version per time chunk. Druid will also only use a single version at a time per time chunk. Together, these properties provide Druid's atomic replacement guarantees.

Druid also supports an experimental segment locking mode that is activated by setting
[`forceTimeChunkLock`](../ingestion/tasks.md#context) to false in the context of an ingestion task. In this case, Druid creates an atomic update group using the existing version for the time chunk, instead of creating a new core set with a new version number. There can be multiple atomic update groups with the same version number per time chunk. Each one replaces a specific set of earlier segments in the same time chunk and with the same version number. Druid will query the latest one that is fully available. This is a more powerful version of the core set concept, because it enables atomically replacing a subset of data for a time chunk, as well as doing atomic replacement and appending simultaneously.

If segments become unavailable due to multiple Historicals going offline simultaneously (beyond your replication factor), then Druid queries will include only the segments that are still available. In the background, Druid will reload these unavailable segments on other Historicals as quickly as possible, at which point they will be included in queries again.
