---
layout: doc_page
title: "Basic Cluster Tuning"
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

# Basic Cluster Tuning

This document provides basic guidelines for configuration properties and cluster architecture considerations related to performance tuning of an Apache Druid (incubating) deployment. 

Please note that this document provides general guidelines and rules-of-thumb: these are not absolute, universal rules for cluster tuning, and this introductory guide is not an exhaustive description of all Druid tuning properties, which are described in the [configuration reference](../configuration/index.html).

If you have questions on tuning Druid for specific use cases, or questions on configuration properties not covered in this guide, please ask the [Druid user mailing list or other community channels](https://druid.apache.org/community/).

## Process-specific guidelines

### Historical

#### Heap sizing

The biggest contributions to heap usage on Historicals are:

- Partial unmerged query results from segments
- The stored maps for [lookups](../querying/lookups.html).

A general rule-of-thumb for sizing the Historical heap is `(0.5GB * number of CPU cores)`, with an upper limit of ~24GB.

This rule-of-thumb scales using the number of CPU cores as a convenient proxy for hardware size and level of concurrency (note: this formula is not a hard rule for sizing Historical heaps).

Having a heap that is too large can result in excessively long GC collection pauses, the ~24GB upper limit is imposed to avoid this.

If caching is enabled on Historicals, the cache is stored on heap, sized by `druid.cache.sizeInBytes`.

Running out of heap on the Historicals can indicate misconfiguration or usage patterns that are overloading the cluster.

##### Lookups

If you are using lookups, calculate the total size of the lookup maps being loaded. 

Druid performs an atomic swap when updating lookup maps (both the old map and the new map will exist in heap during the swap), so the maximum potential heap usage from lookup maps will be (2 * total size of all loaded lookups).

Be sure to add `(2 * total size of all loaded lookups)` to your heap size in addition to the `(0.5GB * number of CPU cores)` guideline.

#### Processing Threads and Buffers

Please see the [General Guidelines for Processing Threads and Buffers](#general-guidelines-for-processing-threads-and-buffers) section for an overview of processing thread/buffer configuration.

On Historicals:

- `druid.processing.numThreads` should generally be set to `(number of cores - 1)`: a smaller value can result in CPU underutilization, while going over the number of cores can result in unnecessary CPU contention.
- `druid.processing.buffer.sizeBytes` can be set to 500MB.
- `druid.processing.numMergeBuffers`, a 1:4 ratio of  merge buffers to processing threads is a reasonable choice for general use.

#### Direct Memory Sizing

The processing and merge buffers described above are direct memory buffers.

When a historical processes a query, it must open a set of segments for reading. This also requires some direct memory space, described in [segment decompression buffers](#segment-decompression).

A formula for estimating direct memory usage follows:

(`druid.processing.numThreads` + `druid.processing.numMergeBuffers` + 1) * `druid.processing.buffer.sizeBytes`

The `+ 1` factor is a fuzzy estimate meant to account for the segment decompression buffers.

#### Connection Pool Sizing

Please see the [General Connection Pool Guidelines](#general-connection-pool-guidelines) section for an overview of connection pool configuration.

For Historicals, `druid.server.http.numThreads` should be set to a value slightly higher than the sum of `druid.broker.http.numConnections` across all the Brokers in the cluster.

Tuning the cluster so that each Historical can accept 50 queries and 10 non-queries is a reasonable starting point.

#### Segment Cache Size

`druid.server.maxSize` controls the total size of segment data that can be assigned by the Coordinator to a Historical.

`druid.segmentCache.locations` specifies locations where segment data can be stored on the Historical. The sum of available disk space across these locations should equal `druid.server.maxSize`.

Segments are memory-mapped by Historical processes using any available free system memory (i.e., memory not used by the Historical JVM and heap/direct memory buffers or other processes on the system). Segments that are not currently in memory will be paged from disk when queried.

Therefore, `druid.server.maxSize` should be set such that a Historical is not allocated an excessive amount of segment data. As the value of (`free system memory` / `druid.server.maxSize`) increases, a greater proportion of segments can be kept in memory, allowing for better query performance.

#### Number of Historicals

The number of Historicals needed in a cluster depends on how much data the cluster has. For good performance, you will want enough Historicals such that each Historical has a good (`free system memory` / `druid.server.maxSize`) ratio, as described in the segment cache size section above.

Having a smaller number of big servers is generally better than having a large number of small servers, as long as you have enough fault tolerance for your use case.

#### SSD storage

We recommend using SSDs for storage on the Historicals, as they handle segment data stored on disk.

#### Total Memory Usage

To estimate total memory usage of the Historical under these guidelines:

- Heap: `(0.5GB * number of CPU cores) + (2 * total size of lookup maps) + druid.cache.sizeInBytes`
- Direct Memory: `(druid.processing.numThreads + druid.processing.numMergeBuffers + 1) * druid.processing.buffer.sizeBytes`

The Historical will use any available free system memory (i.e., memory not used by the Historical JVM and heap/direct memory buffers or other processes on the system) for memory-mapping of segments on disk. For better query performance, you will want to ensure a good (`free system memory` / `druid.server.maxSize`) ratio so that a greater proportion of segments can be kept in memory.

### Broker

#### Heap Sizing

The biggest contributions to heap usage on Brokers are:
- Partial unmerged query results from Historicals and Tasks
- The segment timeline: this consists of location information (which Historical/Task is serving a segment) for all currently [available](../ingestion/index.html#segment-states) segments.
- Cached segment metadata: this consists of metadata, such as per-segment schemas, for all currently available segments.

The Broker heap requirements scale based on the number of segments in the cluster, and the total data size of the segments. 

The heap size will vary based on data size and usage patterns, but 4G to 8G is a good starting point for a small or medium cluster (~15 servers or less). For a rough estimate of memory requirements on the high end, very large clusters with a node count on the order of ~100 nodes may need Broker heaps of 30GB-60GB.

If caching is enabled on the Broker, the cache is stored on heap, sized by `druid.cache.sizeInBytes`.

#### Direct Memory Sizing

On the Broker, the amount of direct memory needed depends on how many merge buffers (used for merging GroupBys) are configured. The Broker does not generally need processing threads or processing buffers, as query results are merged on-heap in the HTTP connection threads instead.

- `druid.processing.buffer.sizeBytes` can be set to 500MB.
- `druid.processing.numThreads`: set this to 1 (the minimum allowed)
- `druid.processing.numMergeBuffers`: set this to the same value as on Historicals or a bit higher

##### Note on the deprecated `chunkPeriod`

There is one exception to the Broker not needing processing threads and processing buffers:

If the deprecated `chunkPeriod` property in the [query context](../querying/query-context.html) is set, GroupBy V1 queries will use processing threads and processing buffers on the Broker.

Both `chunkPeriod` and GroupBy V1 are deprecated (use GroupBy V2 instead) and will be removed in the future, we do not recommend using them. The presence of the deprecated `chunkPeriod` feature is why a minimum of 1 processing thread must be configured, even if it's unused.

#### Connection Pool Sizing

Please see the [General Connection Pool Guidelines](#general-connection-pool-guidelines) section for an overview of connection pool configuration.

On the Brokers, please ensure that the sum of `druid.broker.http.numConnections` across all the Brokers is slightly lower than the value of `druid.server.http.numThreads` on your Historicals and Tasks.

`druid.server.http.numThreads` on the Broker should be set to a value slightly higher than `druid.broker.http.numConnections` on the same Broker.

Tuning the cluster so that each Historical can accept 50 queries and 10 non-queries, adjusting the Brokers accordingly, is a reasonable starting point.

#### Broker Backpressure

When retrieving query results from Historical processes or Tasks, the Broker can optionally specify a maximum buffer size for queued, unread data, and exert backpressure on the channel to the Historical or Tasks when limit is reached (causing writes to the channel to block on the Historical/Task side until the Broker is able to drain some data from the channel).

This buffer size is controlled by the `druid.broker.http.maxQueuedBytes` setting.

The limit is divided across the number of Historicals/Tasks that a query would hit: suppose I have `druid.broker.http.maxQueuedBytes` set to 5MB, and the Broker receives a query that needs to be fanned out to 2 Historicals. Each per-historical channel would get a 2.5MB buffer in this case.

You can generally set this to a value of approximately `2MB * number of Historicals`. As your cluster scales up with more Historicals and Tasks, consider increasing this buffer size and increasing the Broker heap accordingly.

- If the buffer is too small, this can lead to inefficient queries due to the buffer filling up rapidly and stalling the channel
- If the buffer is too large, this puts more memory pressure on the Broker due to more queued result data in the HTTP channels.

#### Number of Brokers

A 1:15 ratio of Brokers to Historicals is a reasonable starting point (this is not a hard rule).

If you need Broker HA, you can deploy 2 initially and then use the 1:15 ratio guideline for additional Brokers.

#### Total Memory Usage

To estimate total memory usage of the Broker under these guidelines:

- Heap: allocated heap size
- Direct Memory: `(druid.processing.numThreads + druid.processing.numMergeBuffers + 1) * druid.processing.buffer.sizeBytes`

### MiddleManager

The MiddleManager is a lightweight task controller/manager that launches Task processes, which perform ingestion work.

#### MiddleManager Heap Sizing

The MiddleManager itself does not require much resources, you can set the heap to ~128MB generally.

#### SSD storage

We recommend using SSDs for storage on the MiddleManagers, as the Tasks launched by MiddleManagers handle segment data stored on disk.

#### Task Count

The number of tasks a MiddleManager can launch is controlled by the `druid.worker.capacity` setting. 

The number of workers needed in your cluster depends on how many concurrent ingestion tasks you need to run for your use cases. The number of workers that can be launched on a given machine depends on the size of resources allocated per worker and available system resources.

You can allocate more MiddleManager machines to your cluster to add task capacity.

#### Task Configurations

The following section below describes configuration for Tasks launched by the MiddleManager. The Tasks can be queried and perform ingestion workloads, so they require more resources than the MM.

##### Task Heap Sizing

A 1GB heap is usually enough for Tasks.

###### Lookups

If you are using lookups, calculate the total size of the lookup maps being loaded. 

Druid performs an atomic swap when updating lookup maps (both the old map and the new map will exist in heap during the swap), so the maximum potential heap usage from lookup maps will be (2 * total size of all loaded lookups).

Be sure to add `(2 * total size of all loaded lookups)` to your Task heap size if you are using lookups.

##### Task processing threads and buffers

For Tasks, 1 or 2 processing threads are often enough, as the Tasks tend to hold much less queryable data than Historical processes.

- `druid.indexer.fork.property.druid.processing.numThreads`: set this to 1 or 2
- `druid.indexer.fork.property.druid.processing.numMergeBuffers`: set this to 2
- `druid.indexer.fork.property.druid.processing.buffer.sizeBytes`: can be set to 100MB

##### Direct Memory Sizing

The processing and merge buffers described above are direct memory buffers.

When a Task processes a query, it must open a set of segments for reading. This also requires some direct memory space, described in [segment decompression buffers](#segment-decompression).

An ingestion Task also needs to merge partial ingestion results, which requires direct memory space, described in [segment merging](#segment-merging).

A formula for estimating direct memory usage follows:

(`druid.processing.numThreads` + `druid.processing.numMergeBuffers` + 1) * `druid.processing.buffer.sizeBytes`

The `+ 1` factor is a fuzzy estimate meant to account for the segment decompression buffers and dictionary merging buffers.

##### Connection Pool Sizing

Please see the [General Connection Pool Guidelines](#general-connection-pool-guidelines) section for an overview of connection pool configuration.

For Tasks, `druid.server.http.numThreads` should be set to a value slightly higher than the sum of `druid.broker.http.numConnections` across all the Brokers in the cluster.

Tuning the cluster so that each Task can accept 50 queries and 10 non-queries is a reasonable starting point.

#### Total Memory Usage

To estimate total memory usage of a Task under these guidelines:

- Heap: `1GB + (2 * total size of lookup maps)`
- Direct Memory: `(druid.processing.numThreads + druid.processing.numMergeBuffers + 1) * druid.processing.buffer.sizeBytes`

The total memory usage of the MiddleManager + Tasks:

`MM heap size + druid.worker.capacity * (single task memory usage)`

##### Configuration Guidelines for Specific Ingestion Types

###### Kafka/Kinesis Ingestion

If you use the [Kafka Indexing Service](../development/extensions-core/kafka-ingestion.html) or [Kinesis Indexing Service](../development/extensions-core/kinesis-ingestion.html), the number of tasks required will depend on the number of partitions and your taskCount/replica settings.

On top of those requirements, allocating more task slots in your cluster is a good idea, so that you have free task slots available for [Compaction Tasks](../ingestion/compaction.html).

###### Hadoop Ingestion

If you are only using [Hadoop Batch Ingestion](../ingestion/hadoop.html) with no other ingestion types, you can lower the amount of resources allocated per Task. Batch ingestion tasks do not need to answer queries, and the bulk of the ingestion workload will be executed on the Hadoop cluster, so the Tasks do not require much resources.

###### Parallel Native Ingestion

If you are using [Parallel Native Ingestion](../ingestion/native_tasks.html), allocating more available task slots is a good idea and will allow greater ingestion concurrency.

## Coordinator

The main performance-related setting on the Coordinator is the heap size.

The heap requirements of the Coordinator scale with the number of servers, segments, and tasks in the cluster.

You can set the Coordinator heap to the same size as your Broker heap, or slightly smaller: both services have to process cluster-wide state and answer API requests about this state.

## Overlord

The main performance-related setting on the Overlord is the heap size.

The heap requirements of the Overlord scale primarily with the number of running Tasks.

The Overlord tends to require less resources than the Coordinator or Broker. You can generally set the Overlord heap to a value that's 25-50% of your Coordinator heap.

## Router

The Router has light resource requirements, as it proxies requests to Brokers without performing much computational work itself.

You can assign it 256MB heap as a starting point, growing it if needed.

# General Guidelines for Processing Threads and Buffers 

## Processing Threads

The `druid.processing.numThreads` configuration controls the size of the processing thread pool used for computing query results. The size of this pool limits how many queries can be concurrently processed.

## Processing Buffers

`druid.processing.buffer.sizeBytes` is a closely related property that controls the size of the off-heap buffers allocated to the processing threads. 

One buffer is allocated for each processing thread. A size between 500MB and 1GB is a reasonable choice for general use.

The TopN and GroupBy queries use these buffers to store intermediate computed results. As the buffer size increases, more data can be processed in a single pass.

## GroupBy Merging Buffers

If you plan to issue GroupBy V2 queries, `druid.processing.numMergeBuffers` is an important configuration property. 

GroupBy V2 queries use an additional pool of off-heap buffers for merging query results. These buffers have the same size as the processing buffers described above, set by the `druid.processing.buffer.sizeBytes` property.

Non-nested GroupBy V2 queries require 1 merge buffer per query, while a nested GroupBy V2 query requires 2 merge buffers (regardless of the depth of nesting). 

The number of merge buffers determines the number of GroupBy V2 queries that can be processed concurrently.

# General Connection Pool Guidelines

Each Druid process has a configuration property for the number of HTTP connection handling threads, `druid.server.http.numThreads`.

The number of HTTP server threads limits how many concurrent HTTP API requests a given process can handle. 

## Sizing the connection pool for queries

The Broker has a setting `druid.broker.http.numConnections` that controls how many outgoing connections it can make to a given Historical or Task process.

These connections are used to send queries to the Historicals or Tasks, with one connection per query; the value of `druid.broker.http.numConnections` is effectively a limit on the number of concurrent queries that a given broker can process.

Suppose we have a cluster with 3 Brokers and `druid.broker.http.numConnections` is set to 10.

This means that each Broker in the cluster will open up to 10 connections to each individual Historical or Task (for a total of 30 incoming query connections per Historical/Task).

On the Historical/Task side, this means that `druid.server.http.numThreads` must be set to a value at least as high as the sum of `druid.broker.http.numConnections` across all the Brokers in the cluster. 

In practice, you will want to allocate additional server threads for non-query API requests such as status checks; adding 10 threads for those is a good general guideline. Using the example with 3 Brokers in the cluster and `druid.broker.http.numConnections` set to 10, a value of 40 would be appropriate for `druid.server.http.numThreads` on Historicals and Tasks.

As a starting point, allowing for 50 concurrent queries (requests that read segment data from datasources) + 10 non-query requests (other requests like status checks) on Historicals and Tasks is reasonable (i.e., set `druid.server.http.numThreads` to 60 there), while sizing `druid.broker.http.numConnections` based on the number of Brokers in the cluster to fit within the 50 query connection limit per Historical/Task.

- If the connection pool across Brokers and Historicals/Tasks is too small, the cluster will be underutilized as there are too few concurrent query slots.
- If the connection pool is too large, you may get out-of-memory errors due to excessive concurrent load, and increased resource contention.
- The connection pool sizing matters most when you require QoS-type guarantees and use query priorities; otherwise, these settings can be more loosely configured.
- If your cluster usage patterns are heavily biased towards a high number of small concurrent queries (where each query takes less than ~15ms), enlarging the connection pool can be a good idea.
- The 50/10 general guideline here is a rough starting point, since different queries impose different amounts of load on the system. To size the connection pool more exactly for your cluster, you would need to know the execution times for your queries and ensure that the rate of incoming queries does not exceed your "drain" rate.

# Garbage Collection Settings

We recommend using the G1GC garbage collector:

`-XX:+UseG1GC`

Enabling process termination on out-of-memory errors is useful as well, since the process generally will not recover from such a state, and it's better to restart the process:

`-XX:+ExitOnOutOfMemoryError`

# Per-Segment Direct Memory Buffers

## Segment Decompression

When opening a segment for reading during segment merging or query processing, Druid allocates a 64KB off-heap decompression buffer for each column being read.

Thus, there is additional direct memory overhead of (64KB * number of columns read per segment * number of segments read) when reading segments.

## Segment Merging

In addition to the segment decompression overhead described above, when a set of segments are merged during ingestion, a direct buffer is allocated for every String typed column, for every segment in the set to be merged. 

The size of these buffers are equal to the cardinality of the String column within its segment, times 4 bytes (the buffers store integers).
 
For example, if two segments are being merged, the first segment having a single String column with cardinality 1000, and the second segment having a String column with cardinality 500, the merge step would allocate (1000 + 500) * 4 = 6000 bytes of direct memory. 
 
These buffers are used for merging the value dictionaries of the String column across segments. These "dictionary merging buffers" are independent of the "merge buffers" configured by `druid.processing.numMergeBuffers`.



