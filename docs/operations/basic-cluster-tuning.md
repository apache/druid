---
id: basic-cluster-tuning
title: "Basic cluster tuning"
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


This document provides basic guidelines for configuration properties and cluster architecture considerations related to performance tuning of an Apache Druid deployment.

Please note that this document provides general guidelines and rules-of-thumb: these are not absolute, universal rules for cluster tuning, and this introductory guide is not an exhaustive description of all Druid tuning properties, which are described in the [configuration reference](../configuration/index.md).

If you have questions on tuning Druid for specific use cases, or questions on configuration properties not covered in this guide, please ask the [Druid user mailing list or other community channels](https://druid.apache.org/community/).

## Process-specific guidelines

### Historical

#### Heap sizing

The biggest contributions to heap usage on Historicals are:

- Partial unmerged query results from segments
- The stored maps for [lookups](../querying/lookups.md).

A general rule-of-thumb for sizing the Historical heap is `(0.5GiB * number of CPU cores)`, with an upper limit of ~24GiB.

This rule-of-thumb scales using the number of CPU cores as a convenient proxy for hardware size and level of concurrency (note: this formula is not a hard rule for sizing Historical heaps).

Having a heap that is too large can result in excessively long GC collection pauses, the ~24GiB upper limit is imposed to avoid this.

If caching is enabled on Historicals, the cache is stored on heap, sized by `druid.cache.sizeInBytes`.

Running out of heap on the Historicals can indicate misconfiguration or usage patterns that are overloading the cluster.

##### Lookups

If you are using lookups, calculate the total size of the lookup maps being loaded.

Druid performs an atomic swap when updating lookup maps (both the old map and the new map will exist in heap during the swap), so the maximum potential heap usage from lookup maps will be (2 * total size of all loaded lookups).

Be sure to add `(2 * total size of all loaded lookups)` to your heap size in addition to the `(0.5GiB * number of CPU cores)` guideline.

#### Processing Threads and Buffers

Please see the [General Guidelines for Processing Threads and Buffers](#processing-threads-buffers) section for an overview of processing thread/buffer configuration.

On Historicals:

- `druid.processing.numThreads` should generally be set to `(number of cores - 1)`: a smaller value can result in CPU underutilization, while going over the number of cores can result in unnecessary CPU contention.
- `druid.processing.buffer.sizeBytes` can be set to 500MiB.
- `druid.processing.numMergeBuffers`, a 1:4 ratio of  merge buffers to processing threads is a reasonable choice for general use.

#### Direct Memory Sizing

The processing and merge buffers described above are direct memory buffers.

When a historical processes a query, it must open a set of segments for reading. This also requires some direct memory space, described in [segment decompression buffers](#segment-decompression).

A formula for estimating direct memory usage follows:

(`druid.processing.numThreads` + `druid.processing.numMergeBuffers` + 1) * `druid.processing.buffer.sizeBytes`

The `+ 1` factor is a fuzzy estimate meant to account for the segment decompression buffers.

#### Connection pool sizing

Please see the [General Connection Pool Guidelines](#connection-pool) section for an overview of connection pool configuration.

For Historicals, `druid.server.http.numThreads` should be set to a value slightly higher than the sum of `druid.broker.http.numConnections` across all the Brokers in the cluster.

Tuning the cluster so that each Historical can accept 50 queries and 10 non-queries is a reasonable starting point.

#### Segment Cache Size

For better query performance, do not allocate segment data to a Historical in excess of the system free memory.  When `free system memory` is greater than or equal to `druid.segmentCache.locations`, the more segment data the Historical can be held in the memory-mapped segment cache.

Druid uses the `druid.segmentCache.locations` to calculate the total segment data size assigned to a Historical. For some rarer use cases, you can override this behavior with `druid.server.maxSize` property.

#### Number of Historicals

The number of Historicals needed in a cluster depends on how much data the cluster has. For good performance, you will want enough Historicals such that each Historical has a good (`free system memory` / total size of all `druid.segmentCache.locations`) ratio, as described in the segment cache size section above.

Having a smaller number of big servers is generally better than having a large number of small servers, as long as you have enough fault tolerance for your use case.

#### SSD storage

We recommend using SSDs for storage on the Historicals, as they handle segment data stored on disk.

#### Total memory usage

To estimate total memory usage of the Historical under these guidelines:

- Heap: `(0.5GiB * number of CPU cores) + (2 * total size of lookup maps) + druid.cache.sizeInBytes`
- Direct Memory: `(druid.processing.numThreads + druid.processing.numMergeBuffers + 1) * druid.processing.buffer.sizeBytes`

The Historical will use any available free system memory (i.e., memory not used by the Historical JVM and heap/direct memory buffers or other processes on the system) for memory-mapping of segments on disk. For better query performance, you will want to ensure a good (`free system memory` / total size of all `druid.segmentCache.locations`) ratio so that a greater proportion of segments can be kept in memory.

#### Segment sizes matter

Be sure to check out [segment size optimization](./segment-optimization.md) to help tune your Historical processes for maximum performance.

### Broker

#### Heap sizing

The biggest contributions to heap usage on Brokers are:
- Partial unmerged query results from Historicals and Tasks
- The segment timeline: this consists of location information (which Historical/Task is serving a segment) for all currently [available](../design/storage.md#segment-lifecycle) segments.
- Cached segment metadata: this consists of metadata, such as per-segment schemas, for all currently available segments.

The Broker heap requirements scale based on the number of segments in the cluster, and the total data size of the segments.

The heap size will vary based on data size and usage patterns, but 4GiB to 8GiB is a good starting point for a small or medium cluster (~15 servers or less). For a rough estimate of memory requirements on the high end, very large clusters with a node count on the order of ~100 nodes may need Broker heaps of 30GiB-60GiB.

If caching is enabled on the Broker, the cache is stored on heap, sized by `druid.cache.sizeInBytes`.

#### Direct memory sizing

On the Broker, the amount of direct memory needed depends on how many merge buffers (used for merging GroupBys) are configured. The Broker does not generally need processing threads or processing buffers, as query results are merged on-heap in the HTTP connection threads instead.

- `druid.processing.buffer.sizeBytes` can be set to 500MiB.
- `druid.processing.numMergeBuffers`: set this to the same value as on Historicals or a bit higher

#### Connection pool sizing

Please see the [General Connection Pool Guidelines](#connection-pool) section for an overview of connection pool configuration.

On the Brokers, please ensure that the sum of `druid.broker.http.numConnections` across all the Brokers is slightly lower than the value of `druid.server.http.numThreads` on your Historicals and Tasks.

`druid.server.http.numThreads` on the Broker should be set to a value slightly higher than `druid.broker.http.numConnections` on the same Broker.

Tuning the cluster so that each Historical can accept 50 queries and 10 non-queries, adjusting the Brokers accordingly, is a reasonable starting point.

#### Broker backpressure

When retrieving query results from Historical processes or Tasks, the Broker can optionally specify a maximum buffer size for queued, unread data, and exert backpressure on the channel to the Historical or Tasks when limit is reached (causing writes to the channel to block on the Historical/Task side until the Broker is able to drain some data from the channel).

This buffer size is controlled by the `druid.broker.http.maxQueuedBytes` setting.

The limit is divided across the number of Historicals/Tasks that a query would hit: suppose I have `druid.broker.http.maxQueuedBytes` set to 5MiB, and the Broker receives a query that needs to be fanned out to 2 Historicals. Each per-historical channel would get a 2.5MiB buffer in this case.

You can generally set this to a value of approximately `2MiB * number of Historicals`. As your cluster scales up with more Historicals and Tasks, consider increasing this buffer size and increasing the Broker heap accordingly.

- If the buffer is too small, this can lead to inefficient queries due to the buffer filling up rapidly and stalling the channel
- If the buffer is too large, this puts more memory pressure on the Broker due to more queued result data in the HTTP channels.

#### Number of brokers

A 1:15 ratio of Brokers to Historicals is a reasonable starting point (this is not a hard rule).

If you need Broker HA, you can deploy 2 initially and then use the 1:15 ratio guideline for additional Brokers.

#### Total memory usage

To estimate total memory usage of the Broker under these guidelines:

- Heap: allocated heap size
- Direct Memory: `(druid.processing.numMergeBuffers + 1) * druid.processing.buffer.sizeBytes`

### MiddleManager

The MiddleManager is a lightweight task controller/manager that launches Task processes, which perform ingestion work.

#### MiddleManager heap sizing

The MiddleManager itself does not require much resources, you can set the heap to ~128MiB generally.

#### SSD storage

We recommend using SSDs for storage on the MiddleManagers, as the Tasks launched by MiddleManagers handle segment data stored on disk.

#### Task Count

The number of tasks a MiddleManager can launch is controlled by the `druid.worker.capacity` setting.

The number of workers needed in your cluster depends on how many concurrent ingestion tasks you need to run for your use cases. The number of workers that can be launched on a given machine depends on the size of resources allocated per worker and available system resources.

You can allocate more MiddleManager machines to your cluster to add task capacity.

#### Task configurations

The following section below describes configuration for Tasks launched by the MiddleManager. The Tasks can be queried and perform ingestion workloads, so they require more resources than the MM.

##### Task heap sizing

A 1GiB heap is usually enough for Tasks.

###### Lookups

If you are using lookups, calculate the total size of the lookup maps being loaded.

Druid performs an atomic swap when updating lookup maps (both the old map and the new map will exist in heap during the swap), so the maximum potential heap usage from lookup maps will be (2 * total size of all loaded lookups).

Be sure to add `(2 * total size of all loaded lookups)` to your Task heap size if you are using lookups.

##### Task processing threads and buffers

For Tasks, 1 or 2 processing threads are often enough, as the Tasks tend to hold much less queryable data than Historical processes.

- `druid.indexer.fork.property.druid.processing.numThreads`: set this to 1 or 2
- `druid.indexer.fork.property.druid.processing.numMergeBuffers`: set this to 2
- `druid.indexer.fork.property.druid.processing.buffer.sizeBytes`: can be set to 100MiB

##### Direct memory sizing

The processing and merge buffers described above are direct memory buffers.

When a Task processes a query, it must open a set of segments for reading. This also requires some direct memory space, described in [segment decompression buffers](#segment-decompression).

An ingestion Task also needs to merge partial ingestion results, which requires direct memory space, described in [segment merging](#segment-merging).

A formula for estimating direct memory usage follows:

(`druid.processing.numThreads` + `druid.processing.numMergeBuffers` + 1) * `druid.processing.buffer.sizeBytes`

The `+ 1` factor is a fuzzy estimate meant to account for the segment decompression buffers and dictionary merging buffers.

##### Connection pool sizing

Please see the [General Connection Pool Guidelines](#connection-pool) section for an overview of connection pool configuration.

For Tasks, `druid.server.http.numThreads` should be set to a value slightly higher than the sum of `druid.broker.http.numConnections` across all the Brokers in the cluster.

Tuning the cluster so that each Task can accept 50 queries and 10 non-queries is a reasonable starting point.

#### Total memory usage

To estimate total memory usage of a Task under these guidelines:

- Heap: `1GiB + (2 * total size of lookup maps)`
- Direct Memory: `(druid.processing.numThreads + druid.processing.numMergeBuffers + 1) * druid.processing.buffer.sizeBytes`

The total memory usage of the MiddleManager + Tasks:

`MM heap size + druid.worker.capacity * (single task memory usage)`

##### Configuration guidelines for specific ingestion types

###### Kafka/Kinesis ingestion

If you use the [Kafka Indexing Service](../ingestion/kafka-ingestion.md) or [Kinesis Indexing Service](../ingestion/kinesis-ingestion.md), the number of tasks required will depend on the number of partitions and your taskCount/replica settings.

On top of those requirements, allocating more task slots in your cluster is a good idea, so that you have free task
slots available for other tasks, such as [compaction tasks](../data-management/compaction.md).

###### Hadoop ingestion

If you are only using [Hadoop-based batch ingestion](../ingestion/hadoop.md) with no other ingestion types, you can lower the amount of resources allocated per Task. Batch ingestion tasks do not need to answer queries, and the bulk of the ingestion workload will be executed on the Hadoop cluster, so the Tasks do not require much resources.

###### Parallel native ingestion

If you are using [parallel native batch ingestion](../ingestion/native-batch.md), allocating more available task slots is a good idea and will allow greater ingestion concurrency.

### Coordinator

The main performance-related setting on the Coordinator is the heap size.

The heap requirements of the Coordinator scale with the number of servers, segments, and tasks in the cluster.

You can set the Coordinator heap to the same size as your Broker heap, or slightly smaller: both services have to process cluster-wide state and answer API requests about this state.

#### Dynamic Configuration

`percentOfSegmentsToConsiderPerMove`
* The default value is 100. This means that the Coordinator will consider all segments when it is looking for a segment to move. The Coordinator makes a weighted choice, with segments on Servers with the least capacity being the most likely segments to be moved.
  * This weighted selection strategy means that the segments on the servers who have the most available capacity are the least likely to be chosen.
  * As the number of segments in the cluster increases, the probability of choosing the Nth segment to move decreases; where N is the last segment considered for moving.
  * An admin can use this config to skip consideration of that Nth segment.
* Instead of skipping a precise amount of segments, we skip a percentage of segments in the cluster.
  * For example, with the value set to 25, only the first 25% of segments will be considered as a segment that can be moved. This 25% of segments will come from the servers that have the least available capacity.
    * In this example, each time the Coordinator looks for a segment to move, it will consider 75% less segments than it did when the configuration was 100. On clusters with hundreds of thousands of segments, this can add up to meaningful coordination time savings.
* General recommendations for this configuration:
  * If you are not worried about the amount of time it takes your Coordinator to complete a full coordination cycle, you likely do not need to modify this config.
  * If you are frustrated with how long the Coordinator takes to run a full coordination cycle, and you have set the Coordinator dynamic config `maxSegmentsToMove` to a value above 0 (the default is 5), setting this config to a non-default value can help shorten coordination time.
    * The recommended starting point value is 66. It represents a meaningful decrease in the percentage of segments considered while also not being too aggressive (You will consider 1/3 fewer segments per move operation with this value).
* The impact that modifying this config will have on your coordination time will be a function of how low you set the config value, the value for `maxSegmentsToMove` and the total number of segments in your cluster.
  * If your cluster has a relatively small number of segments, or you choose to move few segments per coordination cycle, there may not be much savings to be had here.

### Overlord

The main performance-related setting on the Overlord is the heap size.

The heap requirements of the Overlord scale primarily with the number of running Tasks.

The Overlord tends to require less resources than the Coordinator or Broker. You can generally set the Overlord heap to a value that's 25-50% of your Coordinator heap.

### Router

The Router has light resource requirements, as it proxies requests to Brokers without performing much computational work itself.

You can assign it 256MiB heap as a starting point, growing it if needed.

<a name="processing-threads-buffers"></a>

## Guidelines for processing threads and buffers

### Processing threads

The `druid.processing.numThreads` configuration controls the size of the processing thread pool used for computing query results. The size of this pool limits how many queries can be concurrently processed.

### Processing buffers

`druid.processing.buffer.sizeBytes` is a closely related property that controls the size of the off-heap buffers allocated to the processing threads.

One buffer is allocated for each processing thread. A size between 500MiB and 1GiB is a reasonable choice for general use.

The TopN and GroupBy queries use these buffers to store intermediate computed results. As the buffer size increases, more data can be processed in a single pass.

### GroupBy merging buffers

If you plan to issue GroupBy queries, `druid.processing.numMergeBuffers` is an important configuration property.

GroupBy queries use an additional pool of off-heap buffers for merging query results. These buffers have the same size as the processing buffers described above, set by the `druid.processing.buffer.sizeBytes` property.

Non-nested GroupBy queries require 1 merge buffer per query, while a nested GroupBy query requires 2 merge buffers (regardless of the depth of nesting).

The number of merge buffers determines the number of GroupBy queries that can be processed concurrently.

<a name="connection-pool"></a>

## Connection pool guidelines

Each Druid process has a configuration property for the number of HTTP connection handling threads, `druid.server.http.numThreads`.

The number of HTTP server threads limits how many concurrent HTTP API requests a given process can handle.

### Sizing the connection pool for queries

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

## Per-segment direct memory buffers

### Segment decompression

When opening a segment for reading during segment merging or query processing, Druid allocates a 64KiB off-heap decompression buffer for each column being read.

Thus, there is additional direct memory overhead of (64KiB * number of columns read per segment * number of segments read) when reading segments.

### Segment merging

In addition to the segment decompression overhead described above, when a set of segments are merged during ingestion, a direct buffer is allocated for every String typed column, for every segment in the set to be merged.

The size of these buffers are equal to the cardinality of the String column within its segment, times 4 bytes (the buffers store integers).

For example, if two segments are being merged, the first segment having a single String column with cardinality 1000, and the second segment having a String column with cardinality 500, the merge step would allocate (1000 + 500) * 4 = 6000 bytes of direct memory.

These buffers are used for merging the value dictionaries of the String column across segments. These "dictionary merging buffers" are independent of the "merge buffers" configured by `druid.processing.numMergeBuffers`.


## General recommendations

### JVM tuning

#### Garbage Collection
We recommend using the G1GC garbage collector:

`-XX:+UseG1GC`

Enabling process termination on out-of-memory errors is useful as well, since the process generally will not recover from such a state, and it's better to restart the process:

`-XX:+ExitOnOutOfMemoryError`

#### Other generally useful JVM flags

```
-Duser.timezone=UTC
-Dfile.encoding=UTF-8
-Djava.io.tmpdir=<should not be volatile tmpfs and also has good read and write speed. Strongly recommended to avoid using NFS mount>
-Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager
-Dorg.jboss.logging.provider=slf4j
-Dnet.spy.log.LoggerImpl=net.spy.memcached.compat.log.SLF4JLogger
-Dlog4j.shutdownCallbackRegistry=org.apache.druid.common.config.Log4jShutdown
-Dlog4j.shutdownHookEnabled=true
-XX:+PrintGCDetails
-XX:+PrintGCDateStamps
-XX:+PrintGCTimeStamps
-XX:+PrintGCApplicationStoppedTime
-XX:+PrintGCApplicationConcurrentTime
-Xloggc:/var/logs/druid/historical.gc.log
-XX:+UseGCLogFileRotation
-XX:NumberOfGCLogFiles=50
-XX:GCLogFileSize=10m
-XX:+ExitOnOutOfMemoryError
-XX:+HeapDumpOnOutOfMemoryError
-XX:HeapDumpPath=/var/logs/druid/historical.hprof
-XX:MaxDirectMemorySize=1g
```
:::info
 Please note that the flag settings above represent sample, general guidelines only. Be careful to use values appropriate
for your specific scenario and be sure to test any changes in staging environments.
:::

`ExitOnOutOfMemoryError` flag is only supported starting JDK 8u92 . For older versions, `-XX:OnOutOfMemoryError='kill -9 %p'` can be used.

`MaxDirectMemorySize` restricts JVM from allocating more than specified limit, by setting it to unlimited JVM restriction is lifted and OS level memory limits would still be effective. It's still important to make sure that Druid is not configured to allocate more off-heap memory than your machine has available. Important settings here include `druid.processing.numThreads`, `druid.processing.numMergeBuffers`, and `druid.processing.buffer.sizeBytes`.

Additionally, for large JVM heaps, here are a few Garbage Collection efficiency guidelines that have been known to help in some cases.


- Mount /tmp on tmpfs. See [The Four Month Bug: JVM statistics cause garbage collection pauses](http://www.evanjones.ca/jvm-mmap-pause.html).
- On Disk-IO intensive processes (e.g., Historical and MiddleManager), GC and Druid logs should be written to a different disk than where data is written.
- Disable [Transparent Huge Pages](https://www.kernel.org/doc/html/latest/admin-guide/mm/transhuge.html).
- Try disabling biased locking by using `-XX:-UseBiasedLocking` JVM flag. See [Logging Stop-the-world Pauses in JVM](https://dzone.com/articles/logging-stop-world-pauses-jvm).

### Use UTC timezone

We recommend using UTC timezone for all your events and across your hosts, not just for Druid, but for all data infrastructure. This can greatly mitigate potential query problems with inconsistent timezones. To query in a non-UTC timezone see [query granularities](../querying/granularities.md#period-granularities)

### System configuration

#### SSDs

SSDs are highly recommended for Historical, MiddleManager, and Indexer processes if you are not running a cluster that is entirely in memory. SSDs can greatly mitigate the time required to page data in and out of memory.

#### JBOD vs RAID

Historical processes store large number of segments on Disk and support specifying multiple paths for storing those. Typically, hosts have multiple disks configured with RAID which makes them look like a single disk to OS. RAID might have overheads specially if its not hardware controller based but software based. So, Historicals might get improved disk throughput with JBOD.

#### Swap space

We recommend _not_ using swap space for Historical, MiddleManager, and Indexer processes since due to the large number of memory mapped segment files can lead to poor and unpredictable performance.

#### Linux limits

For Historical, MiddleManager, and Indexer processes (and for really large clusters, Broker processes), you might need to adjust some Linux system limits to account for a large number of open files, a large number of network connections, or a large number of memory mapped files.

##### ulimit

The limit on the number of open files can be set permanently by editing `/etc/security/limits.conf`. This value should be substantially greater than the number of segment files that will exist on the server.

##### max_map_count

Historical processes and to a lesser extent, MiddleManager and Indexer processes memory map segment files, so depending on the number of segments per server, `/proc/sys/vm/max_map_count` might also need to be adjusted. Depending on the variant of Linux, this might be done via `sysctl` by placing a file in `/etc/sysctl.d/` that sets `vm.max_map_count`.
