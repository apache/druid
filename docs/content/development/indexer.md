---
layout: doc_page
title: "Indexer Process"
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

# Indexer Process

<div class="note info">
The Indexer is an optional and <a href="../development/experimental.html">experimental</a> feature. Its memory management system is still under development and will be significantly enhanced in later releases. 
</div>

The Apache Druid (incubating) Indexer process is an alternative to the MiddleManager + Peon task execution system. Instead of forking a separate JVM process per-task, the Indexer runs tasks as separate threads within a single JVM process.

The Indexer is designed to be easier to configure and deploy compared to the MiddleManager + Peon system and to better enable resource sharing across tasks.

## Running

```
org.apache.druid.cli.Main server indexer
```

## Task Resource Sharing

The following resources are shared across all tasks running inside an Indexer process.

### Query resources

The query processing threads and buffers are shared across all tasks. The Indexer will serve queries from a single endpoint shared by all tasks.

If [query caching](#indexer-caching) is enabled, the query cache is also shared across all tasks.

### Server HTTP threads

The Indexer maintains two equally sized pools of HTTP threads. 

One pool is exclusively used for task control messages between the Overlord and the Indexer ("chat handler threads"). The other pool is used for handling all other HTTP requests.

The size of the pools are configured by the `druid.server.http.numThreads` configuration (e.g., if this is set to 10, there will be 10 chat handler threads and 10 non-chat handler threads).

In addition to these two pools, 2 separate threads are allocated for lookup handling. If lookups are not used, these threads will not be used.

### Memory Sharing

The Indexer uses the `druid.worker.globalIngestionHeapLimitBytes` configuration to impose a global heap limit across all of the tasks it is running. 

This global limit is evenly divided across the number of task slots configured by `druid.worker.capacity`. 

To apply the per-task heap limit, the Indexer will override `maxBytesInMemory` in task tuning configs (i.e., ignoring the default value or any user configured value). `maxRowsInMemory` will also be overridden to an essentially unlimited value: the Indexer does not support row limits.

By default, `druid.worker.globalIngestionHeapLimitBytes` is set to 60% of the available JVM heap. The remaining portion of the heap is reserved for query processing and segment persist/merge operations, and miscellaneous heap usage.

#### Concurrent Segment Persist/Merge Limits

To help reduce peak memory usage, the Indexer imposes a limit on the number of concurrent segment persist/merge operations across all running tasks.

By default, the number of concurrent persist/merge operations is limited to (`druid.worker.capacity` / 2), rounded down. This limit can be configured with the `druid.worker.numConcurrentMerges` property.

## Runtime Configuration

In addition to the [common configurations](../configuration/index.html#common-configurations), the Indexer accepts the following configurations:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.worker.version`|Version identifier for the Indexer.|0|
|`druid.worker.capacity`|Maximum number of tasks the Indexer can accept.|Number of available processors - 1|
|`druid.worker.globalIngestionHeapLimitBytes`|Total amount of heap available for ingestion processing. This is applied by automatically setting the `maxBytesInMemory` property on tasks.|60% of configured JVM heap|
|`druid.worker.numConcurrentMerges`|Maximum number of segment persist or merge operations that can run concurrently across all tasks.|`druid.worker.capacity` / 2, rounded down|
|`druid.indexer.task.baseDir`|Base temporary working directory.|`System.getProperty("java.io.tmpdir")`|
|`druid.indexer.task.baseTaskDir`|Base temporary working directory for tasks.|`${druid.indexer.task.baseDir}/persistent/tasks`|
|`druid.indexer.task.defaultHadoopCoordinates`|Hadoop version to use with HadoopIndexTasks that do not request a particular version.|org.apache.hadoop:hadoop-client:2.8.3|
|`druid.indexer.task.gracefulShutdownTimeout`|Wait this long on Indexer restart for restorable tasks to gracefully exit.|PT5M|
|`druid.indexer.task.hadoopWorkingPath`|Temporary working directory for Hadoop tasks.|`/tmp/druid-indexing`|
|`druid.indexer.task.restoreTasksOnRestart`|If true, the Indexer will attempt to stop tasks gracefully on shutdown and restore them on restart.|false|
|`druid.peon.taskActionClient.retry.minWait`|The minimum retry time to communicate with Overlord.|PT5S|
|`druid.peon.taskActionClient.retry.maxWait`|The maximum retry time to communicate with Overlord.|PT1M|
|`druid.peon.taskActionClient.retry.maxRetryCount`|The maximum number of retries to communicate with Overlord.|60|

### Concurrent Requests

Druid uses Jetty to serve HTTP requests.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.server.http.numThreads`|Number of threads for HTTP requests. Please see the [Server HTTP threads](#server-http-threads) section for more details on how the Indexer uses this configuration.|max(10, (Number of cores * 17) / 16 + 2) + 30|
|`druid.server.http.queueSize`|Size of the worker queue used by Jetty server to temporarily store incoming client connections. If this value is set and a request is rejected by jetty because queue is full then client would observe request failure with TCP connection being closed immediately with a completely empty response from server.|Unbounded|
|`druid.server.http.maxIdleTime`|The Jetty max idle time for a connection.|PT5M|
|`druid.server.http.enableRequestLimit`|If enabled, no requests would be queued in jetty queue and "HTTP 429 Too Many Requests" error response would be sent. |false|
|`druid.server.http.defaultQueryTimeout`|Query timeout in millis, beyond which unfinished queries will be cancelled|300000|
|`druid.server.http.gracefulShutdownTimeout`|The maximum amount of time Jetty waits after receiving shutdown signal. After this timeout the threads will be forcefully shutdown. This allows any queries that are executing to complete.|`PT0S` (do not wait)|
|`druid.server.http.unannouncePropagationDelay`|How long to wait for zookeeper unannouncements to propagate before shutting down Jetty. This is a minimum and `druid.server.http.gracefulShutdownTimeout` does not start counting down until after this period elapses.|`PT0S` (do not wait)|
|`druid.server.http.maxQueryTimeout`|Maximum allowed value (in milliseconds) for `timeout` parameter. See [query-context](../querying/query-context.html) to know more about `timeout`. Query is rejected if the query context `timeout` is greater than this value. |Long.MAX_VALUE|
|`druid.server.http.maxRequestHeaderSize`|Maximum size of a request header in bytes. Larger headers consume more memory and can make a server more vulnerable to denial of service attacks.|8 * 1024|

### Processing

|Property|Description|Default|
|--------|-----------|-------|
|`druid.processing.buffer.sizeBytes`|This specifies a buffer size for the storage of intermediate results. The computation engine in the Indexer processes will use a scratch buffer of this size to do all of their intermediate computations off-heap. Larger values allow for more aggregations in a single pass over the data while smaller values can require more passes depending on the query that is being executed.|auto (max 1GB)|
|`druid.processing.buffer.poolCacheMaxCount`|processing buffer pool caches the buffers for later use, this is the maximum count cache will grow to. note that pool can create more buffers than it can cache if necessary.|Integer.MAX_VALUE|
|`druid.processing.formatString`|Indexer processes use this format string to name their processing threads.|processing-%s|
|`druid.processing.numMergeBuffers`|The number of direct memory buffers available for merging query results. The buffers are sized by `druid.processing.buffer.sizeBytes`. This property is effectively a concurrency limit for queries that require merging buffers. If you are using any queries that require merge buffers (currently, just groupBy v2) then you should have at least two of these.|`max(2, druid.processing.numThreads / 4)`|
|`druid.processing.numThreads`|The number of processing threads to have available for parallel processing of segments. Our rule of thumb is `num_cores - 1`, which means that even under heavy load there will still be one core available to do background tasks like talking with ZooKeeper and pulling down segments. If only one core is available, this property defaults to the value `1`.|Number of cores - 1 (or 1)|
|`druid.processing.columnCache.sizeBytes`|Maximum size in bytes for the dimension value lookup cache. Any value greater than `0` enables the cache. It is currently disabled by default. Enabling the lookup cache can significantly improve the performance of aggregators operating on dimension values, such as the JavaScript aggregator, or cardinality aggregator, but can slow things down if the cache hit rate is low (i.e. dimensions with few repeating values). Enabling it may also require additional garbage collection tuning to avoid long GC pauses.|`0` (disabled)|
|`druid.processing.fifo`|If the processing queue should treat tasks of equal priority in a FIFO manner|`false`|
|`druid.processing.tmpDir`|Path where temporary files created while processing a query should be stored. If specified, this configuration takes priority over the default `java.io.tmpdir` path.|path represented by `java.io.tmpdir`|

The amount of direct memory needed by Druid is at least
`druid.processing.buffer.sizeBytes * (druid.processing.numMergeBuffers + druid.processing.numThreads + 1)`. You can
ensure at least this amount of direct memory is available by providing `-XX:MaxDirectMemorySize=<VALUE>` at the command
line.

### Query Configurations

See [general query configuration](../configuration/index.html#general-query-configuration).

### Indexer Caching

You can optionally configure caching to be enabled on the Indexer by setting caching configs here.

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.realtime.cache.useCache`|true, false|Enable the cache on the realtime.|false|
|`druid.realtime.cache.populateCache`|true, false|Populate the cache on the realtime.|false|
|`druid.realtime.cache.unCacheable`|All druid query types|All query types to not cache.|`["groupBy", "select"]`|
|`druid.realtime.cache.maxEntrySize`|Maximum cache entry size in bytes.|1_000_000|

See [cache configuration](#cache-configuration) for how to configure cache settings.

Note that only local caches such as the `local`-type cache and `caffeine` cache are supported. If a remote cache such as `memcached` is used, it will be ignored.

## Current Limitations

Separate task logs are not currently supported when using the Indexer; all task log messages will instead be logged in the Indexer process log.

The Indexer currently imposes an identical memory limit on each task. In later releases, the per-task memory limit will be removed and only the global limit will apply. The limit on concurrent merges will also be removed. 

In later releases, per-task memory usage will be dynamically managed. Please see https://github.com/apache/incubator-druid/issues/7900 for details on future enhancements to the Indexer.