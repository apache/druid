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

> The Indexer is an optional and <a href="../development/experimental.html">experimental</a> feature.
> Its memory management system is still under development and will be significantly enhanced in later releases. 

The Apache Druid (incubating) Indexer process is an alternative to the MiddleManager + Peon task execution system. Instead of forking a separate JVM process per-task, the Indexer runs tasks as separate threads within a single JVM process.

The Indexer is designed to be easier to configure and deploy compared to the MiddleManager + Peon system and to better enable resource sharing across tasks.

### Configuration

For Apache Druid (incubating) Indexer Process Configuration, see [Indexer Configuration](../configuration/index.html#indexer).

### HTTP endpoints

The Indexer process shares the same HTTP endpoints as the [MiddleManager](../operations/api-reference.html#middlemanager).

### Running

```
org.apache.druid.cli.Main server indexer
```

### Task resource sharing

The following resources are shared across all tasks running inside an Indexer process.

#### Query resources

The query processing threads and buffers are shared across all tasks. The Indexer will serve queries from a single endpoint shared by all tasks.

If [query caching](../configuration/index.html#indexer-caching) is enabled, the query cache is also shared across all tasks.

#### Server HTTP threads

The Indexer maintains two equally sized pools of HTTP threads. 

One pool is exclusively used for task control messages between the Overlord and the Indexer ("chat handler threads"). The other pool is used for handling all other HTTP requests.

The size of the pools are configured by the `druid.server.http.numThreads` configuration (e.g., if this is set to 10, there will be 10 chat handler threads and 10 non-chat handler threads).

In addition to these two pools, 2 separate threads are allocated for lookup handling. If lookups are not used, these threads will not be used.

#### Memory sharing

The Indexer uses the `druid.worker.globalIngestionHeapLimitBytes` configuration to impose a global heap limit across all of the tasks it is running. 

This global limit is evenly divided across the number of task slots configured by `druid.worker.capacity`.

To apply the per-task heap limit, the Indexer will override `maxBytesInMemory` in task tuning configs (i.e., ignoring the default value or any user configured value). `maxRowsInMemory` will also be overridden to an essentially unlimited value: the Indexer does not support row limits.

By default, `druid.worker.globalIngestionHeapLimitBytes` is set to 1/6th of the available JVM heap. This default is chosen to align with the default value of `maxBytesInMemory` in task tuning configs when using the MiddleManager/Peon system, which is also 1/6th of the JVM heap.

The peak usage for rows held in heap memory relates to the interaction between the `maxBytesInMemory` and `maxPendingPersists` properties in the task tuning configs. When the amount of row data held in-heap by a task reaches the limit specified by `maxBytesInMemory`, a task will persist the in-heap row data. After the persist has been started, the task can again ingest up to `maxBytesInMemory` bytes worth of row data while the persist is running.

This means that the peak in-heap usage for row data can be up to approximately `maxBytesInMemory` * (2 + `maxPendingPersists`). The default value of `maxPendingPersists` is 0, which allows for 1 persist to run concurrently with ingestion work.

The remaining portion of the heap is reserved for query processing and segment persist/merge operations, and miscellaneous heap usage.

#### Concurrent segment persist/merge limits

To help reduce peak memory usage, the Indexer imposes a limit on the number of concurrent segment persist/merge operations across all running tasks.

By default, the number of concurrent persist/merge operations is limited to (`druid.worker.capacity` / 2), rounded down. This limit can be configured with the `druid.worker.numConcurrentMerges` property.

### Current limitations

Separate task logs are not currently supported when using the Indexer; all task log messages will instead be logged in the Indexer process log.

The Indexer currently imposes an identical memory limit on each task. In later releases, the per-task memory limit will be removed and only the global limit will apply. The limit on concurrent merges will also be removed. 

In later releases, per-task memory usage will be dynamically managed. Please see https://github.com/apache/incubator-druid/issues/7900 for details on future enhancements to the Indexer.