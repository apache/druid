---
id: historical
title: "Historical Process"
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


### Configuration

For Apache Druid Historical Process Configuration, see [Historical Configuration](../configuration/index.md#historical).

### HTTP endpoints

For a list of API endpoints supported by the Historical, please see the [API reference](../operations/api-reference.md#historical).

### Running

```
org.apache.druid.cli.Main server historical
```

### Loading and serving segments

Each Historical process copies or "pulls" segment files from Deep Storage to local disk in an area called the *segment cache*.  Set the `druid.segmentCache.locations` to configure the size and location of the segment cache on each Historical process. See [Historical general configuration](../configuration/index.html#historical-general-configuration).

For more information on tuning this value, see the [Tuning Guide](../operations/basic-cluster-tuning.html#segment-cache-size).

The [Coordinator](../design/coordinator.html) leads the assignment of segments to - and balance between - Historical processes.  [Zookeeper](../dependencies/zookeeper.md) is central to this collaboration; Historical processes do not communicate directly with each other, nor do they communicate directly with the Coordinator.  Instead, the Coordinator creates ephemeral Zookeeper entries under a [load queue path](../configuration/index.html#path-configuration) and each Historical process maintains a connection to Zookeeper, watching those paths for segment information.

For more information about how the Coordinator assigns segments to Historical processes, please see [Coordinator](../design/coordinator.html).

When a Historical process sees a new Zookeeper load queue entry, it checks its own segment cache. If no information about the segment exists there, the Historical process first retrieves metadata from Zookeeper about the segment, including where the segment is located in Deep Storage and how it needs to decompress and process it.

For more information about segment metadata and Druid segments in general, please see [Segments](../design/segments.html). 

Once a Historical process has completed pulling down and processing a segment from Deep Storage, the segment is advertised as being available for queries.  This announcement by the Historical is made via Zookeeper, this time under a [served segments path](../configuration/index.html#path-configuration). At this point, the segment is considered available for querying by the Broker.

For more information about how the Broker determines what data is available for queries, please see [Broker](broker.html).

On startup, a Historical process searches through its segment cache and, in order for Historicals to be queried as soon as possible, immediately advertises all segments it finds there.

### Loading and serving segments from cache

A technique called [memory mapping](https://en.wikipedia.org/wiki/Mmap) is used for the segment cache, consuming memory from the underlying operating system so that parts of segment files can be held in memory, increasing query performance at the data level.  The in-memory segment cache is therefore affected by, for example, the size of the Historical JVM, heap / direct memory buffers, and other processes on the operating system itself.

At query time, if the required part of a segment file is available in the memory mapped cache (also known as the "page cache"), it will be re-used and read directly from memory.  If it is not, that part of the segment will be read from disk.  When this happens, there is potential for this new data to evict other segment data from memory.  Consequently, the closer that free operating system memory is to `druid.server.maxSize`, the faster historical processes typically respond at query time since segment data is very likely to be available in memory.  Conversely, the lower the free operating system memory, the more likely a Historical is to read segments from disk.

Note that this memory-mapped segment cache is in addition to other [query-level caches](../querying/caching.html).

### Querying segments

Please see [Querying](../querying/querying.md) for more information on querying Historical processes.

A Historical can be configured to log and report metrics for every query it services.
