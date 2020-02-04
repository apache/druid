---
id: coordinator
title: "Coordinator Process"
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

For Apache Druid Coordinator Process Configuration, see [Coordinator Configuration](../configuration/index.html#coordinator).

### HTTP endpoints

For a list of API endpoints supported by the Coordinator, see [Coordinator API](../operations/api-reference.html#coordinator).

### Overview

The Druid Coordinator process is primarily responsible for segment management and distribution. More specifically, the
Druid Coordinator process communicates to Historical processes to load or drop segments based on configurations. The
Druid Coordinator is responsible for loading new segments, dropping outdated segments, ensuring that segments are
"replicated" (that is, loaded on multiple different Historical nodes) proper (configured) number of times, and moving
("balancing") segments between Historical nodes to keep the latter evenly loaded.

The Druid Coordinator runs its duties periodically and the time between each run is a configurable parameter. On each
run, the Coordinator assesses the current state of the cluster before deciding on the appropriate actions to take.
Similar to the Broker and Historical processes, the Druid Coordinator maintains a connection to a Zookeeper cluster for
current cluster information. The Coordinator also maintains a connection to a database containing information about
"used" segments (that is, the segments that *should* be loaded in the cluster) and the loading rules.

Before any unassigned segments are serviced by Historical processes, the Historical processes for each tier are first
sorted in terms of capacity, with least capacity servers having the highest priority. Unassigned segments are always
assigned to the processes with least capacity to maintain a level of balance between processes. The Coordinator does not
directly communicate with a historical process when assigning it a new segment; instead the Coordinator creates some
temporary information about the new segment under load queue path of the historical process. Once this request is seen,
the historical process will load the segment and begin servicing it.

### Running

```
org.apache.druid.cli.Main server coordinator
```

### Rules

Segments can be automatically loaded and dropped from the cluster based on a set of rules. For more information on rules, see [Rule Configuration](../operations/rule-configuration.md).

### Cleaning up segments

On each run, the Druid Coordinator compares the set of used segments in the database with the segments served by some
Historical nodes in the cluster. Coordinator sends requests to Historical nodes to unload unused segments or segments
that are removed from the database.

Segments that are overshadowed (their versions are too old and their data has been replaced by newer segments) are
marked as unused. During the next Coordinator's run, they will be unloaded from Historical nodes in the cluster.

### Segment availability

If a Historical process restarts or becomes unavailable for any reason, the Druid Coordinator will notice a process has gone missing and treat all segments served by that process as being dropped. Given a sufficient period of time, the segments may be reassigned to other Historical processes in the cluster. However, each segment that is dropped is not immediately forgotten. Instead, there is a transitional data structure that stores all dropped segments with an associated lifetime. The lifetime represents a period of time in which the Coordinator will not reassign a dropped segment. Hence, if a historical process becomes unavailable and available again within a short period of time, the historical process will start up and serve segments from its cache without any those segments being reassigned across the cluster.

### Balancing segment load

To ensure an even distribution of segments across Historical processes in the cluster, the Coordinator process will find the total size of all segments being served by every Historical process each time the Coordinator runs. For every Historical process tier in the cluster, the Coordinator process will determine the Historical process with the highest utilization and the Historical process with the lowest utilization. The percent difference in utilization between the two processes is computed, and if the result exceeds a certain threshold, a number of segments will be moved from the highest utilized process to the lowest utilized process. There is a configurable limit on the number of segments that can be moved from one process to another each time the Coordinator runs. Segments to be moved are selected at random and only moved if the resulting utilization calculation indicates the percentage difference between the highest and lowest servers has decreased.

### Compacting Segments

Each run, the Druid Coordinator compacts segments by merging small segments or splitting a large one. This is useful when your segments are not optimized
in terms of segment size which may degrade query performance. See [Segment Size Optimization](../operations/segment-optimization.md) for details.

The Coordinator first finds the segments to compact based on the [segment search policy](#segment-search-policy).
Once some segments are found, it issues a [compaction task](../ingestion/tasks.md#compact) to compact those segments.
The maximum number of running compaction tasks is `min(sum of worker capacity * slotRatio, maxSlots)`.
Note that even though `min(sum of worker capacity * slotRatio, maxSlots)` = 0, at least one compaction task is always submitted
if the compaction is enabled for a dataSource.
See [Compaction Configuration API](../operations/api-reference.html#compaction-configuration) and [Compaction Configuration](../configuration/index.html#compaction-dynamic-configuration) to enable the compaction.

Compaction tasks might fail due to the following reasons.

- If the input segments of a compaction task are removed or overshadowed before it starts, that compaction task fails immediately.
- If a task of a higher priority acquires a [time chunk lock](../ingestion/tasks.html#locking) for an interval overlapping with the interval of a compaction task, the compaction task fails.

Once a compaction task fails, the Coordinator simply checks the segments in the interval of the failed task again, and issues another compaction task in the next run.

### Segment search policy

#### Recent segment first policy

At every coordinator run, this policy looks up time chunks in order of newest-to-oldest and checks whether the segments in those time chunks
need compaction or not.
A set of segments need compaction if all conditions below are satisfied.

1) Total size of segments in the time chunk is smaller than or equal to the configured `inputSegmentSizeBytes`.
2) Segments have never been compacted yet or compaction spec has been updated since the last compaction, especially `maxRowsPerSegment`, `maxTotalRows`, and `indexSpec`.

Here are some details with an example. Suppose we have two dataSources (`foo`, `bar`) as seen below:

- `foo`
  - `foo_2017-11-01T00:00:00.000Z_2017-12-01T00:00:00.000Z_VERSION`
  - `foo_2017-11-01T00:00:00.000Z_2017-12-01T00:00:00.000Z_VERSION_1`
  - `foo_2017-09-01T00:00:00.000Z_2017-10-01T00:00:00.000Z_VERSION`
- `bar`
  - `bar_2017-10-01T00:00:00.000Z_2017-11-01T00:00:00.000Z_VERSION`
  - `bar_2017-10-01T00:00:00.000Z_2017-11-01T00:00:00.000Z_VERSION_1`

Assuming that each segment is 10 MB and haven't been compacted yet, this policy first returns two segments of
`foo_2017-11-01T00:00:00.000Z_2017-12-01T00:00:00.000Z_VERSION` and `foo_2017-11-01T00:00:00.000Z_2017-12-01T00:00:00.000Z_VERSION_1` to compact together because
`2017-11-01T00:00:00.000Z/2017-12-01T00:00:00.000Z` is the most recent time chunk.

If the coordinator has enough task slots for compaction, this policy will continue searching for the next segments and return
`bar_2017-10-01T00:00:00.000Z_2017-11-01T00:00:00.000Z_VERSION` and `bar_2017-10-01T00:00:00.000Z_2017-11-01T00:00:00.000Z_VERSION_1`.
Finally, `foo_2017-09-01T00:00:00.000Z_2017-10-01T00:00:00.000Z_VERSION` will be picked up even though there is only one segment in the time chunk of `2017-09-01T00:00:00.000Z/2017-10-01T00:00:00.000Z`.

The search start point can be changed by setting [skipOffsetFromLatest](../configuration/index.html#compaction-dynamic-configuration).
If this is set, this policy will ignore the segments falling into the time chunk of (the end time of the most recent segment - `skipOffsetFromLatest`).
This is to avoid conflicts between compaction tasks and realtime tasks.
Note that realtime tasks have a higher priority than compaction tasks by default. Realtime tasks will revoke the locks of compaction tasks if their intervals overlap, resulting in the termination of the compaction task.

> This policy currently cannot handle the situation when there are a lot of small segments which have the same interval,
> and their total size exceeds [inputSegmentSizeBytes](../configuration/index.md#compaction-dynamic-configuration).
> If it finds such segments, it simply skips them.

### The Coordinator console

The Druid Coordinator exposes a web GUI for displaying cluster information and rule configuration. For more details, please see [coordinator console](../operations/management-uis.html#coordinator-consoles).

### FAQ

1. **Do clients ever contact the Coordinator process?**

    The Coordinator is not involved in a query.

    Historical processes never directly contact the Coordinator process. The Druid Coordinator tells the Historical processes to load/drop data via Zookeeper, but the Historical processes are completely unaware of the Coordinator.

    Brokers also never contact the Coordinator. Brokers base their understanding of the data topology on metadata exposed by the Historical processes via ZK and are completely unaware of the Coordinator.

2. **Does it matter if the Coordinator process starts up before or after other processes?**

    No. If the Druid Coordinator is not started up, no new segments will be loaded in the cluster and outdated segments will not be dropped. However, the Coordinator process can be started up at any time, and after a configurable delay, will start running Coordinator tasks.

    This also means that if you have a working cluster and all of your Coordinators die, the cluster will continue to function, it just won’t experience any changes to its data topology.
