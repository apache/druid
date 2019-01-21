---
layout: doc_page
title: "Coordinator Node"
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

# Coordinator Node

### Configuration

For Coordinator Node Configuration, see [Coordinator Configuration](../configuration/index.html#coordinator).

### HTTP endpoints

For a list of API endpoints supported by the Coordinator, see [Coordinator API](../operations/api-reference.html#coordinator).

### Overview

The Druid coordinator node is primarily responsible for segment management and distribution. More specifically, the Druid coordinator node communicates to historical nodes to load or drop segments based on configurations. The Druid coordinator is responsible for loading new segments, dropping outdated segments, managing segment replication, and balancing segment load.

The Druid coordinator runs periodically and the time between each run is a configurable parameter. Each time the Druid coordinator runs, it assesses the current state of the cluster before deciding on the appropriate actions to take. Similar to the broker and historical nodes, the Druid coordinator maintains a connection to a Zookeeper cluster for current cluster information. The coordinator also maintains a connection to a database containing information about available segments and rules. Available segments are stored in a segment table and list all segments that should be loaded in the cluster. Rules are stored in a rule table and indicate how segments should be handled.

Before any unassigned segments are serviced by historical nodes, the available historical nodes for each tier are first sorted in terms of capacity, with least capacity servers having the highest priority. Unassigned segments are always assigned to the nodes with least capacity to maintain a level of balance between nodes. The coordinator does not directly communicate with a historical node when assigning it a new segment; instead the coordinator creates some temporary information about the new segment under load queue path of the historical node. Once this request is seen, the historical node will load the segment and begin servicing it.

### Running

```
org.apache.druid.cli.Main server coordinator
```

### Rules

Segments can be automatically loaded and dropped from the cluster based on a set of rules. For more information on rules, see [Rule Configuration](../operations/rule-configuration.html).

### Cleaning Up Segments

Each run, the Druid coordinator compares the list of available database segments in the database with the current segments in the cluster. Segments that are not in the database but are still being served in the cluster are flagged and appended to a removal list. Segments that are overshadowed (their versions are too old and their data has been replaced by newer segments) are also dropped.
Note that if all segments in database are deleted(or marked unused), then coordinator will not drop anything from the historicals. This is done to prevent a race condition in which the coordinator would drop all segments if it started running cleanup before it finished polling the database for available segments for the first time and believed that there were no segments.

### Segment Availability

If a historical node restarts or becomes unavailable for any reason, the Druid coordinator will notice a node has gone missing and treat all segments served by that node as being dropped. Given a sufficient period of time, the segments may be reassigned to other historical nodes in the cluster. However, each segment that is dropped is not immediately forgotten. Instead, there is a transitional data structure that stores all dropped segments with an associated lifetime. The lifetime represents a period of time in which the coordinator will not reassign a dropped segment. Hence, if a historical node becomes unavailable and available again within a short period of time, the historical node will start up and serve segments from its cache without any those segments being reassigned across the cluster.

### Balancing Segment Load

To ensure an even distribution of segments across historical nodes in the cluster, the coordinator node will find the total size of all segments being served by every historical node each time the coordinator runs. For every historical node tier in the cluster, the coordinator node will determine the historical node with the highest utilization and the historical node with the lowest utilization. The percent difference in utilization between the two nodes is computed, and if the result exceeds a certain threshold, a number of segments will be moved from the highest utilized node to the lowest utilized node. There is a configurable limit on the number of segments that can be moved from one node to another each time the coordinator runs. Segments to be moved are selected at random and only moved if the resulting utilization calculation indicates the percentage difference between the highest and lowest servers has decreased.

### Compacting Segments

Each run, the Druid coordinator compacts small segments abutting each other. This is useful when you have a lot of small
segments which may degrade the query performance as well as increasing the disk space usage.

The coordinator first finds the segments to compact together based on the [segment search policy](#segment-search-policy).
Once some segments are found, it launches a [compaction task](../ingestion/tasks.html#compaction-task) to compact those segments.
The maximum number of running compaction tasks is `min(sum of worker capacity * slotRatio, maxSlots)`.
Note that even though `min(sum of worker capacity * slotRatio, maxSlots)` = 0, at least one compaction task is always submitted
if the compaction is enabled for a dataSource.
See [Compaction Configuration API](../operations/api-reference.html#compaction-configuration) and [Compaction Configuration](../configuration/index.html#compaction-dynamic-configuration) to enable the compaction.

Compaction tasks might fail due to the following reasons.

- If the input segments of a compaction task are removed or overshadowed before it starts, that compaction task fails immediately.
- If a task of a higher priority acquires a lock for an interval overlapping with the interval of a compaction task, the compaction task fails.

Once a compaction task fails, the coordinator simply finds the segments for the interval of the failed task again, and launches a new compaction task in the next run.

### Segment Search Policy

#### Newest Segment First Policy

At every coordinator run, this policy searches for segments to compact by iterating segments from the latest to the oldest.
Once it finds the latest segment among all dataSources, it checks if the segment is _compactible_ with other segments of the same dataSource which have the same or abutting intervals.
Note that segments are compactible if their total size is smaller than or equal to the configured `inputSegmentSizeBytes`.

Here are some details with an example. Let us assume we have two dataSources (`foo`, `bar`)
and 5 segments (`foo_2017-10-01T00:00:00.000Z_2017-11-01T00:00:00.000Z_VERSION`, `foo_2017-11-01T00:00:00.000Z_2017-12-01T00:00:00.000Z_VERSION`, `bar_2017-08-01T00:00:00.000Z_2017-09-01T00:00:00.000Z_VERSION`, `bar_2017-09-01T00:00:00.000Z_2017-10-01T00:00:00.000Z_VERSION`, `bar_2017-10-01T00:00:00.000Z_2017-11-01T00:00:00.000Z_VERSION`).
When each segment has the same size of 10 MB and `inputSegmentSizeBytes` is 20 MB, this policy first returns two segments (`foo_2017-10-01T00:00:00.000Z_2017-11-01T00:00:00.000Z_VERSION` and `foo_2017-11-01T00:00:00.000Z_2017-12-01T00:00:00.000Z_VERSION`) to compact together because
`foo_2017-11-01T00:00:00.000Z_2017-12-01T00:00:00.000Z_VERSION` is the latest segment and `foo_2017-10-01T00:00:00.000Z_2017-11-01T00:00:00.000Z_VERSION` abuts to it.

If the coordinator has enough task slots for compaction, this policy would continue searching for the next segments and return
`bar_2017-10-01T00:00:00.000Z_2017-11-01T00:00:00.000Z_VERSION` and `bar_2017-09-01T00:00:00.000Z_2017-10-01T00:00:00.000Z_VERSION`.
Note that `bar_2017-08-01T00:00:00.000Z_2017-09-01T00:00:00.000Z_VERSION` is not compacted together even though it abuts to `bar_2017-09-01T00:00:00.000Z_2017-10-01T00:00:00.000Z_VERSION`.
This is because the total segment size to compact would be greater than `inputSegmentSizeBytes` if it's included.

The search start point can be changed by setting [skipOffsetFromLatest](../configuration/index.html#compaction-dynamic-configuration).
If this is set, this policy will ignore the segments falling into the interval of (the end time of the very latest segment - `skipOffsetFromLatest`).
This is to avoid conflicts between compaction tasks and realtime tasks.
Note that realtime tasks have a higher priority than compaction tasks by default. Realtime tasks will revoke the locks of compaction tasks if their intervals overlap, resulting in the termination of the compaction task.

<div class="note caution">
This policy currently cannot handle the situation when there are a lot of small segments which have the same interval,
and their total size exceeds <a href="../configuration/index.html#compaction-dynamic-configuration">inputSegmentSizeBytes</a>.
If it finds such segments, it simply skips them.
</div>

### The Coordinator Console

The Druid coordinator exposes a web GUI for displaying cluster information and rule configuration. After the coordinator starts, the console can be accessed at:

```
http://<COORDINATOR_IP>:<COORDINATOR_PORT>
```

 There exists a full cluster view (which shows only the realtime and historical nodes), as well as views for individual historical nodes, datasources and segments themselves. Segment information can be displayed in raw JSON form or as part of a sortable and filterable table.

The coordinator console also exposes an interface to creating and editing rules. All valid datasources configured in the segment database, along with a default datasource, are available for configuration. Rules of different types can be added, deleted or edited.

### FAQ

1. **Do clients ever contact the coordinator node?**

    The coordinator is not involved in a query.

    historical nodes never directly contact the coordinator node. The Druid coordinator tells the historical nodes to load/drop data via Zookeeper, but the historical nodes are completely unaware of the coordinator.

    Brokers also never contact the coordinator. Brokers base their understanding of the data topology on metadata exposed by the historical nodes via ZK and are completely unaware of the coordinator.

2. **Does it matter if the coordinator node starts up before or after other processes?**

    No. If the Druid coordinator is not started up, no new segments will be loaded in the cluster and outdated segments will not be dropped. However, the coordinator node can be started up at any time, and after a configurable delay, will start running coordinator tasks.

    This also means that if you have a working cluster and all of your coordinators die, the cluster will continue to function, it just won’t experience any changes to its data topology.
