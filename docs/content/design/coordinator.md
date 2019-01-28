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

The Druid Coordinator node is primarily responsible for segment management and distribution. More specifically, the Druid Coordinator node communicates to Historical nodes to load or drop segments based on configurations. The Druid Coordinator is responsible for loading new segments, dropping outdated segments, managing segment replication, and balancing segment load.

The Druid Coordinator runs periodically and the time between each run is a configurable parameter. Each time the Druid Coordinator runs, it assesses the current state of the cluster before deciding on the appropriate actions to take. Similar to the Broker and Historical processses, the Druid Coordinator maintains a connection to a Zookeeper cluster for current cluster information. The Coordinator also maintains a connection to a database containing information about available segments and rules. Available segments are stored in a segment table and list all segments that should be loaded in the cluster. Rules are stored in a rule table and indicate how segments should be handled.

Before any unassigned segments are serviced by Historical nodes, the available Historical nodes for each tier are first sorted in terms of capacity, with least capacity servers having the highest priority. Unassigned segments are always assigned to the nodes with least capacity to maintain a level of balance between nodes. The Coordinator does not directly communicate with a historical node when assigning it a new segment; instead the Coordinator creates some temporary information about the new segment under load queue path of the historical node. Once this request is seen, the historical node will load the segment and begin servicing it.

### Running

```
org.apache.druid.cli.Main server coordinator
```

### Rules

Segments can be automatically loaded and dropped from the cluster based on a set of rules. For more information on rules, see [Rule Configuration](../operations/rule-configuration.html).

### Cleaning Up Segments

Each run, the Druid Coordinator compares the list of available database segments in the database with the current segments in the cluster. Segments that are not in the database but are still being served in the cluster are flagged and appended to a removal list. Segments that are overshadowed (their versions are too old and their data has been replaced by newer segments) are also dropped.
Note that if all segments in database are deleted(or marked unused), then Coordinator will not drop anything from the Historicals. This is done to prevent a race condition in which the Coordinator would drop all segments if it started running cleanup before it finished polling the database for available segments for the first time and believed that there were no segments.

### Segment Availability

If a Historical node restarts or becomes unavailable for any reason, the Druid Coordinator will notice a node has gone missing and treat all segments served by that node as being dropped. Given a sufficient period of time, the segments may be reassigned to other Historical nodes in the cluster. However, each segment that is dropped is not immediately forgotten. Instead, there is a transitional data structure that stores all dropped segments with an associated lifetime. The lifetime represents a period of time in which the Coordinator will not reassign a dropped segment. Hence, if a historical node becomes unavailable and available again within a short period of time, the historical node will start up and serve segments from its cache without any those segments being reassigned across the cluster.

### Balancing Segment Load

To ensure an even distribution of segments across Historical nodes in the cluster, the Coordinator node will find the total size of all segments being served by every Historical node each time the Coordinator runs. For every Historical node tier in the cluster, the Coordinator node will determine the Historical node with the highest utilization and the Historical node with the lowest utilization. The percent difference in utilization between the two nodes is computed, and if the result exceeds a certain threshold, a number of segments will be moved from the highest utilized node to the lowest utilized node. There is a configurable limit on the number of segments that can be moved from one node to another each time the Coordinator runs. Segments to be moved are selected at random and only moved if the resulting utilization calculation indicates the percentage difference between the highest and lowest servers has decreased.

### Compacting Segments

Each run, the Druid Coordinator compacts small segments abutting each other. This is useful when you have a lot of small
segments which may degrade the query performance as well as increasing the disk space usage.

The Coordinator first finds the segments to compact together based on the [segment search policy](#segment-search-policy).
Once some segments are found, it launches a [compact task](../ingestion/tasks.html#compaction-task) to compact those segments.
The maximum number of running compact tasks is `min(sum of worker capacity * slotRatio, maxSlots)`.
Note that even though `min(sum of worker capacity * slotRatio, maxSlots)` = 0, at least one compact task is always submitted
if the compaction is enabled for a dataSource.
See [Compaction Configuration API](../operations/api-reference.html#compaction-configuration) and [Compaction Configuration](../configuration/index.html#compaction-dynamic-configuration) to enable the compaction.

Compact tasks might fail due to some reasons.

- If the input segments of a compact task are removed or overshadowed before it starts, that compact task fails immediately.
- If a task of a higher priority acquires a lock for an interval overlapping with the interval of a compact task, the compact task fails.

Once a compact task fails, the Coordinator simply finds the segments for the interval of the failed task again, and launches a new compact task in the next run.

### Segment Search Policy

#### Newest Segment First Policy

This policy searches the segments of _all dataSources_ in inverse order of their intervals.
For example, let me assume there are 3 dataSources (`ds1`, `ds2`, `ds3`) and 5 segments (`seg_ds1_2017-10-01_2017-10-02`, `seg_ds1_2017-11-01_2017-11-02`, `seg_ds2_2017-08-01_2017-08-02`, `seg_ds3_2017-07-01_2017-07-02`, `seg_ds3_2017-12-01_2017-12-02`) for those dataSources.
The segment name indicates its dataSource and interval. The search result of newestSegmentFirstPolicy is [`seg_ds3_2017-12-01_2017-12-02`, `seg_ds1_2017-11-01_2017-11-02`, `seg_ds1_2017-10-01_2017-10-02`, `seg_ds2_2017-08-01_2017-08-02`, `seg_ds3_2017-07-01_2017-07-02`].

Every run, this policy starts searching from the (very latest interval - [skipOffsetFromLatest](../configuration/index.html#compaction-dynamic-configuration)).
This is to handle the late segments ingested to realtime dataSources.

<div class="note caution">
This policy currently cannot handle the situation when there are a lot of small segments which have the same interval,
and their total size exceeds <a href="../configuration/index.html#compaction-dynamic-configuration">targetCompactionSizebytes</a>.
If it finds such segments, it simply skips compacting them.
</div>

### The Coordinator Console

The Druid Coordinator exposes a web GUI for displaying cluster information and rule configuration. After the Coordinator starts, the console can be accessed at:

```
http://<COORDINATOR_IP>:<COORDINATOR_PORT>
```

 There exists a full cluster view (which shows only the realtime and Historical nodes), as well as views for individual Historical nodes, datasources and segments themselves. Segment information can be displayed in raw JSON form or as part of a sortable and filterable table.

The Coordinator console also exposes an interface to creating and editing rules. All valid datasources configured in the segment database, along with a default datasource, are available for configuration. Rules of different types can be added, deleted or edited.

### FAQ

1. **Do clients ever contact the Coordinator node?**

    The Coordinator is not involved in a query.

    Historical nodes never directly contact the Coordinator node. The Druid Coordinator tells the Historical nodes to load/drop data via Zookeeper, but the Historical nodes are completely unaware of the Coordinator.

    Brokers also never contact the Coordinator. Brokers base their understanding of the data topology on metadata exposed by the Historical nodes via ZK and are completely unaware of the Coordinator.

2. **Does it matter if the Coordinator node starts up before or after other processes?**

    No. If the Druid Coordinator is not started up, no new segments will be loaded in the cluster and outdated segments will not be dropped. However, the Coordinator node can be started up at any time, and after a configurable delay, will start running Coordinator tasks.

    This also means that if you have a working cluster and all of your Coordinators die, the cluster will continue to function, it just won’t experience any changes to its data topology.
