---
layout: doc_page
---
Coordinator Node
================

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
segments which may degrade the query performance as well as increasing the disk usage. Note that the data for an interval
cannot be compacted across the segments.

The coordinator first finds the segments to compact together based on the [segment search policy](#segment-search-policy).
Once it finds some segments, it launches a [compact task](../ingestion/tasks.html#compaction-task) to compact those segments.
The maximum number of running compact tasks is `max(sum of worker capacity * slotRatio, maxSlots)`.
Note that even though `max(sum of worker capacity * slotRatio, maxSlots)` = 1, at least one compact task is always submitted
once a compaction is configured for a dataSource. See [Compaction Configuration API](../operations/api-reference.html#compaction-configuration) to set those values.

Compact tasks might fail due to some reasons.

- If the input segments of a compact task are removed or overshadowed before it starts, that compact task fails immediately.
- If a task of a higher priority acquires a lock for an interval overlapping with the interval of a compact task, the compact task fails.

Once a compact task fails, the coordinator simply finds the segments for the interval of the failed task again, and launches a new compact task in the next run.

To use this feature, you need to set some configurations for dataSources you want to compact.
Please see [Compaction Configuration](../configuration/index.html#compaction-dynamic-configuration) for more details.

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
