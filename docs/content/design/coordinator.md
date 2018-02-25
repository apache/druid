---
layout: doc_page
---
Coordinator Node
================
For Coordinator Node Configuration, see [Coordinator Configuration](../configuration/coordinator.html).

The Druid coordinator node is primarily responsible for segment management and distribution. More specifically, the Druid coordinator node communicates to historical nodes to load or drop segments based on configurations. The Druid coordinator is responsible for loading new segments, dropping outdated segments, managing segment replication, and balancing segment load.

The Druid coordinator runs periodically and the time between each run is a configurable parameter. Each time the Druid coordinator runs, it assesses the current state of the cluster before deciding on the appropriate actions to take. Similar to the broker and historical nodes, the Druid coordinator maintains a connection to a Zookeeper cluster for current cluster information. The coordinator also maintains a connection to a database containing information about available segments and rules. Available segments are stored in a segment table and list all segments that should be loaded in the cluster. Rules are stored in a rule table and indicate how segments should be handled.

Before any unassigned segments are serviced by historical nodes, the available historical nodes for each tier are first sorted in terms of capacity, with least capacity servers having the highest priority. Unassigned segments are always assigned to the nodes with least capacity to maintain a level of balance between nodes. The coordinator does not directly communicate with a historical node when assigning it a new segment; instead the coordinator creates some temporary information about the new segment under load queue path of the historical node. Once this request is seen, the historical node will load the segment and begin servicing it.

### Running

```
io.druid.cli.Main server coordinator
```

Rules
-----

Segments can be automatically loaded and dropped from the cluster based on a set of rules. For more information on rules, see [Rule Configuration](../operations/rule-configuration.html).

Cleaning Up Segments
--------------------

Each run, the Druid coordinator compares the list of available database segments in the database with the current segments in the cluster. Segments that are not in the database but are still being served in the cluster are flagged and appended to a removal list. Segments that are overshadowed (their versions are too old and their data has been replaced by newer segments) are also dropped.
Note that if all segments in database are deleted(or marked unused), then coordinator will not drop anything from the historicals. This is done to prevent a race condition in which the coordinator would drop all segments if it started running cleanup before it finished polling the database for available segments for the first time and believed that there were no segments.

Segment Availability
--------------------

If a historical node restarts or becomes unavailable for any reason, the Druid coordinator will notice a node has gone missing and treat all segments served by that node as being dropped. Given a sufficient period of time, the segments may be reassigned to other historical nodes in the cluster. However, each segment that is dropped is not immediately forgotten. Instead, there is a transitional data structure that stores all dropped segments with an associated lifetime. The lifetime represents a period of time in which the coordinator will not reassign a dropped segment. Hence, if a historical node becomes unavailable and available again within a short period of time, the historical node will start up and serve segments from its cache without any those segments being reassigned across the cluster.

Balancing Segment Load
----------------------

To ensure an even distribution of segments across historical nodes in the cluster, the coordinator node will find the total size of all segments being served by every historical node each time the coordinator runs. For every historical node tier in the cluster, the coordinator node will determine the historical node with the highest utilization and the historical node with the lowest utilization. The percent difference in utilization between the two nodes is computed, and if the result exceeds a certain threshold, a number of segments will be moved from the highest utilized node to the lowest utilized node. There is a configurable limit on the number of segments that can be moved from one node to another each time the coordinator runs. Segments to be moved are selected at random and only moved if the resulting utilization calculation indicates the percentage difference between the highest and lowest servers has decreased.

Compacting Segments
-------------------

Each run, the Druid coordinator compacts small segments abutting each other. This is useful when you have a lot of small
segments which may degrade the query performance as well as increasing the disk usage. Note that the data for an interval
cannot be compacted across the segments.

The coordinator first finds the segments to compact together based on the [segment search policy](#segment-search-policy).
Once it finds some segments, it launches a [compact task](../ingestion/tasks.html#compaction-task) to compact those segments.
The maximum number of running compact tasks is `max(sum of worker capacity * slotRatio, maxSlots)`.
Note that even though `max(sum of worker capacity * slotRatio, maxSlots)` = 1, at least one compact task is always submitted
once a compaction is configured for a dataSource. See [HTTP Endpoints](#http-endpoints) to set those values.

Compact tasks might fail due to some reasons.

- If the input segments of a compact task are removed or overshadowed before it starts, that compact task fails immediately.
- If a task of a higher priority acquires a lock for an interval overlapping with the interval of a compact task, the compact task fails.

Once a compact task fails, the coordinator simply finds the segments for the interval of the failed task again, and launches a new compact task in the next run.

To use this feature, you need to set some configurations for dataSources you want to compact.
Please see [Compaction Configuration](../configuration/coordinator.html#compaction-configuration) for more details.

### Segment Search Policy

#### Newest Segment First Policy

This policy searches the segments of _all dataSources_ in inverse order of their intervals.
For example, let me assume there are 3 dataSources (`ds1`, `ds2`, `ds3`) and 5 segments (`seg_ds1_2017-10-01_2017-10-02`, `seg_ds1_2017-11-01_2017-11-02`, `seg_ds2_2017-08-01_2017-08-02`, `seg_ds3_2017-07-01_2017-07-02`, `seg_ds3_2017-12-01_2017-12-02`) for those dataSources.
The segment name indicates its dataSource and interval. The search result of newestSegmentFirstPolicy is [`seg_ds3_2017-12-01_2017-12-02`, `seg_ds1_2017-11-01_2017-11-02`, `seg_ds1_2017-10-01_2017-10-02`, `seg_ds2_2017-08-01_2017-08-02`, `seg_ds3_2017-07-01_2017-07-02`].

Every run, this policy starts searching from the (very latest interval - [skipOffsetFromLatest](../configuration/coordinator.html#compaction-configuration)).
This is to handle the late segments ingested to realtime dataSources.

<div class="note caution">
This policy currently cannot handle the situation when there are a lot of small segments which have the same interval,
and their total size exceeds <a href="../configuration/coordinator.html#compaction-config">targetCompactionSizebytes</a>.
If it finds such segments, it simply skips compacting them.
</div>


HTTP Endpoints
--------------

The coordinator node exposes several HTTP endpoints for interactions.

### GET

* `/status`

Returns the Druid version, loaded extensions, memory used, total memory and other useful information about the node.

#### Coordinator information

* `/druid/coordinator/v1/leader`

Returns the current leader coordinator of the cluster.

* `/druid/coordinator/v1/isLeader`

Returns a JSON object with field "leader", either true or false, indicating if this server is the current leader
coordinator of the cluster. In addition, returns HTTP 200 if the server is the current leader and HTTP 404 if not.
This is suitable for use as a load balancer status check if you only want the active leader to be considered in-service
at the load balancer.

* `/druid/coordinator/v1/loadstatus`

Returns the percentage of segments actually loaded in the cluster versus segments that should be loaded in the cluster.

 * `/druid/coordinator/v1/loadstatus?simple`

Returns the number of segments left to load until segments that should be loaded in the cluster are available for queries. This does not include replication.

* `/druid/coordinator/v1/loadstatus?full`

Returns the number of segments left to load in each tier until segments that should be loaded in the cluster are all available. This includes replication.

* `/druid/coordinator/v1/loadqueue`

Returns the ids of segments to load and drop for each historical node.

* `/druid/coordinator/v1/loadqueue?simple`

Returns the number of segments to load and drop, as well as the total segment load and drop size in bytes for each historical node.

* `/druid/coordinator/v1/loadqueue?full`

Returns the serialized JSON of segments to load and drop for each historical node.

#### Metadata store information

* `/druid/coordinator/v1/metadata/datasources`

Returns a list of the names of enabled datasources in the cluster.

* `/druid/coordinator/v1/metadata/datasources?includeDisabled`

Returns a list of the names of enabled and disabled datasources in the cluster.

* `/druid/coordinator/v1/metadata/datasources?full`

Returns a list of all enabled datasources with all metadata about those datasources as stored in the metadata store.

* `/druid/coordinator/v1/metadata/datasources/{dataSourceName}`

Returns full metadata for a datasource as stored in the metadata store.

* `/druid/coordinator/v1/metadata/datasources/{dataSourceName}/segments`

Returns a list of all segments for a datasource as stored in the metadata store.

* `/druid/coordinator/v1/metadata/datasources/{dataSourceName}/segments?full`

Returns a list of all segments for a datasource with the full segment metadata as stored in the metadata store.

* POST `/druid/coordinator/v1/metadata/datasources/{dataSourceName}/segments`

Returns a list of all segments, overlapping with any of given intervals,  for a datasource as stored in the metadata store. Request body is array of string intervals like [interval1, interval2,...] for example ["2012-01-01T00:00:00.000/2012-01-03T00:00:00.000", "2012-01-05T00:00:00.000/2012-01-07T00:00:00.000"]

* POST `/druid/coordinator/v1/metadata/datasources/{dataSourceName}/segments?full`

Returns a list of all segments, overlapping with any of given intervals, for a datasource with the full segment metadata as stored in the metadata store. Request body is array of string intervals like [interval1, interval2,...] for example ["2012-01-01T00:00:00.000/2012-01-03T00:00:00.000", "2012-01-05T00:00:00.000/2012-01-07T00:00:00.000"]

* `/druid/coordinator/v1/metadata/datasources/{dataSourceName}/segments/{segmentId}`

Returns full segment metadata for a specific segment as stored in the metadata store.

#### Datasources information

* `/druid/coordinator/v1/datasources`

Returns a list of datasource names found in the cluster.

* `/druid/coordinator/v1/datasources?simple`

Returns a list of JSON objects containing the name and properties of datasources found in the cluster.  Properties include segment count, total segment byte size, minTime, and maxTime.

* `/druid/coordinator/v1/datasources?full`

Returns a list of datasource names found in the cluster with all metadata about those datasources.

* `/druid/coordinator/v1/datasources/{dataSourceName}`

Returns a JSON object containing the name and properties of a datasource. Properties include segment count, total segment byte size, minTime, and maxTime.

* `/druid/coordinator/v1/datasources/{dataSourceName}?full`

Returns full metadata for a datasource .

* `/druid/coordinator/v1/datasources/{dataSourceName}/intervals`

Returns a set of segment intervals.

* `/druid/coordinator/v1/datasources/{dataSourceName}/intervals?simple`

Returns a map of an interval to a JSON object containing the total byte size of segments and number of segments for that interval.

* `/druid/coordinator/v1/datasources/{dataSourceName}/intervals?full`

Returns a map of an interval to a map of segment metadata to a set of server names that contain the segment for that interval.

* `/druid/coordinator/v1/datasources/{dataSourceName}/intervals/{interval}`

Returns a set of segment ids for an ISO8601 interval. Note that {interval} parameters are delimited by a `_` instead of a `/` (e.g., 2016-06-27_2016-06-28).

* `/druid/coordinator/v1/datasources/{dataSourceName}/intervals/{interval}?simple`

Returns a map of segment intervals contained within the specified interval to a JSON object containing the total byte size of segments and number of segments for an interval.

* `/druid/coordinator/v1/datasources/{dataSourceName}/intervals/{interval}?full`

Returns a map of segment intervals contained within the specified interval to a map of segment metadata to a set of server names that contain the segment for an interval.

* `/druid/coordinator/v1/datasources/{dataSourceName}/intervals/{interval}/serverview`

Returns a map of segment intervals contained within the specified interval to information about the servers that contain the segment for an interval.

* `/druid/coordinator/v1/datasources/{dataSourceName}/segments`

Returns a list of all segments for a datasource in the cluster.

* `/druid/coordinator/v1/datasources/{dataSourceName}/segments?full`

Returns a list of all segments for a datasource in the cluster with the full segment metadata.

* `/druid/coordinator/v1/datasources/{dataSourceName}/segments/{segmentId}`

Returns full segment metadata for a specific segment in the cluster.

* `/druid/coordinator/v1/datasources/{dataSourceName}/tiers`

Return the tiers that a datasource exists in.

#### Rules

* `/druid/coordinator/v1/rules`

Returns all rules as JSON objects for all datasources in the cluster including the default datasource.

* `/druid/coordinator/v1/rules/{dataSourceName}`

Returns all rules for a specified datasource.


* `/druid/coordinator/v1/rules/{dataSourceName}?full`

Returns all rules for a specified datasource and includes default datasource.

* `/druid/coordinator/v1/rules/history?interval=<interval>`

 Returns audit history of rules for all datasources. default value of interval can be specified by setting `druid.audit.manager.auditHistoryMillis` (1 week if not configured) in coordinator runtime.properties

* `/druid/coordinator/v1/rules/history?count=<n>`

 Returns last <n> entries of audit history of rules for all datasources.

* `/druid/coordinator/v1/rules/{dataSourceName}/history?interval=<interval>`

 Returns audit history of rules for a specified datasource. default value of interval can be specified by setting `druid.audit.manager.auditHistoryMillis` (1 week if not configured) in coordinator runtime.properties

* `/druid/coordinator/v1/rules/{dataSourceName}/history?count=<n>`

 Returns last <n> entries of audit history of rules for a specified datasource.

#### Intervals

Note that {interval} parameters are delimited by a `_` instead of a `/` (e.g., 2016-06-27_2016-06-28).

* `/druid/coordinator/v1/intervals`

Returns all intervals for all datasources with total size and count.

* `/druid/coordinator/v1/intervals/{interval}`

Returns aggregated total size and count for all intervals that intersect given isointerval.

* `/druid/coordinator/v1/intervals/{interval}?simple`

Returns total size and count for each interval within given isointerval.

* `/druid/coordinator/v1/intervals/{interval}?full`

Returns total size and count for each datasource for each interval within given isointerval.

#### Compaction Configs

* `/druid/coordinator/v1/config/compaction/`

Returns all compaction configs.

* `/druid/coordinator/v1/config/compaction/{dataSource}`

Returns a compaction config of a dataSource.

#### Servers

* `/druid/coordinator/v1/servers`

Returns a list of servers URLs using the format `{hostname}:{port}`. Note that
nodes that run with different types will appear multiple times with different
ports.

* `/druid/coordinator/v1/servers?simple`

Returns a list of server data objects in which each object has the following keys:
- `host`: host URL include (`{hostname}:{port}`)
- `type`: node type (`indexer-executor`, `historical`)
- `currSize`: storage size currently used
- `maxSize`: maximum storage size
- `priority`
- `tier`

### POST

#### Datasources

* `/druid/coordinator/v1/datasources/{dataSourceName}`

Enables all segments of datasource which are not overshadowed by others.

* `/druid/coordinator/v1/datasources/{dataSourceName}/segments/{segmentId}`

Enables a segment.

#### Rules

* `/druid/coordinator/v1/rules/{dataSourceName}`

POST with a list of rules in JSON form to update rules.

Optional Header Parameters for auditing the config change can also be specified.

|Header Param Name| Description | Default |
|----------|-------------|---------|
|`X-Druid-Author`| author making the config change|""|
|`X-Druid-Comment`| comment describing the change being done|""|

#### Compaction Configs

* `/druid/coordinator/v1/config/compaction?slotRatio={someRatio}&maxSlots={someMaxSlots}`

Update the capacity for compaction tasks. `slotRatio` and `maxSlots` are used to limit the max number of compaction tasks.
They mean the ratio of the total task slots to the copmaction task slots and the maximum number of task slots for compaction tasks, respectively.
The actual max number of compaction tasks is `min(maxSlots, slotRatio * total task slots)`.
Note that `slotRatio` and `maxSlots` are optional and can be omitted. If they are omitted, default values (0.1 and unbounded)
will be set for them.

* `/druid/coordinator/v1/config/compaction/{dataSource}`

Creates or updates the compaction config for a dataSource. See [Compaction Configuration](../configuration/coordinator.html#compaction-configuration) for configuration details.

### DELETE

#### Datasources

* `/druid/coordinator/v1/datasources/{dataSourceName}`

Disables a datasource.

* `/druid/coordinator/v1/datasources/{dataSourceName}/intervals/{interval}`
* `@Deprecated. /druid/coordinator/v1/datasources/{dataSourceName}?kill=true&interval={myISO8601Interval}`

Runs a [Kill task](../ingestion/tasks.html) for a given interval and datasource.

Note that {interval} parameters are delimited by a `_` instead of a `/` (e.g., 2016-06-27_2016-06-28).

* `/druid/coordinator/v1/datasources/{dataSourceName}/segments/{segmentId}`

Disables a segment.

#### Compaction Configs

* `/druid/coordinator/v1/config/compaction/{dataSource}`

Removes the compaction config for a dataSource.

The Coordinator Console
------------------

The Druid coordinator exposes a web GUI for displaying cluster information and rule configuration. After the coordinator starts, the console can be accessed at:

```
http://<COORDINATOR_IP>:<COORDINATOR_PORT>
```

 There exists a full cluster view (which shows only the realtime and historical nodes), as well as views for individual historical nodes, datasources and segments themselves. Segment information can be displayed in raw JSON form or as part of a sortable and filterable table.

The coordinator console also exposes an interface to creating and editing rules. All valid datasources configured in the segment database, along with a default datasource, are available for configuration. Rules of different types can be added, deleted or edited.

FAQ
---

1. **Do clients ever contact the coordinator node?**

    The coordinator is not involved in a query.

    historical nodes never directly contact the coordinator node. The Druid coordinator tells the historical nodes to load/drop data via Zookeeper, but the historical nodes are completely unaware of the coordinator.

    Brokers also never contact the coordinator. Brokers base their understanding of the data topology on metadata exposed by the historical nodes via ZK and are completely unaware of the coordinator.

2. **Does it matter if the coordinator node starts up before or after other processes?**

    No. If the Druid coordinator is not started up, no new segments will be loaded in the cluster and outdated segments will not be dropped. However, the coordinator node can be started up at any time, and after a configurable delay, will start running coordinator tasks.

    This also means that if you have a working cluster and all of your coordinators die, the cluster will continue to function, it just won’t experience any changes to its data topology.
