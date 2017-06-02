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
