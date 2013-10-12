---
layout: doc_page
---
Coordinator
===========

The Druid coordinator node is primarily responsible for segment management and distribution. More specifically, the Druid coordinator node communicates to historical nodes to load or drop segments based on configurations. The Druid coordinator is responsible for loading new segments, dropping outdated segments, managing segment replication, and balancing segment load.

The Druid coordinator runs periodically and the time between each run is a configurable parameter. Each time the Druid coordinator runs, it assesses the current state of the cluster before deciding on the appropriate actions to take. Similar to the broker and historical nodes, the Druid coordinator maintains a connection to a Zookeeper cluster for current cluster information. The coordinator also maintains a connection to a database containing information about available segments and rules. Available segments are stored in a segment table and list all segments that should be loaded in the cluster. Rules are stored in a rule table and indicate how segments should be handled.

Before any unassigned segments are serviced by historical nodes, the available historical nodes for each tier are first sorted in terms of capacity, with least capacity servers having the highest priority. Unassigned segments are always assigned to the nodes with least capacity to maintain a level of balance between nodes. The coordinator does not directly communicate with a historical node when assigning it a new segment; instead the coordinator creates some temporary information about the new segment under load queue path of the historical node. Once this request is seen, the historical node will load the segment and begin servicing it.

Quick Start
-----------
Run:

```
io.druid.cli.Main server coordinator
```

With the following JVM configuration:

```
-server
-Xmx256m
-Duser.timezone=UTC
-Dfile.encoding=UTF-8

druid.host=localhost
druid.service=coordinator
druid.port=8082

druid.zk.service.host=localhost

druid.db.connector.connectURI=jdbc\:mysql\://localhost\:3306/druid
druid.db.connector.user=druid
druid.db.connector.password=diurd

druid.coordinator.startDelay=PT60s
```

JVM Configuration
-----------------

The coordinator module uses several of the default modules in [Configuration](Configuration.html) and has the following set of configurations as well:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.coordinator.period`|The run period for the coordinator. The coordinator’s operates by maintaining the current state of the world in memory and periodically looking at the set of segments available and segments being served to make decisions about whether any changes need to be made to the data topology. This property sets the delay between each of these runs.|PT60S|
|`druid.coordinator.period.indexingPeriod`|How often to send indexing tasks to the indexing service. Only applies if merge or conversion is turned on.|PT1800S (30 mins)|
|`druid.coordinator.removedSegmentLifetime`|When a node disappears, the coordinator can provide a grace period for how long it waits before deciding that the node really isn’t going to come back and it really should declare that all segments from that node are no longer available. This sets that grace period in number of runs of the coordinator.|1|
|`druid.coordinator.startDelay`|The operation of the Coordinator works on the assumption that it has an up-to-date view of the state of the world when it runs, the current ZK interaction code, however, is written in a way that doesn’t allow the Coordinator to know for a fact that it’s done loading the current state of the world. This delay is a hack to give it enough time to believe that it has all the data.|PT300S|
|`druid.coordinator.merge.on`|Boolean flag for whether or not the coordinator should try and merge small segments into a more optimal segment size.|PT300S|
|`druid.coordinator.conversion.on`|Boolean flag for converting old segment indexing versions to the latest segment indexing version.|false|
|`druid.coordinator.load.timeout`|The timeout duration for when the coordinator assigns a segment to a historical node.|15 minutes|
|`druid.manager.segment.pollDuration`|The duration between polls the Coordinator does for updates to the set of active segments. Generally defines the amount of lag time it can take for the coordinator to notice new segments.|PT1M|
|`druid.manager.rules.pollDuration`|The duration between polls the Coordinator does for updates to the set of active rules. Generally defines the amount of lag time it can take for the coordinator to notice rules.|PT1M|
|`druid.manager.rules.defaultTier`|The default tier from which default rules will be loaded from.|_default|

Dynamic Configuration
---------------------

The coordinator has dynamic configuration to change certain behaviour on the fly. The coordinator a JSON spec object from the Druid [MySQL](MySQL.html) config table. This object is detailed below:

It is recommended that you use the Coordinator Console to configure these parameters. However, if you need to do it via HTTP, the JSON object can be submitted to the overlord via a POST request at:

```
http://<COORDINATOR_IP>:<PORT>/coordinator/config
```

A sample worker setup spec is shown below:

```json
{
  "millisToWaitBeforeDeleting": 900000,
  "mergeBytesLimit": 100000000L,
  "mergeSegmentsLimit" : 1000,
  "maxSegmentsToMove": 5,
  "replicantLifetime": 15,
  "replicationThrottleLimit": 10,
  "emitBalancingStats": false
}
```

Issuing a GET request at the same URL will return the spec that is currently in place. A description of the config setup spec is shown below.

|Property|Description|Default|
|--------|-----------|-------|
|`millisToWaitBeforeDeleting`|How long does the coordinator need to be active before it can start deleting segments.|90000 (15 mins)|
|`mergeBytesLimit`|The maximum number of bytes to merge (for segments).|100000000L|
|`mergeSegmentsLimit`|The maximum number of segments that can be in a single merge [task](Tasks.html).|Integer.MAX_VALUE|
|`maxSegmentsToMove`|The maximum number of segments that can be moved at any given time.|5|
|`replicantLifetime`|The maximum number of coordinator runs for a segment to be replicated before we start alerting.|15|
|`replicationThrottleLimit`|The maximum number of segments that can be replicated at one time.|10|
|`emitBalancingStats`|Boolean flag for whether or not we should emit balancing stats. This is an expensive operation.|false|

### Running

```
io.druid.cli.Main server coordinator
```

Rules
-----

Segments are loaded and dropped from the cluster based on a set of rules. Rules indicate how segments should be assigned to different historical node tiers and how many replicants of a segment should exist in each tier. Rules may also indicate when segments should be dropped entirely from the cluster. The coordinator loads a set of rules from the database. Rules may be specific to a certain datasource and/or a default set of rules can be configured. Rules are read in order and hence the ordering of rules is important. The coordinator will cycle through all available segments and match each segment with the first rule that applies. Each segment may only match a single rule

For more information on rules, see [Rule Configuration](Rule-Configuration.html).

Cleaning Up Segments
--------------------

Each run, the Druid coordinator compares the list of available database segments in the database with the current segments in the cluster. Segments that are not in the database but are still being served in the cluster are flagged and appended to a removal list. Segments that are overshadowed (their versions are too old and their data has been replaced by newer segments) are also dropped.

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

* `/info/coordinator`

    Returns the current true coordinator of the cluster as a JSON object.

* `/info/cluster`

    Returns JSON data about every node and segment in the cluster.  Information about each node and each segment on each node will be returned.

* `/info/servers`

    Returns information about servers in the cluster.  Set the `?full` query parameter to get full metadata about all servers and their segments in the cluster.

* `/info/servers/{serverName}`

    Returns full metadata about a specific server.

* `/info/servers/{serverName}/segments`

    Returns a list of all segments for a server.  Set the `?full` query parameter to get all segment metadata included

* `/info/servers/{serverName}/segments/{segmentId}`

    Returns full metadata for a specific segment.

* `/info/segments`

    Returns all segments in the cluster as a list.  Set the `?full` flag to get all metadata about segments in the cluster

* `/info/segments/{segmentId}`

    Returns full metadata for a specific segment

* `/info/datasources`

    Returns a list of datasources in the cluster.  Set the `?full` flag to get all metadata for every datasource in the cluster

* `/info/datasources/{dataSourceName}`

    Returns full metadata for a datasource

* `/info/datasources/{dataSourceName}/segments`

    Returns a list of all segments for a datasource.  Set the `?full` flag to get full segment metadata for a datasource

* `/info/datasources/{dataSourceName}/segments/{segmentId}`

    Returns full segment metadata for a specific segment

* `/info/rules`

    Returns all rules for all data sources in the cluster including the default datasource.

* `/info/rules/{dataSourceName}` 

    Returns all rules for a specified datasource

### POST

* `/info/rules/{dataSourceName}`

    POST with a list of rules in JSON form to update rules.

The Coordinator Console
------------------

The Druid coordinator exposes a web GUI for displaying cluster information and rule configuration. After the coordinator starts, the console can be accessed at:

```
http://<COORDINATOR_IP>:<COORDINATOR_PORT>
```

 There exists a full cluster view, as well as views for individual historical nodes, datasources and segments themselves. Segment information can be displayed in raw JSON form or as part of a sortable and filterable table.

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