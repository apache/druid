---
layout: default
---
Master
======

The Druid master node is primarily responsible for segment management and distribution. More specifically, the Druid master node communicates to compute nodes to load or drop segments based on configurations. The Druid master is responsible for loading new segments, dropping outdated segments, managing segment replication, and balancing segment load.

The Druid master runs periodically and the time between each run is a configurable parameter. Each time the Druid master runs, it assesses the current state of the cluster before deciding on the appropriate actions to take. Similar to the broker and compute nodes, the Druid master maintains a connection to a Zookeeper cluster for current cluster information. The master also maintains a connection to a database containing information about available segments and rules. Available segments are stored in a segment table and list all segments that should be loaded in the cluster. Rules are stored in a rule table and indicate how segments should be handled.

Before any unassigned segments are serviced by compute nodes, the available compute nodes for each tier are first sorted in terms of capacity, with least capacity servers having the highest priority. Unassigned segments are always assigned to the nodes with least capacity to maintain a level of balance between nodes. The master does not directly communicate with a compute node when assigning it a new segment; instead the master creates some temporary information about the new segment under load queue path of the compute node. Once this request is seen, the compute node will load the segment and begin servicing it.

Rules
-----

Segments are loaded and dropped from the cluster based on a set of rules. Rules indicate how segments should be assigned to different compute node tiers and how many replicants of a segment should exist in each tier. Rules may also indicate when segments should be dropped entirely from the cluster. The master loads a set of rules from the database. Rules may be specific to a certain datasource and/or a default set of rules can be configured. Rules are read in order and hence the ordering of rules is important. The master will cycle through all available segments and match each segment with the first rule that applies. Each segment may only match a single rule

For more information on rules, see [Rule Configuration](Rule Configuration.html).

Cleaning Up Segments
--------------------

Each run, the Druid master compares the list of available database segments in the database with the current segments in the cluster. Segments that are not in the database but are still being served in the cluster are flagged and appended to a removal list. Segments that are overshadowed (their versions are too old and their data has been replaced by newer segments) are also dropped.

Segment Availability
--------------------

If a compute node restarts or becomes unavailable for any reason, the Druid master will notice a node has gone missing and treat all segments served by that node as being dropped. Given a sufficient period of time, the segments may be reassigned to other compute nodes in the cluster. However, each segment that is dropped is not immediately forgotten. Instead, there is a transitional data structure that stores all dropped segments with an associated lifetime. The lifetime represents a period of time in which the master will not reassign a dropped segment. Hence, if a compute node becomes unavailable and available again within a short period of time, the compute node will start up and serve segments from its cache without any those segments being reassigned across the cluster.

Balancing Segment Load
----------------------

To ensure an even distribution of segments across compute nodes in the cluster, the master node will find the total size of all segments being served by every compute node each time the master runs. For every compute node tier in the cluster, the master node will determine the compute node with the highest utilization and the compute node with the lowest utilization. The percent difference in utilization between the two nodes is computed, and if the result exceeds a certain threshold, a number of segments will be moved from the highest utilized node to the lowest utilized node. There is a configurable limit on the number of segments that can be moved from one node to another each time the master runs. Segments to be moved are selected at random and only moved if the resulting utilization calculation indicates the percentage difference between the highest and lowest servers has decreased.

HTTP Endpoints
--------------

The master node exposes several HTTP endpoints for interactions.

### GET

/info/master - returns the current true master of the cluster as a JSON object. E.g. A GET request to <IP>:8080/info/master will yield JSON of the form {[host]("IP"})

/info/cluster - returns JSON data about every node and segment in the cluster. E.g. A GET request to <IP>:8080/info/cluster will yield JSON data organized by nodes. Information about each node and each segment on each node will be returned.

/info/servers (optional param ?full) - returns all segments in the cluster if the full flag is not set, otherwise returns full metadata about all servers in the cluster

/info/servers/{serverName} - returns full metadata about a specific server

/info/servers/{serverName}/segments (optional param ?full) - returns a list of all segments for a server if the full flag is not set, otherwise returns all segment metadata

/info/servers/{serverName}/segments/{segmentId} - returns full metadata for a specific segment

/info/segments (optional param ?full)- returns all segments in the cluster as a list if the full flag is not set, otherwise returns all metadata about segments in the cluster

/info/segments/{segmentId} - returns full metadata for a specific segment

/info/datasources (optional param ?full) - returns a list of datasources in the cluster if the full flag is not set, otherwise returns all the metadata for every datasource in the cluster

/info/datasources/{dataSourceName} - returns full metadata for a datasource

/info/datasources/{dataSourceName}/segments (optional param ?full) - returns a list of all segments for a datasource if the full flag is not set, otherwise returns full segment metadata for a datasource

/info/datasources/{dataSourceName}/segments/{segmentId} - returns full segment metadata for a specific segment

/info/rules - returns all rules for all data sources in the cluster including the default datasource.

/info/rules/{dataSourceName} - returns all rules for a specified datasource

### POST

/info/rules/{dataSourceName} - POST with a list of rules in JSON form to update rules.

The Master Console
------------------

The Druid master exposes a web GUI for displaying cluster information and rule configuration. After the master starts, the console can be accessed at http://HOST:PORT/static/. There exists a full cluster view, as well as views for individual compute nodes, datasources and segments themselves. Segment information can be displayed in raw JSON form or as part of a sortable and filterable table.

The master console also exposes an interface to creating and editing rules. All valid datasources configured in the segment database, along with a default datasource, are available for configuration. Rules of different types can be added, deleted or edited.

FAQ
---

1. **Do clients ever contact the master node?**

The master is not involved in the lifecycle of a query.

Compute nodes never directly contact the master node. The Druid master tells the compute nodes to load/drop data via Zookeeper, but the compute nodes are completely unaware of the master.

Brokers also never contact the master. Brokers base their understanding of the data topology on metadata exposed by the compute nodes via ZK and are completely unaware of the master.

2. **Does it matter if the master node starts up before or after other processes?**

No. If the Druid master is not started up, no new segments will be loaded in the cluster and outdated segments will not be dropped. However, the master node can be started up at any time, and after a configurable delay, will start running master tasks.

This also means that if you have a working cluster and all of your masters die, the cluster will continue to function, it just won’t experience any changes to its data topology.

Running
-------

Master nodes can be run using the `com.metamx.druid.http.MasterMain` class.

Configuration
-------------

See [Configuration](Configuration.html).
