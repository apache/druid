---
layout: doc_page
---
Compute
=======

Compute nodes are the work horses of a cluster. They load up historical segments and expose them for querying.

Loading and Serving Segments
----------------------------

Each compute node maintains a constant connection to Zookeeper and watches a configurable set of Zookeeper paths for new segment information. Compute nodes do not communicate directly with each other or with the master nodes but instead rely on Zookeeper for coordination.

The [Master](Master.html) node is responsible for assigning new segments to compute nodes. Assignment is done by creating an ephemeral Zookeeper entry under a load queue path associated with a compute node. For more information on how the master assigns segments to compute nodes, please see [Master](Master.html).

When a compute node notices a new load queue entry in its load queue path, it will first check a local disk directory (cache) for the information about segment. If no information about the segment exists in the cache, the compute node will download metadata about the new segment to serve from Zookeeper. This metadata includes specifications about where the segment is located in deep storage and about how to decompress and process the segment. For more information about segment metadata and Druid segments in general, please see [Segments](Segments.html). Once a compute node completes processing a segment, the segment is announced in Zookeeper under a served segments path associated with the node. At this point, the segment is available for querying.

Loading and Serving Segments From Cache
---------------------------------------

Recall that when a compute node notices a new segment entry in its load queue path, the compute node first checks a configurable cache directory on its local disk to see if the segment had been previously downloaded. If a local cache entry already exists, the compute node will directly read the segment binary files from disk and load the segment.

The segment cache is also leveraged when a compute node is first started. On startup, a compute node will search through its cache directory and immediately load and serve all segments that are found. This feature allows compute nodes to be queried as soon they come online.

Querying Segments
-----------------

Please see [Querying](Querying.html) for more information on querying compute nodes.

For every query that a compute node services, it will log the query and report metrics on the time taken to run the query.

Running
-------

Compute nodes can be run using the `com.metamx.druid.http.ComputeMain` class.

Configuration
-------------

See [Configuration](Configuration.html).
