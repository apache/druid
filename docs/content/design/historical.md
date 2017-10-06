---
layout: doc_page
---
Historical Node
===============
For Historical Node Configuration, see [Historial Configuration](../configuration/historical.html).

Historical nodes load up historical segments and expose them for querying.

Running
-------

```
io.druid.cli.Main server historical
```

Loading and Serving Segments
----------------------------

Each historical node maintains a constant connection to Zookeeper and watches a configurable set of Zookeeper paths for new segment information. Historical nodes do not communicate directly with each other or with the coordinator nodes but instead rely on Zookeeper for coordination.

The [Coordinator](../design/coordinator.html) node is responsible for assigning new segments to historical nodes. Assignment is done by creating an ephemeral Zookeeper entry under a load queue path associated with a historical node. For more information on how the coordinator assigns segments to historical nodes, please see [Coordinator](../design/coordinator.html).

When a historical node notices a new load queue entry in its load queue path, it will first check a local disk directory (cache) for the information about segment. If no information about the segment exists in the cache, the historical node will download metadata about the new segment to serve from Zookeeper. This metadata includes specifications about where the segment is located in deep storage and about how to decompress and process the segment. For more information about segment metadata and Druid segments in general, please see [Segments](../design/segments.html). Once a historical node completes processing a segment, the segment is announced in Zookeeper under a served segments path associated with the node. At this point, the segment is available for querying.

Loading and Serving Segments From Cache
---------------------------------------

Recall that when a historical node notices a new segment entry in its load queue path, the historical node first checks a configurable cache directory on its local disk to see if the segment had been previously downloaded. If a local cache entry already exists, the historical node will directly read the segment binary files from disk and load the segment.

The segment cache is also leveraged when a historical node is first started. On startup, a historical node will search through its cache directory and immediately load and serve all segments that are found. This feature allows historical nodes to be queried as soon they come online.

Querying Segments
-----------------

Please see [Querying](../querying/querying.html) for more information on querying historical nodes.

A historical can be configured to log and report metrics for every query it services.

HTTP Endpoints
--------------

The historical node exposes several HTTP endpoints for interactions.

### GET

* `/status`

Returns the Druid version, loaded extensions, memory used, total memory and other useful information about the node.

* `/druid/historical/v1/loadstatus`

Returns JSON of the form `{"cacheInitialized":<value>}`, where value is either `true` or `false` indicating if all
segments in the local cache have been loaded. This can be used to know when a historical node is ready
to be queried after a restart.

* `/druid/historical/v1/loadStatusCode`

Similar to `/druid/historical/v1/loadstatus`, but instead or returning JSON with a flag, responses 200 OK with an
empty JSON `{}` body, if segments in the local cache have been loaded, and 503 SERVICE UNAVAILABLE, if they haven't.
