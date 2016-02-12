---
layout: doc_page
---
Broker
======
For Broker Node Configuration, see [Broker Configuration](../configuration/broker.html).

The Broker is the node to route queries to if you want to run a distributed cluster. It understands the metadata published to ZooKeeper about what segments exist on what nodes and routes queries such that they hit the right nodes. This node also merges the result sets from all of the individual nodes together.
On start up, Realtime nodes announce themselves and the segments they are serving in Zookeeper. 

Running
-------

```
io.druid.cli.Main server broker
```

Forwarding Queries
------------------

Most druid queries contain an interval object that indicates a span of time for which data is requested. Likewise, Druid [Segments](../design/segments.html) are partitioned to contain data for some interval of time and segments are distributed across a cluster. Consider a simple datasource with 7 segments where each segment contains data for a given day of the week. Any query issued to the datasource for more than one day of data will hit more than one segment. These segments will likely be distributed across multiple nodes, and hence, the query will likely hit multiple nodes.

To determine which nodes to forward queries to, the Broker node first builds a view of the world from information in Zookeeper. Zookeeper maintains information about [Historical](../design/historical.html) and [Realtime](../design/realtime.html) nodes and the segments they are serving. For every datasource in Zookeeper, the Broker node builds a timeline of segments and the nodes that serve them. When queries are received for a specific datasource and interval, the Broker node performs a lookup into the timeline associated with the query datasource for the query interval and retrieves the nodes that contain data for the query. The Broker node then forwards down the query to the selected nodes.

Caching
-------

Broker nodes employ a cache with a LRU cache invalidation strategy. The broker cache stores per-segment results. The cache can be local to each broker node or shared across multiple nodes using an external distributed cache such as [memcached](http://memcached.org/). Each time a broker node receives a query, it ﬁrst maps the query to a set of segments. A subset of these segment results may already exist in the cache and the results can be directly pulled from the cache. For any segment results that do not exist in the cache, the broker node will forward the query to the
historical nodes. Once the historical nodes return their results, the broker will store those results in the cache. Real-time segments are never cached and hence requests for real-time data will always be forwarded to real-time nodes. Real-time data is perpetually changing and caching the results would be unreliable.

HTTP Endpoints
--------------

The broker node exposes several HTTP endpoints for interactions.

### GET

* `/status`

Returns the Druid version, loaded extensions, memory used, total memory and other useful information about the node.

* `/druid/v2/datasources`

Returns a list of queryable datasources.

* `/druid/v2/datasources/{dataSourceName}`

Returns the dimensions and metrics of the datasource. Optionally, you can provide request parameter "full" to get list of served intervals with dimensions and metrics being served for those intervals. You can also provide request param "interval" explicitly to refer to a particular interval.

If no interval is specified, a default interval spanning a configurable period before the current time will be used. The duration of this interval is specified in ISO8601 format via:

druid.query.segmentMetadata.defaultHistory

* `/druid/v2/datasources/{dataSourceName}/dimensions`

Returns the dimensions of the datasource.

* `/druid/v2/datasources/{dataSourceName}/metrics`

Returns the metrics of the datasource.

* `/druid/broker/v1/loadstatus`

Returns a flag indicating if the broker knows about all segments in Zookeeper. This can be used to know when a broker node is ready to be queried after a restart.

