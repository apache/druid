---
layout: doc_page
---
Broker
======

The Broker is the node to route queries to if you want to run a distributed cluster. It understands the metadata published to ZooKeeper about what segments exist on what nodes and routes queries such that they hit the right nodes. This node also merges the result sets from all of the individual nodes together.
On start up, Realtime nodes announce themselves and the segments they are serving in Zookeeper. 

Quick Start
-----------
Run:

```
io.druid.cli.Main server broker
```

With the following JVM configuration:

```
-server
-Xmx256m
-Duser.timezone=UTC
-Dfile.encoding=UTF-8

druid.host=localhost
druid.service=broker
druid.port=8080

druid.zk.service.host=localhost
```

JVM Configuration
-----------------

The broker module uses several of the default modules in [Configuration](Configuration.html) and has the following set of configurations as well:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.broker.cache.type`|Choices: local, memcache. The type of cache to use for queries.|local|

#### Local Cache

|Property|Description|Default|
|--------|-----------|-------|
|`druid.broker.cache.sizeInBytes`|Maximum size of the cache. If this is zero, cache is disabled.|0|
|`druid.broker.cache.initialSize`|The initial size of the cache in bytes.|500000|
|`druid.broker.cache.logEvictionCount`|If this is non-zero, there will be an eviction of entries.|0|

#### Memcache

|Property|Description|Default|
|--------|-----------|-------|
|`druid.broker.cache.expiration`|Memcache [expiration time ](https://code.google.com/p/memcached/wiki/NewCommands#Standard_Protocol).|2592000 (30 days)|
|`druid.broker.cache.timeout`|Maximum time in milliseconds to wait for a response from Memcache.|500|
|`druid.broker.cache.hosts`|Memcache hosts.|none|
|`druid.broker.cache.maxObjectSize`|Maximum object size in bytes for a Memcache object.|52428800 (50 MB)|
|`druid.broker.cache.memcachedPrefix`|Key prefix for all keys in Memcache.|druid|

Running
-------

```
io.druid.cli.Main server broker
```


Forwarding Queries
------------------

Most druid queries contain an interval object that indicates a span of time for which data is requested. Likewise, Druid [Segments](Segments.html) are partitioned to contain data for some interval of time and segments are distributed across a cluster. Consider a simple datasource with 7 segments where each segment contains data for a given day of the week. Any query issued to the datasource for more than one day of data will hit more than one segment. These segments will likely be distributed across multiple nodes, and hence, the query will likely hit multiple nodes.

To determine which nodes to forward queries to, the Broker node first builds a view of the world from information in Zookeeper. Zookeeper maintains information about [Historical](Historical.html) and [Realtime](Realtime.html) nodes and the segments they are serving. For every datasource in Zookeeper, the Broker node builds a timeline of segments and the nodes that serve them. When queries are received for a specific datasource and interval, the Broker node performs a lookup into the timeline associated with the query datasource for the query interval and retrieves the nodes that contain data for the query. The Broker node then forwards down the query to the selected nodes.

Caching
-------

Broker nodes employ a cache with a LRU cache invalidation strategy. The broker cache stores per segment results. The cache can be local to each broker node or shared across multiple nodes using an external distributed cache such as [memcached](http://memcached.org/). Each time a broker node receives a query, it ﬁrst maps the query to a set of segments. A subset of these segment results may already exist in the cache and the results can be directly pulled from the cache. For any segment results that do not exist in the cache, the broker node will forward the query to the
historical nodes. Once the historical nodes return their results, the broker will store those results in the cache. Real-time segments are never cached and hence requests for real-time data will always be forwarded to real-time nodes. Real-time data is perpetually changing and caching the results would be unreliable.