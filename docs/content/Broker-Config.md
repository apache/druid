---
layout: doc_page
---
Broker Node Configuration
=========================
For general Broker Node information, see [here](Broker.html).

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

Production Configs
------------------
These production configs are using S3 as a deep store.

JVM settings:

```
-server
-Xmx#{HEAP_MAX}g
-Xms#{HEAP_MIN}g
-XX:NewSize=#{NEW_SIZE}g
-XX:MaxNewSize=#{MAX_NEW_SIZE}g
-XX:+UseConcMarkSweepGC
-XX:+PrintGCDetails
-XX:+PrintGCTimeStamps
-Duser.timezone=UTC
-Dfile.encoding=UTF-8
-Djava.io.tmpdir=/mnt/tmp

-Dcom.sun.management.jmxremote.port=17071
-Dcom.sun.management.jmxremote.authenticate=false
-Dcom.sun.management.jmxremote.ssl=false
```

Runtime.properties:

```
druid.host=#{IP_ADDR}:8080
druid.port=8080
druid.service=druid/prod/broker

druid.zk.service.host=#{ZK_IPs}
druid.zk.paths.base=/druid/prod

druid.discovery.curator.path=/prod/discovery

druid.broker.cache.type=memcached
druid.broker.cache.hosts=#{MC_HOST1}:11211,#{MC_HOST2}:11211,#{MC_HOST3}:11211
druid.broker.cache.expiration=2147483647
druid.broker.cache.memcachedPrefix=d1
druid.broker.http.numConnections=20
druid.broker.http.readTimeout=PT5M

druid.server.http.numThreads=50

druid.request.logging.type=emitter
druid.request.logging.feed=druid_requests

druid.monitoring.monitors=["com.metamx.metrics.SysMonitor","com.metamx.metrics.JvmMonitor", "io.druid.client.cache.CacheMonitor"]

# Emit metrics over http
druid.emitter=http
druid.emitter.http.recipientBaseUrl=#{EMITTER_URL}

# If you choose to compress ZK announcements, you must do so for every node type
druid.announcer.type=batch
druid.curator.compress=true
```

Runtime Configuration
---------------------

The broker module uses several of the default modules in [Configuration](Configuration.html) and has the following set of configurations as well:

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.broker.cache.type`|`local`, `memcached`|The type of cache to use for queries.|`local`|
|`druid.broker.balancer.type`|`random`, `connectionCount`|Determines how the broker balances connections to historical nodes. `random` choose randomly, `connectionCount` picks the node with the fewest number of active connections to|`random`|

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
