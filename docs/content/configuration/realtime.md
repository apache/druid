---
layout: doc_page
---

Realtime Node Configuration
==============================
For general Realtime Node information, see [here](../design/realtime.html).

Runtime Configuration
---------------------

The realtime node uses several of the global configs in [Configuration](../configuration/index.html) and has the following set of configurations as well:

### Node Config

|Property|Description|Default|
|--------|-----------|-------|
|`druid.host`|The host for the current node. This is used to advertise the current processes location as reachable from another node and should generally be specified such that `http://${druid.host}/` could actually talk to this process|InetAddress.getLocalHost().getCanonicalHostName()|
|`druid.plaintextPort`|This is the port to actually listen on; unless port mapping is used, this will be the same port as is on `druid.host`|8084|
|`druid.tlsPort`|TLS port for HTTPS connector, if [druid.server.http.tls](../operations/tls-support.html) is set then this config will be used. If `druid.host` contains port then that port will be ignored. This should be a non-negative Integer.|8284|
|`druid.service`|The name of the service. This is used as a dimension when emitting metrics and alerts to differentiate between the various services|druid/realtime|

### Realtime Operation

|Property|Description|Default|
|--------|-----------|-------|
|`druid.publish.type`|Where to publish segments. Choices are "noop" or "metadata".|metadata|
|`druid.realtime.specFile`|File location of realtime specFile.|none|

### Storing Intermediate Segments

|Property|Description|Default|
|--------|-----------|-------|
|`druid.segmentCache.locations`|Where intermediate segments are stored. The maxSize should always be zero.|none|


### Query Configs

#### Processing

|Property|Description|Default|
|--------|-----------|-------|
|`druid.processing.buffer.sizeBytes`|This specifies a buffer size for the storage of intermediate results. The computation engine in both the Historical and Realtime nodes will use a scratch buffer of this size to do all of their intermediate computations off-heap. Larger values allow for more aggregations in a single pass over the data while smaller values can require more passes depending on the query that is being executed.|1073741824 (1GB)|
|`druid.processing.formatString`|Realtime and historical nodes use this format string to name their processing threads.|processing-%s|
|`druid.processing.numMergeBuffers`|The number of direct memory buffers available for merging query results. The buffers are sized by `druid.processing.buffer.sizeBytes`. This property is effectively a concurrency limit for queries that require merging buffers. If you are using any queries that require merge buffers (currently, just groupBy v2) then you should have at least two of these.|`max(2, druid.processing.numThreads / 4)`|
|`druid.processing.numThreads`|The number of processing threads to have available for parallel processing of segments. Our rule of thumb is `num_cores - 1`, which means that even under heavy load there will still be one core available to do background tasks like talking with ZooKeeper and pulling down segments. If only one core is available, this property defaults to the value `1`.|Number of cores - 1 (or 1)|
|`druid.processing.columnCache.sizeBytes`|Maximum size in bytes for the dimension value lookup cache. Any value greater than `0` enables the cache. It is currently disabled by default. Enabling the lookup cache can significantly improve the performance of aggregators operating on dimension values, such as the JavaScript aggregator, or cardinality aggregator, but can slow things down if the cache hit rate is low (i.e. dimensions with few repeating values). Enabling it may also require additional garbage collection tuning to avoid long GC pauses.|`0` (disabled)|
|`druid.processing.tmpDir`|Path where temporary files created while processing a query should be stored. If specified, this configuration takes priority over the default `java.io.tmpdir` path.|path represented by `java.io.tmpdir`|

The amount of direct memory needed by Druid is at least
`druid.processing.buffer.sizeBytes * (druid.processing.numMergeBuffers + druid.processing.numThreads + 1)`. You can
ensure at least this amount of direct memory is available by providing `-XX:MaxDirectMemorySize=<VALUE>` at the command
line.

#### General Query Configuration

##### GroupBy Query Config

See [groupBy server configuration](../querying/groupbyquery.html#server-configuration).

##### Search Query Config

|Property|Description|Default|
|--------|-----------|-------|
|`druid.query.search.maxSearchLimit`|Maximum number of search results to return.|1000|

### Caching

You can optionally configure caching to be enabled on the realtime node by setting caching configs here.

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.realtime.cache.useCache`|true, false|Enable the cache on the realtime.|false|
|`druid.realtime.cache.populateCache`|true, false|Populate the cache on the realtime.|false|
|`druid.realtime.cache.unCacheable`|All druid query types|All query types to not cache.|`["groupBy", "select"]`|

See [cache configuration](caching.html) for how to configure cache settings.
