---
layout: doc_page
---
Historical Node Configuration
=============================
For general Historical Node information, see [here](Historical.html).

Runtime Configuration
---------------------

The historical node uses several of the global configs in [Configuration](Configuration.html) and has the following set of configurations as well:

### Node Configs

|Property|Description|Default|
|--------|-----------|-------|
|`druid.host`|The host for the current node. This is used to advertise the current processes location as reachable from another node and should generally be specified such that `http://${druid.host}/` could actually talk to this process|none|
|`druid.port`|This is the port to actually listen on; unless port mapping is used, this will be the same port as is on `druid.host`|none|
|`druid.service`|The name of the service. This is used as a dimension when emitting metrics and alerts to differentiate between the various services|none|

### General Configuration

|Property|Description|Default|
|--------|-----------|-------|
|`druid.server.tier`| A string to name the distribution tier that the storage node belongs to. Many of the [rules Coordinator nodes use](Rule-Configuration.html) to manage segments can be keyed on tiers. |  `_default_tier` |
|`druid.server.priority`|In a tiered architecture, the priority of the tier, thus allowing control over which nodes are queried. Higher numbers mean higher priority. The default (no priority) works for architecture with no cross replication (tiers that have no data-storage overlap). Data centers typically have equal priority. | 0 |

### Storing Segments

|Property|Description|Default|
|--------|-----------|-------|
|`druid.segmentCache.locations`|Segments assigned to a Historical node are first stored on the local file system (in a disk cache) and then served by the Historical node. These locations define where that local cache resides. | none (no caching) |
|`druid.segmentCache.deleteOnRemove`|Delete segment files from cache once a node is no longer serving a segment.|true|
|`druid.segmentCache.dropSegmentDelayMillis`|How long a node delays before completely dropping segment.|30000 (30 seconds)|
|`druid.segmentCache.infoDir`|Historical nodes keep track of the segments they are serving so that when the process is restarted they can reload the same segments without waiting for the Coordinator to reassign. This path defines where this metadata is kept. Directory will be created if needed.|${first_location}/info_dir|
|`druid.segmentCache.announceIntervalMillis`|How frequently to announce segments while segments are loading from cache. Set this value to zero to wait for all segments to be loaded before announcing.|5000 (5 seconds)|
|`druid.segmentCache.numLoadingThreads`|How many segments to load concurrently from from deep storage.|1|

### Query Configs

#### Concurrent Requests

Druid uses Jetty to serve HTTP requests.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.server.http.numThreads`|Number of threads for HTTP requests.|10|
|`druid.server.http.maxIdleTime`|The Jetty max idle time for a connection.|PT5m|

#### Processing

|Property|Description|Default|
|--------|-----------|-------|
|`druid.processing.buffer.sizeBytes`|This specifies a buffer size for the storage of intermediate results. The computation engine in both the Historical and Realtime nodes will use a scratch buffer of this size to do all of their intermediate computations off-heap. Larger values allow for more aggregations in a single pass over the data while smaller values can require more passes depending on the query that is being executed.|1073741824 (1GB)|
|`druid.processing.formatString`|Realtime and historical nodes use this format string to name their processing threads.|processing-%s|
|`druid.processing.numThreads`|The number of processing threads to have available for parallel processing of segments. Our rule of thumb is `num_cores - 1`, which means that even under heavy load there will still be one core available to do background tasks like talking with ZooKeeper and pulling down segments. If only one core is available, this property defaults to the value `1`.|Number of cores - 1 (or 1)|
|`druid.processing.columnCache.sizeBytes`|Maximum size in bytes for the dimension value lookup cache. Any value greater than `0` enables the cache. It is currently disabled by default. Enabling the lookup cache can significantly improve the performance of aggregators operating on dimension values, such as the JavaScript aggregator, or cardinality aggregator, but can slow things down if the cache hit rate is low (i.e. dimensions with few repeating values). Enabling it may also require additional garbage collection tuning to avoid long GC pauses.|`0` (disabled)|

#### General Query Configuration

##### GroupBy Query Config

|Property|Description|Default|
|--------|-----------|-------|
|`druid.query.groupBy.singleThreaded`|Run single threaded group By queries.|false|
|`druid.query.groupBy.maxIntermediateRows`|Maximum number of intermediate rows.|50000|
|`druid.query.groupBy.maxResults`|Maximum number of results.|500000|

##### Search Query Config

|Property|Description|Default|
|--------|-----------|-------|
|`druid.query.search.maxSearchLimit`|Maximum number of search results to return.|1000|

### Caching

You can optionally only configure caching to be enabled on the historical by setting caching configs here.

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.historical.cache.useCache`|true, false|Enable the cache on the historical.|false|
|`druid.historical.cache.populateCache`|true, false|Populate the cache on the historical.|false|
|`druid.cache.type`|`local`, `memcached`|The type of cache to use for queries.|`local`|
|`druid.cache.unCacheable`|All druid query types|All query types to not cache.|["groupBy", "select"]|

#### Local Cache

|Property|Description|Default|
|--------|-----------|-------|
|`druid.cache.sizeInBytes`|Maximum cache size in bytes. Zero disables caching.|0|
|`druid.cache.initialSize`|Initial size of the hashtable backing the cache.|500000|
|`druid.cache.logEvictionCount`|If non-zero, log cache eviction every `logEvictionCount` items.|0|

#### Memcache

|Property|Description|Default|
|--------|-----------|-------|
|`druid.cache.expiration`|Memcached [expiration time](https://code.google.com/p/memcached/wiki/NewCommands#Standard_Protocol).|2592000 (30 days)|
|`druid.cache.timeout`|Maximum time in milliseconds to wait for a response from Memcached.|500|
|`druid.cache.hosts`|Command separated list of Memcached hosts `<host:port>`.|none|
|`druid.cache.maxObjectSize`|Maximum object size in bytes for a Memcached object.|52428800 (50 MB)|
|`druid.cache.memcachedPrefix`|Key prefix for all keys in Memcached.|druid|
