---
layout: doc_page
---

# Caching

Caching can optionally be enabled on the broker, historical, and realtime
processing. See [broker](broker.html#caching), 
[historical](historical.html#caching), and [realtime](realtime.html#caching)  
configuration options for how to enable it for different processes.

Druid uses a local in-memory cache by default, unless a diffrent type of cache is specified.
Use the `druid.cache.type` configuration to set a different kind of cache.

## Cache configuration

Cache settings are set globally, so the same configuration can be re-used
for both broker and historical nodes, when defined in the common properties file.

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.cache.type`|`local`, `memcached`, `hybrid`, `caffeine`|The type of cache to use for queries. See below of the configuration options for each cache type|`caffeine`|


#### Local Cache

<div class="note caution">
DEPRECATED: Use caffeine (default as of v0.12.0) instead
</div>

The local cache is deprecated in favor of the Caffeine cache, and may be removed in a future version of Druid. The Caffeine cache affords significantly better performance and control over eviction behavior compared to `local` cache, and is recommended in any situation where you are using JRE 8u60 or higher.

A simple in-memory LRU cache. Local cache resides in JVM heap memory, so if you enable it, make sure you increase heap size accordingly.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.cache.sizeInBytes`|Maximum cache size in bytes. Zero disables caching.|0|
|`druid.cache.initialSize`|Initial size of the hashtable backing the cache.|500000|
|`druid.cache.logEvictionCount`|If non-zero, log cache eviction every `logEvictionCount` items.|0|

### Caffeine Cache

A highly performant local cache implementation for Druid based on [Caffeine](https://github.com/ben-manes/caffeine). Requires a JRE8u60 or higher if using `COMMON_FJP`.

##### Configuration

Below are the configuration options known to this module:

|`runtime.properties`|Description|Default|
|--------------------|-----------|-------|
|`druid.cache.type`| Set this to `caffeine` or leave out parameter|`caffeine`|
|`druid.cache.sizeInBytes`|The maximum size of the cache in bytes on heap.|min(1GB, Runtime.maxMemory / 10)|
|`druid.cache.expireAfter`|The time (in ms) after an access for which a cache entry may be expired|None (no time limit)|
|`druid.cache.cacheExecutorFactory`|The executor factory to use for Caffeine maintenance. One of `COMMON_FJP`, `SINGLE_THREAD`, or `SAME_THREAD`|ForkJoinPool common pool (`COMMON_FJP`)|
|`druid.cache.evictOnClose`|If a close of a namespace (ex: removing a segment from a node) should cause an eager eviction of associated cache values|`false`|

##### `druid.cache.cacheExecutorFactory`

Here are the possible values for `druid.cache.cacheExecutorFactory`, which controls how maintenance tasks are run

* `COMMON_FJP` (default) use the common ForkJoinPool. Should use with [JRE 8u60 or higher](https://github.com/druid-io/druid/pull/4810#issuecomment-329922810). Older versions of the JRE may have worse performance than newer JRE versions.
* `SINGLE_THREAD` Use a single-threaded executor.
* `SAME_THREAD` Cache maintenance is done eagerly.

#### Metrics
In addition to the normal cache metrics, the caffeine cache implementation also reports the following in both `total` and `delta`

|Metric|Description|Normal value|
|------|-----------|------------|
|`query/cache/caffeine/*/requests`|Count of hits or misses|hit + miss|
|`query/cache/caffeine/*/loadTime`|Length of time caffeine spends loading new values (unused feature)|0|
|`query/cache/caffeine/*/evictionBytes`|Size in bytes that have been evicted from the cache|Varies, should tune cache `sizeInBytes` so that `sizeInBytes`/`evictionBytes` is approximately the rate of cache churn you desire|


#### Memcached

Uses memcached as cache backend. This allows all nodes to share the same cache.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.cache.expiration`|Memcached [expiration time](https://code.google.com/p/memcached/wiki/NewCommands#Standard_Protocol).|2592000 (30 days)|
|`druid.cache.timeout`|Maximum time in milliseconds to wait for a response from Memcached.|500|
|`druid.cache.hosts`|Comma separated list of Memcached hosts `<host:port>`.|none|
|`druid.cache.maxObjectSize`|Maximum object size in bytes for a Memcached object.|52428800 (50 MB)|
|`druid.cache.memcachedPrefix`|Key prefix for all keys in Memcached.|druid|
|`druid.cache.numConnections`|Number of memcached connections to use.|1|
|`druid.cache.protocol`|Memcached communication protocol. Can be binary or text.|binary|
|`druid.cache.locator`|Memcached locator. Can be consistent or array_mod.|consistent|

#### Hybrid

Uses a combination of any two caches as a two-level L1 / L2 cache.
This may be used to combine a local in-memory cache with a remote memcached cache.

Cache requests will first check L1 cache before checking L2.
If there is an L1 miss and L2 hit, it will also populate L1.


|Property|Description|Default|
|--------|-----------|-------|
|`druid.cache.l1.type`|type of cache to use for L1 cache. See `druid.cache.type` configuration for valid types.|`local`|
|`druid.cache.l2.type`|type of cache to use for L2 cache. See `druid.cache.type` configuration for valid types.|`local`|
|`druid.cache.l1.*`|Any property valid for the given type of L1 cache can be set using this prefix. For instance, if you are using a `local` L1 cache, specify `druid.cache.l1.sizeInBytes` to set its size.|defaults are the same as for the given cache type.|
|`druid.cache.l2.*`|Prefix for L2 cache settings, see description for L1.|defaults are the same as for the given cache type.|
|`druid.cache.useL2`|A boolean indicating whether to query L2 cache, if it's a miss in L1. It makes sense to configure this to `false` on historical nodes, if L2 is a remote cache like `memcached`, and this cache also used on brokers, because in this case if a query reached historical it means that a broker didn't find corresponding results in the same remote cache, so a query to the remote cache from historical is guaranteed to be a miss.|`true`|
|`druid.cache.populateL2`|A boolean indicating whether to put results into L2 cache.|`true`|
