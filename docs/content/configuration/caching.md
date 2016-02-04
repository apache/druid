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
|`druid.cache.type`|`local`, `memcached`, `hybrid`|The type of cache to use for queries. See below of the configuration options for each cache type|`local`|


#### Local Cache

A simple in-memory LRU cache. Local cache resides in JVM heap memory, so if you enable it, make sure you increase heap size accordingly.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.cache.sizeInBytes`|Maximum cache size in bytes. Zero disables caching.|0|
|`druid.cache.initialSize`|Initial size of the hashtable backing the cache.|500000|
|`druid.cache.logEvictionCount`|If non-zero, log cache eviction every `logEvictionCount` items.|0|


#### Memcached

Uses memcached as cache backend. This allows all nodes to share the same cache.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.cache.expiration`|Memcached [expiration time](https://code.google.com/p/memcached/wiki/NewCommands#Standard_Protocol).|2592000 (30 days)|
|`druid.cache.timeout`|Maximum time in milliseconds to wait for a response from Memcached.|500|
|`druid.cache.hosts`|Command separated list of Memcached hosts `<host:port>`.|none|
|`druid.cache.maxObjectSize`|Maximum object size in bytes for a Memcached object.|52428800 (50 MB)|
|`druid.cache.memcachedPrefix`|Key prefix for all keys in Memcached.|druid|
|`druid.cache.numConnections`|Number of memcached connections to use.|1|


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
