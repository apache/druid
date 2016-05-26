---
layout: doc_page
---

Druid Caffeine Cache
--------------------

A local cache implementation for Druid based on [Caffeine](https://github.com/ben-manes/caffeine). Requires a JRE with a fix for https://bugs.openjdk.java.net/browse/JDK-8078490

# Versioning

The versioning works like this: `druid_version.patch_set`. Such that `druid_version` is the version of druid the extension was compiled against, and `patch_set` is the "version" of the extension.

# How to use
The maven artifact coordinate for this extension is `io.druid.extensions:druid-caffeine-cache`

For Druid 0.9.0 and later, the extension can be included by pulling the jars using the `pull-deps` tool, and including the extension directory in the extension load list.

### Jars

For the sake of sanity if you are manually making an extension directory, release requires the following jars: 

* caffeine-2.3.0.jar
* druid-caffeine-cache-0.9.1.jar (or whatever the correct version for druid is that you are using)

# Configuration
Below are the configuration options known to this module:

|`runtime.properties`|Description|Default|
|--------------------|-----------|-------|
|`druid.cache.sizeInBytes`|The maximum size of the cache in bytes on heap.|None (unlimited)|
|`druid.cache.expireAfter`|The time (in ms) after an access for which a cache entry may be expired|None (no time limit)|
|`druid.cache.cacheExecutorFactory`|The executor factory to use for Caffeine maintenance. One of `COMMON_FJP`, `SINGLE_THREAD`, or `SAME_THREAD`|ForkJoinPool common pool (`COMMON_FJP`)|
|`druid.cache.evictOnClose`|If a close of a namespace (ex: removing a segment from a node) should cause an eager eviction of associated cache values|`false`|

To enable the caffeine cache, include this module on the loadList and set `druid.cache.type` to `caffeine` in your properties.

# Metrics
In addition to the normal cache metrics, the caffeine cache implementation also reports the following in both `total` and `delta`

|Metric|Description|Normal value|
|------|-----------|------------|
|`query/cache/caffeine/*/requests`|Count of hits or misses|hit + miss|
|`query/cache/caffeine/*/loadTime`|Length of time caffeine spends loading new values (unused feature)|0|
|`query/cache/caffeine/*/evictionBytes`|Size in bytes that have been evicted from the cache|Varies, should tune cache `sizeInBytes` so that `sizeInBytes`/`evictionBytes` is approximately the rate of cache churn you desire|
