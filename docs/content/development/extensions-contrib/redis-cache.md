---
layout: doc_page
---

Druid Redis Cache
--------------------

A cache implementation for Druid based on [Redis](https://github.com/antirez/redis).

# Configuration
Below are the configuration options known to this module.

Note that just adding these properties does not enable the cache. You still need to add the `druid.<nodetype>.cache.useCache` and `druid.<nodetype>.cache.populateCache` properties for the nodes you want to enable the cache on as described in the [cache configuration docs](../../configuration/caching.html).

A possible configuration would be to keep the properties below in your `common.runtime.properties` file (present on all nodes) and then add `druid.<nodetype>.cache.useCache` and `druid.<nodetype>.cache.populateCache` in the `runtime.properties` file of the node types you want to enable caching on.


|`common.runtime.properties`|Description|Default|Required|
|--------------------|-----------|-------|--------|
|`druid.cache.host`|Redis server host|None|yes|
|`druid.cache.port`|Redis server port|None|yes|
|`druid.cache.expiration`|Expiration(in milliseconds) for cache entries|24 * 3600 * 1000|no|
|`druid.cache.timeout`|Timeout(in milliseconds) for get cache entries from Redis|2000|no|
|`druid.cache.maxTotalConnections`|Max total connections to Redis|8|no|
|`druid.cache.maxIdleConnections`|Max idle connections to Redis|8|no|
|`druid.cache.minIdleConnections`|Min idle connections to Redis|0|no|

# Enabling

To enable the redis cache, include this module on the loadList and set `druid.cache.type` to `redis` in your properties.

# Metrics
In addition to the normal cache metrics, the redis cache implementation also reports the following in both `total` and `delta`

|Metric|Description|Normal value|
|------|-----------|------------|
|`query/cache/redis/*/requests`|Count of requests to redis cache|whatever request to redis will increase request count by 1|
