---
layout: doc_page
---

Druid Redis Cache
--------------------

A cache implementation for Druid based on [Redis](https://github.com/antirez/redis).

# Configuration
Below are the configuration options known to this module:

|`runtime.properties`|Description|Default|Required|
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