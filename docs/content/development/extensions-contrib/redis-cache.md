---
layout: doc_page
title: "Druid Redis Cache"
---

<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->

# Druid Redis Cache

A cache implementation for Druid based on [Redis](https://github.com/antirez/redis).

# Configuration
Below are the configuration options known to this module.

Note that just adding these properties does not enable the cache. You still need to add the `druid.<process-type>.cache.useCache` and `druid.<process-type>.cache.populateCache` properties for the processes you want to enable the cache on as described in the [cache configuration docs](../../configuration/index.html#cache-configuration).

A possible configuration would be to keep the properties below in your `common.runtime.properties` file (present on all processes) and then add `druid.<nodetype>.cache.useCache` and `druid.<nodetype>.cache.populateCache` in the `runtime.properties` file of the process types you want to enable caching on.


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
