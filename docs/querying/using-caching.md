---
id: using-caching
title: "Using query caching"
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


This topic covers how to configure services to populate and use the Druid query cache. For a conceptual overview and use cases, see [Query caching](./caching.md). For information on how to configure the caching mechanism, see [Cache configuration](../configuration/index.md#cache-configuration).

All query caches have a pair of parameters that control the way individual queries interact with the cache:

- `useCache` to instruct queries to use the cache for results.
- `populateCache` to instruct a query to cache its results.

The separation of concerns, usage and population, lets you include cached results for queries on uncommon data without polluting the cache with results that are unlikely to be reused by other queries, for example, large reports or queries on very old data.

To use caching, you must first enable the settings for a service in the runtime properties. Afterward, you can control which queries use the cache or populate the cache on a per-query basis within the query context.

The query context settings do not override the server settings. For example, it is possible to set a query context to use the cache, but if it is not enabled at the service level, there's no cache to use.

## Enabling query caching on Brokers
Brokers support both segment-level and whole-query result level caching.

To control **segment caching** on the Broker, set the `useCache` and `populateCache`runtime properties. For example, to set the Broker to use and populate the segment cache for queries:
```
druid.broker.cache.useCache=true
druid.broker.cache.populateCache=true
```
To control **whole-query caching** on the Broker, set the `useResultLevelCache` and `populateResultLevelCache` runtime properties. For example, to set the Broker to use and populate the whole-query cache for queries:
```
druid.broker.cache.useResultLevelCache=true
druid.broker.cache.populateResultLevelCache=true
```
See [Broker caching](../configuration/index.md#broker-caching) for a description of all available Broker cache configurations.
 
## Enabling query caching on Historicals
Historicals only support **segment-level** caching. To control caching on the Historical, set the `useCache` and `populateCache` runtime properties. For example, to set the Historical to both use and populate the segment cache for queries:
 ```
 druid.broker.cache.useCache=true
 druid.broker.cache.populateCache=true
 ```
See [Historical caching](../configuration/index.md#historical-caching) for a description of all available Historical cache configurations.
 
## Enabling query caching on task executor services
Task executor services, the Peon or the Indexer, only support **segment-level** caching. To control caching on a task executor service, set the `useCache` and `populateCache` runtime properties. For example, to set the Peon to both use and populate the segment cache for queries:

```
druid.realtime.cache.useCache=true
druid.realtime.cache.populateCache=true
```

See [Peon caching](configuration/index.md#peon-caching) for a description of all available task executor service caching options.

## Enabling caching in the query context
After you enable caching for a service, set cache options for individual queries in the query [context](./query-context.md). For example, you can `POST` a Druid SQL request to the HTTP POST API and include the context as a JSON object:

```
{
  "query" : "SELECT COUNT(*) FROM data_source WHERE foo = 'bar' AND __time > TIMESTAMP '2020-01-01 00:00:00'",
  "context" : {
    "useCache" : "true",
    "populateCache" : "false"
  }
}
```
In this example `populateCache` is `false` because the query references data that is over a year old. For more information, see [Druid SQL client APIs](./sql.md#client-apis).

reiterate that populateCache : true must be set somehwere on at least 1 service. By defaults it's on on Historical and that serves the majority of use cases. 

## Learn more
See the following topics for more information:
- [Query caching](./caching.md) for an overview of caching.
- [Query context](./query-context.md)
- [Cache configuration](../configuration/index.md#cache-configuration) for information about different cache types and additional configuration options.
