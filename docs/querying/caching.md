---
id: caching
title: "Query caching"
description: "Describes Apache Druid per-segment and whole-query cache types. Identifies services where you can enable caching and suggestions for caching strategy." 
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

You can enable caching in Apache Druid to improve query times for frequently accessed data. This topic defines the different types of caching for Druid. It describes the default caching behavior and provides guidance and examples to help you hone your caching strategy.

If you're unfamiliar with Druid architecture, review the following topics before proceeding with caching:
- [Druid Design](../design/architecture.md)
- [Segments](../design/segments.md)
- [Query execution](./query-execution.md)

For instructions to configure query caching see [Using query caching](./using-caching.md).

Cache monitoring, including the hit rate and number of evictions, is available in [Druid metrics](../operations/metrics.html#cache).

Query-level caching is in addition to [data-level caching](../design/historical.md) on Historicals.

## Cache types

Druid supports two types of query caching:

- [Per-segment caching](#per-segment-caching) stores partial query results for a specific segment. It is enabled by default.
- [Whole-query caching](#whole-query-caching) stores final query results.

Druid invalidates any cache the moment any underlying data change to avoid returning stale results. This is especially important for `table` datasources that have highly-variable underlying data segments, including real-time data segments.

> **Druid can store cache data on the local JVM heap or in an external distributed key/value store (e.g. memcached)**
>
> The default is a local cache based upon [Caffeine](https://github.com/ben-manes/caffeine). The default maximum cache storage size is the minimum of 1 GiB / ten percent of maximum runtime memory for the JVM, with no cache expiration. See [Cache configuration](../configuration/index.md#cache-configuration) for information on how to configure cache storage.  When using caffeine, the cache is inside the JVM heap and is directly measurable.  Heap usage will grow up to the maximum configured size, and then the least recently used segment results will be evicted and replaced with newer results.

### Per-segment caching

The primary form of caching in Druid is a *per-segment results cache*.  This cache stores partial query results on a per-segment basis and is enabled on Historical services by default.

The per-segment results cache allows Druid to maintain a low-eviction-rate cache for segments that do not change, especially important for those segments that [historical](../design/historical.html) processes pull into their local _segment cache_ from [deep storage](../dependencies/deep-storage.html). Real-time segments, on the other hand, continue to have results computed at query time.

Druid may potentially merge per-segment cached results with the results of later queries that use a similar basic shape with similar filters, aggregations, etc. For example, if the query is identical except that it covers a different time period.

Per-segment caching is controlled by the parameters `useCache` and `populateCache`.

Use per-segment caching with real-time data. For example, your queries request data actively arriving from Kafka alongside intervals in segments that are loaded on Historicals.  Druid can merge cached results from Historical segments with real-time results from the stream.  [Whole-query caching](#whole-query-caching), on the other hand, is not helpful in this scenario because new data from real-time ingestion will continually invalidate the entire cached result.

### Whole-query caching

With *whole-query caching*, Druid caches the entire results of individual queries, meaning the Broker no longer needs to merge per-segment results from data processes.

Use *whole-query caching* on the Broker to increase query efficiency when there is little risk of ingestion invalidating the cache at a segment level.  This applies particularly, for example, when _not_ using real-time ingestion.  Perhaps your queries tend to use batch-ingested data, in which case per-segment caching would be less efficient since the underlying segments hardly ever change, yet Druid would continue to acquire per-segment results for each query.

## Where to enable caching

**Per-segment cache** is available as follows:

- On Historicals, the default. Enable segment-level cache population on Historicals for larger production clusters to prevent Brokers from having to merge all query results. When you enable cache population on Historicals instead of Brokers, the Historicals merge their own local results and put less strain on the Brokers.

- On ingestion tasks in the Peon or Indexer service. Larger production clusters should enable segment-level cache population on task services only to prevent Brokers from having to merge all query results. When you enable cache population on task execution services instead of Brokers, the task execution services to merge their own local results and put less strain on the Brokers.

     Task executor services only support caches that store data locally. For example the `caffeine` cache. This restriction exists because the cache stores results at the level of intermediate partial segments generated by the ingestion tasks. These intermediate partial segments may not be identical across task replicas. Therefore task executor services ignore remote cache types such as `memcached`.

- On Brokers for small production clusters with less than five servers. 

Avoid using per-segment cache at the Broker for large production clusters. When the Broker cache is enabled (`druid.broker.cache.populateCache` is `true`) and `populateCache` _is not_ `false` in the [query context](../querying/query-context.html), individual Historicals will _not_ merge individual segment-level results, and instead pass these back to the lead Broker.  The Broker must then carry out a large merge from _all_ segments on its own.

**Whole-query cache** is available exclusively on Brokers.

## Performance considerations for caching
Caching enables increased concurrency on the same system, therefore leading to noticeable performance improvements for queries on Druid clusters handling throughput for concurrent, mixed workloads.

If you are looking to improve response time for a single query or page load, you should ignore caching. In general, response time for a single task should meet performance objectives even when the cache is cold.

During query processing, the per-segment cache intercepts the query and sends the results directly to the Broker. This way the query bypasses the data server processing threads. For queries requiring minimal processing in the Broker, cached queries are very quick. If work done on the Broker causes a query bottleneck, enabling caching results in little noticeable query improvement.

The largest performance gains from segment caching tend to apply to `topN` and time series queries. For `groupBy` queries, if the bottleneck is in the merging phase on the broker, the impact is less. The same applies to queries with or without joins.

### Scenarios where caching does not increase query performance

Caching does not solve all types of query performance issues. For each cache type there are scenarios where caching is likely to be of little benefit.

**Per-segment caching** doesn't work for the following:
- queries containing a sub-query in them. However the output of sub-queries may be cached. See [Query execution](./query-execution.md) for more details on sub-queries execution.
- queries with joins do not support any caching on the broker.
- GroupBy v2 queries do not support any caching on broker.
- queries with `bySegment` set in the query context are not cached on the broker.

**Whole-query caching** doesn't work for the following:
- queries that involve an inline datasource or a lookup datasource.
- GroupBy v2 queries.
- queries with joins.
- queries with a union datasource.


## Learn more
See the following topics for more information:
- [Using query caching](./using-caching.md) to learn how to configure and use caching.
- [Druid Design](../design/architecture.md) to learn about Druid processes.  
- [Segments](../design/segments.md) to learn how Druid stores data.
- [Query execution](./query-execution.md) to learn how Druid services process query statements.

