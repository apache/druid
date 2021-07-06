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

## Cache types

Druid supports the following types of caches:

- **Per-segment** caching which stores _partial results_ of a query for a specific segment. Per-segment caching is enabled on Historicals by default.
- **Whole-query** caching which stores all results for a query.

To avoid returning stale results, Druid invalidates the cache the moment any underlying data changes for both types of cache.

Druid can store cache data on the local JVM heap or in an external distributed key/value store. The default is a local cache based upon [Caffeine](https://github.com/ben-manes/caffeine). Maximum cache storage defaults to the minimum value of 1 GiB or the ten percent of the maximum runtime memory for the JVM with no cache expiration. See [Cache configuration](../configuration/index.md#cache-configuration) for information on how to configure cache storage.

### Per-segment caching

The primary form of caching in Druid is the **per-segment cache** which stores query results on a per-segment basis. It is enabled on Historical services by default.

When your queries include data from segments that are mutable and undergoing real-time ingestion, use a segment cache. In this case Druid caches query results for immutable historical segments when possible. It re-computes results for the real-time segments at query time.

For example, you have queries that frequently include incoming data from a Kafka or Kinesis stream alongside unchanging segments. Per-segment caching lets Druid cache results from older immutable segments and merge them with updated data. Whole-query caching would not be helpful in this scenario because the new data from real-time ingestion continually invalidates the cache.

### Whole-query caching

If real-time ingestion invalidating the cache is not an issue for your queries, you can use **whole-query caching** on the Broker to increase query efficiency. The Broker performs whole-query caching operations before sending fan out queries to Historicals. Therefore Druid no longer needs to merge the per-segment results on the Broker.

For instance, whole-query caching is a good option when you have queries that include data from a batch ingestion task that runs every few hours or once a day. Per-segment caching would be less efficient in this case because it requires Druid to merge the per-segment results for each query, even when the results are cached.

## Where to enable caching

**Per-segment cache** is available as follows:

- On Historicals, the default. Enable segment-level cache population on Historicals for larger production clusters to prevent Brokers from having to merge all query results. When you enable cache population on Historicals instead of Brokers, the Historicals merge their own local results and put less strain on the Brokers.

- On ingestion tasks in the Peon or Indexer service. Larger production clusters should enable segment-level cache population on task services only to prevent Brokers from having to merge all query results. When you enable cache population on task execution services instead of Brokers, the task execution services to merge their own local results and put less strain on the Brokers.

     Task executor services only support caches that store data locally. For example the `caffeine` cache. This restriction exists because the cache stores results at the level of intermediate partial segments generated by the ingestion tasks. These intermediate partial segments may not be identical across task replicas. Therefore task executor services ignore remote cache types such as `memcached`.

- On Brokers for small production clusters with less than five servers. 

     Do not use per-segment caches on the Broker for large production clusters. When `druid.broker.cache.populateCache` is `true` and query context parameter `populateCache` _is not_ `false`, Historicals return results on a per-segment basis without merging results locally thus negatively impacting cluster scalability.

**Whole-query cache** is only available on Brokers.

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

