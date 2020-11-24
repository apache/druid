---
id: caching
title: "Query caching"
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


Apache Druid supports query result caching at both the segment and whole-query result level. Cache data can be stored in the
local JVM heap or in an external distributed key/value store. In all cases, the Druid cache is a query result cache.
The only difference is whether the result is a _partial result_ for a particular segment, or the result for an entire
query. In both cases, the cache is invalidated as soon as any underlying data changes; it will never return a stale
result.

Segment-level caching allows the cache to be leveraged even when some of the underling segments are mutable and
undergoing real-time ingestion. In this case, Druid will potentially cache query results for immutable historical
segments, while re-computing results for the real-time segments on each query. Whole-query result level caching is not
useful in this scenario, since it would be continuously invalidated.

Segment-level caching does require Druid to merge the per-segment results on each query, even when they are served
from the cache. For this reason, whole-query result level caching can be more efficient if invalidation due to real-time
ingestion is not an issue.


## Using and populating cache

All caches have a pair of parameters that control the behavior of how individual queries interact with the cache, a 'use' cache parameter, and a 'populate' cache parameter. These settings must be enabled at the service level via [runtime properties](../configuration/index.md) to utilize cache, but can be controlled on a per query basis by setting them on the [query context](../querying/query-context.md). The 'use' parameter obviously controls if a query will utilize cached results. The 'populate' parameter controls if a query will update cached results. These are separate parameters to allow queries on uncommon data to utilize cached results without polluting the cache with results that are unlikely to be re-used by other queries, for example large reports or very old data.

## Query caching on Brokers

Brokers support both segment-level and whole-query result level caching. Segment-level caching is controlled by the
parameters `useCache` and `populateCache`. Whole-query result level caching is controlled by the parameters
`useResultLevelCache` and `populateResultLevelCache` and [runtime properties](../configuration/index.md)
`druid.broker.cache.*`.

Enabling segment-level caching on the Broker can yield faster results than if query caches were enabled on Historicals for small
clusters. This is the recommended setup for smaller production clusters (< 5 servers). Populating segment-level caches on
the Broker is _not_ recommended for large production clusters, since when the property `druid.broker.cache.populateCache` is
set to `true` (and query context parameter `populateCache` is _not_ set to `false`), results from Historicals are returned
on a per segment basis, and Historicals will not be able to do any local result merging. This impairs the ability of the
Druid cluster to scale well.

## Query caching on Historicals

Historicals only support segment-level caching. Segment-level caching is controlled by the query context
parameters `useCache` and `populateCache` and [runtime properties](../configuration/index.md)
`druid.historical.cache.*`.

Larger production clusters should enable segment-level cache population on Historicals only (not on Brokers) to avoid
having to use Brokers to merge all query results. Enabling cache population on the Historicals instead of the Brokers
enables the Historicals to do their own local result merging and puts less strain on the Brokers.

## Query caching on Ingestion Tasks

Task executor processes such as the Peon or the experimental Indexer only support segment-level caching. Segment-level 
caching is controlled by the query context parameters `useCache` and `populateCache` 
and [runtime properties](../configuration/index.html) `druid.realtime.cache.*`.

Larger production clusters should enable segment-level cache population on task execution processes only 
(not on Brokers) to avoid having to use Brokers to merge all query results. Enabling cache population on the 
task execution processes instead of the Brokers enables the task execution processes to do their own local 
result merging and puts less strain on the Brokers.

Note that the task executor processes only support caches that keep their data locally, such as the `caffeine` cache.
This restriction exists because the cache stores results at the level of intermediate partial segments generated by the
ingestion tasks. These intermediate partial segments will not necessarily be identical across task replicas, so
remote cache types such as `memcached` will be ignored by task executor processes.

## Unsupported queries

Query caching is not available for following:
- Queries, that involve a `union` datasource, do not support result-level caching. Refer to the 
[related issue](https://github.com/apache/druid/issues/8713) for details. Please note that not all union SQL queries are executed using a union datasource. You can use the `explain` operation to see how the union query in sql will be executed.
- Queries, that involve an `Inline` datasource or a `Lookup` datasource, do not support any caching. 
- Queries, with a sub-query in them, do not support any caching though the output of sub-queries itself may be cached. 
Refer to the [Query execution](query-execution.md#query) page for more details on how sub-queries are executed.
- Join queries do not support any caching on the broker [More details](https://github.com/apache/druid/issues/10444).
- GroupBy v2 queries do not support any caching on broker [More details](https://github.com/apache/druid/issues/3820).
- Data Source Metadata queries are not cached anywhere.
- Queries, that have `bySegment` set in the query context, are not cached on the broker. They are currently cached on 
historical but this behavior will potentially be removed in the future.  
