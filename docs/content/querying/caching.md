---
layout: doc_page
title: "Query Caching"
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

# Query Caching

Druid supports query result caching at both the segment and whole-query result level. Cache data can be stored in the
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

## Query caching on Brokers

Brokers support both segment-level and whole-query result level caching. Segment-level caching is controlled by the
parameters `useCache` and `populateCache`. Whole-query result level caching is controlled by the parameters
`useResultLevelCache` and `populateResultLevelCache` and [runtime properties](../configuration/index.html)
`druid.broker.cache.*`..

Enabling segment-level caching on the Broker can yield faster results than if query caches were enabled on Historicals for small
clusters. This is the recommended setup for smaller production clusters (< 5 servers). Populating segment-level caches on
the Broker is _not_ recommended for large production clusters, since when the property `druid.broker.cache.populateCache` is
set to `true` (and query context parameter `populateCache` is _not_ set to `false`), results from Historicals are returned
on a per segment basis, and Historicals will not be able to do any local result merging. This impairs the ability of the
Druid cluster to scale well.

## Query caching on Historicals

Historicals only support segment-level caching. Segment-level caching is controlled by the query context
parameters `useCache` and `populateCache` and [runtime properties](../configuration/index.html)
`druid.historical.cache.*`.

Larger production clusters should enable segment-level cache population on Historicals only (not on Brokers) to avoid
having to use Brokers to merge all query results. Enabling cache population on the Historicals instead of the Brokers
enables the Historicals to do their own local result merging and puts less strain on the Brokers.
