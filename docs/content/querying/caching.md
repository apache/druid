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

Druid supports query result caching through an LRU cache. Results are stored as a whole or either on a per segment basis along with the 
parameters of a given query. Segment level caching allows Druid to return final results based partially on segment results in the cache 
and partially on segment results from scanning historical/real-time segments. Result level caching enables Druid to cache the entire 
result set, so that query results can be completely retrieved from the cache for identical queries.

Segment results can be stored in a local heap cache or in an external distributed key/value store. Segment query caches 
can be enabled at either the Historical and Broker level (it is not recommended to enable caching on both).

## Query caching on Brokers

Enabling caching on the Broker can yield faster results than if query caches were enabled on Historicals for small clusters. This is 
the recommended setup for smaller production clusters (< 20 servers). Take note that when caching is enabled on the Broker, 
results from Historicals are returned on a per segment basis, and Historicals will not be able to do any local result merging.
Result level caching is enabled only on the Broker side.

## Query caching on Historicals

Larger production clusters should enable caching only on the Historicals to avoid having to use Brokers to merge all query 
results. Enabling caching on the Historicals instead of the Brokers enables the Historicals to do their own local result
merging and puts less strain on the Brokers.
