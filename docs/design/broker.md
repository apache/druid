---
id: broker
title: "Broker service"
sidebar_label: "Broker"
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


The Broker service routes queries in a distributed cluster setup. It interprets the metadata published to ZooKeeper about segment distribution across services and routes queries accordingly. Additionally, the Broker service consolidates result sets from individual services.

## Configuration

For Apache Druid Broker service configuration, see [Broker Configuration](../configuration/index.md#broker).

For basic tuning guidance for the Broker service, see [Basic cluster tuning](../operations/basic-cluster-tuning.md#broker).

## HTTP endpoints

For a list of API endpoints supported by the Broker, see [Broker API](../api-reference/legacy-metadata-api.md#broker).

## Running

```
org.apache.druid.cli.Main server broker
```

## Forwarding queries

Most Druid queries contain an interval object that indicates a span of time for which data is requested. Similarly, Druid partitions [segments](../design/segments.md) to contain data for some interval of time and distributes the segments across a cluster. Consider a simple datasource with seven segments where each segment contains data for a given day of the week. Any query issued to the datasource for more than one day of data will hit more than one segment. These segments will likely be distributed across multiple services, and hence, the query will likely hit multiple services.

To determine which services to forward queries to, the Broker service first builds a view of the world from information in ZooKeeper. ZooKeeper maintains information about [Historical](../design/historical.md) and streaming ingestion [Peon](../design/peons.md) services and the segments they are serving. For every datasource in ZooKeeper, the Broker service builds a timeline of segments and the services that serve them. When queries are received for a specific datasource and interval, the Broker service performs a lookup into the timeline associated with the query datasource for the query interval and retrieves the services that contain data for the query. The Broker service then forwards down the query to the selected services.

## Caching

Broker services employ a cache with an LRU cache invalidation strategy. The Broker cache stores per-segment results. The cache can be local to each Broker service or shared across multiple services using an external distributed cache such as [memcached](http://memcached.org/). Each time a Broker service receives a query, it first maps the query to a set of segments. A subset of these segment results may already exist in the cache and the results can be directly pulled from the cache. For any segment results that do not exist in the cache, the Broker service will forward the query to the
Historical services. Once the Historical services return their results, the Broker will store those results in the cache. Real-time segments are never cached and hence requests for real-time data will always be forwarded to real-time services. Real-time data is perpetually changing and caching the results would be unreliable.