---
id: legacy-metadata-api
title: Legacy metadata API
sidebar_label: Legacy metadata
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

This document describes the legacy API endpoints to retrieve datasource metadata from Apache Druid. Use the [SQL metadata tables](../querying/sql-metadata-tables.md) to retrieve datasource metadata instead.

## Segment loading

`GET /druid/coordinator/v1/loadstatus`

Returns the percentage of segments actually loaded in the cluster versus segments that should be loaded in the cluster.

`GET /druid/coordinator/v1/loadstatus?simple`

Returns the number of segments left to load until segments that should be loaded in the cluster are available for queries. This does not include segment replication counts.

`GET /druid/coordinator/v1/loadstatus?full`

Returns the number of segments left to load in each tier until segments that should be loaded in the cluster are all available. This includes segment replication counts.

`GET /druid/coordinator/v1/loadstatus?full&computeUsingClusterView`

Returns the number of segments not yet loaded for each tier until all segments loading in the cluster are available.
The result includes segment replication counts. It also factors in the number of available nodes that are of a service type that can load the segment when computing the number of segments remaining to load.
A segment is considered fully loaded when:
- Druid has replicated it the number of times configured in the corresponding load rule.
- Or the number of replicas for the segment in each tier where it is configured to be replicated equals the available nodes of a service type that are currently allowed to load the segment in the tier.

`GET /druid/coordinator/v1/loadqueue`

Returns the ids of segments to load and drop for each Historical process.

`GET /druid/coordinator/v1/loadqueue?simple`

Returns the number of segments to load and drop, as well as the total segment load and drop size in bytes for each Historical process.

`GET /druid/coordinator/v1/loadqueue?full`

Returns the serialized JSON of segments to load and drop for each Historical process.

## Segment loading by datasource

Note that all _interval_ query parameters are ISO 8601 strings&mdash;for example, 2016-06-27/2016-06-28.
Also note that these APIs only guarantees that the segments are available at the time of the call.
Segments can still become missing because of historical process failures or any other reasons afterward.

`GET /druid/coordinator/v1/datasources/{dataSourceName}/loadstatus?forceMetadataRefresh={boolean}&interval={myInterval}`

Returns the percentage of segments actually loaded in the cluster versus segments that should be loaded in the cluster for the given 
datasource over the given interval (or last 2 weeks if interval is not given). `forceMetadataRefresh` is required to be set. 
* Setting `forceMetadataRefresh` to true will force the coordinator to poll latest segment metadata from the metadata store 
(Note: `forceMetadataRefresh=true` refreshes Coordinator's metadata cache of all datasources. This can be a heavy operation in terms 
of the load on the metadata store but can be necessary to make sure that we verify all the latest segments' load status)
* Setting `forceMetadataRefresh` to false will use the metadata cached on the coordinator from the last force/periodic refresh. 
If no used segments are found for the given inputs, this API returns `204 No Content`

`GET /druid/coordinator/v1/datasources/{dataSourceName}/loadstatus?simple&forceMetadataRefresh={boolean}&interval={myInterval}`

Returns the number of segments left to load until segments that should be loaded in the cluster are available for the given datasource 
over the given interval (or last 2 weeks if interval is not given). This does not include segment replication counts. `forceMetadataRefresh` is required to be set. 
* Setting `forceMetadataRefresh` to true will force the coordinator to poll latest segment metadata from the metadata store 
(Note: `forceMetadataRefresh=true` refreshes Coordinator's metadata cache of all datasources. This can be a heavy operation in terms 
of the load on the metadata store but can be necessary to make sure that we verify all the latest segments' load status)
* Setting `forceMetadataRefresh` to false will use the metadata cached on the coordinator from the last force/periodic refresh. 
If no used segments are found for the given inputs, this API returns `204 No Content`

`GET /druid/coordinator/v1/datasources/{dataSourceName}/loadstatus?full&forceMetadataRefresh={boolean}&interval={myInterval}`

Returns the number of segments left to load in each tier until segments that should be loaded in the cluster are all available for the given datasource  over the given interval (or last 2 weeks if interval is not given). This includes segment replication counts. `forceMetadataRefresh` is required to be set. 
* Setting `forceMetadataRefresh` to true will force the coordinator to poll latest segment metadata from the metadata store 
(Note: `forceMetadataRefresh=true` refreshes Coordinator's metadata cache of all datasources. This can be a heavy operation in terms 
of the load on the metadata store but can be necessary to make sure that we verify all the latest segments' load status)
* Setting `forceMetadataRefresh` to false will use the metadata cached on the coordinator from the last force/periodic refresh. 
  
You can pass the optional query parameter `computeUsingClusterView` to factor in the available cluster services when calculating
the segments left to load. See [Coordinator Segment Loading](#segment-loading) for details.
If no used segments are found for the given inputs, this API returns `204 No Content`

## Metadata store information

:::info
 Note: Much of this information is available in a simpler, easier-to-use form through the Druid SQL
 [`sys.segments`](../querying/sql-metadata-tables.md#segments-table) table.
:::

`GET /druid/coordinator/v1/metadata/segments`

Returns a list of all segments for each datasource enabled in the cluster.

`GET /druid/coordinator/v1/metadata/segments?datasources={dataSourceName1}&datasources={dataSourceName2}`

Returns a list of all segments for one or more specific datasources enabled in the cluster.

`GET /druid/coordinator/v1/metadata/segments?includeOvershadowedStatus`

Returns a list of all segments for each datasource with the full segment metadata and an extra field `overshadowed`.

`GET /druid/coordinator/v1/metadata/segments?includeOvershadowedStatus&includeRealtimeSegments`

Returns a list of all published and realtime segments for each datasource with the full segment metadata and extra fields `overshadowed`,`realtime` & `numRows`. Realtime segments are returned only when `druid.centralizedDatasourceSchema.enabled` is set on the Coordinator. 

`GET /druid/coordinator/v1/metadata/segments?includeOvershadowedStatus&datasources={dataSourceName1}&datasources={dataSourceName2}`

Returns a list of all segments for one or more specific datasources with the full segment metadata and an extra field `overshadowed`.

`GET /druid/coordinator/v1/metadata/segments?includeOvershadowedStatus&includeRealtimeSegments&datasources={dataSourceName1}&datasources={dataSourceName2}`

Returns a list of all published and realtime segments for the specified datasources with the full segment metadata and extra fields `overshadwed`,`realtime` & `numRows`. Realtime segments are returned only when `druid.centralizedDatasourceSchema.enabled` is set on the Coordinator.

`GET /druid/coordinator/v1/metadata/datasources`

Returns a list of the names of datasources with at least one used segment in the cluster, retrieved from the metadata database. Users should call this API to get the eventual state that the system will be in.

`GET /druid/coordinator/v1/metadata/datasources?includeUnused`

Returns a list of the names of datasources, regardless of whether there are used segments belonging to those datasources in the cluster or not.

`GET /druid/coordinator/v1/metadata/datasources?includeDisabled`

Returns a list of the names of datasources, regardless of whether the datasource is disabled or not.

`GET /druid/coordinator/v1/metadata/datasources?full`

Returns a list of all datasources with at least one used segment in the cluster. Returns all metadata about those datasources as stored in the metadata store.

`GET /druid/coordinator/v1/metadata/datasources/{dataSourceName}`

Returns full metadata for a datasource as stored in the metadata store.

`GET /druid/coordinator/v1/metadata/datasources/{dataSourceName}/segments`

Returns a list of all segments for a datasource as stored in the metadata store.

`GET /druid/coordinator/v1/metadata/datasources/{dataSourceName}/segments?full`

Returns a list of all segments for a datasource with the full segment metadata as stored in the metadata store.

`GET /druid/coordinator/v1/metadata/datasources/{dataSourceName}/segments/{segmentId}`

Returns full segment metadata for a specific segment as stored in the metadata store, if the segment is used. If the
segment is unused, or is unknown, a 404 response is returned.

`GET /druid/coordinator/v1/metadata/datasources/{dataSourceName}/segments/{segmentId}?includeUnused=true`

Returns full segment metadata for a specific segment as stored in the metadata store. If it is unknown, a 404 response
is returned.

`GET /druid/coordinator/v1/metadata/datasources/{dataSourceName}/segments`

Returns a list of all segments, overlapping with any of given intervals,  for a datasource as stored in the metadata store. Request body is array of string IS0 8601 intervals like `[interval1, interval2,...]`&mdash;for example, `["2012-01-01T00:00:00.000/2012-01-03T00:00:00.000", "2012-01-05T00:00:00.000/2012-01-07T00:00:00.000"]`.

`GET /druid/coordinator/v1/metadata/datasources/{dataSourceName}/segments?full`

Returns a list of all segments, overlapping with any of given intervals, for a datasource with the full segment metadata as stored in the metadata store. Request body is array of string ISO 8601 intervals like `[interval1, interval2,...]`&mdash;for example, `["2012-01-01T00:00:00.000/2012-01-03T00:00:00.000", "2012-01-05T00:00:00.000/2012-01-07T00:00:00.000"]`.

`POST /druid/coordinator/v1/metadata/dataSourceInformation`

Returns information about the specified datasources, including the datasource schema.  

<a name="coordinator-datasources"></a>

## Datasources

Note that all _interval_ URL parameters are ISO 8601 strings delimited by a `_` instead of a `/`&mdash;for example, `2016-06-27_2016-06-28`.

`GET /druid/coordinator/v1/datasources`

Returns a list of datasource names found in the cluster as seen by the coordinator. This view is updated every [`druid.coordinator.period`](../configuration/index.md#coordinator-operation).

`GET /druid/coordinator/v1/datasources?simple`

Returns a list of JSON objects containing the name and properties of datasources found in the cluster. Properties include segment count, total segment byte size, replicated total segment byte size, minTime, and maxTime.

`GET /druid/coordinator/v1/datasources?full`

Returns a list of datasource names found in the cluster with all metadata about those datasources.

`GET /druid/coordinator/v1/datasources/{dataSourceName}`

Returns a JSON object containing the name and properties of a datasource. Properties include segment count, total segment byte size, replicated total segment byte size, minTime, and maxTime.

`GET /druid/coordinator/v1/datasources/{dataSourceName}?full`

Returns full metadata for a datasource.

`GET /druid/coordinator/v1/datasources/{dataSourceName}/intervals`

Returns a set of segment intervals.

`GET /druid/coordinator/v1/datasources/{dataSourceName}/intervals?simple`

Returns a map of an interval to a JSON object containing the total byte size of segments and number of segments for that interval.

`GET /druid/coordinator/v1/datasources/{dataSourceName}/intervals?full`

Returns a map of an interval to a map of segment metadata to a set of server names that contain the segment for that interval.

`GET /druid/coordinator/v1/datasources/{dataSourceName}/intervals/{interval}`

Returns a set of segment ids for an interval.

`GET /druid/coordinator/v1/datasources/{dataSourceName}/intervals/{interval}?simple`

Returns a map of segment intervals contained within the specified interval to a JSON object containing the total byte size of segments and number of segments for an interval.

`GET /druid/coordinator/v1/datasources/{dataSourceName}/intervals/{interval}?full`

Returns a map of segment intervals contained within the specified interval to a map of segment metadata to a set of server names that contain the segment for an interval.

`GET /druid/coordinator/v1/datasources/{dataSourceName}/intervals/{interval}/serverview`

Returns a map of segment intervals contained within the specified interval to information about the servers that contain the segment for an interval.

`GET /druid/coordinator/v1/datasources/{dataSourceName}/segments`

Returns a list of all segments for a datasource in the cluster.

`GET /druid/coordinator/v1/datasources/{dataSourceName}/segments?full`

Returns a list of all segments for a datasource in the cluster with the full segment metadata.

`GET /druid/coordinator/v1/datasources/{dataSourceName}/segments/{segmentId}`

Returns full segment metadata for a specific segment in the cluster.

`GET /druid/coordinator/v1/datasources/{dataSourceName}/tiers`

Return the tiers that a datasource exists in.

## Intervals

Note that all _interval_ URL parameters are ISO 8601 strings delimited by a `_` instead of a `/` as in `2016-06-27_2016-06-28`.

`GET /druid/coordinator/v1/intervals`

Returns all intervals for all datasources with total size and count.

`GET /druid/coordinator/v1/intervals/{interval}`

Returns aggregated total size and count for all intervals that intersect given ISO interval.

`GET /druid/coordinator/v1/intervals/{interval}?simple`

Returns total size and count for each interval within given ISO interval.

`GET /druid/coordinator/v1/intervals/{interval}?full`

Returns total size and count for each datasource for each interval within given ISO interval.

## Server information

`GET /druid/coordinator/v1/servers`

Returns a list of servers URLs using the format `{hostname}:{port}`. Note that
processes that run with different types will appear multiple times with different
ports.

`GET /druid/coordinator/v1/servers?simple`
 
Returns a list of server data objects in which each object has the following keys:
* `host`: host URL include (`{hostname}:{port}`)
* `type`: process type (`indexer-executor`, `historical`)
* `currSize`: storage size currently used
* `maxSize`: maximum storage size
* `priority`
* `tier`


## Query server

This section documents the API endpoints for the services that reside on Query servers (Brokers) in the suggested [three-server configuration](../design/architecture.md#druid-servers).

### Broker

#### Datasource information

Note that all _interval_ URL parameters are ISO 8601 strings delimited by a `_` instead of a `/`
as in `2016-06-27_2016-06-28`.

:::info
 Note: Much of this information is available in a simpler, easier-to-use form through the Druid SQL
 [`INFORMATION_SCHEMA.TABLES`](../querying/sql-metadata-tables.md#tables-table),
 [`INFORMATION_SCHEMA.COLUMNS`](../querying/sql-metadata-tables.md#columns-table), and
 [`sys.segments`](../querying/sql-metadata-tables.md#segments-table) tables.
:::

`GET /druid/v2/datasources`

Returns a list of queryable datasources.

`GET /druid/v2/datasources/{dataSourceName}`

Returns the dimensions and metrics of the datasource. Optionally, you can provide request parameter "full" to get list of served intervals with dimensions and metrics being served for those intervals. You can also provide request param "interval" explicitly to refer to a particular interval.

If no interval is specified, a default interval spanning a configurable period before the current time will be used. The default duration of this interval is specified in ISO 8601 duration format via: `druid.query.segmentMetadata.defaultHistory`

`GET /druid/v2/datasources/{dataSourceName}/dimensions`

:::info
 This API is deprecated and will be removed in future releases. Please use [SegmentMetadataQuery](../querying/segmentmetadataquery.md) instead
 which provides more comprehensive information and supports all dataSource types including streaming dataSources. It's also encouraged to use [INFORMATION_SCHEMA tables](../querying/sql-metadata-tables.md)
 if you're using SQL.
:::

Returns the dimensions of the datasource.

`GET /druid/v2/datasources/{dataSourceName}/metrics`

:::info
 This API is deprecated and will be removed in future releases. Please use [SegmentMetadataQuery](../querying/segmentmetadataquery.md) instead
 which provides more comprehensive information and supports all dataSource types including streaming dataSources. It's also encouraged to use [INFORMATION_SCHEMA tables](../querying/sql-metadata-tables.md)
 if you're using SQL.
:::

Returns the metrics of the datasource.

`GET /druid/v2/datasources/{dataSourceName}/candidates?intervals={comma-separated-intervals}&numCandidates={numCandidates}`

Returns segment information lists including server locations for the given datasource and intervals. If "numCandidates" is not specified, it will return all servers for each interval.
