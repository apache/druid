---
id: api-reference
title: "API reference"
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


This page documents all of the API endpoints for each Druid service type.

## Common

The following endpoints are supported by all processes.

### Process information

`GET /status`

Returns the Druid version, loaded extensions, memory used, total memory and other useful information about the process.

`GET /status/health`

An endpoint that always returns a boolean "true" value with a 200 OK response, useful for automated health checks.

`GET /status/properties`

Returns the current configuration properties of the process.

`GET /status/selfDiscovered/status`

Returns a JSON map of the form `{"selfDiscovered": true/false}`, indicating whether the node has received a confirmation
from the central node discovery mechanism (currently ZooKeeper) of the Druid cluster that the node has been added to the
cluster. It is recommended to not consider a Druid node "healthy" or "ready" in automated deployment/container
management systems until it returns `{"selfDiscovered": true}` from this endpoint. This is because a node may be
isolated from the rest of the cluster due to network issues and it doesn't make sense to consider nodes "healthy" in
this case. Also, when nodes such as Brokers use ZooKeeper segment discovery for building their view of the Druid cluster
(as opposed to HTTP segment discovery), they may be unusable until the ZooKeeper client is fully initialized and starts
to receive data from the ZooKeeper cluster. `{"selfDiscovered": true}` is a proxy event indicating that the ZooKeeper
client on the node has started to receive data from the ZooKeeper cluster and it's expected that all segments and other
nodes will be discovered by this node timely from this point.

`GET /status/selfDiscovered`

Similar to `/status/selfDiscovered/status`, but returns 200 OK response with empty body if the node has discovered itself
and 503 SERVICE UNAVAILABLE if the node hasn't discovered itself yet. This endpoint might be useful because some
monitoring checks such as AWS load balancer health checks are not able to look at the response body.

## Master Server

This section documents the API endpoints for the processes that reside on Master servers (Coordinators and Overlords)
in the suggested [three-server configuration](../design/processes.md#server-types).

### Coordinator

#### Leadership

`GET /druid/coordinator/v1/leader`

Returns the current leader Coordinator of the cluster.

`GET /druid/coordinator/v1/isLeader`

Returns a JSON object with field "leader", either true or false, indicating if this server is the current leader
Coordinator of the cluster. In addition, returns HTTP 200 if the server is the current leader and HTTP 404 if not.
This is suitable for use as a load balancer status check if you only want the active leader to be considered in-service
at the load balancer.


<a name="coordinator-segment-loading"></a>

#### Segment Loading

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


#### Segment Loading by Datasource

Note that all _interval_ query parameters are ISO 8601 strings (e.g., 2016-06-27/2016-06-28).
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
the segments left to load. See [Coordinator Segment Loading](#coordinator-segment-loading) for details.
If no used segments are found for the given inputs, this API returns `204 No Content`

#### Metadata store information

> Note: Much of this information is available in a simpler, easier-to-use form through the Druid SQL
> [`sys.segments`](../querying/sql-metadata-tables.md#segments-table) table.

`GET /druid/coordinator/v1/metadata/segments`

Returns a list of all segments for each datasource enabled in the cluster.

`GET /druid/coordinator/v1/metadata/segments?datasources={dataSourceName1}&datasources={dataSourceName2}`

Returns a list of all segments for one or more specific datasources enabled in the cluster.

`GET /druid/coordinator/v1/metadata/segments?includeOvershadowedStatus`

Returns a list of all segments for each datasource with the full segment metadata and an extra field `overshadowed`.

`GET /druid/coordinator/v1/metadata/segments?includeOvershadowedStatus&datasources={dataSourceName1}&datasources={dataSourceName2}`

Returns a list of all segments for one or more specific datasources with the full segment metadata and an extra field `overshadowed`.

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

##### POST

`GET /druid/coordinator/v1/metadata/datasources/{dataSourceName}/segments`

Returns a list of all segments, overlapping with any of given intervals,  for a datasource as stored in the metadata store. Request body is array of string IS0 8601 intervals like [interval1, interval2,...] for example ["2012-01-01T00:00:00.000/2012-01-03T00:00:00.000", "2012-01-05T00:00:00.000/2012-01-07T00:00:00.000"]

`GET /druid/coordinator/v1/metadata/datasources/{dataSourceName}/segments?full`

Returns a list of all segments, overlapping with any of given intervals, for a datasource with the full segment metadata as stored in the metadata store. Request body is array of string ISO 8601 intervals like [interval1, interval2,...] for example ["2012-01-01T00:00:00.000/2012-01-03T00:00:00.000", "2012-01-05T00:00:00.000/2012-01-07T00:00:00.000"]

<a name="coordinator-datasources"></a>

#### Datasources

Note that all _interval_ URL parameters are ISO 8601 strings delimited by a `_` instead of a `/`
(e.g., 2016-06-27_2016-06-28).

`GET /druid/coordinator/v1/datasources`

Returns a list of datasource names found in the cluster as seen by the coordinator. This view is updated every [`druid.coordinator.period`](../configuration/index.md#coordinator-operation).

`GET /druid/coordinator/v1/datasources?simple`

Returns a list of JSON objects containing the name and properties of datasources found in the cluster.  Properties include segment count, total segment byte size, replicated total segment byte size, minTime, and maxTime.

`GET /druid/coordinator/v1/datasources?full`

Returns a list of datasource names found in the cluster with all metadata about those datasources.

`GET /druid/coordinator/v1/datasources/{dataSourceName}`

Returns a JSON object containing the name and properties of a datasource. Properties include segment count, total segment byte size, replicated total segment byte size, minTime, and maxTime.

`GET /druid/coordinator/v1/datasources/{dataSourceName}?full`

Returns full metadata for a datasource .

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

#### Note for coordinator's POST and DELETE API's
The segments would be enabled when these API's are called, but then can be disabled again by the coordinator if any dropRule matches. Segments enabled by these API's might not be loaded by historical processes if no loadRule matches.  If an indexing or kill task runs at the same time as these API's are invoked, the behavior is undefined. Some segments might be killed and others might be enabled. It's also possible that all segments might be disabled but at the same time, the indexing task is able to read data from those segments and succeed.

> Caution : Avoid using indexing or kill tasks and these API's at the same time for the same datasource and time chunk. (It's fine if the time chunks or datasource don't overlap)

`POST /druid/coordinator/v1/datasources/{dataSourceName}`

Marks as used all segments belonging to a datasource. Returns a JSON object of the form
`{"numChangedSegments": <number>}` with the number of segments in the database whose state has been changed (that is,
the segments were marked as used) as the result of this API call.

`POST /druid/coordinator/v1/datasources/{dataSourceName}/segments/{segmentId}`

Marks as used a segment of a datasource. Returns a JSON object of the form `{"segmentStateChanged": <boolean>}` with
the boolean indicating if the state of the segment has been changed (that is, the segment was marked as used) as the
result of this API call.

`POST /druid/coordinator/v1/datasources/{dataSourceName}/markUsed`

`POST /druid/coordinator/v1/datasources/{dataSourceName}/markUnused`

Marks segments (un)used for a datasource by interval or set of segment Ids. When marking used only segments that are not overshadowed will be updated.

The request payload contains the interval or set of segment Ids to be marked unused.
Either interval or segment ids should be provided, if both or none are provided in the payload, the API would throw an error (400 BAD REQUEST).

Interval specifies the start and end times as IS0 8601 strings. `interval=(start/end)` where start and end both are inclusive and only the segments completely contained within the specified interval will be disabled, partially overlapping segments will not be affected.

JSON Request Payload:

 |Key|Description|Example|
|----------|-------------|---------|
|`interval`|The interval for which to mark segments unused|"2015-09-12T03:00:00.000Z/2015-09-12T05:00:00.000Z"|
|`segmentIds`|Set of segment Ids to be marked unused|["segmentId1", "segmentId2"]|


`DELETE /druid/coordinator/v1/datasources/{dataSourceName}`

Marks as unused all segments belonging to a datasource. Returns a JSON object of the form
`{"numChangedSegments": <number>}` with the number of segments in the database whose state has been changed (that is,
the segments were marked as unused) as the result of this API call.

`DELETE /druid/coordinator/v1/datasources/{dataSourceName}/intervals/{interval}`
`@Deprecated. /druid/coordinator/v1/datasources/{dataSourceName}?kill=true&interval={myInterval}`

Runs a [Kill task](../ingestion/tasks.md) for a given interval and datasource.

`DELETE /druid/coordinator/v1/datasources/{dataSourceName}/segments/{segmentId}`

Marks as unused a segment of a datasource. Returns a JSON object of the form `{"segmentStateChanged": <boolean>}` with
the boolean indicating if the state of the segment has been changed (that is, the segment was marked as unused) as the
result of this API call.

#### Retention Rules

Note that all _interval_ URL parameters are ISO 8601 strings delimited by a `_` instead of a `/`
(e.g., 2016-06-27_2016-06-28).

`GET /druid/coordinator/v1/rules`

Returns all rules as JSON objects for all datasources in the cluster including the default datasource.

`GET /druid/coordinator/v1/rules/{dataSourceName}`

Returns all rules for a specified datasource.

`GET /druid/coordinator/v1/rules/{dataSourceName}?full`

Returns all rules for a specified datasource and includes default datasource.

`GET /druid/coordinator/v1/rules/history?interval=<interval>`

Returns audit history of rules for all datasources. default value of interval can be specified by setting `druid.audit.manager.auditHistoryMillis` (1 week if not configured) in Coordinator runtime.properties

`GET /druid/coordinator/v1/rules/history?count=<n>`

Returns last `n` entries of audit history of rules for all datasources.

`GET /druid/coordinator/v1/rules/{dataSourceName}/history?interval=<interval>`

Returns audit history of rules for a specified datasource. default value of interval can be specified by setting `druid.audit.manager.auditHistoryMillis` (1 week if not configured) in Coordinator runtime.properties

`GET /druid/coordinator/v1/rules/{dataSourceName}/history?count=<n>`

Returns last `n` entries of audit history of rules for a specified datasource.

`POST /druid/coordinator/v1/rules/{dataSourceName}`

POST with a list of rules in JSON form to update rules.

Optional Header Parameters for auditing the config change can also be specified.

|Header Param Name| Description | Default |
|----------|-------------|---------|
|`X-Druid-Author`| author making the config change|""|
|`X-Druid-Comment`| comment describing the change being done|""|

#### Intervals

Note that all _interval_ URL parameters are ISO 8601 strings delimited by a `_` instead of a `/`
(e.g., 2016-06-27_2016-06-28).

`GET /druid/coordinator/v1/intervals`

Returns all intervals for all datasources with total size and count.

`GET /druid/coordinator/v1/intervals/{interval}`

Returns aggregated total size and count for all intervals that intersect given isointerval.

`GET /druid/coordinator/v1/intervals/{interval}?simple`

Returns total size and count for each interval within given isointerval.

`GET /druid/coordinator/v1/intervals/{interval}?full`

Returns total size and count for each datasource for each interval within given isointerval.

#### Dynamic configuration

See [Coordinator Dynamic Configuration](../configuration/index.md#dynamic-configuration) for details.

Note that all _interval_ URL parameters are ISO 8601 strings delimited by a `_` instead of a `/`
(e.g., 2016-06-27_2016-06-28).

`GET /druid/coordinator/v1/config`

Retrieves current coordinator dynamic configuration.

`GET /druid/coordinator/v1/config/history?interval={interval}&count={count}`

Retrieves history of changes to overlord dynamic configuration. Accepts `interval` and  `count` query string parameters
to filter by interval and limit the number of results respectively.

`POST /druid/coordinator/v1/config`

Update overlord dynamic worker configuration.

#### Automatic compaction status

`GET /druid/coordinator/v1/compaction/progress?dataSource={dataSource}`

Returns the total size of segments awaiting compaction for the given dataSource. The specified dataSource must have [automatic compaction](../data-management/automatic-compaction.md) enabled.



`GET /druid/coordinator/v1/compaction/status`

Returns the status and statistics from the auto-compaction run of all dataSources which have auto-compaction enabled in the latest run. The response payload includes a list of `latestStatus` objects. Each `latestStatus` represents the status for a dataSource (which has/had auto-compaction enabled).
The `latestStatus` object has the following keys:
* `dataSource`: name of the datasource for this status information
* `scheduleStatus`: auto-compaction scheduling status. Possible values are `NOT_ENABLED` and `RUNNING`. Returns `RUNNING ` if the dataSource has an active auto-compaction config submitted. Otherwise, returns `NOT_ENABLED`.
* `bytesAwaitingCompaction`: total bytes of this datasource waiting to be compacted by the auto-compaction (only consider intervals/segments that are eligible for auto-compaction)
* `bytesCompacted`: total bytes of this datasource that are already compacted with the spec set in the auto-compaction config
* `bytesSkipped`: total bytes of this datasource that are skipped (not eligible for auto-compaction) by the auto-compaction
* `segmentCountAwaitingCompaction`: total number of segments of this datasource waiting to be compacted by the auto-compaction (only consider intervals/segments that are eligible for auto-compaction)
* `segmentCountCompacted`: total number of segments of this datasource that are already compacted with the spec set in the auto-compaction config
* `segmentCountSkipped`: total number of segments of this datasource that are skipped (not eligible for auto-compaction) by the auto-compaction
* `intervalCountAwaitingCompaction`: total number of intervals of this datasource waiting to be compacted by the auto-compaction (only consider intervals/segments that are eligible for auto-compaction)
* `intervalCountCompacted`: total number of intervals of this datasource that are already compacted with the spec set in the auto-compaction config
* `intervalCountSkipped`: total number of intervals of this datasource that are skipped (not eligible for auto-compaction) by the auto-compaction

`GET /druid/coordinator/v1/compaction/status?dataSource={dataSource}`

Similar to the API `/druid/coordinator/v1/compaction/status` above but filters response to only return information for the {dataSource} given. 
Note that {dataSource} given must have/had auto-compaction enabled.

#### Automatic compaction configuration

`GET /druid/coordinator/v1/config/compaction`

Returns all automatic compaction configs.

`GET /druid/coordinator/v1/config/compaction/{dataSource}`

Returns an automatic compaction config of a dataSource.

`POST /druid/coordinator/v1/config/compaction/taskslots?ratio={someRatio}&max={someMaxSlots}`

Update the capacity for compaction tasks. `ratio` and `max` are used to limit the max number of compaction tasks.
They mean the ratio of the total task slots to the compaction task slots and the maximum number of task slots for compaction tasks, respectively. The actual max number of compaction tasks is `min(max, ratio * total task slots)`.
Note that `ratio` and `max` are optional and can be omitted. If they are omitted, default values (0.1 and unbounded)
will be set for them.

`POST /druid/coordinator/v1/config/compaction`

Creates or updates the [automatic compaction](../data-management/automatic-compaction.md) config for a dataSource. See [Automatic compaction dynamic configuration](../configuration/index.md#automatic-compaction-dynamic-configuration) for configuration details.

`DELETE /druid/coordinator/v1/config/compaction/{dataSource}`

Removes the automatic compaction config for a dataSource.

#### Server information

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

### Overlord

#### Leadership

`GET /druid/indexer/v1/leader`

Returns the current leader Overlord of the cluster. If you have multiple Overlords, just one is leading at any given time. The others are on standby.

`GET /druid/indexer/v1/isLeader`

This returns a JSON object with field "leader", either true or false. In addition, this call returns HTTP 200 if the
server is the current leader and HTTP 404 if not. This is suitable for use as a load balancer status check if you
only want the active leader to be considered in-service at the load balancer.

#### Tasks

Note that all _interval_ URL parameters are ISO 8601 strings delimited by a `_` instead of a `/`
(e.g., 2016-06-27_2016-06-28).

`GET /druid/indexer/v1/tasks`

Retrieve list of tasks. Accepts query string parameters `state`, `datasource`, `createdTimeInterval`, `max`, and `type`.

|Query Parameter |Description |
|---|---|
|`state`|filter list of tasks by task state, valid options are `running`, `complete`, `waiting`, and `pending`.|
| `datasource`| return tasks filtered by Druid datasource.|
| `createdTimeInterval`| return tasks created within the specified interval. |
| `max`| maximum number of `"complete"` tasks to return. Only applies when `state` is set to `"complete"`.|
| `type`| filter tasks by task type. See [task documentation](../ingestion/tasks.md) for more details.|


`GET /druid/indexer/v1/completeTasks`

Retrieve list of complete tasks. Equivalent to `/druid/indexer/v1/tasks?state=complete`.

`GET /druid/indexer/v1/runningTasks`

Retrieve list of running tasks. Equivalent to `/druid/indexer/v1/tasks?state=running`.

`GET /druid/indexer/v1/waitingTasks`

Retrieve list of waiting tasks. Equivalent to `/druid/indexer/v1/tasks?state=waiting`.

`GET /druid/indexer/v1/pendingTasks`

Retrieve list of pending tasks. Equivalent to `/druid/indexer/v1/tasks?state=pending`.

`GET /druid/indexer/v1/task/{taskId}`

Retrieve the 'payload' of a task.

`GET /druid/indexer/v1/task/{taskId}/status`

Retrieve the status of a task.

`GET /druid/indexer/v1/task/{taskId}/segments`

> This API is deprecated and will be removed in future releases.

Retrieve information about the segments of a task.

`GET /druid/indexer/v1/task/{taskId}/reports`

Retrieve a [task completion report](../ingestion/tasks.md#task-reports) for a task. Only works for completed tasks.

`POST /druid/indexer/v1/task`

Endpoint for submitting tasks and supervisor specs to the Overlord. Returns the taskId of the submitted task.

`POST /druid/indexer/v1/task/{taskId}/shutdown`

Shuts down a task.

`POST /druid/indexer/v1/datasources/{dataSource}/shutdownAllTasks`

Shuts down all tasks for a dataSource.

`POST /druid/indexer/v1/taskStatus`

Retrieve list of task status objects for list of task id strings in request body.

`DELETE /druid/indexer/v1/pendingSegments/{dataSource}`

Manually clean up pending segments table in metadata storage for `datasource`. Returns a JSON object response with
`numDeleted` and count of rows deleted from the pending segments table. This API is used by the
`druid.coordinator.kill.pendingSegments.on` [coordinator setting](../configuration/index.md#coordinator-operation)
which automates this operation to perform periodically.

#### Supervisors

`GET /druid/indexer/v1/supervisor`

Returns a list of strings of the currently active supervisor ids.

`GET /druid/indexer/v1/supervisor?full`

Returns a list of objects of the currently active supervisors.

|Field|Type|Description|
|---|---|---|
|`id`|String|supervisor unique identifier|
|`state`|String|basic state of the supervisor. Available states:`UNHEALTHY_SUPERVISOR`, `UNHEALTHY_TASKS`, `PENDING`, `RUNNING`, `SUSPENDED`, `STOPPING`. Check [Kafka Docs](../development/extensions-core/kafka-supervisor-operations.md) for details.|
|`detailedState`|String|supervisor specific state. (See documentation of specific supervisor for details), e.g. [Kafka](../development/extensions-core/kafka-ingestion.md) or [Kinesis](../development/extensions-core/kinesis-ingestion.md))|
|`healthy`|Boolean|true or false indicator of overall supervisor health|
|`spec`|SupervisorSpec|json specification of supervisor (See Supervisor Configuration for details)|

`GET /druid/indexer/v1/supervisor?state=true`

Returns a list of objects of the currently active supervisors and their current state.

|Field|Type|Description|
|---|---|---|
|`id`|String|supervisor unique identifier|
|`state`|String|basic state of the supervisor. Available states: `UNHEALTHY_SUPERVISOR`, `UNHEALTHY_TASKS`, `PENDING`, `RUNNING`, `SUSPENDED`, `STOPPING`. Check [Kafka Docs](../development/extensions-core/kafka-supervisor-operations.md) for details.|
|`detailedState`|String|supervisor specific state. (See documentation of the specific supervisor for details, e.g. [Kafka](../development/extensions-core/kafka-ingestion.md) or [Kinesis](../development/extensions-core/kinesis-ingestion.md))|
|`healthy`|Boolean|true or false indicator of overall supervisor health|
|`suspended`|Boolean|true or false indicator of whether the supervisor is in suspended state|

`GET /druid/indexer/v1/supervisor/<supervisorId>`

Returns the current spec for the supervisor with the provided ID.

`GET /druid/indexer/v1/supervisor/<supervisorId>/status`

Returns the current status of the supervisor with the provided ID.

`GET/druid/indexer/v1/supervisor/history`

Returns an audit history of specs for all supervisors (current and past).

`GET /druid/indexer/v1/supervisor/<supervisorId>/history`

Returns an audit history of specs for the supervisor with the provided ID.

`POST /druid/indexer/v1/supervisor`

Create a new supervisor or update an existing one.

`POST /druid/indexer/v1/supervisor/<supervisorId>/suspend`

Suspend the current running supervisor of the provided ID. Responds with updated SupervisorSpec.

`POST /druid/indexer/v1/supervisor/suspendAll`

Suspend all supervisors at once.

`POST /druid/indexer/v1/supervisor/<supervisorId>/resume`

Resume indexing tasks for a supervisor. Responds with updated SupervisorSpec.

`POST /druid/indexer/v1/supervisor/resumeAll`

Resume all supervisors at once.

`POST /druid/indexer/v1/supervisor/<supervisorId>/reset`

Reset the specified supervisor.

`POST /druid/indexer/v1/supervisor/<supervisorId>/terminate`

Terminate a supervisor of the provided ID.

`POST /druid/indexer/v1/supervisor/terminateAll`

Terminate all supervisors at once.

`POST /druid/indexer/v1/supervisor/<supervisorId>/shutdown`

> This API is deprecated and will be removed in future releases.
> Please use the equivalent 'terminate' instead.

Shutdown a supervisor.

#### Dynamic configuration

See [Overlord Dynamic Configuration](../configuration/index.md#overlord-dynamic-configuration) for details.

Note that all _interval_ URL parameters are ISO 8601 strings delimited by a `_` instead of a `/`
(e.g., 2016-06-27_2016-06-28).

`GET /druid/indexer/v1/worker`

Retrieves current overlord dynamic configuration.

`GET /druid/indexer/v1/worker/history?interval={interval}&count={count}`

Retrieves history of changes to overlord dynamic configuration. Accepts `interval` and  `count` query string parameters
to filter by interval and limit the number of results respectively.

`GET /druid/indexer/v1/workers`

Retrieves a list of all the worker nodes in the cluster along with its metadata.

`GET /druid/indexer/v1/scaling`

Retrieves overlord scaling events if auto-scaling runners are in use.

`POST /druid/indexer/v1/worker`

Update overlord dynamic worker configuration.

## Data Server

This section documents the API endpoints for the processes that reside on Data servers (MiddleManagers/Peons and Historicals)
in the suggested [three-server configuration](../design/processes.md#server-types).

### MiddleManager

`GET /druid/worker/v1/enabled`

Check whether a MiddleManager is in an enabled or disabled state. Returns JSON object keyed by the combined `druid.host`
and `druid.port` with the boolean state as the value.

    ```json
    {"localhost:8091":true}
    ```

`GET /druid/worker/v1/tasks`

Retrieve a list of active tasks being run on MiddleManager. Returns JSON list of taskid strings.  Normal usage should
prefer to use the `/druid/indexer/v1/tasks` [Overlord API](#overlord) or one of it's task state specific variants instead.

```json
["index_wikiticker_2019-02-11T02:20:15.316Z"]
```

`GET /druid/worker/v1/task/{taskid}/log`

Retrieve task log output stream by task id. Normal usage should prefer to use the `/druid/indexer/v1/task/{taskId}/log`
[Overlord API](#overlord) instead.

`POST /druid/worker/v1/disable`

Disable a MiddleManager, causing it to stop accepting new tasks but complete all existing tasks. Returns JSON  object
keyed by the combined `druid.host` and `druid.port`:

    ```json
    {"localhost:8091":"disabled"}
    ```

`POST /druid/worker/v1/enable`

Enable a MiddleManager, allowing it to accept new tasks again if it was previously disabled. Returns JSON  object
keyed by the combined `druid.host` and `druid.port`:

    ```json
    {"localhost:8091":"enabled"}
    ```

`POST /druid/worker/v1/task/{taskid}/shutdown`

Shutdown a running task by `taskid`. Normal usage should prefer to use the `/druid/indexer/v1/task/{taskId}/shutdown`
[Overlord API](#overlord) instead. Returns JSON:

    ```json
    {"task":"index_kafka_wikiticker_f7011f8ffba384b_fpeclode"}
    ```


### Peon

`GET /druid/worker/v1/chat/{taskId}/rowStats`

Retrieve a live row stats report from a Peon. See [task reports](../ingestion/tasks.md#task-reports) for more details.

`GET /druid/worker/v1/chat/{taskId}/unparseableEvents`

Retrieve an unparseable events report from a Peon. See [task reports](../ingestion/tasks.md#task-reports) for more details.

### Historical

#### Segment Loading

`GET /druid/historical/v1/loadstatus`

Returns JSON of the form `{"cacheInitialized":<value>}`, where value is either `true` or `false` indicating if all
segments in the local cache have been loaded. This can be used to know when a Historical process is ready
to be queried after a restart.

`GET /druid/historical/v1/readiness`

Similar to `/druid/historical/v1/loadstatus`, but instead of returning JSON with a flag, responses 200 OK if segments
in the local cache have been loaded, and 503 SERVICE UNAVAILABLE, if they haven't.


## Query Server

This section documents the API endpoints for the processes that reside on Query servers (Brokers) in the suggested [three-server configuration](../design/processes.md#server-types).

### Broker

#### Datasource Information

Note that all _interval_ URL parameters are ISO 8601 strings delimited by a `_` instead of a `/`
(e.g., 2016-06-27_2016-06-28).

> Note: Much of this information is available in a simpler, easier-to-use form through the Druid SQL
> [`INFORMATION_SCHEMA.TABLES`](../querying/sql-metadata-tables.md#tables-table),
> [`INFORMATION_SCHEMA.COLUMNS`](../querying/sql-metadata-tables.md#columns-table), and
> [`sys.segments`](../querying/sql-metadata-tables.md#segments-table) tables.

`GET /druid/v2/datasources`

Returns a list of queryable datasources.

`GET /druid/v2/datasources/{dataSourceName}`

Returns the dimensions and metrics of the datasource. Optionally, you can provide request parameter "full" to get list of served intervals with dimensions and metrics being served for those intervals. You can also provide request param "interval" explicitly to refer to a particular interval.

If no interval is specified, a default interval spanning a configurable period before the current time will be used. The default duration of this interval is specified in ISO 8601 duration format via: `druid.query.segmentMetadata.defaultHistory`

`GET /druid/v2/datasources/{dataSourceName}/dimensions`

> This API is deprecated and will be removed in future releases. Please use [SegmentMetadataQuery](../querying/segmentmetadataquery.md) instead
> which provides more comprehensive information and supports all dataSource types including streaming dataSources. It's also encouraged to use [INFORMATION_SCHEMA tables](../querying/sql-metadata-tables.md)
> if you're using SQL.
> 
Returns the dimensions of the datasource.

`GET /druid/v2/datasources/{dataSourceName}/metrics`

> This API is deprecated and will be removed in future releases. Please use [SegmentMetadataQuery](../querying/segmentmetadataquery.md) instead
> which provides more comprehensive information and supports all dataSource types including streaming dataSources. It's also encouraged to use [INFORMATION_SCHEMA tables](../querying/sql-metadata-tables.md)
> if you're using SQL.

Returns the metrics of the datasource.

`GET /druid/v2/datasources/{dataSourceName}/candidates?intervals={comma-separated-intervals}&numCandidates={numCandidates}`

Returns segment information lists including server locations for the given datasource and intervals. If "numCandidates" is not specified, it will return all servers for each interval.

#### Load Status

`GET /druid/broker/v1/loadstatus`

Returns a flag indicating if the Broker knows about all segments in the cluster. This can be used to know when a Broker process is ready to be queried after a restart.

`GET /druid/broker/v1/readiness`

Similar to `/druid/broker/v1/loadstatus`, but instead of returning a JSON, responses 200 OK if its ready and otherwise 503 SERVICE UNAVAILABLE.

#### Queries

`POST /druid/v2/`

The endpoint for submitting queries. Accepts an option `?pretty` that pretty prints the results.

`POST /druid/v2/candidates/`

Returns segment information lists including server locations for the given query..

### Router

> Note: Much of this information is available in a simpler, easier-to-use form through the Druid SQL
> [`INFORMATION_SCHEMA.TABLES`](../querying/sql-metadata-tables.md#tables-table),
> [`INFORMATION_SCHEMA.COLUMNS`](../querying/sql-metadata-tables.md#columns-table), and
> [`sys.segments`](../querying/sql-metadata-tables.md#segments-table) tables.

`GET /druid/v2/datasources`

Returns a list of queryable datasources.

`GET /druid/v2/datasources/{dataSourceName}`

Returns the dimensions and metrics of the datasource.

`GET /druid/v2/datasources/{dataSourceName}/dimensions`

Returns the dimensions of the datasource.

`GET /druid/v2/datasources/{dataSourceName}/metrics`

Returns the metrics of the datasource.
