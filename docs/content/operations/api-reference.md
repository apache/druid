---
layout: doc_page
title: "API Reference"
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

# API Reference

This page documents all of the API endpoints for each Druid service type.

## Table of Contents
  * [Common](#common)
  * [Master Server](#master-server)
    * [Coordinator](#coordinator)
    * [Overlord](#overlord)
  * [Data Server](#data-server)
    * [MiddleManager](#middlemanager)
    * [Peon](#peon)
    * [Historical](#historical)
  * [Query Server](#query-server)
    * [Broker](#broker)

## Common

The following endpoints are supported by all processes.

### Process information

#### GET

* `/status`

Returns the Druid version, loaded extensions, memory used, total memory and other useful information about the process.

* `/status/health`

An endpoint that always returns a boolean "true" value with a 200 OK response, useful for automated health checks.

* `/status/properties`

Returns the current configuration properties of the process.

## Master Server

This section documents the API endpoints for the processes that reside on Master servers (Coordinators and Overlords) in the suggested [three-server configuration](../design/processes.html#server-types).

### Coordinator

#### Leadership

##### GET

* `/druid/coordinator/v1/leader`

Returns the current leader Coordinator of the cluster.

* `/druid/coordinator/v1/isLeader`

Returns a JSON object with field "leader", either true or false, indicating if this server is the current leader
Coordinator of the cluster. In addition, returns HTTP 200 if the server is the current leader and HTTP 404 if not.
This is suitable for use as a load balancer status check if you only want the active leader to be considered in-service
at the load balancer.

#### Segment Loading

##### GET

* `/druid/coordinator/v1/loadstatus`

Returns the percentage of segments actually loaded in the cluster versus segments that should be loaded in the cluster.

 * `/druid/coordinator/v1/loadstatus?simple`

Returns the number of segments left to load until segments that should be loaded in the cluster are available for queries. This does not include replication.

* `/druid/coordinator/v1/loadstatus?full`

Returns the number of segments left to load in each tier until segments that should be loaded in the cluster are all available. This includes replication.

* `/druid/coordinator/v1/loadqueue`

Returns the ids of segments to load and drop for each Historical process.

* `/druid/coordinator/v1/loadqueue?simple`

Returns the number of segments to load and drop, as well as the total segment load and drop size in bytes for each Historical process.

* `/druid/coordinator/v1/loadqueue?full`

Returns the serialized JSON of segments to load and drop for each Historical process.

#### Metadata store information

##### GET

* `/druid/coordinator/v1/metadata/datasources`

Returns a list of the names of enabled datasources in the cluster.

* `/druid/coordinator/v1/metadata/datasources?includeDisabled`

Returns a list of the names of enabled and disabled datasources in the cluster.

* `/druid/coordinator/v1/metadata/datasources?full`

Returns a list of all enabled datasources with all metadata about those datasources as stored in the metadata store.

* `/druid/coordinator/v1/metadata/datasources/{dataSourceName}`

Returns full metadata for a datasource as stored in the metadata store.

* `/druid/coordinator/v1/metadata/datasources/{dataSourceName}/segments`

Returns a list of all segments for a datasource as stored in the metadata store.

* `/druid/coordinator/v1/metadata/datasources/{dataSourceName}/segments?full`

Returns a list of all segments for a datasource with the full segment metadata as stored in the metadata store.

* `/druid/coordinator/v1/metadata/datasources/{dataSourceName}/segments/{segmentId}`

Returns full segment metadata for a specific segment as stored in the metadata store.

##### POST

* `/druid/coordinator/v1/metadata/datasources/{dataSourceName}/segments`

Returns a list of all segments, overlapping with any of given intervals,  for a datasource as stored in the metadata store. Request body is array of string IS0 8601 intervals like [interval1, interval2,...] for example ["2012-01-01T00:00:00.000/2012-01-03T00:00:00.000", "2012-01-05T00:00:00.000/2012-01-07T00:00:00.000"]

* `/druid/coordinator/v1/metadata/datasources/{dataSourceName}/segments?full`

Returns a list of all segments, overlapping with any of given intervals, for a datasource with the full segment metadata as stored in the metadata store. Request body is array of string ISO 8601 intervals like [interval1, interval2,...] for example ["2012-01-01T00:00:00.000/2012-01-03T00:00:00.000", "2012-01-05T00:00:00.000/2012-01-07T00:00:00.000"]

#### Datasources

Note that all _interval_ URL parameters are ISO 8601 strings delimited by a `_` instead of a `/` 
(e.g., 2016-06-27_2016-06-28).

##### GET

* `/druid/coordinator/v1/datasources`

Returns a list of datasource names found in the cluster.

* `/druid/coordinator/v1/datasources?simple`

Returns a list of JSON objects containing the name and properties of datasources found in the cluster.  Properties include segment count, total segment byte size, minTime, and maxTime.

* `/druid/coordinator/v1/datasources?full`

Returns a list of datasource names found in the cluster with all metadata about those datasources.

* `/druid/coordinator/v1/datasources/{dataSourceName}`

Returns a JSON object containing the name and properties of a datasource. Properties include segment count, total segment byte size, minTime, and maxTime.

* `/druid/coordinator/v1/datasources/{dataSourceName}?full`

Returns full metadata for a datasource .

* `/druid/coordinator/v1/datasources/{dataSourceName}/intervals`

Returns a set of segment intervals.

* `/druid/coordinator/v1/datasources/{dataSourceName}/intervals?simple`

Returns a map of an interval to a JSON object containing the total byte size of segments and number of segments for that interval.

* `/druid/coordinator/v1/datasources/{dataSourceName}/intervals?full`

Returns a map of an interval to a map of segment metadata to a set of server names that contain the segment for that interval.

* `/druid/coordinator/v1/datasources/{dataSourceName}/intervals/{interval}`

Returns a set of segment ids for an interval.

* `/druid/coordinator/v1/datasources/{dataSourceName}/intervals/{interval}?simple`

Returns a map of segment intervals contained within the specified interval to a JSON object containing the total byte size of segments and number of segments for an interval.

* `/druid/coordinator/v1/datasources/{dataSourceName}/intervals/{interval}?full`

Returns a map of segment intervals contained within the specified interval to a map of segment metadata to a set of server names that contain the segment for an interval.

* `/druid/coordinator/v1/datasources/{dataSourceName}/intervals/{interval}/serverview`

Returns a map of segment intervals contained within the specified interval to information about the servers that contain the segment for an interval.

* `/druid/coordinator/v1/datasources/{dataSourceName}/segments`

Returns a list of all segments for a datasource in the cluster.

* `/druid/coordinator/v1/datasources/{dataSourceName}/segments?full`

Returns a list of all segments for a datasource in the cluster with the full segment metadata.

* `/druid/coordinator/v1/datasources/{dataSourceName}/segments/{segmentId}`

Returns full segment metadata for a specific segment in the cluster.

* `/druid/coordinator/v1/datasources/{dataSourceName}/tiers`

Return the tiers that a datasource exists in.

##### POST

* `/druid/coordinator/v1/datasources/{dataSourceName}`

Enables all segments of datasource which are not overshadowed by others.

* `/druid/coordinator/v1/datasources/{dataSourceName}/segments/{segmentId}`

Enables a segment of a datasource.

##### DELETE<a name="coordinator-delete"></a>

* `/druid/coordinator/v1/datasources/{dataSourceName}`

Disables a datasource.

* `/druid/coordinator/v1/datasources/{dataSourceName}/intervals/{interval}`
* `@Deprecated. /druid/coordinator/v1/datasources/{dataSourceName}?kill=true&interval={myInterval}`

Runs a [Kill task](../ingestion/tasks.html) for a given interval and datasource.

* `/druid/coordinator/v1/datasources/{dataSourceName}/segments/{segmentId}`

Disables a segment.

#### Retention Rules

Note that all _interval_ URL parameters are ISO 8601 strings delimited by a `_` instead of a `/` 
(e.g., 2016-06-27_2016-06-28).

##### GET

* `/druid/coordinator/v1/rules`

Returns all rules as JSON objects for all datasources in the cluster including the default datasource.

* `/druid/coordinator/v1/rules/{dataSourceName}`

Returns all rules for a specified datasource.


* `/druid/coordinator/v1/rules/{dataSourceName}?full`

Returns all rules for a specified datasource and includes default datasource.

* `/druid/coordinator/v1/rules/history?interval=<interval>`

 Returns audit history of rules for all datasources. default value of interval can be specified by setting `druid.audit.manager.auditHistoryMillis` (1 week if not configured) in Coordinator runtime.properties

* `/druid/coordinator/v1/rules/history?count=<n>`

 Returns last <n> entries of audit history of rules for all datasources.

* `/druid/coordinator/v1/rules/{dataSourceName}/history?interval=<interval>`

 Returns audit history of rules for a specified datasource. default value of interval can be specified by setting `druid.audit.manager.auditHistoryMillis` (1 week if not configured) in Coordinator runtime.properties

* `/druid/coordinator/v1/rules/{dataSourceName}/history?count=<n>`

 Returns last <n> entries of audit history of rules for a specified datasource.
 
##### POST

* `/druid/coordinator/v1/rules/{dataSourceName}`

POST with a list of rules in JSON form to update rules.

Optional Header Parameters for auditing the config change can also be specified.

|Header Param Name| Description | Default |
|----------|-------------|---------|
|`X-Druid-Author`| author making the config change|""|
|`X-Druid-Comment`| comment describing the change being done|""|

#### Intervals

Note that all _interval_ URL parameters are ISO 8601 strings delimited by a `_` instead of a `/` 
(e.g., 2016-06-27_2016-06-28).

##### GET

* `/druid/coordinator/v1/intervals`

Returns all intervals for all datasources with total size and count.

* `/druid/coordinator/v1/intervals/{interval}`

Returns aggregated total size and count for all intervals that intersect given isointerval.

* `/druid/coordinator/v1/intervals/{interval}?simple`

Returns total size and count for each interval within given isointerval.

* `/druid/coordinator/v1/intervals/{interval}?full`

Returns total size and count for each datasource for each interval within given isointerval.

#### Compaction Configuration

##### GET

* `/druid/coordinator/v1/config/compaction`

Returns all compaction configs.

* `/druid/coordinator/v1/config/compaction/{dataSource}`

Returns a compaction config of a dataSource.

##### POST

* `/druid/coordinator/v1/config/compaction/taskslots?ratio={someRatio}&max={someMaxSlots}`

Update the capacity for compaction tasks. `ratio` and `max` are used to limit the max number of compaction tasks.
They mean the ratio of the total task slots to the copmaction task slots and the maximum number of task slots for compaction tasks, respectively.
The actual max number of compaction tasks is `min(max, ratio * total task slots)`.
Note that `ratio` and `max` are optional and can be omitted. If they are omitted, default values (0.1 and unbounded)
will be set for them.

* `/druid/coordinator/v1/config/compaction`

Creates or updates the compaction config for a dataSource.
See [Compaction Configuration](../configuration/index.html#compaction-dynamic-configuration) for configuration details.


##### DELETE

* `/druid/coordinator/v1/config/compaction/{dataSource}`

Removes the compaction config for a dataSource.

#### Server Information

##### GET

* `/druid/coordinator/v1/servers`

Returns a list of servers URLs using the format `{hostname}:{port}`. Note that
processes that run with different types will appear multiple times with different
ports.

* `/druid/coordinator/v1/servers?simple`

Returns a list of server data objects in which each object has the following keys:
* `host`: host URL include (`{hostname}:{port}`)
* `type`: process type (`indexer-executor`, `historical`)
* `currSize`: storage size currently used
* `maxSize`: maximum storage size
* `priority`
* `tier`

### Overlord

#### Leadership

##### GET

* `/druid/indexer/v1/leader` 

Returns the current leader Overlord of the cluster. If you have multiple Overlords, just one is leading at any given time. The others are on standby.

* `/druid/indexer/v1/isLeader`

This returns a JSON object with field "leader", either true or false. In addition, this call returns HTTP 200 if the
server is the current leader and HTTP 404 if not. This is suitable for use as a load balancer status check if you
only want the active leader to be considered in-service at the load balancer.

#### Tasks<a name="overlord-tasks"></a> 

Note that all _interval_ URL parameters are ISO 8601 strings delimited by a `_` instead of a `/` 
(e.g., 2016-06-27_2016-06-28).

##### GET

* `/druid/indexer/v1/tasks`

Retrieve list of tasks. Accepts query string parameters `state`, `datasource`, `createdTimeInterval`, `max`, and `type`.

|Query Parameter |Description |
|---|---|
|`state`|filter list of tasks by task state, valid options are `running`, `complete`, `waiting`, and `pending`.|
| `datasource`| return tasks filtered by Druid datasource.|
| `createdTimeInterval`| return tasks created within the specified interval. | 
| `max`| maximum number of `"complete"` tasks to return. Only applies when `state` is set to `"complete"`.|
| `type`| filter tasks by task type. See [task documentation](../ingestion/tasks.html) for more details.|


* `/druid/indexer/v1/completeTasks`

Retrieve list of complete tasks. Equivalent to `/druid/indexer/v1/tasks?state=complete`.

* `/druid/indexer/v1/runningTasks`

Retrieve list of running tasks. Equivalent to `/druid/indexer/v1/tasks?state=running`.

* `/druid/indexer/v1/waitingTasks`

Retrieve list of waiting tasks. Equivalent to `/druid/indexer/v1/tasks?state=waiting`.

* `/druid/indexer/v1/pendingTasks`

Retrieve list of pending tasks. Equivalent to `/druid/indexer/v1/tasks?state=pending`.

* `/druid/indexer/v1/task/{taskId}`

Retrieve the 'payload' of a task.

* `/druid/indexer/v1/task/{taskId}/status`

Retrieve the status of a task.

* `/druid/indexer/v1/task/{taskId}/segments`

Retrieve information about the segments of a task.

<div class="note caution">
This API is deprecated and will be removed in future releases.
</div>

* `/druid/indexer/v1/task/{taskId}/reports`

Retrieve a [task completion report](../ingestion/reports.html) for a task. Only works for completed tasks.

##### POST

* `/druid/indexer/v1/task` 

Endpoint for submitting tasks and supervisor specs to the Overlord. Returns the taskId of the submitted task.

* `/druid/indexer/v1/task/{taskId}/shutdown`

Shuts down a task.

* `/druid/indexer/v1/datasources/{dataSource}/shutdownAllTasks`

Shuts down all tasks for a dataSource.

* `/druid/indexer/v1/taskStatus`

Retrieve list of task status objects for list of task id strings in request body.

##### DELETE

* `/druid/indexer/v1/pendingSegments/{dataSource}`

Manually clean up pending segments table in metadata storage for `datasource`. Returns a JSON object response with 
`numDeleted` and count of rows deleted from the pending segments table. This API is used by the 
`druid.coordinator.kill.pendingSegments.on` [coordinator setting](../configuration/index.html#coordinator-operation)
which automates this operation to perform periodically.

#### Supervisors

##### GET

* `/druid/indexer/v1/supervisor`

Returns a list of strings of the currently active supervisor ids.

* `/druid/indexer/v1/supervisor?full`

Returns a list of objects of the currently active supervisors.

|Field|Type|Description|
|---|---|---|
|`id`|String|supervisor unique identifier|
|`spec`|SupervisorSpec|json specification of supervisor (See Supervisor Configuration for details)|

* `/druid/indexer/v1/supervisor/<supervisorId>`

Returns the current spec for the supervisor with the provided ID.

* `/druid/indexer/v1/supervisor/<supervisorId>/status`

Returns the current status of the supervisor with the provided ID.

* `/druid/indexer/v1/supervisor/history`

Returns an audit history of specs for all supervisors (current and past).

* `/druid/indexer/v1/supervisor/<supervisorId>/history`

Returns an audit history of specs for the supervisor with the provided ID.

##### POST

* `/druid/indexer/v1/supervisor`

Create a new supervisor or update an existing one.

* `/druid/indexer/v1/supervisor/<supervisorId>/suspend`

Suspend the current running supervisor of the provided ID. Responds with updated SupervisorSpec.

* `/druid/indexer/v1/supervisor/suspendAll`

Suspend all supervisors at once.

* `/druid/indexer/v1/supervisor/<supervisorId>/resume`

Resume indexing tasks for a supervisor. Responds with updated SupervisorSpec.

* `/druid/indexer/v1/supervisor/resumeAll`

Resume all supervisors at once.

* `/druid/indexer/v1/supervisor/<supervisorId>/reset`

Reset the specified supervisor.

* `/druid/indexer/v1/supervisor/<supervisorId>/terminate`

Terminate a supervisor of the provided ID.

* `/druid/indexer/v1/supervisor/terminateAll`

Terminate all supervisors at once.

* `/druid/indexer/v1/supervisor/<supervisorId>/shutdown`

Shutdown a supervisor.

<div class="note caution">
This API is deprecated and will be removed in future releases.
Please use the equivalent 'terminate' instead.
</div>

#### Dynamic Configuration
See [Overlord Dynamic Configuration](../configuration/index.html#overlord-dynamic-configuration) for details. 

Note that all _interval_ URL parameters are ISO 8601 strings delimited by a `_` instead of a `/` 
(e.g., 2016-06-27_2016-06-28).

##### GET

* `/druid/indexer/v1/worker`

Retreives current overlord dynamic configuration. 

* `/druid/indexer/v1/worker/history?interval={interval}&counter={count}`

Retrieves history of changes to overlord dynamic configuration. Accepts `interval` and  `count` query string parameters 
to filter by interval and limit the number of results respectively.

* `/druid/indexer/v1/scaling`

Retrieves overlord scaling events if auto-scaling runners are in use.

##### POST

* /druid/indexer/v1/worker

Update overlord dynamic worker configuration.

## Data Server

This section documents the API endpoints for the processes that reside on Data servers (MiddleManagers/Peons and Historicals) 
in the suggested [three-server configuration](../design/processes.html#server-types).

### MiddleManager

##### GET

* `/druid/worker/v1/enabled`

Check whether a MiddleManager is in an enabled or disabled state. Returns JSON object keyed by the combined `druid.host` 
and `druid.port` with the boolean state as the value.

```json
{"localhost:8091":true}
```

* `/druid/worker/v1/tasks`

Retrieve a list of active tasks being run on MiddleManager. Returns JSON list of taskid strings.  Normal usage should 
prefer to use the `/druid/indexer/v1/tasks` [Overlord API](#overlord) or one of it's task state specific variants instead.

```json
["index_wikiticker_2019-02-11T02:20:15.316Z"]
```

* `/druid/worker/v1/task/{taskid}/log` 

Retrieve task log output stream by task id. Normal usage should prefer to use the `/druid/indexer/v1/task/{taskId}/log`
[Overlord API](#overlord) instead.

##### POST

* `/druid/worker/v1/disable`

'Disable' a MiddleManager, causing it to stop accepting new tasks but complete all existing tasks. Returns JSON  object 
keyed by the combined `druid.host` and `druid.port`:

```json
{"localhost:8091":"disabled"}
```

* `/druid/worker/v1/enable`

'Enable' a MiddleManager, allowing it to accept new tasks again if it was previously disabled. Returns JSON  object 
keyed by the combined `druid.host` and `druid.port`:

```json
{"localhost:8091":"enabled"}
```

* `/druid/worker/v1/task/{taskid}/shutdown`

Shutdown a running task by `taskid`. Normal usage should prefer to use the `/druid/indexer/v1/task/{taskId}/shutdown` 
[Overlord API](#overlord) instead. Returns JSON:

```json
{"task":"index_kafka_wikiticker_f7011f8ffba384b_fpeclode"}
```


### Peon

#### GET

* `/druid/worker/v1/chat/{taskId}/rowStats`

Retrieve a live row stats report from a Peon. See [task reports](../ingestion/reports.html) for more details.

* `/druid/worker/v1/chat/{taskId}/unparseableEvents`

Retrieve an unparseable events report from a Peon. See [task reports](../ingestion/reports.html) for more details.

### Historical

#### Segment Loading

##### GET

* `/druid/historical/v1/loadstatus`

Returns JSON of the form `{"cacheInitialized":<value>}`, where value is either `true` or `false` indicating if all
segments in the local cache have been loaded. This can be used to know when a Historical process is ready
to be queried after a restart.

* `/druid/historical/v1/readiness`

Similar to `/druid/historical/v1/loadstatus`, but instead of returning JSON with a flag, responses 200 OK if segments
in the local cache have been loaded, and 503 SERVICE UNAVAILABLE, if they haven't.


## Query Server

This section documents the API endpoints for the processes that reside on Query servers (Brokers) in the suggested [three-server configuration](../design/processes.html#server-types).

### Broker

#### Datasource Information

Note that all _interval_ URL parameters are ISO 8601 strings delimited by a `_` instead of a `/` 
(e.g., 2016-06-27_2016-06-28).

##### GET

* `/druid/v2/datasources`

Returns a list of queryable datasources.

* `/druid/v2/datasources/{dataSourceName}`

Returns the dimensions and metrics of the datasource. Optionally, you can provide request parameter "full" to get list of served intervals with dimensions and metrics being served for those intervals. You can also provide request param "interval" explicitly to refer to a particular interval.

If no interval is specified, a default interval spanning a configurable period before the current time will be used. The default duration of this interval is specified in ISO 8601 duration format via:

druid.query.segmentMetadata.defaultHistory

* `/druid/v2/datasources/{dataSourceName}/dimensions`

Returns the dimensions of the datasource.

<div class="note caution">
This API is deprecated and will be removed in future releases. Please use <a href="../querying/segmentmetadataquery.html">SegmentMetadataQuery</a> instead
which provides more comprehensive information and supports all dataSource types including streaming dataSources. It's also encouraged to use [INFORMATION_SCHEMA tables](../querying/sql.html#retrieving-metadata)
if you're using SQL.
</div>

* `/druid/v2/datasources/{dataSourceName}/metrics`

Returns the metrics of the datasource.

<div class="note caution">
This API is deprecated and will be removed in future releases. Please use <a href="../querying/segmentmetadataquery.html">SegmentMetadataQuery</a> instead
which provides more comprehensive information and supports all dataSource types including streaming dataSources. It's also encouraged to use [INFORMATION_SCHEMA tables](../querying/sql.html#retrieving-metadata)
if you're using SQL.
</div>

* `/druid/v2/datasources/{dataSourceName}/candidates?intervals={comma-separated-intervals}&numCandidates={numCandidates}`

Returns segment information lists including server locations for the given datasource and intervals. If "numCandidates" is not specified, it will return all servers for each interval.

#### Load Status

##### GET

* `/druid/broker/v1/loadstatus`

Returns a flag indicating if the Broker knows about all segments in Zookeeper. This can be used to know when a Broker process is ready to be queried after a restart.

#### Queries

##### POST

* `/druid/v2/`

The endpoint for submitting queries. Accepts an option `?pretty` that pretty prints the results.

* `/druid/v2/candidates/`

Returns segment information lists including server locations for the given query..

