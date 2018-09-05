---
layout: doc_page
---

# API Reference

This page documents all of the API endpoints for each Druid service type.

## Table of Contents
  * [Common](#common)
  * [Coordinator](#coordinator)
  * [Overlord](#overlord)
  * [MiddleManager](#middlemanager)
  * [Peon](#peon)
  * [Broker](#broker)
  * [Historical](#historical)

## Common

The following endpoints are supported by all nodes.

### Node information

#### GET

* `/status`

Returns the Druid version, loaded extensions, memory used, total memory and other useful information about the node.

* `/status/health`

An endpoint that always returns a boolean "true" value with a 200 OK response, useful for automated health checks.

* `/status/properties`

Returns the current configuration properties of the node.

## Coordinator

### Leadership

#### GET

* `/druid/coordinator/v1/leader`

Returns the current leader coordinator of the cluster.

* `/druid/coordinator/v1/isLeader`

Returns true if the coordinator receiving the request is the current leader.

### Segment Loading

#### GET

Returns a JSON object with field "leader", either true or false, indicating if this server is the current leader
coordinator of the cluster. In addition, returns HTTP 200 if the server is the current leader and HTTP 404 if not.
This is suitable for use as a load balancer status check if you only want the active leader to be considered in-service
at the load balancer.

* `/druid/coordinator/v1/loadstatus`

Returns the percentage of segments actually loaded in the cluster versus segments that should be loaded in the cluster.

 * `/druid/coordinator/v1/loadstatus?simple`

Returns the number of segments left to load until segments that should be loaded in the cluster are available for queries. This does not include replication.

* `/druid/coordinator/v1/loadstatus?full`

Returns the number of segments left to load in each tier until segments that should be loaded in the cluster are all available. This includes replication.

* `/druid/coordinator/v1/loadqueue`

Returns the ids of segments to load and drop for each historical node.

* `/druid/coordinator/v1/loadqueue?simple`

Returns the number of segments to load and drop, as well as the total segment load and drop size in bytes for each historical node.

* `/druid/coordinator/v1/loadqueue?full`

Returns the serialized JSON of segments to load and drop for each historical node.

### Metadata store information

#### GET

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

#### POST

* `/druid/coordinator/v1/metadata/datasources/{dataSourceName}/segments`

Returns a list of all segments, overlapping with any of given intervals,  for a datasource as stored in the metadata store. Request body is array of string intervals like [interval1, interval2,...] for example ["2012-01-01T00:00:00.000/2012-01-03T00:00:00.000", "2012-01-05T00:00:00.000/2012-01-07T00:00:00.000"]

* `/druid/coordinator/v1/metadata/datasources/{dataSourceName}/segments?full`

Returns a list of all segments, overlapping with any of given intervals, for a datasource with the full segment metadata as stored in the metadata store. Request body is array of string intervals like [interval1, interval2,...] for example ["2012-01-01T00:00:00.000/2012-01-03T00:00:00.000", "2012-01-05T00:00:00.000/2012-01-07T00:00:00.000"]


### Datasources

#### GET

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

Returns a set of segment ids for an ISO8601 interval. Note that {interval} parameters are delimited by a `_` instead of a `/` (e.g., 2016-06-27_2016-06-28).

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

#### POST

* `/druid/coordinator/v1/datasources/{dataSourceName}`

Enables all segments of datasource which are not overshadowed by others.

* `/druid/coordinator/v1/datasources/{dataSourceName}/segments/{segmentId}`

Enables a segment of a datasource.

#### DELETE<a name="coordinator-delete"></a>

* `/druid/coordinator/v1/datasources/{dataSourceName}`

Disables a datasource.

* `/druid/coordinator/v1/datasources/{dataSourceName}/intervals/{interval}`
* `@Deprecated. /druid/coordinator/v1/datasources/{dataSourceName}?kill=true&interval={myISO8601Interval}`

Runs a [Kill task](../ingestion/tasks.html) for a given interval and datasource.

Note that {interval} parameters are delimited by a `_` instead of a `/` (e.g., 2016-06-27_2016-06-28).

* `/druid/coordinator/v1/datasources/{dataSourceName}/segments/{segmentId}`

Disables a segment.

### Retention Rules

#### GET

* `/druid/coordinator/v1/rules`

Returns all rules as JSON objects for all datasources in the cluster including the default datasource.

* `/druid/coordinator/v1/rules/{dataSourceName}`

Returns all rules for a specified datasource.


* `/druid/coordinator/v1/rules/{dataSourceName}?full`

Returns all rules for a specified datasource and includes default datasource.

* `/druid/coordinator/v1/rules/history?interval=<interval>`

 Returns audit history of rules for all datasources. default value of interval can be specified by setting `druid.audit.manager.auditHistoryMillis` (1 week if not configured) in coordinator runtime.properties

* `/druid/coordinator/v1/rules/history?count=<n>`

 Returns last <n> entries of audit history of rules for all datasources.

* `/druid/coordinator/v1/rules/{dataSourceName}/history?interval=<interval>`

 Returns audit history of rules for a specified datasource. default value of interval can be specified by setting `druid.audit.manager.auditHistoryMillis` (1 week if not configured) in coordinator runtime.properties

* `/druid/coordinator/v1/rules/{dataSourceName}/history?count=<n>`

 Returns last <n> entries of audit history of rules for a specified datasource.
 
#### POST

* `/druid/coordinator/v1/rules/{dataSourceName}`

POST with a list of rules in JSON form to update rules.

Optional Header Parameters for auditing the config change can also be specified.

|Header Param Name| Description | Default |
|----------|-------------|---------|
|`X-Druid-Author`| author making the config change|""|
|`X-Druid-Comment`| comment describing the change being done|""|

### Intervals

#### GET

Note that {interval} parameters are delimited by a `_` instead of a `/` (e.g., 2016-06-27_2016-06-28).

* `/druid/coordinator/v1/intervals`

Returns all intervals for all datasources with total size and count.

* `/druid/coordinator/v1/intervals/{interval}`

Returns aggregated total size and count for all intervals that intersect given isointerval.

* `/druid/coordinator/v1/intervals/{interval}?simple`

Returns total size and count for each interval within given isointerval.

* `/druid/coordinator/v1/intervals/{interval}?full`

Returns total size and count for each datasource for each interval within given isointerval.

### Compaction Configuration

#### GET

* `/druid/coordinator/v1/config/compaction/`

Returns all compaction configs.

* `/druid/coordinator/v1/config/compaction/{dataSource}`

Returns a compaction config of a dataSource.

#### POST

* `/druid/coordinator/v1/config/compaction?slotRatio={someRatio}&maxSlots={someMaxSlots}`

Update the capacity for compaction tasks. `slotRatio` and `maxSlots` are used to limit the max number of compaction tasks.
They mean the ratio of the total task slots to the copmaction task slots and the maximum number of task slots for compaction tasks, respectively.
The actual max number of compaction tasks is `min(maxSlots, slotRatio * total task slots)`.
Note that `slotRatio` and `maxSlots` are optional and can be omitted. If they are omitted, default values (0.1 and unbounded)
will be set for them.

* `/druid/coordinator/v1/config/compaction/{dataSource}`

Creates or updates the compaction config for a dataSource. See [Compaction Configuration](../configuration/index.html#compaction-dynamic-configuration) for configuration details.

#### DELETE

* `/druid/coordinator/v1/config/compaction/{dataSource}`

Removes the compaction config for a dataSource.

### Server Information

#### GET

* `/druid/coordinator/v1/servers`

Returns a list of servers URLs using the format `{hostname}:{port}`. Note that
nodes that run with different types will appear multiple times with different
ports.

* `/druid/coordinator/v1/servers?simple`

Returns a list of server data objects in which each object has the following keys:
- `host`: host URL include (`{hostname}:{port}`)
- `type`: node type (`indexer-executor`, `historical`)
- `currSize`: storage size currently used
- `maxSize`: maximum storage size
- `priority`
- `tier`

## Overlord

### Leadership

#### GET

* `/druid/indexer/v1/leader` 

Returns the current leader overlord of the cluster. If you have multiple overlords, just one is leading at any given time. The others are on standby.

* `/druid/indexer/v1/isLeader`

This returns a JSON object with field "leader", either true or false. In addition, this call returns HTTP 200 if the
server is the current leader and HTTP 404 if not. This is suitable for use as a load balancer status check if you
only want the active leader to be considered in-service at the load balancer.

### Tasks<a name="overlord-tasks"></a> 

#### GET

* `/druid/indexer/v1/task/{taskId}/status`

Retrieve the status of a task.

* `/druid/indexer/v1/task/{taskId}/segments`

Retrieve information about the segments of a task.

#### POST

* `/druid/indexer/v1/task` 

Endpoint for submitting tasks and supervisor specs to the overlord. Returns the taskId of the submitted task.

* `druid/indexer/v1/task/{taskId}/shutdown`

Shuts down a task.

* `druid/indexer/v1/task/{dataSource}/shutdownAllTasks`

Shuts down all tasks for a dataSource.

## MiddleManager

The MiddleManager does not have any API endpoints beyond the [common endpoints](#common).

## Peon

The Peon does not have any API endpoints beyond the [common endpoints](#common).

## Broker

### Datasource Information

#### GET

* `/druid/v2/datasources`

Returns a list of queryable datasources.

* `/druid/v2/datasources/{dataSourceName}`

Returns the dimensions and metrics of the datasource. Optionally, you can provide request parameter "full" to get list of served intervals with dimensions and metrics being served for those intervals. You can also provide request param "interval" explicitly to refer to a particular interval.

If no interval is specified, a default interval spanning a configurable period before the current time will be used. The duration of this interval is specified in ISO8601 format via:

druid.query.segmentMetadata.defaultHistory

* `/druid/v2/datasources/{dataSourceName}/dimensions`

Returns the dimensions of the datasource.

* `/druid/v2/datasources/{dataSourceName}/metrics`

Returns the metrics of the datasource.

* `/druid/v2/datasources/{dataSourceName}/candidates?intervals={comma-separated-intervals-in-ISO8601-format}&numCandidates={numCandidates}`

Returns segment information lists including server locations for the given datasource and intervals. If "numCandidates" is not specified, it will return all servers for each interval.

### Load Status

#### GET

* `/druid/broker/v1/loadstatus`

Returns a flag indicating if the broker knows about all segments in Zookeeper. This can be used to know when a broker node is ready to be queried after a restart.


### Queries

#### POST

* `/druid/v2/`

The endpoint for submitting queries. Accepts an option `?pretty` that pretty prints the results.

* `/druid/v2/candidates/`

Returns segment information lists including server locations for the given query..


## Historical

### Segment Loading

#### GET

* `/druid/historical/v1/loadstatus`

Returns JSON of the form `{"cacheInitialized":<value>}`, where value is either `true` or `false` indicating if all
segments in the local cache have been loaded. This can be used to know when a historical node is ready
to be queried after a restart.

* `/druid/historical/v1/readiness`

Similar to `/druid/historical/v1/loadstatus`, but instead of returning JSON with a flag, responses 200 OK if segments
in the local cache have been loaded, and 503 SERVICE UNAVAILABLE, if they haven't.
