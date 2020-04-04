---
id: metadatatables
title: "Metadata tables"
sidebar_label: "Metadata tables"
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

<!--
  The format of the tables that describe the functions and operators
  should not be changed without updating the script create-sql-function-doc
  in web-console/script/create-sql-function-doc, because the script detects
  patterns in this markdown file and parse it to TypeScript file for web console
-->


Druid Brokers infer table and column metadata for each datasource from segments loaded in the cluster, and use this to
plan SQL queries. This metadata is cached on Broker startup and also updated periodically in the background through
[SegmentMetadata queries](segmentmetadataquery.html). Background metadata refreshing is triggered by
segments entering and exiting the cluster, and can also be throttled through configuration.

Druid exposes system information through special system tables. There are two such schemas available: Information Schema and Sys Schema.
Information schema provides details about table and column types. The "sys" schema provides information about Druid internals like segments/tasks/servers.

### INFORMATION SCHEMA

You can access table and column metadata through JDBC using `connection.getMetaData()`, or through the
INFORMATION_SCHEMA tables described below. For example, to retrieve metadata for the Druid
datasource "foo", use the query:

```sql
SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'druid' AND TABLE_NAME = 'foo'
```

#### SCHEMATA table

|Column|Notes|
|------|-----|
|CATALOG_NAME|Unused|
|SCHEMA_NAME||
|SCHEMA_OWNER|Unused|
|DEFAULT_CHARACTER_SET_CATALOG|Unused|
|DEFAULT_CHARACTER_SET_SCHEMA|Unused|
|DEFAULT_CHARACTER_SET_NAME|Unused|
|SQL_PATH|Unused|

#### TABLES table

|Column|Notes|
|------|-----|
|TABLE_CATALOG|Unused|
|TABLE_SCHEMA||
|TABLE_NAME||
|TABLE_TYPE|"TABLE" or "SYSTEM_TABLE"|

#### COLUMNS table

|Column|Notes|
|------|-----|
|TABLE_CATALOG|Unused|
|TABLE_SCHEMA||
|TABLE_NAME||
|COLUMN_NAME||
|ORDINAL_POSITION||
|COLUMN_DEFAULT|Unused|
|IS_NULLABLE||
|DATA_TYPE||
|CHARACTER_MAXIMUM_LENGTH|Unused|
|CHARACTER_OCTET_LENGTH|Unused|
|NUMERIC_PRECISION||
|NUMERIC_PRECISION_RADIX||
|NUMERIC_SCALE||
|DATETIME_PRECISION||
|CHARACTER_SET_NAME||
|COLLATION_NAME||
|JDBC_TYPE|Type code from java.sql.Types (Druid extension)|

### SYSTEM SCHEMA

The "sys" schema provides visibility into Druid segments, servers and tasks.

#### SEGMENTS table

Segments table provides details on all Druid segments, whether they are published yet or not.

|Column|Type|Notes|
|------|-----|-----|
|segment_id|STRING|Unique segment identifier|
|datasource|STRING|Name of datasource|
|start|STRING|Interval start time (in ISO 8601 format)|
|end|STRING|Interval end time (in ISO 8601 format)|
|size|LONG|Size of segment in bytes|
|version|STRING|Version string (generally an ISO8601 timestamp corresponding to when the segment set was first started). Higher version means the more recently created segment. Version comparing is based on string comparison.|
|partition_num|LONG|Partition number (an integer, unique within a datasource+interval+version; may not necessarily be contiguous)|
|num_replicas|LONG|Number of replicas of this segment currently being served|
|num_rows|LONG|Number of rows in current segment, this value could be null if unknown to Broker at query time|
|is_published|LONG|Boolean is represented as long type where 1 = true, 0 = false. 1 represents this segment has been published to the metadata store with `used=1`. See the [Architecture page](../design/architecture.md#segment-lifecycle) for more details.|
|is_available|LONG|Boolean is represented as long type where 1 = true, 0 = false. 1 if this segment is currently being served by any process(Historical or realtime). See the [Architecture page](../design/architecture.md#segment-lifecycle) for more details.|
|is_realtime|LONG|Boolean is represented as long type where 1 = true, 0 = false. 1 if this segment is _only_ served by realtime tasks, and 0 if any historical process is serving this segment.|
|is_overshadowed|LONG|Boolean is represented as long type where 1 = true, 0 = false. 1 if this segment is published and is _fully_ overshadowed by some other published segments. Currently, is_overshadowed is always false for unpublished segments, although this may change in the future. You can filter for segments that "should be published" by filtering for `is_published = 1 AND is_overshadowed = 0`. Segments can briefly be both published and overshadowed if they were recently replaced, but have not been unpublished yet. See the [Architecture page](../design/architecture.md#segment-lifecycle) for more details.|
|payload|STRING|JSON-serialized data segment payload|

For example to retrieve all segments for datasource "wikipedia", use the query:

```sql
SELECT * FROM sys.segments WHERE datasource = 'wikipedia'
```

Another example to retrieve segments total_size, avg_size, avg_num_rows and num_segments per datasource:

```sql
SELECT
    datasource,
    SUM("size") AS total_size,
    CASE WHEN SUM("size") = 0 THEN 0 ELSE SUM("size") / (COUNT(*) FILTER(WHERE "size" > 0)) END AS avg_size,
    CASE WHEN SUM(num_rows) = 0 THEN 0 ELSE SUM("num_rows") / (COUNT(*) FILTER(WHERE num_rows > 0)) END AS avg_num_rows,
    COUNT(*) AS num_segments
FROM sys.segments
GROUP BY 1
ORDER BY 2 DESC
```

*Caveat:* Note that a segment can be served by more than one stream ingestion tasks or Historical processes, in that case it would have multiple replicas. These replicas are weakly consistent with each other when served by multiple ingestion tasks, until a segment is eventually served by a Historical, at that point the segment is immutable. Broker prefers to query a segment from Historical over an ingestion task. But if a segment has multiple realtime replicas, for e.g.. Kafka index tasks, and one task is slower than other, then the sys.segments query results can vary for the duration of the tasks because only one of the ingestion tasks is queried by the Broker and it is not guaranteed that the same task gets picked every time. The `num_rows` column of segments table can have inconsistent values during this period. There is an open [issue](https://github.com/apache/druid/issues/5915) about this inconsistency with stream ingestion tasks.

#### SERVERS table

Servers table lists all discovered servers in the cluster.

|Column|Type|Notes|
|------|-----|-----|
|server|STRING|Server name in the form host:port|
|host|STRING|Hostname of the server|
|plaintext_port|LONG|Unsecured port of the server, or -1 if plaintext traffic is disabled|
|tls_port|LONG|TLS port of the server, or -1 if TLS is disabled|
|server_type|STRING|Type of Druid service. Possible values include: COORDINATOR, OVERLORD,  BROKER, ROUTER, HISTORICAL, MIDDLE_MANAGER or PEON.|
|tier|STRING|Distribution tier see [druid.server.tier](../configuration/index.html#historical-general-configuration). Only valid for HISTORICAL type, for other types it's null|
|current_size|LONG|Current size of segments in bytes on this server. Only valid for HISTORICAL type, for other types it's 0|
|max_size|LONG|Max size in bytes this server recommends to assign to segments see [druid.server.maxSize](../configuration/index.html#historical-general-configuration). Only valid for HISTORICAL type, for other types it's 0|

To retrieve information about all servers, use the query:

```sql
SELECT * FROM sys.servers;
```

#### SERVER_SEGMENTS table

SERVER_SEGMENTS is used to join servers with segments table

|Column|Type|Notes|
|------|-----|-----|
|server|STRING|Server name in format host:port (Primary key of [servers table](#servers-table))|
|segment_id|STRING|Segment identifier (Primary key of [segments table](#segments-table))|

JOIN between "servers" and "segments" can be used to query the number of segments for a specific datasource,
grouped by server, example query:

```sql
SELECT count(segments.segment_id) as num_segments from sys.segments as segments
INNER JOIN sys.server_segments as server_segments
ON segments.segment_id  = server_segments.segment_id
INNER JOIN sys.servers as servers
ON servers.server = server_segments.server
WHERE segments.datasource = 'wikipedia'
GROUP BY servers.server;
```

#### TASKS table

The tasks table provides information about active and recently-completed indexing tasks. For more information
check out the documentation for [ingestion tasks](../ingestion/tasks.html).

|Column|Type|Notes|
|------|-----|-----|
|task_id|STRING|Unique task identifier|
|group_id|STRING|Task group ID for this task, the value depends on the task `type`. For example, for native index tasks, it's same as `task_id`, for sub tasks, this value is the parent task's ID|
|type|STRING|Task type, for example this value is "index" for indexing tasks. See [tasks-overview](../ingestion/tasks.html)|
|datasource|STRING|Datasource name being indexed|
|created_time|STRING|Timestamp in ISO8601 format corresponding to when the ingestion task was created. Note that this value is populated for completed and waiting tasks. For running and pending tasks this value is set to 1970-01-01T00:00:00Z|
|queue_insertion_time|STRING|Timestamp in ISO8601 format corresponding to when this task was added to the queue on the Overlord|
|status|STRING|Status of a task can be RUNNING, FAILED, SUCCESS|
|runner_status|STRING|Runner status of a completed task would be NONE, for in-progress tasks this can be RUNNING, WAITING, PENDING|
|duration|LONG|Time it took to finish the task in milliseconds, this value is present only for completed tasks|
|location|STRING|Server name where this task is running in the format host:port, this information is present only for RUNNING tasks|
|host|STRING|Hostname of the server where task is running|
|plaintext_port|LONG|Unsecured port of the server, or -1 if plaintext traffic is disabled|
|tls_port|LONG|TLS port of the server, or -1 if TLS is disabled|
|error_msg|STRING|Detailed error message in case of FAILED tasks|

For example, to retrieve tasks information filtered by status, use the query

```sql
SELECT * FROM sys.tasks WHERE status='FAILED';
```

#### SUPERVISORS table

The supervisors table provides information about supervisors.

|Column|Type|Notes|
|------|-----|-----|
|supervisor_id|STRING|Supervisor task identifier|
|state|STRING|Basic state of the supervisor. Available states: `UNHEALTHY_SUPERVISOR`, `UNHEALTHY_TASKS`, `PENDING`, `RUNNING`, `SUSPENDED`, `STOPPING`. Check [Kafka Docs](../development/extensions-core/kafka-ingestion.html#operations) for details.|
|detailed_state|STRING|Supervisor specific state. (See documentation of the specific supervisor for details, e.g. [Kafka](../development/extensions-core/kafka-ingestion.html) or [Kinesis](../development/extensions-core/kinesis-ingestion.html))|
|healthy|LONG|Boolean represented as long type where 1 = true, 0 = false. 1 indicates a healthy supervisor|
|type|STRING|Type of supervisor, e.g. `kafka`, `kinesis` or `materialized_view`|
|source|STRING|Source of the supervisor, e.g. Kafka topic or Kinesis stream|
|suspended|LONG|Boolean represented as long type where 1 = true, 0 = false. 1 indicates supervisor is in suspended state|
|spec|STRING|JSON-serialized supervisor spec|

For example, to retrieve supervisor tasks information filtered by health status, use the query

```sql
SELECT * FROM sys.supervisors WHERE healthy=0;
```

Note that sys tables may not support all the Druid SQL Functions.