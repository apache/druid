---
id: sql-metadata-tables
title: "SQL metadata tables"
sidebar_label: "SQL metadata tables"
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

> Apache Druid supports two query languages: Druid SQL and [native queries](querying.md).
> This document describes the SQL language.


Druid Brokers infer table and column metadata for each datasource from segments loaded in the cluster, and use this to
plan [SQL queries](./sql.md). This metadata is cached on Broker startup and also updated periodically in the background through
[SegmentMetadata queries](segmentmetadataquery.md). Background metadata refreshing is triggered by
segments entering and exiting the cluster, and can also be throttled through configuration.

Druid exposes system information through special system tables. There are two such schemas available: Information Schema and Sys Schema.
Information schema provides details about table and column types. The "sys" schema provides information about Druid internals like segments/tasks/servers.

## INFORMATION SCHEMA

You can access table and column metadata through JDBC using `connection.getMetaData()`, or through the
INFORMATION_SCHEMA tables described below. For example, to retrieve metadata for the Druid
datasource "foo", use the query:

```sql
SELECT *
FROM INFORMATION_SCHEMA.COLUMNS
WHERE "TABLE_SCHEMA" = 'druid' AND "TABLE_NAME" = 'foo'
```

> Note: INFORMATION_SCHEMA tables do not currently support Druid-specific functions like `TIME_PARSE` and
> `APPROX_QUANTILE_DS`. Only standard SQL functions can be used.

### SCHEMATA table
`INFORMATION_SCHEMA.SCHEMATA` provides a list of all known schemas, which include `druid` for standard [Druid Table datasources](datasource.md#table), `lookup` for [Lookups](datasource.md#lookup), `sys` for the virtual [System metadata tables](#system-schema), and `INFORMATION_SCHEMA` for these virtual tables. Tables are allowed to have the same name across different schemas, so the schema may be included in an SQL statement to distinguish them, e.g. `lookup.table` vs `druid.table`.

|Column|Notes|
|------|-----|
|CATALOG_NAME|Always set as `druid`|
|SCHEMA_NAME|`druid`, `lookup`, `sys`, or `INFORMATION_SCHEMA`|
|SCHEMA_OWNER|Unused|
|DEFAULT_CHARACTER_SET_CATALOG|Unused|
|DEFAULT_CHARACTER_SET_SCHEMA|Unused|
|DEFAULT_CHARACTER_SET_NAME|Unused|
|SQL_PATH|Unused|

### TABLES table
`INFORMATION_SCHEMA.TABLES` provides a list of all known tables and schemas.

|Column|Notes|
|------|-----|
|TABLE_CATALOG|Always set as `druid`|
|TABLE_SCHEMA|The 'schema' which the table falls under, see [SCHEMATA table for details](#schemata-table)|
|TABLE_NAME|Table name. For the `druid` schema, this is the `dataSource`.|
|TABLE_TYPE|"TABLE" or "SYSTEM_TABLE"|
|IS_JOINABLE|If a table is directly joinable if on the right hand side of a `JOIN` statement, without performing a subquery, this value will be set to `YES`, otherwise `NO`. Lookups are always joinable because they are globally distributed among Druid query processing nodes, but Druid datasources are not, and will use a less efficient subquery join.|
|IS_BROADCAST|If a table is 'broadcast' and distributed among all Druid query processing nodes, this value will be set to `YES`, such as lookups and Druid datasources which have a 'broadcast' load rule, else `NO`.|

### COLUMNS table
`INFORMATION_SCHEMA.COLUMNS` provides a list of all known columns across all tables and schema.

|Column|Notes|
|------|-----|
|TABLE_CATALOG|Always set as `druid`|
|TABLE_SCHEMA|The 'schema' which the table column falls under, see [SCHEMATA table for details](#schemata-table)|
|TABLE_NAME|The 'table' which the column belongs to, see [TABLES table for details](#tables-table)|
|COLUMN_NAME|The column name|
|ORDINAL_POSITION|The order in which the column is stored in a table|
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

For example, this query returns [data type](sql-data-types.md) information for columns in the `foo` table:

```sql
SELECT "ORDINAL_POSITION", "COLUMN_NAME", "IS_NULLABLE", "DATA_TYPE", "JDBC_TYPE"
FROM INFORMATION_SCHEMA.COLUMNS
WHERE "TABLE_NAME" = 'foo'
```

## SYSTEM SCHEMA

The "sys" schema provides visibility into Druid segments, servers and tasks.

> Note: "sys" tables do not currently support Druid-specific functions like `TIME_PARSE` and
> `APPROX_QUANTILE_DS`. Only standard SQL functions can be used.

### SEGMENTS table

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
|num_rows|LONG|Number of rows in this segment, or zero if the number of rows is not known.<br /><br />This row count is gathered by the Broker in the background. It will be zero if the Broker has not gathered a row count for this segment yet. For segments ingested from streams, the reported row count may lag behind the result of a `count(*)` query because the cached `num_rows` on the Broker may be out of date. This will settle shortly after new rows stop being written to that particular segment.|
|is_active|LONG|True for segments that represent the latest state of a datasource.<br /><br />Equivalent to `(is_published = 1 AND is_overshadowed = 0) OR is_realtime = 1`. In steady state, when no ingestion or data management operations are happening, `is_active` will be equivalent to `is_available`. However, they may differ from each other when ingestion or data management operations have executed recently. In these cases, Druid will load and unload segments appropriately to bring actual availability in line with the expected state given by `is_active`.|
|is_published|LONG|Boolean represented as long type where 1 = true, 0 = false. 1 if this segment has been published to the metadata store and is marked as used. See the [segment lifecycle documentation](../design/architecture.md#segment-lifecycle) for more details.|
|is_available|LONG|Boolean represented as long type where 1 = true, 0 = false. 1 if this segment is currently being served by any data serving process, like a Historical or a realtime ingestion task. See the [segment lifecycle documentation](../design/architecture.md#segment-lifecycle) for more details.|
|is_realtime|LONG|Boolean represented as long type where 1 = true, 0 = false. 1 if this segment is _only_ served by realtime tasks, and 0 if any Historical process is serving this segment.|
|is_overshadowed|LONG|Boolean represented as long type where 1 = true, 0 = false. 1 if this segment is published and is _fully_ overshadowed by some other published segments. Currently, `is_overshadowed` is always 0 for unpublished segments, although this may change in the future. You can filter for segments that "should be published" by filtering for `is_published = 1 AND is_overshadowed = 0`. Segments can briefly be both published and overshadowed if they were recently replaced, but have not been unpublished yet. See the [segment lifecycle documentation](../design/architecture.md#segment-lifecycle) for more details.||shard_spec|STRING|JSON-serialized form of the segment `ShardSpec`|
|dimensions|STRING|JSON-serialized form of the segment dimensions|
|metrics|STRING|JSON-serialized form of the segment metrics|
|last_compaction_state|STRING|JSON-serialized form of the compaction task's config (compaction task which created this segment). May be null if segment was not created by compaction task.|

For example, to retrieve all currently active segments for datasource "wikipedia", use the query:

```sql
SELECT * FROM sys.segments
WHERE datasource = 'wikipedia'
AND is_active = 1
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
WHERE is_active = 1
GROUP BY 1
ORDER BY 2 DESC
```

This query goes a step further and shows the overall profile of available, non-realtime segments across buckets of 1 million rows each for the `foo` datasource:

```sql
SELECT ABS("num_rows" /  1000000) as "bucket",
  COUNT(*) as segments,
  SUM("size") / 1048576 as totalSizeMiB,
  MIN("size") / 1048576 as minSizeMiB,
  AVG("size") / 1048576 as averageSizeMiB,
  MAX("size") / 1048576 as maxSizeMiB,
  SUM("num_rows") as totalRows,
  MIN("num_rows") as minRows,
  AVG("num_rows") as averageRows,
  MAX("num_rows") as maxRows,
  (AVG("size") / AVG("num_rows"))  as avgRowSizeB
FROM sys.segments
WHERE is_available = 1 AND is_realtime = 0 AND "datasource" = `foo`
GROUP BY 1
ORDER BY 1
```

If you want to retrieve segment that was compacted (ANY compaction):

```sql
SELECT * FROM sys.segments WHERE is_active = 1 AND last_compaction_state IS NOT NULL
```

or if you want to retrieve segment that was compacted only by a particular compaction spec (such as that of the auto compaction):

```sql
SELECT * FROM sys.segments WHERE is_active = 1 AND last_compaction_state = 'CompactionState{partitionsSpec=DynamicPartitionsSpec{maxRowsPerSegment=5000000, maxTotalRows=9223372036854775807}, indexSpec={bitmap={type=roaring, compressRunOnSerialization=true}, dimensionCompression=lz4, metricCompression=lz4, longEncoding=longs, segmentLoader=null}}'
```

### SERVERS table

Servers table lists all discovered servers in the cluster.

|Column|Type|Notes|
|------|-----|-----|
|server|STRING|Server name in the form host:port|
|host|STRING|Hostname of the server|
|plaintext_port|LONG|Unsecured port of the server, or -1 if plaintext traffic is disabled|
|tls_port|LONG|TLS port of the server, or -1 if TLS is disabled|
|server_type|STRING|Type of Druid service. Possible values include: COORDINATOR, OVERLORD,  BROKER, ROUTER, HISTORICAL, MIDDLE_MANAGER or PEON.|
|tier|STRING|Distribution tier see [druid.server.tier](../configuration/index.md#historical-general-configuration). Only valid for HISTORICAL type, for other types it's null|
|current_size|LONG|Current size of segments in bytes on this server. Only valid for HISTORICAL type, for other types it's 0|
|max_size|LONG|Max size in bytes this server recommends to assign to segments see [druid.server.maxSize](../configuration/index.md#historical-general-configuration). Only valid for HISTORICAL type, for other types it's 0|
|is_leader|LONG|1 if the server is currently the 'leader' (for services which have the concept of leadership), otherwise 0 if the server is not the leader, or the default long value (0 or null depending on `druid.generic.useDefaultValueForNull`) if the server type does not have the concept of leadership|

To retrieve information about all servers, use the query:

```sql
SELECT * FROM sys.servers;
```

### SERVER_SEGMENTS table

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

### TASKS table

The tasks table provides information about active and recently-completed indexing tasks. For more information
check out the documentation for [ingestion tasks](../ingestion/tasks.md).

|Column|Type|Notes|
|------|-----|-----|
|task_id|STRING|Unique task identifier|
|group_id|STRING|Task group ID for this task, the value depends on the task `type`. For example, for native index tasks, it's same as `task_id`, for sub tasks, this value is the parent task's ID|
|type|STRING|Task type, for example this value is "index" for indexing tasks. See [tasks-overview](../ingestion/tasks.md)|
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

### SUPERVISORS table

The supervisors table provides information about supervisors.

|Column|Type|Notes|
|------|-----|-----|
|supervisor_id|STRING|Supervisor task identifier|
|state|STRING|Basic state of the supervisor. Available states: `UNHEALTHY_SUPERVISOR`, `UNHEALTHY_TASKS`, `PENDING`, `RUNNING`, `SUSPENDED`, `STOPPING`. Check [Kafka Docs](../development/extensions-core/kafka-supervisor-operations.md) for details.|
|detailed_state|STRING|Supervisor specific state. (See documentation of the specific supervisor for details, e.g. [Kafka](../development/extensions-core/kafka-ingestion.md) or [Kinesis](../development/extensions-core/kinesis-ingestion.md))|
|healthy|LONG|Boolean represented as long type where 1 = true, 0 = false. 1 indicates a healthy supervisor|
|type|STRING|Type of supervisor, e.g. `kafka`, `kinesis` or `materialized_view`|
|source|STRING|Source of the supervisor, e.g. Kafka topic or Kinesis stream|
|suspended|LONG|Boolean represented as long type where 1 = true, 0 = false. 1 indicates supervisor is in suspended state|
|spec|STRING|JSON-serialized supervisor spec|

For example, to retrieve supervisor tasks information filtered by health status, use the query

```sql
SELECT * FROM sys.supervisors WHERE healthy=0;
```
