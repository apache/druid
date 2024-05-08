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

:::info
 Apache Druid supports two query languages: Druid SQL and [native queries](querying.md).
 This document describes the SQL language.
:::


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

:::info
 Note: INFORMATION_SCHEMA tables do not currently support Druid-specific functions like `TIME_PARSE` and
 `APPROX_QUANTILE_DS`. Only standard SQL functions can be used.
:::

### SCHEMATA table
`INFORMATION_SCHEMA.SCHEMATA` provides a list of all known schemas, which include `druid` for standard [Druid Table datasources](datasource.md#table), `lookup` for [Lookups](datasource.md#lookup), `sys` for the virtual [System metadata tables](#system-schema), and `INFORMATION_SCHEMA` for these virtual tables. Tables are allowed to have the same name across different schemas, so the schema may be included in an SQL statement to distinguish them, e.g. `lookup.table` vs `druid.table`.

|Column|Type|Notes|
|------|----|-----|
|CATALOG_NAME|VARCHAR|Always set as `druid`|
|SCHEMA_NAME|VARCHAR|`druid`, `lookup`, `sys`, or `INFORMATION_SCHEMA`|
|SCHEMA_OWNER|VARCHAR|Unused|
|DEFAULT_CHARACTER_SET_CATALOG|VARCHAR|Unused|
|DEFAULT_CHARACTER_SET_SCHEMA|VARCHAR|Unused|
|DEFAULT_CHARACTER_SET_NAME|VARCHAR|Unused|
|SQL_PATH|VARCHAR|Unused|

### TABLES table
`INFORMATION_SCHEMA.TABLES` provides a list of all known tables and schemas.

|Column|Type|Notes|
|------|----|-----|
|TABLE_CATALOG|VARCHAR|Always set as `druid`|
|TABLE_SCHEMA|VARCHAR|The 'schema' which the table falls under, see [SCHEMATA table for details](#schemata-table)|
|TABLE_NAME|VARCHAR|Table name. For the `druid` schema, this is the `dataSource`.|
|TABLE_TYPE|VARCHAR|"TABLE" or "SYSTEM_TABLE"|
|IS_JOINABLE|VARCHAR|If a table is directly joinable if on the right hand side of a `JOIN` statement, without performing a subquery, this value will be set to `YES`, otherwise `NO`. Lookups are always joinable because they are globally distributed among Druid query processing nodes, but Druid datasources are not, and will use a less efficient subquery join.|
|IS_BROADCAST|VARCHAR|If a table is 'broadcast' and distributed among all Druid query processing nodes, this value will be set to `YES`, such as lookups and Druid datasources which have a 'broadcast' load rule, else `NO`.|

### COLUMNS table
`INFORMATION_SCHEMA.COLUMNS` provides a list of all known columns across all tables and schema.

|Column|Type|Notes|
|------|----|-----|
|TABLE_CATALOG|VARCHAR|Always set as `druid`|
|TABLE_SCHEMA|VARCHAR|The 'schema' which the table column falls under, see [SCHEMATA table for details](#schemata-table)|
|TABLE_NAME|VARCHAR|The 'table' which the column belongs to, see [TABLES table for details](#tables-table)|
|COLUMN_NAME|VARCHAR|The column name|
|ORDINAL_POSITION|BIGINT|The order in which the column is stored in a table|
|COLUMN_DEFAULT|VARCHAR|Unused|
|IS_NULLABLE|VARCHAR||
|DATA_TYPE|VARCHAR||
|CHARACTER_MAXIMUM_LENGTH|BIGINT|Unused|
|CHARACTER_OCTET_LENGTH|BIGINT|Unused|
|NUMERIC_PRECISION|BIGINT||
|NUMERIC_PRECISION_RADIX|BIGINT||
|NUMERIC_SCALE|BIGINT||
|DATETIME_PRECISION|BIGINT||
|CHARACTER_SET_NAME|VARCHAR||
|COLLATION_NAME|VARCHAR||
|JDBC_TYPE|BIGINT|Type code from java.sql.Types (Druid extension)|

For example, this query returns [data type](sql-data-types.md) information for columns in the `foo` table:

```sql
SELECT "ORDINAL_POSITION", "COLUMN_NAME", "IS_NULLABLE", "DATA_TYPE", "JDBC_TYPE"
FROM INFORMATION_SCHEMA.COLUMNS
WHERE "TABLE_NAME" = 'foo'
```
### ROUTINES table
`INFORMATION_SCHEMA.ROUTINES` provides a list of all known functions.

|Column|Type| Notes|
|------|----|------|
|ROUTINE_CATALOG|VARCHAR| The catalog that contains the routine. Always set as `druid`|
|ROUTINE_SCHEMA|VARCHAR| The schema that contains the routine. Always set as `INFORMATION_SCHEMA`|
|ROUTINE_NAME|VARCHAR| THe routine name|
|ROUTINE_TYPE|VARCHAR| The routine type. Always set as `FUNCTION`|
|IS_AGGREGATOR|VARCHAR| If a routine is an aggregator function, then the value will be set to `YES`, else `NO`|
|SIGNATURES|VARCHAR| One or more routine signatures|

For example, this query returns information about all the aggregator functions:

```sql
SELECT "ROUTINE_CATALOG", "ROUTINE_SCHEMA", "ROUTINE_NAME", "ROUTINE_TYPE", "IS_AGGREGATOR", "SIGNATURES"
FROM "INFORMATION_SCHEMA"."ROUTINES"
WHERE "IS_AGGREGATOR" = 'YES'
```


## SYSTEM SCHEMA

The "sys" schema provides visibility into Druid segments, servers and tasks.

:::info
 Note: "sys" tables do not currently support Druid-specific functions like `TIME_PARSE` and
 `APPROX_QUANTILE_DS`. Only standard SQL functions can be used.
:::

### SEGMENTS table

Segments table provides details on all Druid segments, whether they are published yet or not.

|Column|Type|Notes|
|------|-----|-----|
|segment_id|VARCHAR|Unique segment identifier|
|datasource|VARCHAR|Name of datasource|
|start|VARCHAR|Interval start time (in ISO 8601 format)|
|end|VARCHAR|Interval end time (in ISO 8601 format)|
|size|BIGINT|Size of segment in bytes|
|version|VARCHAR|Version string (generally an ISO8601 timestamp corresponding to when the segment set was first started). Higher version means the more recently created segment. Version comparing is based on string comparison.|
|partition_num|BIGINT|Partition number (an integer, unique within a datasource+interval+version; may not necessarily be contiguous)|
|num_replicas|BIGINT|Number of replicas of this segment currently being served|
|num_rows|BIGINT|Number of rows in this segment, or zero if the number of rows is not known.<br /><br />This row count is gathered by the Broker in the background. It will be zero if the Broker has not gathered a row count for this segment yet. For segments ingested from streams, the reported row count may lag behind the result of a `count(*)` query because the cached `num_rows` on the Broker may be out of date. This will settle shortly after new rows stop being written to that particular segment.|
|is_active|BIGINT|True for segments that represent the latest state of a datasource.<br /><br />Equivalent to `(is_published = 1 AND is_overshadowed = 0) OR is_realtime = 1`. In steady state, when no ingestion or data management operations are happening, `is_active` will be equivalent to `is_available`. However, they may differ from each other when ingestion or data management operations have executed recently. In these cases, Druid will load and unload segments appropriately to bring actual availability in line with the expected state given by `is_active`.|
|is_published|BIGINT|Boolean represented as long type where 1 = true, 0 = false. 1 if this segment has been published to the metadata store and is marked as used. See the [segment lifecycle documentation](../design/storage.md#segment-lifecycle) for more details.|
|is_available|BIGINT|Boolean represented as long type where 1 = true, 0 = false. 1 if this segment is currently being served by any data serving process, like a Historical or a realtime ingestion task. See the [segment lifecycle documentation](../design/storage.md#segment-lifecycle) for more details.|
|is_realtime|BIGINT|Boolean represented as long type where 1 = true, 0 = false. 1 if this segment is _only_ served by realtime tasks, and 0 if any Historical process is serving this segment.|
|is_overshadowed|BIGINT|Boolean represented as long type where 1 = true, 0 = false. 1 if this segment is published and is _fully_ overshadowed by some other published segments. Currently, `is_overshadowed` is always 0 for unpublished segments, although this may change in the future. You can filter for segments that "should be published" by filtering for `is_published = 1 AND is_overshadowed = 0`. Segments can briefly be both published and overshadowed if they were recently replaced, but have not been unpublished yet. See the [segment lifecycle documentation](../design/storage.md#segment-lifecycle) for more details.|
|shard_spec|VARCHAR|JSON-serialized form of the segment `ShardSpec`|
|dimensions|VARCHAR|JSON-serialized form of the segment dimensions|
|metrics|VARCHAR|JSON-serialized form of the segment metrics|
|last_compaction_state|VARCHAR|JSON-serialized form of the compaction task's config (compaction task which created this segment). May be null if segment was not created by compaction task.|
|replication_factor|BIGINT|Total number of replicas of the segment that are required to be loaded across all historical tiers, based on the load rule that currently applies to this segment. If this value is 0, the segment is not assigned to any historical and will not be loaded. This value is -1 if load rules for the segment have not been evaluated yet.|

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
SELECT * FROM sys.segments WHERE is_active = 1 AND last_compaction_state = 'CompactionState{partitionsSpec=DynamicPartitionsSpec{maxRowsPerSegment=5000000, maxTotalRows=9223372036854775807}, indexSpec={bitmap={type=roaring}, dimensionCompression=lz4, metricCompression=lz4, longEncoding=longs, segmentLoader=null}}'
```

### SERVERS table

Servers table lists all discovered servers in the cluster.

|Column|Type|Notes|
|------|-----|-----|
|server|VARCHAR|Server name in the form host:port|
|host|VARCHAR|Hostname of the server|
|plaintext_port|BIGINT|Unsecured port of the server, or -1 if plaintext traffic is disabled|
|tls_port|BIGINT|TLS port of the server, or -1 if TLS is disabled|
|server_type|VARCHAR|Type of Druid service. Possible values include: COORDINATOR, OVERLORD,  BROKER, ROUTER, HISTORICAL, MIDDLE_MANAGER or PEON.|
|tier|VARCHAR|Distribution tier see [druid.server.tier](../configuration/index.md#historical-general-configuration). Only valid for HISTORICAL type, for other types it's null|
|current_size|BIGINT|Current size of segments in bytes on this server. Only valid for HISTORICAL type, for other types it's 0|
|max_size|BIGINT|Max size in bytes this server recommends to assign to segments see [druid.server.maxSize](../configuration/index.md#historical-general-configuration). Only valid for HISTORICAL type, for other types it's 0|
|is_leader|BIGINT|1 if the server is currently the 'leader' (for services which have the concept of leadership), otherwise 0 if the server is not the leader, or the default long value (null or zero depending on `druid.generic.useDefaultValueForNull`) if the server type does not have the concept of leadership|
|start_time|STRING|Timestamp in ISO8601 format when the server was announced in the cluster|
To retrieve information about all servers, use the query:

```sql
SELECT * FROM sys.servers;
```

### SERVER_SEGMENTS table

SERVER_SEGMENTS is used to join servers with segments table

|Column|Type|Notes|
|------|-----|-----|
|server|VARCHAR|Server name in format host:port (Primary key of [servers table](#servers-table))|
|segment_id|VARCHAR|Segment identifier (Primary key of [segments table](#segments-table))|

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
|task_id|VARCHAR|Unique task identifier|
|group_id|VARCHAR|Task group ID for this task, the value depends on the task `type`. For example, for native index tasks, it's same as `task_id`, for sub tasks, this value is the parent task's ID|
|type|VARCHAR|Task type, for example this value is "index" for indexing tasks. See [tasks-overview](../ingestion/tasks.md)|
|datasource|VARCHAR|Datasource name being indexed|
|created_time|VARCHAR|Timestamp in ISO8601 format corresponding to when the ingestion task was created. Note that this value is populated for completed and waiting tasks. For running and pending tasks this value is set to 1970-01-01T00:00:00Z|
|queue_insertion_time|VARCHAR|Timestamp in ISO8601 format corresponding to when this task was added to the queue on the Overlord|
|status|VARCHAR|Status of a task can be RUNNING, FAILED, SUCCESS|
|runner_status|VARCHAR|Runner status of a completed task would be NONE, for in-progress tasks this can be RUNNING, WAITING, PENDING|
|duration|BIGINT|Time it took to finish the task in milliseconds, this value is present only for completed tasks|
|location|VARCHAR|Server name where this task is running in the format host:port, this information is present only for RUNNING tasks|
|host|VARCHAR|Hostname of the server where task is running|
|plaintext_port|BIGINT|Unsecured port of the server, or -1 if plaintext traffic is disabled|
|tls_port|BIGINT|TLS port of the server, or -1 if TLS is disabled|
|error_msg|VARCHAR|Detailed error message in case of FAILED tasks|

For example, to retrieve tasks information filtered by status, use the query

```sql
SELECT * FROM sys.tasks WHERE status='FAILED';
```

### SUPERVISORS table

The supervisors table provides information about supervisors.

|Column|Type|Notes|
|------|-----|-----|
|supervisor_id|VARCHAR|Supervisor task identifier|
|state|VARCHAR|Basic state of the supervisor. Available states: `UNHEALTHY_SUPERVISOR`, `UNHEALTHY_TASKS`, `PENDING`, `RUNNING`, `SUSPENDED`, `STOPPING`. See [Supervisor reference](../ingestion/supervisor.md) for more information.|
|detailed_state|VARCHAR|Supervisor specific state. See documentation of the specific supervisor for details: [Kafka](../ingestion/kafka-ingestion.md) or [Kinesis](../ingestion/kinesis-ingestion.md).|
|healthy|BIGINT|Boolean represented as long type where 1 = true, 0 = false. 1 indicates a healthy supervisor|
|type|VARCHAR|Type of supervisor, e.g. `kafka`, `kinesis` or `materialized_view`|
|source|VARCHAR|Source of the supervisor, e.g. Kafka topic or Kinesis stream|
|suspended|BIGINT|Boolean represented as long type where 1 = true, 0 = false. 1 indicates supervisor is in suspended state|
|spec|VARCHAR|JSON-serialized supervisor spec|

For example, to retrieve supervisor tasks information filtered by health status, use the query

```sql
SELECT * FROM sys.supervisors WHERE healthy=0;
```
