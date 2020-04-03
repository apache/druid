---
id: sql
title: "SQL"
sidebar_label: "Druid SQL syntax"
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


> Apache Druid supports two query languages: Druid SQL and [native queries](querying.md), which
> SQL queries are planned into, and which end users can also issue directly. This document describes the SQL language.

Druid SQL is a built-in SQL layer and an alternative to Druid's native JSON-based query language, and is powered by a
parser and planner based on [Apache Calcite](https://calcite.apache.org/). Druid SQL translates SQL into native Druid
queries on the query Broker (the first process you query), which are then passed down to data processes as native Druid
queries. Other than the (slight) overhead of translating SQL on the Broker, there isn't an additional performance
penalty versus native queries.

## Query syntax

Each Druid datasource appears as a table in the "druid" schema. This is also the default schema, so Druid datasources
can be referenced as either `druid.dataSourceName` or simply `dataSourceName`.

Identifiers like datasource and column names can optionally be quoted using double quotes. To escape a double quote
inside an identifier, use another double quote, like `"My ""very own"" identifier"`. All identifiers are case-sensitive
and no implicit case conversions are performed.

Literal strings should be quoted with single quotes, like `'foo'`. Literal strings with Unicode escapes can be written
like `U&'fo\00F6'`, where character codes in hex are prefixed by a backslash. Literal numbers can be written in forms
like `100` (denoting an integer), `100.0` (denoting a floating point value), or `1.0e5` (scientific notation). Literal
timestamps can be written like `TIMESTAMP '2000-01-01 00:00:00'`. Literal intervals, used for time arithmetic, can be
written like `INTERVAL '1' HOUR`, `INTERVAL '1 02:03' DAY TO MINUTE`, `INTERVAL '1-2' YEAR TO MONTH`, and so on.

Druid SQL supports dynamic parameters in question mark (`?`) syntax, where parameters are bound to the `?` placeholders at execution time. To use dynamic parameters, replace any literal in the query with a `?` character and ensure that corresponding parameter values are provided at execution time. Parameters are bound to the placeholders in the order in which they are passed.
 
Druid SQL supports SELECT queries with the following structure:

```
[ EXPLAIN PLAN FOR ]
[ WITH tableName [ ( column1, column2, ... ) ] AS ( query ) ]
SELECT [ ALL | DISTINCT ] { * | exprs }
FROM table
[ WHERE expr ]
[ GROUP BY [ exprs | GROUPING SETS ( (exprs), ... ) | ROLLUP (exprs) | CUBE (exprs) ] ]
[ HAVING expr ]
[ ORDER BY expr [ ASC | DESC ], expr [ ASC | DESC ], ... ]
[ LIMIT limit ]
[ UNION ALL <another query> ]
```

The FROM clause refers to either a Druid datasource, like `druid.foo`, an [INFORMATION_SCHEMA table](#metadata-tables), a
subquery, or a common-table-expression provided in the WITH clause. If the FROM clause references a subquery or a
common-table-expression, and both levels of queries are aggregations and they cannot be combined into a single level of
aggregation, the overall query will be executed as a [nested GroupBy](groupbyquery.html#nested-groupbys).

The WHERE clause refers to columns in the FROM table, and will be translated to [native filters](filters.html). The
WHERE clause can also reference a subquery, like `WHERE col1 IN (SELECT foo FROM ...)`. Queries like this are executed
as [semi-joins](#query-execution), described below.

The GROUP BY clause refers to columns in the FROM table. Using GROUP BY, DISTINCT, or any aggregation functions will
trigger an aggregation query using one of Druid's [three native aggregation query types](#query-execution). GROUP BY
can refer to an expression or a select clause ordinal position (like `GROUP BY 2` to group by the second selected
column).

The GROUP BY clause can also refer to multiple grouping sets in three ways. The most flexible is GROUP BY GROUPING SETS,
for example `GROUP BY GROUPING SETS ( (country, city), () )`. This example is equivalent to a `GROUP BY country, city`
followed by `GROUP BY ()` (a grand total). With GROUPING SETS, the underlying data is only scanned one time, leading to
better efficiency. Second, GROUP BY ROLLUP computes a grouping set for each level of the grouping expressions. For
example `GROUP BY ROLLUP (country, city)` is equivalent to `GROUP BY GROUPING SETS ( (country, city), (country), () )`
and will produce grouped rows for each country / city pair, along with subtotals for each country, along with a grand
total. Finally, GROUP BY CUBE computes a grouping set for each combination of grouping expressions. For example,
`GROUP BY CUBE (country, city)` is equivalent to `GROUP BY GROUPING SETS ( (country, city), (country), (city), () )`.
Grouping columns that do not apply to a particular row will contain `NULL`. For example, when computing
`GROUP BY GROUPING SETS ( (country, city), () )`, the grand total row corresponding to `()` will have `NULL` for the
"country" and "city" columns.

When using GROUP BY GROUPING SETS, GROUP BY ROLLUP, or GROUP BY CUBE, be aware that results may not be generated in the
order that you specify your grouping sets in the query. If you need results to be generated in a particular order, use
the ORDER BY clause.

The HAVING clause refers to columns that are present after execution of GROUP BY. It can be used to filter on either
grouping expressions or aggregated values. It can only be used together with GROUP BY.

The ORDER BY clause refers to columns that are present after execution of GROUP BY. It can be used to order the results
based on either grouping expressions or aggregated values. ORDER BY can refer to an expression or a select clause
ordinal position (like `ORDER BY 2` to order by the second selected column). For non-aggregation queries, ORDER BY
can only order by the `__time` column. For aggregation queries, ORDER BY can order by any column.

The LIMIT clause can be used to limit the number of rows returned. It can be used with any query type. It is pushed down
to data processes for queries that run with the native TopN query type, but not the native GroupBy query type. Future
versions of Druid will support pushing down limits using the native GroupBy query type as well. If you notice that
adding a limit doesn't change performance very much, then it's likely that Druid didn't push down the limit for your
query.

The "UNION ALL" operator can be used to fuse multiple queries together. Their results will be concatenated, and each
query will run separately, back to back (not in parallel). Druid does not currently support "UNION" without "ALL".

Add "EXPLAIN PLAN FOR" to the beginning of any query to see how it would be run as a native Druid query. In this case,
the query will not actually be executed.


### Unsupported features

Druid does not support all SQL features, including:

- OVER clauses, and analytic functions such as `LAG` and `LEAD`.
- JOIN clauses, other than semi-joins as described above.
- OFFSET clauses.
- DDL and DML.

Additionally, some Druid features are not supported by the SQL language. Some unsupported Druid features include:

- [Multi-value dimensions](multi-value-dimensions.html).
- [Set operations on DataSketches aggregators](../development/extensions-core/datasketches-extension.html).
- [Spatial filters](../development/geo.html).
- [Query cancellation](querying.html#query-cancellation).

<div style="color:red">

## Query execution

Queries without aggregations will use Druid's [Scan](scan-query.html) native query type.

Aggregation queries (using GROUP BY, DISTINCT, or any aggregation functions) will use one of Druid's three native
aggregation query types. Two (Timeseries and TopN) are specialized for specific types of aggregations, whereas the other
(GroupBy) is general-purpose.

- [Timeseries](timeseriesquery.html) is used for queries that GROUP BY `FLOOR(__time TO <unit>)` or `TIME_FLOOR(__time,
period)`, have no other grouping expressions, no HAVING or LIMIT clauses, no nesting, and either no ORDER BY, or an
ORDER BY that orders by same expression as present in GROUP BY. It also uses Timeseries for "grand total" queries that
have aggregation functions but no GROUP BY. This query type takes advantage of the fact that Druid segments are sorted
by time.

- [TopN](topnquery.html) is used by default for queries that group by a single expression, do have ORDER BY and LIMIT
clauses, do not have HAVING clauses, and are not nested. However, the TopN query type will deliver approximate ranking
and results in some cases; if you want to avoid this, set "useApproximateTopN" to "false". TopN results are always
computed in memory. See the TopN documentation for more details.

- [GroupBy](groupbyquery.html) is used for all other aggregations, including any nested aggregation queries. Druid's
GroupBy is a traditional aggregation engine: it delivers exact results and rankings and supports a wide variety of
features. GroupBy aggregates in memory if it can, but it may spill to disk if it doesn't have enough memory to complete
your query. Results are streamed back from data processes through the Broker if you ORDER BY the same expressions in your
GROUP BY clause, or if you don't have an ORDER BY at all. If your query has an ORDER BY referencing expressions that
don't appear in the GROUP BY clause (like aggregation functions) then the Broker will materialize a list of results in
memory, up to a max of your LIMIT, if any. See the GroupBy documentation for details about tuning performance and memory
use.

If your query does nested aggregations (an aggregation subquery in your FROM clause) then Druid will execute it as a
[nested GroupBy](groupbyquery.html#nested-groupbys). In nested GroupBys, the innermost aggregation is distributed, but
all outer aggregations beyond that take place locally on the query Broker.

Semi-join queries containing WHERE clauses like `col IN (SELECT expr FROM ...)` are executed with a special process. The
Broker will first translate the subquery into a GroupBy to find distinct values of `expr`. Then, the broker will rewrite
the subquery to a literal filter, like `col IN (val1, val2, ...)` and run the outer query. The configuration parameter
druid.sql.planner.maxSemiJoinRowsInMemory controls the maximum number of values that will be materialized for this kind
of plan.

For all native query types, filters on the `__time` column will be translated into top-level query "intervals" whenever
possible, which allows Druid to use its global time index to quickly prune the set of data that must be scanned. In
addition, Druid will use indexes local to each data process to further speed up WHERE evaluation. This can typically be
done for filters that involve boolean combinations of references to and functions of single columns, like
`WHERE col1 = 'a' AND col2 = 'b'`, but not `WHERE col1 = col2`.

### Approximate algorithms

Druid SQL will use approximate algorithms in some situations:

- The `COUNT(DISTINCT col)` aggregation functions by default uses a variant of
[HyperLogLog](http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf), a fast approximate distinct counting
algorithm. Druid SQL will switch to exact distinct counts if you set "useApproximateCountDistinct" to "false", either
through query context or through Broker configuration.
- GROUP BY queries over a single column with ORDER BY and LIMIT may be executed using the TopN engine, which uses an
approximate algorithm. Druid SQL will switch to an exact grouping algorithm if you set "useApproximateTopN" to "false",
either through query context or through Broker configuration.
- The APPROX_COUNT_DISTINCT and APPROX_QUANTILE aggregation functions always use approximate algorithms, regardless
of configuration.

</div>

## Client APIs

### JSON over HTTP

You can make Druid SQL queries using JSON over HTTP by posting to the endpoint `/druid/v2/sql/`. The request should
be a JSON object with a "query" field, like `{"query" : "SELECT COUNT(*) FROM data_source WHERE foo = 'bar'"}`.

##### Request
      
|Property|Type|Description|Required|
|--------|----|-----------|--------|
|`query`|`String`| SQL query to run| yes |
|`resultFormat`|`String` (`ResultFormat`)| Result format for output | no (default `"object"`)|
|`header`|`Boolean`| Write column name header for supporting formats| no (default `false`)|
|`context`|`Object`| Connection context map. see [connection context parameters](#connection-context)| no |
|`parameters`|`SqlParameter` list| List of query parameters for parameterized queries. | no |


You can use _curl_ to send SQL queries from the command-line:

```bash
$ cat query.json
{"query":"SELECT COUNT(*) AS TheCount FROM data_source"}

$ curl -XPOST -H'Content-Type: application/json' http://BROKER:8082/druid/v2/sql/ -d @query.json
[{"TheCount":24433}]
```

There are a variety of [connection context parameters](#connection-context) you can provide by adding a "context" map,
like:

```json
{
  "query" : "SELECT COUNT(*) FROM data_source WHERE foo = 'bar' AND __time > TIMESTAMP '2000-01-01 00:00:00'",
  "context" : {
    "sqlTimeZone" : "America/Los_Angeles"
  }
}
```

Parameterized SQL queries are also supported:

```json
{
  "query" : "SELECT COUNT(*) FROM data_source WHERE foo = ? AND __time > ?",
  "parameters": [
    { "type": "VARCHAR", "value": "bar"},
    { "type": "TIMESTAMP", "value": "2000-01-01 00:00:00" }
  ]
}
```

##### SqlParameter

|Property|Type|Description|Required|
|--------|----|-----------|--------|
|`type`|`String` (`SqlType`) | String value of `SqlType` of parameter. [`SqlType`](https://calcite.apache.org/avatica/javadocAggregate/org/apache/calcite/avatica/SqlType.html) is a friendly wrapper around [`java.sql.Types`](https://docs.oracle.com/javase/8/docs/api/java/sql/Types.html?is-external=true)|yes|
|`value`|`Object`| Value of the parameter|yes|


Metadata is also available over the HTTP API by querying [system tables](#metadata-tables).

#### Responses

Druid SQL supports a variety of result formats. You can specify these by adding a "resultFormat" parameter, like:

```json
{
  "query" : "SELECT COUNT(*) FROM data_source WHERE foo = 'bar' AND __time > TIMESTAMP '2000-01-01 00:00:00'",
  "resultFormat" : "object"
}
```

The supported result formats are:

|Format|Description|Content-Type|
|------|-----------|------------|
|`object`|The default, a JSON array of JSON objects. Each object's field names match the columns returned by the SQL query, and are provided in the same order as the SQL query.|application/json|
|`array`|JSON array of JSON arrays. Each inner array has elements matching the columns returned by the SQL query, in order.|application/json|
|`objectLines`|Like "object", but the JSON objects are separated by newlines instead of being wrapped in a JSON array. This can make it easier to parse the entire response set as a stream, if you do not have ready access to a streaming JSON parser. To make it possible to detect a truncated response, this format includes a trailer of one blank line.|text/plain|
|`arrayLines`|Like "array", but the JSON arrays are separated by newlines instead of being wrapped in a JSON array. This can make it easier to parse the entire response set as a stream, if you do not have ready access to a streaming JSON parser. To make it possible to detect a truncated response, this format includes a trailer of one blank line.|text/plain|
|`csv`|Comma-separated values, with one row per line. Individual field values may be escaped by being surrounded in double quotes. If double quotes appear in a field value, they will be escaped by replacing them with double-double-quotes like `""this""`. To make it possible to detect a truncated response, this format includes a trailer of one blank line.|text/csv|

You can additionally request a header by setting "header" to true in your request, like:

```json
{
  "query" : "SELECT COUNT(*) FROM data_source WHERE foo = 'bar' AND __time > TIMESTAMP '2000-01-01 00:00:00'",
  "resultFormat" : "arrayLines",
  "header" : true
}
```

In this case, the first result returned will be a header. For the `csv`, `array`, and `arrayLines` formats, the header
will be a list of column names. For the `object` and `objectLines` formats, the header will be an object where the
keys are column names, and the values are null.

Errors that occur before the response body is sent will be reported in JSON, with an HTTP 500 status code, in the
same format as [native Druid query errors](../querying/querying.html#query-errors). If an error occurs while the response body is
being sent, at that point it is too late to change the HTTP status code or report a JSON error, so the response will
simply end midstream and an error will be logged by the Druid server that was handling your request.

As a caller, it is important that you properly handle response truncation. This is easy for the "object" and "array"
formats, since truncated responses will be invalid JSON. For the line-oriented formats, you should check the
trailer they all include: one blank line at the end of the result set. If you detect a truncated response, either
through a JSON parsing error or through a missing trailing newline, you should assume the response was not fully
delivered due to an error.

### JDBC

You can make Druid SQL queries using the [Avatica JDBC driver](https://calcite.apache.org/avatica/downloads/). Once
you've downloaded the Avatica client jar, add it to your classpath and use the connect string
`jdbc:avatica:remote:url=http://BROKER:8082/druid/v2/sql/avatica/`.

Example code:

```java
// Connect to /druid/v2/sql/avatica/ on your Broker.
String url = "jdbc:avatica:remote:url=http://localhost:8082/druid/v2/sql/avatica/";

// Set any connection context parameters you need here (see "Connection context" below).
// Or leave empty for default behavior.
Properties connectionProperties = new Properties();

try (Connection connection = DriverManager.getConnection(url, connectionProperties)) {
  try (
      final Statement statement = connection.createStatement();
      final ResultSet resultSet = statement.executeQuery(query)
  ) {
    while (resultSet.next()) {
      // Do something
    }
  }
}
```

Table metadata is available over JDBC using `connection.getMetaData()` or by querying the
["INFORMATION_SCHEMA" tables](#metadata-tables).

#### Connection stickiness

Druid's JDBC server does not share connection state between Brokers. This means that if you're using JDBC and have
multiple Druid Brokers, you should either connect to a specific Broker, or use a load balancer with sticky sessions
enabled. The Druid Router process provides connection stickiness when balancing JDBC requests, and can be used to achieve
the necessary stickiness even with a normal non-sticky load balancer. Please see the
[Router](../design/router.md) documentation for more details.

Note that the non-JDBC [JSON over HTTP](#json-over-http) API is stateless and does not require stickiness.

### Dynamic Parameters

You can also use parameterized queries in JDBC code, as in this example;

```java
PreparedStatement statement = connection.prepareStatement("SELECT COUNT(*) AS cnt FROM druid.foo WHERE dim1 = ? OR dim1 = ?");
statement.setString(1, "abc");
statement.setString(2, "def");
final ResultSet resultSet = statement.executeQuery();
```

### Connection context

Druid SQL supports setting connection parameters on the client. The parameters in the table below affect SQL planning.
All other context parameters you provide will be attached to Druid queries and can affect how they run. See
[Query context](query-context.html) for details on the possible options.

Note that to specify an unique identifier for SQL query, use `sqlQueryId` instead of `queryId`. Setting `queryId` for a SQL
request has no effect, all native queries underlying SQL will use auto-generated queryId.

Connection context can be specified as JDBC connection properties or as a "context" object in the JSON API.

|Parameter|Description|Default value|
|---------|-----------|-------------|
|`sqlQueryId`|Unique identifier given to this SQL query. For HTTP client, it will be returned in `X-Druid-SQL-Query-Id` header.|auto-generated|
|`sqlTimeZone`|Sets the time zone for this connection, which will affect how time functions and timestamp literals behave. Should be a time zone name like "America/Los_Angeles" or offset like "-08:00".|druid.sql.planner.sqlTimeZone on the Broker (default: UTC)|
|`useApproximateCountDistinct`|Whether to use an approximate cardinality algorithm for `COUNT(DISTINCT foo)`.|druid.sql.planner.useApproximateCountDistinct on the Broker (default: true)|
|`useApproximateTopN`|Whether to use approximate [TopN queries](topnquery.html) when a SQL query could be expressed as such. If false, exact [GroupBy queries](groupbyquery.html) will be used instead.|druid.sql.planner.useApproximateTopN on the Broker (default: true)|

## Metadata tables

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

## Server configuration

The Druid SQL server is configured through the following properties on the Broker.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.sql.enable`|Whether to enable SQL at all, including background metadata fetching. If false, this overrides all other SQL-related properties and disables SQL metadata, serving, and planning completely.|true|
|`druid.sql.avatica.enable`|Whether to enable JDBC querying at `/druid/v2/sql/avatica/`.|true|
|`druid.sql.avatica.maxConnections`|Maximum number of open connections for the Avatica server. These are not HTTP connections, but are logical client connections that may span multiple HTTP connections.|25|
|`druid.sql.avatica.maxRowsPerFrame`|Maximum number of rows to return in a single JDBC frame. Setting this property to -1 indicates that no row limit should be applied. Clients can optionally specify a row limit in their requests; if a client specifies a row limit, the lesser value of the client-provided limit and `maxRowsPerFrame` will be used.|5,000|
|`druid.sql.avatica.maxStatementsPerConnection`|Maximum number of simultaneous open statements per Avatica client connection.|4|
|`druid.sql.avatica.connectionIdleTimeout`|Avatica client connection idle timeout.|PT5M|
|`druid.sql.http.enable`|Whether to enable JSON over HTTP querying at `/druid/v2/sql/`.|true|
|`druid.sql.planner.maxTopNLimit`|Maximum threshold for a [TopN query](../querying/topnquery.md). Higher limits will be planned as [GroupBy queries](../querying/groupbyquery.md) instead.|100000|
|`druid.sql.planner.metadataRefreshPeriod`|Throttle for metadata refreshes.|PT1M|
|`druid.sql.planner.useApproximateCountDistinct`|Whether to use an approximate cardinality algorithm for `COUNT(DISTINCT foo)`.|true|
|`druid.sql.planner.useApproximateTopN`|Whether to use approximate [TopN queries](../querying/topnquery.html) when a SQL query could be expressed as such. If false, exact [GroupBy queries](../querying/groupbyquery.html) will be used instead.|true|
|`druid.sql.planner.requireTimeCondition`|Whether to require SQL to have filter conditions on __time column so that all generated native queries will have user specified intervals. If true, all queries without filter condition on __time column will fail|false|
|`druid.sql.planner.sqlTimeZone`|Sets the default time zone for the server, which will affect how time functions and timestamp literals behave. Should be a time zone name like "America/Los_Angeles" or offset like "-08:00".|UTC|
|`druid.sql.planner.metadataSegmentCacheEnable`|Whether to keep a cache of published segments in broker. If true, broker polls coordinator in background to get segments from metadata store and maintains a local cache. If false, coordinator's REST API will be invoked when broker needs published segments info.|false|
|`druid.sql.planner.metadataSegmentPollPeriod`|How often to poll coordinator for published segments list if `druid.sql.planner.metadataSegmentCacheEnable` is set to true. Poll period is in milliseconds. |60000|

> Previous versions of Druid had properties named `druid.sql.planner.maxQueryCount` and `druid.sql.planner.maxSemiJoinRowsInMemory`.
> These properties are no longer available. Since Druid 0.18.0, you can use `druid.server.http.maxSubqueryRows` to control the maximum
> number of rows permitted across all subqueries.

## SQL Metrics

Broker will emit the following metrics for SQL.

|Metric|Description|Dimensions|Normal Value|
|------|-----------|----------|------------|
|`sqlQuery/time`|Milliseconds taken to complete a SQL.|id, nativeQueryIds, dataSource, remoteAddress, success.|< 1s|
|`sqlQuery/bytes`|number of bytes returned in SQL response.|id, nativeQueryIds, dataSource, remoteAddress, success.| |

## Authorization Permissions

Please see [Defining SQL permissions](../development/extensions-core/druid-basic-security.html#sql-permissions) for information on what permissions are needed for making SQL queries in a secured cluster.
