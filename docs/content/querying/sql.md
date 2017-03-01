---
layout: doc_page
---
# SQL Support for Druid

## Built-in SQL

<div class="note caution">
Built-in SQL is an <a href="../development/experimental.html">experimental</a> feature. The API described here is
subject to change.
</div>

Druid includes a native SQL layer with an [Apache Calcite](https://calcite.apache.org/)-based parser and planner. All
parsing and planning takes place on the Broker, where SQL is converted to native Druid queries. Those native Druid
queries are then passed down to data nodes. Each Druid datasource appears as a table in the "druid" schema. Datasource
and column names are both case-sensitive and can optionally be quoted using double quotes. Literal strings should be
quoted with single quotes, like `'foo'`. Literal strings with Unicode escapes can be written like `U&'fo\00F6'`, where
character codes in hex are prefixed by a backslash.

Add "EXPLAIN PLAN FOR" to the beginning of any query to see how Druid will plan that query.

### Querying with JDBC

You can make Druid SQL queries using the [Avatica JDBC driver](https://calcite.apache.org/avatica/downloads/). Once
you've downloaded the Avatica client jar, add it to your classpath and use the connect string:

```
jdbc:avatica:remote:url=http://BROKER:8082/druid/v2/sql/avatica/
```

Example code:

```java
// Connect to /druid/v2/sql/avatica/ on your broker.
String url = "jdbc:avatica:remote:url=http://localhost:8082/druid/v2/sql/avatica/";

// Set any connection context parameters you need here (see "Connection context" below).
// Or leave empty for default behavior.
Properties connectionProperties = new Properties();

try (Connection connection = DriverManager.getConnection(url, connectionProperties)) {
  try (ResultSet resultSet = connection.createStatement().executeQuery("SELECT COUNT(*) AS cnt FROM data_source")) {
    while (resultSet.next()) {
      // Do something
    }
  }
}
```

Table metadata is available over JDBC using `connection.getMetaData()` or by querying the "INFORMATION_SCHEMA" tables
(see below).

Parameterized queries don't work properly, so avoid those.

### Querying with JSON over HTTP

You can make Druid SQL queries using JSON over HTTP by POSTing to the endpoint `/druid/v2/sql/`. The request format
is:

```json
{
  "query" : "SELECT COUNT(*) FROM data_source WHERE foo = 'bar'"
}
```

You can use _curl_ to send these queries from the command-line:

```bash
curl -XPOST -H'Content-Type: application/json' http://BROKER:8082/druid/v2/sql/ -d '{"query":"SELECT COUNT(*) FROM data_source"}'
```

Metadata is only available over the HTTP API by querying the "INFORMATION_SCHEMA" tables (see below).

You can provide [connection context parameters](#connection-context) by adding a "context" map, like:

```json
{
  "query" : "SELECT COUNT(*) FROM data_source WHERE foo = 'bar' AND __time > TIMESTAMP '2000-01-01 00:00:00'",
  "context" : {
    "sqlTimeZone" : "America/Los_Angeles"
  }
}
```

### Metadata

Druid brokers infer table and column metadata for each dataSource from segments loaded in the cluster, and use this to
plan SQL queries. This metadata is cached on broker startup and also updated periodically in the background through
[SegmentMetadata queries](../querying/segmentmetadataquery.html). Background metadata refreshing is triggered by
segments entering and exiting the cluster, and can also be throttled through configuration.

This cached metadata is queryable through "INFORMATION_SCHEMA" tables. For example, to retrieve metadata for the Druid
datasource "foo", use the query:

```sql
SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE SCHEMA_NAME = 'druid' AND TABLE_NAME = 'foo'
```

See the [INFORMATION_SCHEMA tables](#information_schema-tables) section below for details on the available metadata.

You can access table and column metadata through JDBC using `connection.getMetaData()`.

### Approximate queries

The following SQL queries and features may be executed using approximate algorithms:

- `COUNT(DISTINCT col)` and `APPROX_COUNT_DISTINCT(col)` aggregations use
[HyperLogLog](http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf), a fast approximate distinct counting
algorithm. If you need exact distinct counts, you can instead use
`SELECT COUNT(*) FROM (SELECT DISTINCT col FROM data_source)`, which will use a slower and more resource intensive exact
algorithm.
- TopN-style queries with a single grouping column, like
`SELECT col1, SUM(col2) FROM data_source GROUP BY col1 ORDER BY SUM(col2) DESC LIMIT 100`, by default will be executed
as [TopN queries](topnquery.html), which use an approximate algorithm. To disable this behavior, and use exact
algorithms for topN-style queries, set "useApproximateTopN" to "false", either through query context or through broker
configuration.

### Time functions

Druid's SQL language supports a number of time operations, including:

- `FLOOR(__time TO <granularity>)` for grouping or filtering on time buckets, like `SELECT FLOOR(__time TO MONTH), SUM(cnt) FROM data_source GROUP BY FLOOR(__time TO MONTH)`
- `EXTRACT(<granularity> FROM __time)` for grouping or filtering on time parts, like `SELECT EXTRACT(HOUR FROM __time), SUM(cnt) FROM data_source GROUP BY EXTRACT(HOUR FROM __time)`
- Comparisons to `TIMESTAMP '<time string>'` for time filters, like `SELECT COUNT(*) FROM data_source WHERE __time >= TIMESTAMP '2000-01-01 00:00:00' AND __time < TIMESTAMP '2001-01-01 00:00:00'`
- `CURRENT_TIMESTAMP` for the current time, usable in filters like `SELECT COUNT(*) FROM data_source WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR`

By default, time operations use the UTC time zone. You can change the time zone for time operations by setting the
connection context parameter "sqlTimeZone" to the name of the time zone, like "America/Los_Angeles".

### Subqueries

Druid's SQL layer supports many types of subqueries, including the ones listed below.

#### Nested groupBy

Subqueries involving `FROM (SELECT ... GROUP BY ...)` may be executed as
[nested groupBys](groupbyquery.html#nested-groupbys). For example, the following query can be used to perform an
exact distinct count using a nested groupBy.

```sql
SELECT COUNT(*) FROM (SELECT DISTINCT col FROM data_source)
```

#### Semi-joins

Semi-join subqueries involving `WHERE ... IN (SELECT ...)`, like the following, are executed with a special process.

```sql
SELECT x, COUNT(*)
FROM data_source_1
WHERE x IN (SELECT x FROM data_source_2 WHERE y = 'baz')
GROUP BY x
```

For this query, the broker will first translate the inner select on data_source_2 into a groupBy to find distinct
`x` values. Then it'll use those distinct values to build an "in" filter on data_source_1 for the outer query. The
configuration parameter `druid.sql.planner.maxSemiJoinRowsInMemory` controls the maximum number of values that will be
materialized for this kind of plan.

### Connection context

Druid's SQL layer supports a connection context that influences SQL query planning and Druid native query execution.
The parameters in the table below affect SQL planning. All other context parameters you provide will be attached to
Druid queries and can affect how they run. See [Query context](query-context.html) for details on the possible options.

|Parameter|Description|Default value|
|---------|-----------|-------------|
|`sqlTimeZone`|Sets the time zone for this connection. Should be a time zone name like "America/Los_Angeles".|UTC|
|`useApproximateCountDistinct`|Whether to use an approximate cardinalty algorithm for `COUNT(DISTINCT foo)`.|druid.sql.planner.useApproximateCountDistinct on the broker|
|`useApproximateTopN`|Whether to use approximate [TopN queries](topnquery.html) when a SQL query could be expressed as such. If false, exact [GroupBy queries](groupbyquery.html) will be used instead.|druid.sql.planner.useApproximateTopN on the broker|
|`useFallback`|Whether to evaluate operations on the broker when they cannot be expressed as Druid queries. This option is not recommended for production since it can generate unscalable query plans. If false, SQL queries that cannot be translated to Druid queries will fail.|druid.sql.planner.useFallback on the broker|

Connection context can be specified as JDBC connection properties or as a "context" object in the JSON API.

### Configuration

Druid's SQL layer can be configured through the following properties in common.runtime.properties or the broker's
runtime.properties. Either location is equivalent since these properties are only respected by the broker.

#### SQL Server Configuration

The broker's [built-in SQL server](../querying/sql.html) can be configured through the following properties.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.sql.enable`|Whether to enable SQL at all, including background metadata fetching. If false, this overrides all other SQL-related properties and disables SQL metadata, serving, and planning completely.|false|
|`druid.sql.avatica.enable`|Whether to enable an Avatica server at `/druid/v2/sql/avatica/`.|true|
|`druid.sql.avatica.connectionIdleTimeout`|Avatica client connection idle timeout.|PT30M|
|`druid.sql.avatica.maxConnections`|Maximum number of open connections for the Avatica server. These are not HTTP connections, but are logical client connections that may span multiple HTTP connections.|25|
|`druid.sql.avatica.maxStatementsPerConnection`|Maximum number of simultaneous open statements per Avatica client connection.|4|
|`druid.sql.http.enable`|Whether to enable a simple JSON over HTTP route at `/druid/v2/sql/`.|true|

#### SQL Planner Configuration

The broker's [SQL planner](../querying/sql.html) can be configured through the following properties.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.sql.planner.maxQueryCount`|Maximum number of queries to issue, including nested queries. Set to 1 to disable sub-queries, or set to 0 for unlimited.|8|
|`druid.sql.planner.maxSemiJoinRowsInMemory`|Maximum number of rows to keep in memory for executing two-stage semi-join queries like `SELECT * FROM Employee WHERE DeptName IN (SELECT DeptName FROM Dept)`.|100000|
|`druid.sql.planner.maxTopNLimit`|Maximum threshold for a [TopN query](../querying/topnquery.html). Higher limits will be planned as [GroupBy queries](../querying/groupbyquery.html) instead.|100000|
|`druid.sql.planner.metadataRefreshPeriod`|Throttle for metadata refreshes.|PT1M|
|`druid.sql.planner.selectPageSize`|Page size threshold for [Select queries](../querying/select-query.html). Select queries for larger resultsets will be issued back-to-back using pagination.|1000|
|`druid.sql.planner.useApproximateCountDistinct`|Whether to use an approximate cardinalty algorithm for `COUNT(DISTINCT foo)`.|true|
|`druid.sql.planner.useApproximateTopN`|Whether to use approximate [TopN queries](../querying/topnquery.html) when a SQL query could be expressed as such. If false, exact [GroupBy queries](../querying/groupbyquery.html) will be used instead.|true|
|`druid.sql.planner.useFallback`|Whether to evaluate operations on the broker when they cannot be expressed as Druid queries. This option is not recommended for production since it can generate unscalable query plans. If false, SQL queries that cannot be translated to Druid queries will fail.|false|

### Extensions

Some Druid extensions also include SQL language extensions.

If the [approximate histogram extension](../development/extensions-core/approximate-histograms.html) is loaded:

- `APPROX_QUANTILE(column, probability)` or `APPROX_QUANTILE(column, probability, resolution)` on numeric or
approximate histogram columns computes approximate quantiles. The "probability" should be between 0 and 1 (exclusive).
The "resolution" is the number of centroids to use for the computation. Higher resolutions will be give more
precise results but also have higher overhead. If not provided, the default resolution is 50.

### Unsupported features

Druid does not support all SQL features. Most of these are due to missing features in Druid's native JSON-based query
language. Some unsupported SQL features include:

- Grouping on functions of multiple columns, like concatenation: `SELECT COUNT(*) FROM data_source GROUP BY dim1 || ' ' || dim2`
- Filtering on non-boolean interactions between columns, like two columns equaling each other: `SELECT COUNT(*) FROM data_source WHERE dim1 = dim2`.
- A number of miscellaneous functions, like `TRIM`.
- Joins, other than semi-joins as described above.

Additionally, some Druid features are not supported by the SQL language. Some unsupported Druid features include:

- [Multi-value dimensions](multi-value-dimensions.html).
- [Query-time lookups](lookups.html).
- [DataSketches](../development/extensions-core/datasketches-aggregators.html).

## Third-party SQL libraries

A number of third parties have also released SQL libraries for Druid. Links to popular options can be found on
our [libraries](/libraries.html) page. These libraries make native Druid JSON queries and do not use Druid's SQL layer.

## INFORMATION_SCHEMA tables

Druid metadata is queryable through "INFORMATION_SCHEMA" tables described below.

### SCHEMATA table

|Column|Notes|
|------|-----|
|CATALOG_NAME|Unused|
|SCHEMA_NAME||
|SCHEMA_OWNER|Unused|
|DEFAULT_CHARACTER_SET_CATALOG|Unused|
|DEFAULT_CHARACTER_SET_SCHEMA|Unused|
|DEFAULT_CHARACTER_SET_NAME|Unused|
|SQL_PATH|Unused|

### TABLES table

|Column|Notes|
|------|-----|
|TABLE_CATALOG|Unused|
|TABLE_SCHEMA||
|TABLE_NAME||
|TABLE_TYPE|"TABLE" or "SYSTEM_TABLE"|

### COLUMNS table

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
