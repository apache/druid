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
queries are then passed down to data nodes. Each Druid dataSource appears as a table in the "druid" schema.

Add "EXPLAIN PLAN FOR" to the beginning of any query to see how Druid will plan that query.

### Querying with JDBC

You can make Druid SQL queries using the [Avatica JDBC driver](https://calcite.apache.org/avatica/downloads/). Once
you've downloaded the Avatica client jar, add it to your classpath and use the connect string:

```
jdbc:avatica:remote:url=http://BROKER:8082/druid/v2/sql/avatica/
```

Example code:

```java
Connection connection = DriverManager.getConnection("jdbc:avatica:remote:url=http://localhost:8082/druid/v2/sql/avatica/");
ResultSet resultSet = connection.createStatement().executeQuery("SELECT COUNT(*) AS cnt FROM druid.foo");
while (resultSet.next()) {
  // Do something
}
```

Table metadata is available over JDBC using `connection.getMetaData()`.

Parameterized queries don't work properly, so avoid those.

### Querying with JSON over HTTP

You can make Druid SQL queries using JSON over HTTP by POSTing to the endpoint `/druid/v2/sql/`. The request format
is:

```json
{
  "query" : "SELECT COUNT(*) FROM druid.ds WHERE foo = ?"
}
```

You can use _curl_ to send these queries from the command-line:

```bash
curl -XPOST -H'Content-Type: application/json' http://BROKER:8082/druid/v2/sql/ -d '{"query":"SELECT COUNT(*) FROM druid.ds"}'
```

Metadata is not available over the HTTP API.

### Metadata

Druid brokers cache column type metadata for each dataSource and use it to plan SQL queries. This cache is updated
on broker startup and also periodically in the background through
[SegmentMetadata queries](../querying/segmentmetadataquery.html). Background metadata refreshing is triggered by
segments entering and exiting the cluster, and can also be throttled through configuration.

This cached metadata is queryable through the "metadata.COLUMNS" and "metadata.TABLES" tables. When
`druid.sql.planner.useFallback` is disabled (the default), only full scans of this table are possible. For example, to
retrieve column metadata, use the query:

```sql
SELECT * FROM metadata.COLUMNS
```

If `druid.sql.planner.useFallback` is enabled, full SQL is possible on metadata tables. However, useFallback is not
recommended in production since it can generate unscalable query plans. The JDBC driver allows accessing
table and column metadata through `connection.getMetaData()` even if useFallback is off.

### Time functions

Druid's SQL language supports a number of time operations, including:

- `FLOOR(__time TO <granularity>)` for grouping or filtering on time buckets, like `SELECT FLOOR(__time TO MONTH), SUM(cnt) FROM druid.foo GROUP BY FLOOR(__time TO MONTH)`
- `EXTRACT(<granularity> FROM __time)` for grouping or filtering on time parts, like `SELECT EXTRACT(HOUR FROM __time), SUM(cnt) FROM druid.foo GROUP BY EXTRACT(HOUR FROM __time)`
- Comparisons to `TIMESTAMP '<time string>'` for time filters, like `SELECT COUNT(*) FROM druid.foo WHERE __time >= TIMESTAMP '2000-01-01 00:00:00' AND __time < TIMESTAMP '2001-01-01 00:00:00'`

### Semi-joins

Semi-joins involving `IN (SELECT ...)`, like the following, are planned with a special process.

```sql
SELECT x, count(*)
FROM druid.foo
WHERE x IN (SELECT x FROM druid.bar WHERE y = 'baz')
GROUP BY x
```

For this query, the broker will first translate the inner select on dataSource `bar` into a groupBy to find distinct
`x` values. Then it'll use those distinct values to build an "in" filter on dataSource `foo` for the outer query. The
configuration parameter `druid.sql.planner.maxSemiJoinRowsInMemory` controls the maximum number of values that will be
materialized for this kind of plan.

### Configuration

Druid's SQL planner can be configured on the [Broker node](../configuration/broker.html#sql-planner-configuration).

### Unsupported features

Druid does not support all SQL features. Most of these are due to missing features in Druid's native JSON-based query
language. Some unsupported SQL features include:

- Grouping on functions of multiple columns, like concatenation: `SELECT COUNT(*) FROM druid.foo GROUP BY dim1 || ' ' || dim2`
- Grouping on long and float columns.
- Filtering on float columns.
- Filtering on non-boolean interactions between columns, like two columns equaling each other: `SELECT COUNT(*) FROM druid.foo WHERE dim1 = dim2`.
- A number of miscellaneous functions, like `TRIM`.
- Joins, other than semi-joins as described above.

Additionally, some Druid features are not supported by the SQL language. Some unsupported Druid features include:

- [Multi-value dimensions](multi-value-dimensions.html).
- [Query-time lookups](lookups.html).
- [Nested groupBy queries](groupbyquery.html#nested-groupbys).
- Extensions, including [approximate histograms](../development/extensions-core/approximate-histograms.html) and
[DataSketches](../development/extensions-core/datasketches-aggregators.html).

## Third-party SQL libraries

A number of third parties have also released SQL libraries for Druid. Links to popular options can be found on
our [libraries](/libraries.html) page. These libraries make native Druid JSON queries and do not use Druid's SQL layer.
