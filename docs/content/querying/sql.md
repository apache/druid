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

### Approximate queries

The following SQL queries and features may be executed using approximate algorithms:

- `COUNT(DISTINCT col)` aggregations use [HyperLogLog](http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf), a
fast approximate distinct counting algorithm. If you need exact distinct counts, you can instead use
`SELECT COUNT(*) FROM (SELECT DISTINCT col FROM druid.foo)`, which will use a slower and more resource intensive exact
algorithm.
- TopN-style queries with a single grouping column, like
`SELECT col1, SUM(col2) FROM druid.foo GROUP BY col1 ORDER BY SUM(col2) DESC LIMIT 100`, by default will be executed
as [TopN queries](topnquery.html), which use an approximate algorithm. To disable this behavior, and use exact
algorithms for topN-style queries, set
[druid.sql.planner.useApproximateTopN](../configuration/broker.html#sql-planner-configuration) to "false".

### Time functions

Druid's SQL language supports a number of time operations, including:

- `FLOOR(__time TO <granularity>)` for grouping or filtering on time buckets, like `SELECT FLOOR(__time TO MONTH), SUM(cnt) FROM druid.foo GROUP BY FLOOR(__time TO MONTH)`
- `EXTRACT(<granularity> FROM __time)` for grouping or filtering on time parts, like `SELECT EXTRACT(HOUR FROM __time), SUM(cnt) FROM druid.foo GROUP BY EXTRACT(HOUR FROM __time)`
- Comparisons to `TIMESTAMP '<time string>'` for time filters, like `SELECT COUNT(*) FROM druid.foo WHERE __time >= TIMESTAMP '2000-01-01 00:00:00' AND __time < TIMESTAMP '2001-01-01 00:00:00'`

### Subqueries

Druid's SQL layer supports many types of subqueries, including the ones listed below.

#### Nested groupBy

Subqueries involving `FROM (SELECT ... GROUP BY ...)` may be executed as
[nested groupBys](groupbyquery.html#nested-groupbys). For example, the following query can be used to perform an
exact distinct count using a nested groupBy.

```sql
SELECT COUNT(*) FROM (SELECT DISTINCT col FROM druid.foo)
```

Note that groupBys require a separate merge buffer on the broker for each layer beyond the first layer of the groupBy.
With the v2 groupBy strategy, this can potentially lead to deadlocks for groupBys nested beyond two layers, since the
merge buffers are limited in number and are acquired one-by-one and not as a complete set. At this time we recommend
that you avoid deeply-nested groupBys with the v2 strategy. Doubly-nested groupBys (groupBy -> groupBy -> table) are
safe and do not suffer from this issue. If you like, you can forbid deeper nesting by setting
`druid.sql.planner.maxQueryCount = 2`.

#### Semi-joins

Semi-join subqueries involving `WHERE ... IN (SELECT ...)`, like the following, are executed with a special process.

```sql
SELECT x, COUNT(*)
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
- Extensions, including [approximate histograms](../development/extensions-core/approximate-histograms.html) and
[DataSketches](../development/extensions-core/datasketches-aggregators.html).

## Third-party SQL libraries

A number of third parties have also released SQL libraries for Druid. Links to popular options can be found on
our [libraries](/libraries.html) page. These libraries make native Druid JSON queries and do not use Druid's SQL layer.
