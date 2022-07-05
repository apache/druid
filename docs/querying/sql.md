---
id: sql
title: "Druid SQL overview"
sidebar_label: "Overview and syntax"
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

You can query data in Druid datasources using [Druid SQL](./sql.md). Druid translates SQL queries into its [native query language](./querying.md). To learn about translation and how to get the best performance from Druid SQL, see [SQL query translation](./sql-translation.md).

Druid SQL planning occurs on the Broker.
Set [Broker runtime properties](../configuration/index.md#sql) to configure the query plan and JDBC querying.

For information on permissions needed to make SQL queries, see [Defining SQL permissions](../operations/security-user-auth.md#sql-permissions).

This topic introduces Druid SQL syntax.
For more information and SQL querying options see:
- [Data types](./sql-data-types.md) for a list of supported data types for Druid columns.
- [Aggregation functions](./sql-aggregations.md) for a list of aggregation functions available for Druid SQL SELECT statements.
- [Scalar functions](./sql-scalar.md) for Druid SQL scalar functions including numeric and string functions, IP address functions, Sketch functions, and more.
- [SQL multi-value string functions](./sql-multivalue-string-functions.md) for operations you can perform on string dimensions containing multiple values.
- [Query translation](./sql-translation.md) for information about how Druid translates SQL queries to native queries before running them.

For information about APIs, see:
- [Druid SQL API](./sql-api.md) for information on the HTTP API.
- [SQL JDBC driver API](./sql-jdbc.md) for information about the JDBC driver API.
- [SQL query context](./sql-query-context.md) for information about the query context parameters that affect SQL planning.

## Syntax

Druid SQL supports SELECT queries with the following structure:

```
[ EXPLAIN PLAN FOR ]
[ WITH tableName [ ( column1, column2, ... ) ] AS ( query ) ]
SELECT [ ALL | DISTINCT ] { * | exprs }
FROM { <table> | (<subquery>) | <o1> [ INNER | LEFT ] JOIN <o2> ON condition }
[ WHERE expr ]
[ GROUP BY [ exprs | GROUPING SETS ( (exprs), ... ) | ROLLUP (exprs) | CUBE (exprs) ] ]
[ HAVING expr ]
[ ORDER BY expr [ ASC | DESC ], expr [ ASC | DESC ], ... ]
[ LIMIT limit ]
[ OFFSET offset ]
[ UNION ALL <another query> ]
```

## FROM

The FROM clause can refer to any of the following:

- [Table datasources](datasource.md#table) from the `druid` schema. This is the default schema, so Druid table
datasources can be referenced as either `druid.dataSourceName` or simply `dataSourceName`.
- [Lookups](datasource.md#lookup) from the `lookup` schema, for example `lookup.countries`. Note that lookups can
also be queried using the [`LOOKUP` function](sql-scalar.md#string-functions).
- [Subqueries](datasource.md#query).
- [Joins](datasource.md#join) between anything in this list, except between native datasources (table, lookup,
query) and system tables. The join condition must be an equality between expressions from the left- and right-hand side
of the join.
- [Metadata tables](sql-metadata-tables.md) from the `INFORMATION_SCHEMA` or `sys` schemas. Unlike the other options for the
FROM clause, metadata tables are not considered datasources. They exist only in the SQL layer.

For more information about table, lookup, query, and join datasources, refer to the [Datasources](datasource.md)
documentation.

## WHERE

The WHERE clause refers to columns in the FROM table, and will be translated to [native filters](filters.md). The
WHERE clause can also reference a subquery, like `WHERE col1 IN (SELECT foo FROM ...)`. Queries like this are executed
as a join on the subquery, described in the [Query translation](sql-translation.md#subqueries) section.

Strings and numbers can be compared in the WHERE clause of a SQL query through implicit type conversion.
For example, you can evaluate `WHERE stringDim = 1` for a string-typed dimension named `stringDim`.
However, for optimal performance, you should explicitly cast the reference number as a string when comparing against a string dimension:
```
WHERE stringDim = '1'
```

Similarly, if you compare a string-typed dimension with reference to an array of numbers, cast the numbers to strings:
```
WHERE stringDim IN ('1', '2', '3')
```

Note that explicit type casting does not lead to significant performance improvement when comparing strings and numbers involving numeric dimensions since numeric dimensions are not indexed.


## GROUP BY

The GROUP BY clause refers to columns in the FROM table. Using GROUP BY, DISTINCT, or any aggregation functions will
trigger an aggregation query using one of Druid's [three native aggregation query types](sql-translation.md#query-types). GROUP BY
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
"country" and "city" columns. Column may also be `NULL` if it was `NULL` in the data itself. To differentiate such rows, 
you can use `GROUPING` aggregation. 

When using GROUP BY GROUPING SETS, GROUP BY ROLLUP, or GROUP BY CUBE, be aware that results may not be generated in the
order that you specify your grouping sets in the query. If you need results to be generated in a particular order, use
the ORDER BY clause.

## HAVING

The HAVING clause refers to columns that are present after execution of GROUP BY. It can be used to filter on either
grouping expressions or aggregated values. It can only be used together with GROUP BY.

## ORDER BY

The ORDER BY clause refers to columns that are present after execution of GROUP BY. It can be used to order the results
based on either grouping expressions or aggregated values. ORDER BY can refer to an expression or a select clause
ordinal position (like `ORDER BY 2` to order by the second selected column). For non-aggregation queries, ORDER BY
can only order by the `__time` column. For aggregation queries, ORDER BY can order by any column.

## LIMIT

The LIMIT clause limits the number of rows returned. In some situations Druid will push down this limit to data servers,
which boosts performance. Limits are always pushed down for queries that run with the native Scan or TopN query types.
With the native GroupBy query type, it is pushed down when ordering on a column that you are grouping by. If you notice
that adding a limit doesn't change performance very much, then it's possible that Druid wasn't able to push down the
limit for your query.

## OFFSET

The OFFSET clause skips a certain number of rows when returning results.

If both LIMIT and OFFSET are provided, then OFFSET will be applied first, followed by LIMIT. For example, using
LIMIT 100 OFFSET 10 will return 100 rows, starting from row number 10.

Together, LIMIT and OFFSET can be used to implement pagination. However, note that if the underlying datasource is
modified between page fetches, then the different pages will not necessarily align with each other.

There are two important factors that can affect the performance of queries that use OFFSET:

- Skipped rows still need to be generated internally and then discarded, meaning that raising offsets to high values
  can cause queries to use additional resources.
- OFFSET is only supported by the Scan and GroupBy [native query types](sql-translation.md#query-types). Therefore, a query with OFFSET
  will use one of those two types, even if it might otherwise have run as a Timeseries or TopN. Switching query engines
  in this way can affect performance.

## UNION ALL

The UNION ALL operator fuses multiple queries together. Druid SQL supports the UNION ALL operator in two situations: top-level and table-level, as described below. Queries that use UNION ALL in any other way will fail.

### Top-level

In top-level queries, you can use UNION ALL at the very top outer layer of the query - not in a subquery, and not in the FROM clause. The underlying queries run sequentially. Druid concatenates their results so that they appear one after the other.

For example:

```
SELECT COUNT(*) FROM tbl WHERE my_column = 'value1'
UNION ALL
SELECT COUNT(*) FROM tbl WHERE my_column = 'value2'
```

> With top-level queries, you can't apply GROUP BY, ORDER BY, or any other operator to the results of a UNION ALL.

### Table-level

In table-level queries, you must use UNION ALL in a subquery in the FROM clause, and create the lower-level subqueries that are inputs to the UNION ALL operator as simple table SELECTs. You can't use features like expressions, column aliasing, JOIN, GROUP BY, or ORDER BY in table-level queries.

The query runs natively using a [union datasource](datasource.md#union).

At table-level queries, you must select the same columns from each table in the same order, and those columns must either have the same types, or types that can be implicitly cast to each other (such as different numeric types). For this reason, it is generally more robust to write your queries to select specific columns. If you use `SELECT *`, you must modify your queries if a new column is added to one table but not to the others.

For example:

```
SELECT col1, COUNT(*)
FROM (
  SELECT col1, col2, col3 FROM tbl1
  UNION ALL
  SELECT col1, col2, col3 FROM tbl2
)
GROUP BY col1
```

With table-level UNION ALL, the rows from the unioned tables are not guaranteed to process in any particular order. They may process in an interleaved fashion. If you need a particular result ordering, use [ORDER BY](#order-by) on the outer query.

## EXPLAIN PLAN

Add "EXPLAIN PLAN FOR" to the beginning of any query to get information about how it will be translated. In this case,
the query will not actually be executed. Refer to the [Query translation](sql-translation.md#interpreting-explain-plan-output)
documentation for more information on the output of EXPLAIN PLAN.

> Be careful when interpreting EXPLAIN PLAN output, and use [request logging](../configuration/index.md#request-logging) if in doubt.
Request logs show the exact native query that will be run.

## Identifiers and literals

Identifiers like datasource and column names can optionally be quoted using double quotes. To escape a double quote
inside an identifier, use another double quote, like `"My ""very own"" identifier"`. All identifiers are case-sensitive
and no implicit case conversions are performed.

Literal strings should be quoted with single quotes, like `'foo'`. Literal strings with Unicode escapes can be written
like `U&'fo\00F6'`, where character codes in hex are prefixed by a backslash. Literal numbers can be written in forms
like `100` (denoting an integer), `100.0` (denoting a floating point value), or `1.0e5` (scientific notation). Literal
timestamps can be written like `TIMESTAMP '2000-01-01 00:00:00'`. Literal intervals, used for time arithmetic, can be
written like `INTERVAL '1' HOUR`, `INTERVAL '1 02:03' DAY TO MINUTE`, `INTERVAL '1-2' YEAR TO MONTH`, and so on.

## Dynamic parameters

Druid SQL supports dynamic parameters using question mark (`?`) syntax, where parameters are bound to `?` placeholders
at execution time. To use dynamic parameters, replace any literal in the query with a `?` character and provide a
corresponding parameter value when you execute the query. Parameters are bound to the placeholders in the order in
which they are passed. Parameters are supported in both the [HTTP POST](sql-api.md) and [JDBC](sql-jdbc.md) APIs.

In certain cases, using dynamic parameters in expressions can cause type inference issues which cause your query to fail, for example:

```sql
SELECT * FROM druid.foo WHERE dim1 like CONCAT('%', ?, '%')
```

To solve this issue, explicitly provide the type of the dynamic parameter using the `CAST` keyword. Consider the fix for the preceding example:

```
SELECT * FROM druid.foo WHERE dim1 like CONCAT('%', CAST (? AS VARCHAR), '%')
```





