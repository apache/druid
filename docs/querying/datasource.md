---
id: datasource
title: "Datasources"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';


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

Datasources in Apache Druid are things that you can query. The most common kind of datasource is a table datasource,
and in many contexts the word "datasource" implicitly refers to table datasources. This is especially true
[during data ingestion](../ingestion/index.md), where ingestion is always creating or writing into a table
datasource. But at query time, there are many other types of datasources available.

The word "datasource" is generally spelled `dataSource` (with a capital S) when it appears in API requests and
responses.

## Datasource type

### `table`

<Tabs>
<TabItem value="1" label="SQL">

```sql
SELECT column1, column2 FROM "druid"."dataSourceName"
```
</TabItem>
<TabItem value="2" label="Native">

```json
{
  "queryType": "scan",
  "dataSource": "dataSourceName",
  "columns": ["column1", "column2"],
  "intervals": ["0000/3000"]
}
```
</TabItem>
</Tabs>

The table datasource is the most common type. This is the kind of datasource you get when you perform
[data ingestion](../ingestion/index.md). They are split up into segments, distributed around the cluster,
and queried in parallel.

In [Druid SQL](sql.md#from), table datasources reside in the `druid` schema. This is the default schema, so table
datasources can be referenced as either `druid.dataSourceName` or simply `dataSourceName`.

In native queries, table datasources can be referenced using their names as strings (as in the example above), or by
using JSON objects of the form:

```json
"dataSource": {
  "type": "table",
  "name": "dataSourceName"
}
```

To see a list of all table datasources, use the SQL query
`SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'druid'`.

### `lookup`

<Tabs>
<TabItem value="3" label="SQL">

```sql
SELECT k, v FROM lookup.countries
```
</TabItem>
<TabItem value="4" label="Native">

```json
{
  "queryType": "scan",
  "dataSource": {
    "type": "lookup",
    "lookup": "countries"
  },
  "columns": ["k", "v"],
  "intervals": ["0000/3000"]
}
```
</TabItem>
</Tabs>

Lookup datasources correspond to Druid's key-value [lookup](lookups.md) objects. In [Druid SQL](sql.md#from),
they reside in the `lookup` schema. They are preloaded in memory on all servers, so they can be accessed rapidly.
They can be joined onto regular tables using the [join operator](#join).

Lookup datasources are key-value oriented and always have exactly two columns: `k` (the key) and `v` (the value), and
both are always strings.

To see a list of all lookup datasources, use the SQL query
`SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'lookup'`.

:::info
 Performance tip: Lookups can be joined with a base table either using an explicit [join](#join), or by using the
 SQL [`LOOKUP` function](sql-scalar.md#string-functions).
 However, the join operator must evaluate the condition on each row, whereas the
 `LOOKUP` function can defer evaluation until after an aggregation phase. This means that the `LOOKUP` function is
 usually faster than joining to a lookup datasource.
:::

Refer to the [Query execution](query-execution.md#table) page for more details on how queries are executed when you
use table datasources.

### `union`

<Tabs>
<TabItem value="5" label="SQL">

```sql
SELECT column1, column2
FROM (
  SELECT column1, column2 FROM table1
  UNION ALL
  SELECT column1, column2 FROM table2
  UNION ALL
  SELECT column1, column2 FROM table3
)
```
</TabItem>
<TabItem value="6" label="Native">

```json
{
  "queryType": "scan",
  "dataSource": {
    "type": "union",
    "dataSources": ["table1", "table2", "table3"]
  },
  "columns": ["column1", "column2"],
  "intervals": ["0000/3000"]
}
```
</TabItem>
</Tabs>

Unions allow you to treat two or more tables as a single datasource. In SQL, this is done with the UNION ALL operator
applied directly to tables, called a ["table-level union"](sql.md#table-level). In native queries, this is done with a
"union" datasource.

With SQL [table-level unions](sql.md#table-level) the same columns must be selected from each table in the same order,
and those columns must either have the same types, or types that can be implicitly cast to each other (such as different
numeric types). For this reason, it is more robust to write your queries to select specific columns.

With the native union datasource, the tables being unioned do not need to have identical schemas. If they do not fully
match up, then columns that exist in one table but not another will be treated as if they contained all null values in
the tables where they do not exist.

In either case, features like expressions, column aliasing, JOIN, GROUP BY, ORDER BY, and so on cannot be used with
table unions.

Refer to the [Query execution](query-execution.md#union) page for more details on how queries are executed when you
use union datasources.

### `inline`

<Tabs>
<TabItem value="7" label="Native">

```json
{
  "queryType": "scan",
  "dataSource": {
    "type": "inline",
    "columnNames": ["country", "city"],
    "rows": [
      ["United States", "San Francisco"],
      ["Canada", "Calgary"]
    ]
  },
  "columns": ["country", "city"],
  "intervals": ["0000/3000"]
}
```
</TabItem>
</Tabs>

Inline datasources allow you to query a small amount of data that is embedded in the query itself. They are useful when
you want to write a query on a small amount of data without loading it first. They are also useful as inputs into a
[join](#join). Druid also uses them internally to handle subqueries that need to be inlined on the Broker. See the
[`query` datasource](#query) documentation for more details.

There are two fields in an inline datasource: an array of `columnNames` and an array of `rows`. Each row is an array
that must be exactly as long as the list of `columnNames`. The first element in each row corresponds to the first
column in `columnNames`, and so on.

Inline datasources are not available in Druid SQL.

Refer to the [Query execution](query-execution.md#inline) page for more details on how queries are executed when you
use inline datasources.

### `query`

<Tabs>
<TabItem value="8" label="SQL">

```sql
-- Uses a subquery to count hits per page, then takes the average.
SELECT
  AVG(cnt) AS average_hits_per_page
FROM
  (SELECT page, COUNT(*) AS hits FROM site_traffic GROUP BY page)
```
</TabItem>
<TabItem value="9" label="Native">

```json
{
  "queryType": "timeseries",
  "dataSource": {
    "type": "query",
    "query": {
      "queryType": "groupBy",
      "dataSource": "site_traffic",
      "intervals": ["0000/3000"],
      "granularity": "all",
      "dimensions": ["page"],
      "aggregations": [
        { "type": "count", "name": "hits" }
      ]
    }
  },
  "intervals": ["0000/3000"],
  "granularity": "all",
  "aggregations": [
    { "type": "longSum", "name": "hits", "fieldName": "hits" },
    { "type": "count", "name": "pages" }
  ],
  "postAggregations": [
    { "type": "expression", "name": "average_hits_per_page", "expression": "hits / pages" }
  ]
}
```
</TabItem>
</Tabs>

Query datasources allow you to issue subqueries. In native queries, they can appear anywhere that accepts a
`dataSource` (except underneath a `union`). In SQL, they can appear in the following places, always surrounded by parentheses:

- The FROM clause: `FROM (<subquery>)`.
- As inputs to a JOIN: `<table-or-subquery-1> t1 INNER JOIN <table-or-subquery-2> t2 ON t1.<col1> = t2.<col2>`.
- In the WHERE clause: `WHERE <column> { IN | NOT IN } (<subquery>)`. These are translated to joins by the SQL planner.

:::info
 Performance tip: In most cases, subquery results are fully buffered in memory on the Broker and then further
 processing occurs on the Broker itself. This means that subqueries with large result sets can cause performance
 bottlenecks or run into memory usage limits on the Broker. See the [Query execution](query-execution.md#query)
 page for more details on how subqueries are executed and what limits will apply.
:::

### `join`

<Tabs>
<TabItem value="10" label="SQL">

```sql
-- Joins "sales" with "countries" (using "store" as the join key) to get sales by country.
SELECT
  store_to_country.v AS country,
  SUM(sales.revenue) AS country_revenue
FROM
  sales
  INNER JOIN lookup.store_to_country ON sales.store = store_to_country.k
GROUP BY
  countries.v
```
</TabItem>
<TabItem value="11" label="Native">

```json
{
  "queryType": "groupBy",
  "dataSource": {
    "type": "join",
    "left": "sales",
    "right": {
      "type": "lookup",
      "lookup": "store_to_country"
    },
    "rightPrefix": "r.",
    "condition": "store == \"r.k\"",
    "joinType": "INNER"
  },
  "intervals": ["0000/3000"],
  "granularity": "all",
  "dimensions": [
    { "type": "default", "outputName": "country", "dimension": "r.v" }
  ],
  "aggregations": [
    { "type": "longSum", "name": "country_revenue", "fieldName": "revenue" }
  ]
}
```
</TabItem>
</Tabs>

Join datasources allow you to do a SQL-style join of two datasources. Stacking joins on top of each other allows
you to join arbitrarily many datasources.

In Druid {{DRUIDVERSION}}, joins in native queries are implemented with a broadcast hash-join algorithm. This means
that all datasources other than the leftmost "base" datasource must fit in memory. In native queries, the join condition
must be an equality. In SQL, any join condition is accepted, but only equalities of a certain form
(see [Joins in SQL](#joins-in-sql)) execute efficiently as part of a native join. For other kinds of conditions, planner will try
to re-arrange condition such that some of the sub-conditions are evaluated as a filter on top of join and other 
sub-conditions are left out in the join condition. In worst case scenario, SQL will execute the join condition as a 
cross join (cartesian product) plus a filter.

This feature is intended mainly to allow joining regular Druid tables with [lookup](#lookup), [inline](#inline), and
[query](#query) datasources. Refer to the [Query execution](query-execution.md#join) page for more details on how
queries are executed when you use join datasources.

#### Joins in SQL

SQL joins take the form:

```
<o1> [ INNER | LEFT [OUTER] ] JOIN <o2> ON <condition>
```

Any condition is accepted, but only certain kinds of conditions execute efficiently
as a native join. The condition must be a single clause like the following, or an `AND` of clauses involving at
least one of the following:

- Equality between fields of the same type on each side, like `t1 JOIN t2 ON t1.x = t2.x`.
- Equality between a function call on one side, and a field on the other side, like `t1 JOIN t2 ON LOWER(t1.x) = t2.x`.
- The equality operator may be `=` (which does not match nulls) or `IS NOT DISTINCT FROM` (which does match nulls).

In other cases, Druid will either insert a subquery below the join, or will use a cross join (cartesian product)
followed by a filter. Joins executed in these ways may run into resource or performance constraints. To determine
if your query is using one of these execution paths, run `EXPLAIN PLAN FOR <query>` and look for the following:

- `query` type datasources under the `left` or `right` key of your `join` datasource.
- `join` type datasource with `condition` set to `"1"` (cartesian product) followed by a `filter` that encodes the
  condition you provided.

In these cases, you may be able to improve the performance of your query by rewriting it.

For more information about how Druid translates SQL to native queries, refer to the
[Druid SQL](sql-translation.md) documentation.

#### Joins in native queries

Native join datasources have the following properties. All are required.

|Field|Description|
|-----|-----------|
|`left`|Left-hand datasource. Must be of type `table`, `join`, `lookup`, `query`, or `inline`. Placing another join as the left datasource allows you to join arbitrarily many datasources.|
|`right`|Right-hand datasource. Must be of type `lookup`, `query`, or `inline`. Note that this is more rigid than what Druid SQL requires.|
|`rightPrefix`|String prefix that will be applied to all columns from the right-hand datasource, to prevent them from colliding with columns from the left-hand datasource. Can be any string, so long as it is nonempty and is not be a prefix of the string `__time`. Any columns from the left-hand side that start with your `rightPrefix` will be shadowed. It is up to you to provide a prefix that will not shadow any important columns from the left side.|
|`condition`|[Expression](math-expr.md) that must be an equality where one side is an expression of the left-hand side, and the other side is a simple column reference to the right-hand side. Note that this is more rigid than what Druid SQL requires: here, the right-hand reference must be a simple column reference; in SQL it can be an expression.|
|`joinType`|`INNER` or `LEFT`.|

#### Join performance

Joins are a feature that can significantly affect performance of your queries. Some performance tips and notes:

1. Joins are especially useful with [lookup datasources](#lookup), but in most cases, the
[`LOOKUP` function](sql-scalar.md#string-functions) performs better than a join. Consider using the `LOOKUP` function if
it is appropriate for your use case.
2. When using joins in Druid SQL, keep in mind that it can generate subqueries that you did not explicitly include in
your queries. Refer to the [Druid SQL](sql-translation.md) documentation for more details about when this happens
and how to detect it.
3. One common reason for implicit subquery generation is if the types of the two halves of an equality do not match.
For example, since lookup keys are always strings, the condition `druid.d JOIN lookup.l ON d.field = l.field` will
perform best if `d.field` is a string.
4. As of Druid {{DRUIDVERSION}}, the join operator must evaluate the condition for each row. In the future, we expect
to implement both early and deferred condition evaluation, which we expect to improve performance considerably for
common use cases.
5. Currently, Druid does not support pushing down predicates (condition and filter) past a Join (i.e. into
Join's children). Druid only supports pushing predicates into the join if they originated from
above the join. Hence, the location of predicates and filters in your Druid SQL is very important.
Also, as a result of this, comma joins should be avoided.

#### Future work for joins

Joins are an area of active development in Druid. The following features are missing today but may appear in
future versions:

- Reordering of join operations to get the most performant plan.
- Preloaded dimension tables that are wider than lookups (i.e. supporting more than a single key and single value).
- RIGHT OUTER and FULL OUTER joins in the native query engine. Currently, they are partially implemented. Queries run
  but results are not always correct.
- Performance-related optimizations as mentioned in the [previous section](#join-performance).
- Join conditions on a column containing a multi-value dimension.

### `unnest`

Use the `unnest` datasource to unnest a column with multiple values in an array.
For example, you have a source column that looks like this:

| Nested |
| -- |
| [a, b] |
| [c, d] |
| [e, [f,g]] |

When you use the `unnest` datasource, the unnested column looks like this:

| Unnested |
| -- |
| a |
| b |
| c |
| d |
| e |
| [f, g] |

When unnesting data, keep the following in mind:

- The total number of rows will grow to accommodate the new rows that the unnested data occupy.
- You can unnest the values in more than one column in a single `unnest` datasource, but this can lead to a very large number of new rows depending on your dataset.

The `unnest` datasource uses the following syntax:

```json
  "dataSource": {
    "type": "unnest",
    "base": {
      "type": "table",
      "name": "nested_data"
    },
    "virtualColumn": {
      "type": "expression",
      "name": "output_column",
      "expression": "\"column_reference\""
    },
    "unnestFilter": "optional_filter"
  }
```

* `dataSource.type`: Set this to `unnest`.
* `dataSource.base`: Defines the datasource you want to unnest.
  * `dataSource.base.type`: The type of datasource you want to unnest, such as a table.
* `dataSource.virtualColumn`: [Virtual column](virtual-columns.md) that references the nested values. The output name of this column is reused as the name of the column that contains unnested values. You can replace the source column with the unnested column by specifying the source column's name or a new column by specifying a different name. Outputting it to a new column can help you verify that you get the results that you expect but isn't required.
* `unnestFilter`: A filter only on the output column. You can omit this or set it to null if there are no filters.

To learn more about how to use the `unnest` datasource, see the [unnest tutorial](../tutorials/tutorial-unnest-arrays.md).
