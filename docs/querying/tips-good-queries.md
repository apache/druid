---
id: tips-good-queries
title: "Tips for writing good queries in Druid"
sidebar_label: "Tips for writing good queries"
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

This topic includes tips and examples that can help you investigate and improve query performance and accuracy using [Apache Druid SQL](./sql.md). Use this topic as a companion to the Jupyter Notebook tutorial [Learn the basics of Druid SQL](https://github.com/apache/druid/blob/master/examples/quickstart/jupyter-notebooks/notebooks/03-query/00-using-sql-with-druidapi.ipynb).

Your ability to effectively query your data depends in large part on the way you've ingested and stored the data in Apache Druid. This document assumes that you've followed the best practices described in [Schema design tips and best practices](../ingestion/schema-design.md#general-tips-and-best-practices) when modeling your data. 

## Investigate query performance

If your queries run slower than anticipated, you can use the following tools to investigate query performance issues.

### Analyze query metrics

You can configure Druid processes to emit metrics that are essential for monitoring query execution. See [Query metrics](../operations/metrics.md#query-metrics) for more information. 

### Generate an explain plan

An explain plan shows the full query details and all of the operations Druid performs to execute it. You can use the information in the plan to identify possible areas of query improvement.

See [Explain plan](./sql.md#explain-plan) and [Interpreting explain plan output](./sql-translation.md#interpreting-explain-plan-output) for more information.

You can follow the [Get to know Query view tutorial](../tutorials/tutorial-sql-query-view.md) to create an example explain plan in the Druid console.

## Improve query performance

In most cases, you can improve query performance by adjusting Druid settings and by manually tuning your queries.

### Adjust Druid settings

This section outlines Druid settings that can help to improve query performance.

#### Turn on query caching

You can enable caching in Druid to improve query times for frequently accessed data. Caching enables increased concurrency on the same system, leading to noticeable performance improvements for queries handling throughput for concurrent, mixed workloads.

The largest performance gains from caching tend to apply to TopN and timeseries queries. For GroupBy queries, if the bottleneck is in the merging phase on the Broker, enabling caching results in little noticeable query improvement. See [Performance considerations for caching](./caching.md#performance-considerations-for-caching) for more information.

#### Use approximation

When possible, design your SQL queries in such a way that they match the rules for TopN approximation, so that Druid enables TopN by default. For Druid to automatically optimize for TopN, your SQL query must include the following:

- GROUP BY on one dimension, and
- ORDER BY on one aggregate.

 See [TopN queries](./topnquery.md) for more information.

Note that TopN queries are approximate in that each data process ranks its top K results and only returns those top K results to the Broker.

You can follow the tutorial [Using TopN approximation in Druid queries](https://github.com/apache/druid/blob/master/examples/quickstart/jupyter-notebooks/notebooks/03-query/02-approxRanking.ipynb) to work through some examples with approximation turned on and off. The tutorial [Get to know Query view](../tutorials/tutorial-sql-query-view.md) demonstrates running aggregate queries in the Druid console.

### Manually tune your queries

This section outlines techniques you can use to improve your query accuracy and performance.

#### Query one table at a time

Query a single table at a time to minimize the load on the Druid processor.

#### Select specific columns

Only select the columns needed for the query instead of retrieving all columns from the table. This reduces the amount of data retrieved from the database, which improves query performance.

#### Use filters

Use filters, for example the WHERE clause, and filter on time. Try to minimize the use of inequality filters, because they're very resource-intensive.

The following example query filters on `__time` and `product`:

```
SELECT
  FLOOR(__time to day),
  product,
  sum(quantity * price) as revenue
FROM "orders"
WHERE
  __time > '2023-08-20' and product = 'product 1'
GROUP BY 1, 2
```

The following example uses a wildcard filter on the `diffUrl` column:

```
SELECT * from Wikipedia
WHERE diffUrl LIKE 'https://en.wikipedia%'
AND TIME_IN_INTERVAL(__time, '2016-06-27T01:00:00/2016-06-27T02:00:00')
```

#### Shorten your queries

Make your queries shorter where possible&mdash;Druid processes shorter queries faster. You might also be able to divide a single query into multiple queries.

For example, the following query aggregates over multiple datasources using UNION ALL:

```
SELECT id, SUM(revenue) FROM
   (SELECT id, revenue from datasource_1
UNION ALL
  SELECT id, revenue FROM datasource_2)
...
UNION ALL
   SELECT id, revenue FROM datasource_n)
GROUP BY id
```

To simplify this query, you could split it into several queries, for example:

```
SELECT id, SUM(revenue) FROM datasource_1

SELECT id, SUM(revenue) FROM datasource_2
...
SELECT id, SUM(revenue) FROM datasource_n
```

You could then manually aggregate the results of the individual queries.

#### Minimize or remove subqueries

Consider whether you can pre-compute a subquery task and store it as a join or make it a part of the datasource. See [Datasources: join](./datasource.md#join) and [SQL query translation: Joins](./sql-translation.md#joins) for more information and examples.

#### Consider alternatives to GroupBy

Consider using Timeseries and TopN as alternatives to GroupBy. See [GroupBy queries: alternatives](./groupbyquery.md#alternatives) for more information.

Avoid grouping on high cardinality columns, for example user ID. Investigate whether you can apply a filter first, to reduce the number of results for grouping. 

#### Query over smaller intervals

Consider whether you can query a smaller time interval to return a smaller results set.

For example, the following query doesn't limit on time and could be resource-intensive:

```
SELECT cust_id, sum(revenue) FROM myDatasource
GROUP BY cust_id
```

This query could be split into multiple queries over smaller time spans, with the results combined client-side. For example:

```
SELECT cust_id, sum(revenue) FROM myDatasource
GROUP BY cust_id
WHERE __time BETWEEN '2023-07-01' AND '2023-07-31'

SELECT cust_id, sum(revenue) FROM myDatasource
GROUP BY cust_id
WHERE __time BETWEEN '2023-08-01' AND '2023-08-31'
```

#### Reduce the computation in your queries

Examine your query to see if it uses a lot of transformations, functions, and expressions. Consider whether you could rewrite the query to reduce the level of computation.

## Druid SQL query example

The following example query demonstrates many of the tips outlined in this topic.
The query:

- selects specific dimensions and metrics
- uses approximation
- selects from a single table
- groups by low cardinality columns
- filters on both dimensions and time
- orders by a dimension and a measure
- includes a limit

```
SELECT
   FLOOR() AS month,
   country,
   SUM(price),
   APPROX_COUNT_DISTINCT_DS_HLL(userid)
FROM sales
GROUP BY month, country
WHERE artist = 'Madonna' AND TIME_IN_INTERVAL(__time, '2023-08-01/P1M')
ORDER BY country, SUM(price) DESC
LIMIT 100
```
