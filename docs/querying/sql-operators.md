---
id: sql-operators
title: "Druid SQL Operators"
sidebar_label: "Operators"
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
  should not be changed without updating the script create-sql-docs
  in web-console/script/create-sql-docs, because the script detects
  patterns in this markdown file and parse it to TypeScript file for web console
-->


:::info
 Apache Druid supports two query languages: Druid SQL and [native queries](querying.md).
 This document describes the SQL language.
:::


Operators in [Druid SQL](./sql.md) typically operate on one or two values and return a result based on the values. Types of operators in Druid SQL include arithmetic, comparison, logical, and more, as described here. 

When performing math operations, Druid uses 64-bit integer (long) data type unless there are double or float values. If an operation uses float or double values, then the result is a double, which is a 64-bit float. The precision of float and double values is defined by [Java](https://docs.oracle.com/javase/specs/jls/se8/html/jls-5.html) and [the IEEE standard](https://en.wikipedia.org/wiki/IEEE_754).

Keep the following guidelines in mind to help you manage precision issues:

- Long values can store up to 2^63 accurately with an additional bit used for the sign.
- Float values use 32 bits, and doubles use 64 bits. Both types are impacted by floating point precision. If you need exact decimal values, consider storing the number in a non-decimal format as a long value (up to the limit for longs). For example, if you need three decimal places, store the number multiplied by 1000 and then divide by 1000 when querying.

## Arithmetic operators

|Operator|Description|
|--------|-----------|
|`x + y` |Add|
|`x - y` |Subtract|
|`x * y` |Multiply|
|`x / y` |Divide|

## Datetime arithmetic operators

For the datetime arithmetic operators, `interval_expr` can include interval literals like `INTERVAL '2' HOUR`.
This operator treats days as uniformly 86400 seconds long, and does not take into account daylight savings time.
To account for daylight savings time, use the [`TIME_SHIFT` function](sql-scalar.md#date-and-time-functions).
Also see [`TIMESTAMPADD`](sql-scalar.md#date-and-time-functions) for datetime arithmetic.

|Operator|Description|
|--------|-----------|
|`timestamp_expr + interval_expr`|Add an amount of time to a timestamp.|
|`timestamp_expr - interval_expr`|Subtract an amount of time from a timestamp.|

## Concatenation operator
Also see the [CONCAT function](sql-scalar.md#string-functions).

|Operator|Description|
|--------|-----------|
|<code>x &#124;&#124; y</code>|Concatenate strings `x` and `y`.|

## Comparison operators

|Operator|Description|
|--------|-----------|
|`x = y` |Equal to|
|`x IS NOT DISTINCT FROM y`|Equal to, considering `NULL` as a value. Never returns `NULL`.|
|`x <> y`|Not equal to|
|`x IS DISTINCT FROM y`|Not equal to, considering `NULL` as a value. Never returns `NULL`.|
|`x > y` |Greater than|
|`x >= y`|Greater than or equal to|
|`x < y` |Less than|
|`x <= y`|Less than or equal to|

## Logical operators

|Operator|Description|
|--------|-----------|
|`x AND y`|Boolean AND|
|`x OR y`|Boolean OR|
|`NOT x`|Boolean NOT|
|`x IS NULL`|True if _x_ is NULL or empty string|
|`x IS NOT NULL`|True if _x_ is neither NULL nor empty string|
|`x IS TRUE`|True if _x_ is true|
|`x IS NOT TRUE`|True if _x_ is not true|
|`x IS FALSE`|True if _x_ is false|
|`x IS NOT FALSE`|True if _x_ is not false|
|`x BETWEEN y AND z`|Equivalent to `x >= y AND x <= z`|
|`x NOT BETWEEN y AND z`|Equivalent to `x < y OR x > z`|
|`x LIKE pattern [ESCAPE esc]`|True if _x_ matches a SQL LIKE pattern (with an optional escape)|
|`x NOT LIKE pattern [ESCAPE esc]`|True if _x_ does not match a SQL LIKE pattern (with an optional escape)|
|`x IN (values)`|True if _x_ is one of the listed values|
|`x NOT IN (values)`|True if _x_ is not one of the listed values|
|`x IN (subquery)`|True if _x_ is returned by the subquery. This will be translated into a join; see [Query translation](sql-translation.md) for details.|
|`x NOT IN (subquery)`|True if _x_ is not returned by the subquery. This will be translated into a join; see [Query translation](sql-translation.md) for details.|

## Other operators

|Operator|Description|
|--------|-----------|
|`PIVOT (aggregation_function(column_to_aggregate) FOR column_with_values_to_pivot IN (pivoted_column1 [, pivoted_column2 ...]))`|Carries out an aggregation and transforms rows into columns in the output.|
|`UNPIVOT (values_column FOR names_column IN (unpivoted_column1 [, unpivoted_column2 ... ]))`|Transforms existing column values into rows.|