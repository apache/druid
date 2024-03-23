---
id: sql-data-types
title: "SQL data types"
sidebar_label: "SQL data types"
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

Druid associates each column with a specific data type. This topic describes supported data types in [Druid SQL](./sql.md).

## Standard types

Druid natively supports the following basic column types:

* LONG: 64-bit signed int
* FLOAT: 32-bit float
* DOUBLE: 64-bit float
* STRING: UTF-8 encoded strings and string arrays
* COMPLEX: non-standard data types, such as nested JSON, hyperUnique and approxHistogram, and DataSketches
* ARRAY: arrays composed of any of these types

Druid treats timestamps (including the `__time` column) as LONG, with the value being the number of
milliseconds since 1970-01-01 00:00:00 UTC, not counting leap seconds. Therefore, timestamps in Druid do not carry any
timezone information. They only carry information about the exact moment in time they represent. See
[Time functions](sql-scalar.md#date-and-time-functions) for more information about timestamp handling.

The following table describes how Druid maps SQL types onto native types when running queries:

|SQL type|Druid runtime type|Default value<sup>*</sup>|Notes|
|--------|------------------|-------------|-----|
|CHAR|STRING|`''`||
|VARCHAR|STRING|`''`|Druid STRING columns are reported as VARCHAR. Can include [multi-value strings](#multi-value-strings) as well.|
|DECIMAL|DOUBLE|`0.0`|DECIMAL uses floating point, not fixed point math|
|FLOAT|FLOAT|`0.0`|Druid FLOAT columns are reported as FLOAT|
|REAL|DOUBLE|`0.0`||
|DOUBLE|DOUBLE|`0.0`|Druid DOUBLE columns are reported as DOUBLE|
|BOOLEAN|LONG|`false`||
|TINYINT|LONG|`0`||
|SMALLINT|LONG|`0`||
|INTEGER|LONG|`0`||
|BIGINT|LONG|`0`|Druid LONG columns (except `__time`) are reported as BIGINT|
|TIMESTAMP|LONG|`0`, meaning 1970-01-01 00:00:00 UTC|Druid's `__time` column is reported as TIMESTAMP. Casts between string and timestamp types assume standard SQL formatting, such as `2000-01-02 03:04:05`, not ISO 8601 formatting. For handling other formats, use one of the [time functions](sql-scalar.md#date-and-time-functions).|
|DATE|LONG|`0`, meaning 1970-01-01|Casting TIMESTAMP to DATE rounds down the timestamp to the nearest day. Casts between string and date types assume standard SQL formatting&mdash;for example, `2000-01-02`. For handling other formats, use one of the [time functions](sql-scalar.md#date-and-time-functions).|
|ARRAY|ARRAY|`NULL`|Druid native array types work as SQL arrays, and multi-value strings can be converted to arrays. See [Arrays](#arrays) for more information.|
|OTHER|COMPLEX|none|May represent various Druid column types such as hyperUnique, approxHistogram, etc.|

<sup>*</sup> 
The default value is <code>NULL</code> for all types, except in the deprecated legacy mode (<code>druid.generic.useDefaultValueForNull = true</code>) which initialize a default value. 
<br /><br />
For casts between two SQL types, the behavior depends on the runtime type:

* Casts between two SQL types with the same Druid runtime type have no effect other than the exceptions noted in the table.

* Casts between two SQL types that have different Druid runtime types generate a runtime cast in Druid.

If a value cannot be cast to the target type, as in `CAST('foo' AS BIGINT)`, Druid a substitutes [NULL](#null-values).
When `druid.generic.useDefaultValueForNull = true` (deprecated legacy mode), Druid instead substitutes a default value, including when NULL values cast to non-nullable types. For example, if `druid.generic.useDefaultValueForNull = true`, a null VARCHAR cast to BIGINT is converted to a zero.

## Arrays

Druid supports [`ARRAY` types](arrays.md), which behave as standard SQL arrays, where results are grouped by matching entire arrays. The [`UNNEST` operator](./sql.md#unnest) can be used to perform operations on individual array elements, translating each element into a separate row. 

`ARRAY` typed columns can be stored in segments with JSON-based ingestion using the 'auto' typed dimension schema shared with [schema auto-discovery](../ingestion/schema-design.md#schema-auto-discovery-for-dimensions) to detect and ingest arrays as ARRAY typed columns. For [SQL based ingestion](../multi-stage-query/index.md), the query context parameter `arrayIngestMode` must be specified as `"array"` to ingest ARRAY types. In Druid 28, the default mode for this parameter is `"mvd"` for backwards compatibility, which instead can only handle `ARRAY<STRING>` which it stores in [multi-value string columns](#multi-value-strings). 

You can convert multi-value dimensions to standard SQL arrays explicitly with `MV_TO_ARRAY` or implicitly using [array functions](./sql-array-functions.md). You can also use the array functions to construct arrays from multiple columns.

Druid serializes `ARRAY` results as a JSON string of the array by default, which can be controlled by the context parameter
[`sqlStringifyArrays`](sql-query-context.md). When set to `false` and using JSON [result formats](../api-reference/sql-api.md#responses), the arrays will instead be returned as regular JSON arrays instead of in stringified form.

## Multi-value strings

Druid's native type system allows strings to have multiple values. These [multi-value string dimensions](multi-value-dimensions.md) are reported in SQL as type VARCHAR and can be
syntactically used like any other VARCHAR. Regular string functions that refer to multi-value string dimensions are applied to all values for each row individually.

You can treat multi-value string dimensions as arrays using special
[multi-value string functions](sql-multivalue-string-functions.md), which perform powerful array-aware operations, but retain their VARCHAR type and behavior.

Grouping by multi-value dimensions observes the native Druid multi-value aggregation behavior, which is similar to an implicit SQL UNNEST. See [Grouping](multi-value-dimensions.md#grouping) for more information.

:::info
Because the SQL planner treats multi-value dimensions as VARCHAR, there are some inconsistencies between how they are handled in Druid SQL and in native queries. For instance, expressions involving multi-value dimensions may be incorrectly optimized by the Druid SQL planner. For example, `multi_val_dim = 'a' AND multi_val_dim = 'b'` is optimized to
`false`, even though it is possible for a single row to have both `'a'` and `'b'` as values for `multi_val_dim`.

The SQL behavior of multi-value dimensions may change in a future release to more closely align with their behavior in native queries, but the [multi-value string functions](./sql-multivalue-string-functions.md) should be able to provide nearly all possible native functionality.
:::

## Multi-value strings behavior

The behavior of Druid [multi-value string dimensions](multi-value-dimensions.md) varies depending on the context of
their usage.

When used with standard VARCHAR functions which expect a single input value per row, such as CONCAT, Druid will map
the function across all values in the row. If the row is null or empty, the function receives `NULL` as its input.

When used with the explicit [multi-value string functions](./sql-multivalue-string-functions.md), Druid processes the
row values as if they were ARRAY typed. Any operations which produce null and empty rows are distinguished as
separate values (unlike implicit mapping behavior). These multi-value string functions, typically denoted with an `MV_`
prefix, retain their VARCHAR type after the computation is complete. Note that Druid multi-value columns do _not_
distinguish between empty and null rows. An empty row will never appear natively as input to a multi-valued function,
but any multi-value function which manipulates the array form of the value may produce an empty array, which is handled
separately while processing.

:::info
 Do not mix the usage of multi-value functions and normal scalar functions within the same expression, as the planner will be unable
 to determine how to properly process the value given its ambiguous usage. A multi-value string must be treated consistently within
 an expression.
:::

When converted to ARRAY or used with [array functions](./sql-array-functions.md), multi-value strings behave as standard SQL arrays and can no longer
be manipulated with non-array functions.

By default Druid serializes multi-value VARCHAR results as a JSON string of the array, if grouping was not applied on the value.
If the value was grouped, due to the implicit UNNEST behavior, all results will always be standard single value
VARCHAR. ARRAY typed results serialization is controlled with the context parameter [`sqlStringifyArrays`](sql-query-context.md). When set
to `false` and using JSON [result formats](../api-reference/sql-api.md#responses), the arrays will instead be returned
as regular JSON arrays instead of in stringified form.


## NULL values

The [`druid.generic.useDefaultValueForNull`](../configuration/index.md#sql-compatible-null-handling)
runtime property controls Druid's NULL handling mode. For the most SQL compliant behavior, set this to `false` (the default).

When `druid.generic.useDefaultValueForNull = false` (the default), NULLs are treated more closely to the SQL standard. In this mode,
numeric NULL is permitted, and NULLs and empty strings are no longer treated as interchangeable. This property
affects both storage and querying, and must be set on all Druid service types to be available at both ingestion time
and query time. There is some overhead associated with the ability to handle NULLs; see
the [segment internals](../design/segments.md#handling-null-values) documentation for more details.

When `druid.generic.useDefaultValueForNull = true` (deprecated legacy mode), Druid treats NULLs and empty strings
interchangeably, rather than according to the SQL standard. In this mode Druid SQL only has partial support for NULLs.
For example, the expressions `col IS NULL` and `col = ''` are equivalent, and both evaluate to true if `col` contains
an empty string. Similarly, the expression `COALESCE(col1, col2)` returns `col2` if `col1` is an empty string. While
the `COUNT(*)` aggregator counts all rows, the `COUNT(expr)` aggregator counts the number of rows where `expr` is
neither null nor the empty string. Numeric columns in this mode are not nullable; any null or missing values are
treated as zeroes. This was the default prior to Druid 28.0.0, but will be removed in a future release so that Druid
always behaves in an SQL compatible manner.

## Boolean logic

By default, Druid uses [SQL three-valued logic](https://en.wikipedia.org/wiki/Three-valued_logic#SQL) for filter processing
and boolean expression evaluation. This behavior relies on three settings:

*  [`druid.generic.useDefaultValueForNull`](../configuration/index.md#sql-compatible-null-handling) must be set to false (default), a runtime property which allows NULL values to exist in numeric columns and expressions, and string typed columns to distinguish between NULL and the empty string 
*  [`druid.expressions.useStrictBooleans`](../configuration/index.md#expression-processing-configurations) must be set to true (default), a runtime property controls Druid's boolean logic mode for expressions, as well as coercing all expression boolean values to be represented with a 1 for true and 0 for false
*  [`druid.generic.useThreeValueLogicForNativeFilters`](../configuration/index.md#sql-compatible-null-handling) must be set to true (default), a runtime property which decouples three-value logic handling from `druid.generic.useDefaultValueForNull` and `druid.expressions.useStrictBooleans` for backwards compatibility with older versions of Druid that did not fully support SQL compatible null value logic handling

If any of these settings is configured with a non-default value, Druid will use two-valued logic for non-expression based filters. Expression based filters are controlled independently with `druid.expressions.useStrictBooleans`, which if set to false Druid will use two-valued logic for expressions.

These configurations have been deprecated and will be removed in a future release so that Druid always has SQL compliant behavior.

## Nested columns

Druid supports storing nested data structures in segments using the native `COMPLEX<json>` type. See [Nested columns](./nested-columns.md) for more information.

You can interact with nested data using [JSON functions](./sql-json-functions.md), which can extract nested values, parse from string, serialize to string, and create new `COMPLEX<json>` structures.

COMPLEX types have limited functionality outside the specialized functions that use them, so their behavior is undefined when:

* Grouping on complex values.
* Filtering directly on complex values.
* Used as inputs to aggregators without specialized handling for a specific complex type.

In many cases, functions are provided to translate COMPLEX value types to STRING, which serves as a workaround solution until COMPLEX type functionality can be improved.
