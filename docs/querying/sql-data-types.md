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

> Apache Druid supports two query languages: Druid SQL and [native queries](querying.md).
> This document describes the SQL language.


Columns in Druid are associated with a specific data type. This topic describes supported data types in [Druid SQL](./sql.md). 

## Standard types

Druid natively supports five basic column types: "long" (64 bit signed int), "float" (32 bit float), "double" (64 bit
float) "string" (UTF-8 encoded strings and string arrays), and "complex" (catch-all for more exotic data types like
json, hyperUnique, and approxHistogram columns).

Timestamps (including the `__time` column) are treated by Druid as longs, with the value being the number of
milliseconds since 1970-01-01 00:00:00 UTC, not counting leap seconds. Therefore, timestamps in Druid do not carry any
timezone information, but only carry information about the exact moment in time they represent. See the
[Time functions](sql-scalar.md#date-and-time-functions) section for more information about timestamp handling.

The following table describes how Druid maps SQL types onto native types at query runtime. Casts between two SQL types
that have the same Druid runtime type will have no effect, other than exceptions noted in the table. Casts between two
SQL types that have different Druid runtime types will generate a runtime cast in Druid. If a value cannot be properly
cast to another value, as in `CAST('foo' AS BIGINT)`, the runtime will substitute a default value. NULL values cast
to non-nullable types will also be substituted with a default value (for example, nulls cast to numbers will be
converted to zeroes).

|SQL type|Druid runtime type|Default value|Notes|
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
|TIMESTAMP|LONG|`0`, meaning 1970-01-01 00:00:00 UTC|Druid's `__time` column is reported as TIMESTAMP. Casts between string and timestamp types assume standard SQL formatting, e.g. `2000-01-02 03:04:05`, _not_ ISO8601 formatting. For handling other formats, use one of the [time functions](sql-scalar.md#date-and-time-functions).|
|DATE|LONG|`0`, meaning 1970-01-01|Casting TIMESTAMP to DATE rounds down the timestamp to the nearest day. Casts between string and date types assume standard SQL formatting, e.g. `2000-01-02`. For handling other formats, use one of the [time functions](sql-scalar.md#date-and-time-functions).|
|OTHER|COMPLEX|none|May represent various Druid column types such as hyperUnique, approxHistogram, etc.|

## Multi-value strings

Druid's native type system allows strings to potentially have multiple values. These
[multi-value string dimensions](multi-value-dimensions.md) will be reported in SQL as `VARCHAR` typed, and can be
syntactically used like any other VARCHAR. Regular string functions that refer to multi-value string dimensions will be
applied to all values for each row individually. Multi-value string dimensions can also be treated as arrays via special
[multi-value string functions](sql-multivalue-string-functions.md), which can perform powerful array-aware operations.

Grouping by a multi-value expression will observe the native Druid multi-value aggregation behavior, which is similar to
the `UNNEST` functionality available in some other SQL dialects. Refer to the documentation on
[multi-value string dimensions](multi-value-dimensions.md) for additional details.

> Because multi-value dimensions are treated by the SQL planner as `VARCHAR`, there are some inconsistencies between how
> they are handled in Druid SQL and in native queries. For example, expressions involving multi-value dimensions may be
> incorrectly optimized by the Druid SQL planner: `multi_val_dim = 'a' AND multi_val_dim = 'b'` will be optimized to
> `false`, even though it is possible for a single row to have both "a" and "b" as values for `multi_val_dim`. The
> SQL behavior of multi-value dimensions will change in a future release to more closely align with their behavior
> in native queries.

## NULL values

The [`druid.generic.useDefaultValueForNull`](../configuration/index.md#sql-compatible-null-handling)
runtime property controls Druid's NULL handling mode. For the most SQL compliant behavior, set this to `false`.

When `druid.generic.useDefaultValueForNull = true` (the default mode), Druid treats NULLs and empty strings
interchangeably, rather than according to the SQL standard. In this mode Druid SQL only has partial support for NULLs.
For example, the expressions `col IS NULL` and `col = ''` are equivalent, and both will evaluate to true if `col`
contains an empty string. Similarly, the expression `COALESCE(col1, col2)` will return `col2` if `col1` is an empty
string. While the `COUNT(*)` aggregator counts all rows, the `COUNT(expr)` aggregator will count the number of rows
where `expr` is neither null nor the empty string. Numeric columns in this mode are not nullable; any null or missing
values will be treated as zeroes.

When `druid.generic.useDefaultValueForNull = false`, NULLs are treated more closely to the SQL standard. In this mode,
numeric NULL is permitted, and NULLs and empty strings are no longer treated as interchangeable. This property
affects both storage and querying, and must be set on all Druid service types to be available at both ingestion time
and query time. There is some overhead associated with the ability to handle NULLs; see
the [segment internals](../design/segments.md#handling-null-values) documentation for more details.

## Boolean logic

The [`druid.expressions.useStrictBooleans`](../configuration/index.md#expression-processing-configurations)
runtime property controls Druid's boolean logic mode. For the most SQL compliant behavior, set this to `true`.

When `druid.expressions.useStrictBooleans = false` (the default mode), Druid uses two-valued logic.

When `druid.expressions.useStrictBooleans = true`, Druid uses three-valued logic for
[expressions](../misc/math-expr.md) evaluation, such as `expression` virtual columns or `expression` filters.
However, even in this mode, Druid uses two-valued logic for filter types other than `expression`.

## Nested columns

Druid supports storing nested data structures in segments using the native `COMPLEX<json>` type. See [Nested columns](./nested-columns.md) for more information.

You can interact with nested data using [JSON functions](./sql-json-functions.md), which can extract nested values, parse from string, serialize to string, and create new `COMPLEX<json>` structures.

`COMPLEX` types have limited functionality outside the specialized functions that use them, so their behavior is undefined when:

* Grouping on complex values.
* Filtering directly on complex values, such as `WHERE json is NULL`.
* Used as inputs to aggregators without specialized handling for a specific complex type.

In many cases, functions are provided to translate `COMPLEX` value types to `STRING`, which serves as a workaround solution until `COMPLEX` type functionality can be improved.
