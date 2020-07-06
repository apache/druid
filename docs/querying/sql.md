---
id: sql
title: "SQL"
sidebar_label: "Druid SQL"
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


> Apache Druid supports two query languages: Druid SQL and [native queries](querying.md).
> This document describes the SQL language.

Druid SQL is a built-in SQL layer and an alternative to Druid's native JSON-based query language, and is powered by a
parser and planner based on [Apache Calcite](https://calcite.apache.org/). Druid SQL translates SQL into native Druid
queries on the query Broker (the first process you query), which are then passed down to data processes as native Druid
queries. Other than the (slight) overhead of [translating](#query-translation) SQL on the Broker, there isn't an
additional performance penalty versus native queries.

## Query syntax

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
[ UNION ALL <another query> ]
```

### FROM

The FROM clause can refer to any of the following:

- [Table datasources](datasource.html#table) from the `druid` schema. This is the default schema, so Druid table
datasources can be referenced as either `druid.dataSourceName` or simply `dataSourceName`.
- [Lookups](datasource.html#lookup) from the `lookup` schema, for example `lookup.countries`. Note that lookups can
also be queried using the [`LOOKUP` function](#string-functions).
- [Subqueries](datasource.html#query).
- [Joins](datasource.html#join) between anything in this list, except between native datasources (table, lookup,
query) and system tables. The join condition must be an equality between expressions from the left- and right-hand side
of the join.
- [Metadata tables](#metadata-tables) from the `INFORMATION_SCHEMA` or `sys` schemas. Unlike the other options for the
FROM clause, metadata tables are not considered datasources. They exist only in the SQL layer.

For more information about table, lookup, query, and join datasources, refer to the [Datasources](datasource.html)
documentation.

### WHERE

The WHERE clause refers to columns in the FROM table, and will be translated to [native filters](filters.html). The
WHERE clause can also reference a subquery, like `WHERE col1 IN (SELECT foo FROM ...)`. Queries like this are executed
as a join on the subquery, described below in the [Query translation](#subqueries) section.

### GROUP BY

The GROUP BY clause refers to columns in the FROM table. Using GROUP BY, DISTINCT, or any aggregation functions will
trigger an aggregation query using one of Druid's [three native aggregation query types](#query-types). GROUP BY
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

### HAVING

The HAVING clause refers to columns that are present after execution of GROUP BY. It can be used to filter on either
grouping expressions or aggregated values. It can only be used together with GROUP BY.

### ORDER BY

The ORDER BY clause refers to columns that are present after execution of GROUP BY. It can be used to order the results
based on either grouping expressions or aggregated values. ORDER BY can refer to an expression or a select clause
ordinal position (like `ORDER BY 2` to order by the second selected column). For non-aggregation queries, ORDER BY
can only order by the `__time` column. For aggregation queries, ORDER BY can order by any column.

### LIMIT

The LIMIT clause can be used to limit the number of rows returned. It can be used with any query type. It is pushed down
to Data processes for queries that run with the native TopN query type, but not the native GroupBy query type. Future
versions of Druid will support pushing down limits using the native GroupBy query type as well. If you notice that
adding a limit doesn't change performance very much, then it's likely that Druid didn't push down the limit for your
query.

### UNION ALL

The "UNION ALL" operator can be used to fuse multiple queries together. Their results will be concatenated, and each
query will run separately, back to back (not in parallel). Druid does not currently support "UNION" without "ALL".
UNION ALL must appear at the very outer layer of a SQL query (it cannot appear in a subquery or in the FROM clause).

Note that despite the similar name, UNION ALL is not the same thing as as [union datasource](datasource.md#union).
UNION ALL allows unioning the results of queries, whereas union datasources allow unioning tables.

### EXPLAIN PLAN

Add "EXPLAIN PLAN FOR" to the beginning of any query to get information about how it will be translated. In this case,
the query will not actually be executed. Refer to the [Query translation](#query-translation) documentation for help
interpreting EXPLAIN PLAN output.

### Identifiers and literals

Identifiers like datasource and column names can optionally be quoted using double quotes. To escape a double quote
inside an identifier, use another double quote, like `"My ""very own"" identifier"`. All identifiers are case-sensitive
and no implicit case conversions are performed.

Literal strings should be quoted with single quotes, like `'foo'`. Literal strings with Unicode escapes can be written
like `U&'fo\00F6'`, where character codes in hex are prefixed by a backslash. Literal numbers can be written in forms
like `100` (denoting an integer), `100.0` (denoting a floating point value), or `1.0e5` (scientific notation). Literal
timestamps can be written like `TIMESTAMP '2000-01-01 00:00:00'`. Literal intervals, used for time arithmetic, can be
written like `INTERVAL '1' HOUR`, `INTERVAL '1 02:03' DAY TO MINUTE`, `INTERVAL '1-2' YEAR TO MONTH`, and so on.

### Dynamic parameters

Druid SQL supports dynamic parameters using question mark (`?`) syntax, where parameters are bound to `?` placeholders
at execution time. To use dynamic parameters, replace any literal in the query with a `?` character and provide a
corresponding parameter value when you execute the query. Parameters are bound to the placeholders in the order in
which they are passed. Parameters are supported in both the [HTTP POST](#http-post) and [JDBC](#jdbc) APIs.

## Data types

### Standard types

Druid natively supports five basic column types: "long" (64 bit signed int), "float" (32 bit float), "double" (64 bit
float) "string" (UTF-8 encoded strings and string arrays), and "complex" (catch-all for more exotic data types like
hyperUnique and approxHistogram columns).

Timestamps (including the `__time` column) are treated by Druid as longs, with the value being the number of
milliseconds since 1970-01-01 00:00:00 UTC, not counting leap seconds. Therefore, timestamps in Druid do not carry any
timezone information, but only carry information about the exact moment in time they represent. See the
[Time functions](#time-functions) section for more information about timestamp handling.

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
|TIMESTAMP|LONG|`0`, meaning 1970-01-01 00:00:00 UTC|Druid's `__time` column is reported as TIMESTAMP. Casts between string and timestamp types assume standard SQL formatting, e.g. `2000-01-02 03:04:05`, _not_ ISO8601 formatting. For handling other formats, use one of the [time functions](#time-functions)|
|DATE|LONG|`0`, meaning 1970-01-01|Casting TIMESTAMP to DATE rounds down the timestamp to the nearest day. Casts between string and date types assume standard SQL formatting, e.g. `2000-01-02`. For handling other formats, use one of the [time functions](#time-functions)|
|OTHER|COMPLEX|none|May represent various Druid column types such as hyperUnique, approxHistogram, etc|

### Multi-value strings

Druid's native type system allows strings to potentially have multiple values. These
[multi-value string dimensions](multi-value-dimensions.html) will be reported in SQL as `VARCHAR` typed, and can be
syntactically used like any other VARCHAR. Regular string functions that refer to multi-value string dimensions will be
applied to all values for each row individually. Multi-value string dimensions can also be treated as arrays via special
[multi-value string functions](#multi-value-string-functions), which can perform powerful array-aware operations.

Grouping by a multi-value expression will observe the native Druid multi-value aggregation behavior, which is similar to
the `UNNEST` functionality available in some other SQL dialects. Refer to the documentation on
[multi-value string dimensions](multi-value-dimensions.html) for additional details.

### NULL values

The `druid.generic.useDefaultValueForNull` [runtime property](../configuration/index.html#sql-compatible-null-handling)
controls Druid's NULL handling mode.

In the default mode (`true`), Druid treats NULLs and empty strings interchangeably, rather than according to the SQL
standard. In this mode Druid SQL only has partial support for NULLs. For example, the expressions `col IS NULL` and
`col = ''` are equivalent, and both will evaluate to true if `col` contains an empty string. Similarly, the expression
`COALESCE(col1, col2)` will return `col2` if `col1` is an empty string. While the `COUNT(*)` aggregator counts all rows,
the `COUNT(expr)` aggregator will count the number of rows where expr is neither null nor the empty string. Numeric
columns in this mode are not nullable; any null or missing values will be treated as zeroes.

In SQL compatible mode (`false`), NULLs are treated more closely to the SQL standard. The property affects both storage
and querying, so for best behavior, it should be set at both ingestion time and query time. There is some overhead
associated with the ability to handle NULLs; see the [segment internals](../design/segments.md#sql-compatible-null-handling)
documentation for more details.

## Aggregation functions

Aggregation functions can appear in the SELECT clause of any query. Any aggregator can be filtered using syntax like
`AGG(expr) FILTER(WHERE whereExpr)`. Filtered aggregators will only aggregate rows that match their filter. It's
possible for two aggregators in the same SQL query to have different filters.

Only the COUNT aggregation can accept DISTINCT.

|Function|Notes|
|--------|-----|
|`COUNT(*)`|Counts the number of rows.|
|`COUNT(DISTINCT expr)`|Counts distinct values of expr, which can be string, numeric, or hyperUnique. By default this is approximate, using a variant of [HyperLogLog](http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf). To get exact counts set "useApproximateCountDistinct" to "false". If you do this, expr must be string or numeric, since exact counts are not possible using hyperUnique columns. See also `APPROX_COUNT_DISTINCT(expr)`. In exact mode, only one distinct count per query is permitted.|
|`SUM(expr)`|Sums numbers.|
|`MIN(expr)`|Takes the minimum of numbers.|
|`MAX(expr)`|Takes the maximum of numbers.|
|`AVG(expr)`|Averages numbers.|
|`APPROX_COUNT_DISTINCT(expr)`|Counts distinct values of expr, which can be a regular column or a hyperUnique column. This is always approximate, regardless of the value of "useApproximateCountDistinct". This uses Druid's built-in "cardinality" or "hyperUnique" aggregators. See also `COUNT(DISTINCT expr)`.|
|`APPROX_COUNT_DISTINCT_DS_HLL(expr, [lgK, tgtHllType])`|Counts distinct values of expr, which can be a regular column or an [HLL sketch](../development/extensions-core/datasketches-hll.html) column. The `lgK` and `tgtHllType` parameters are described in the HLL sketch documentation. This is always approximate, regardless of the value of "useApproximateCountDistinct". See also `COUNT(DISTINCT expr)`. The [DataSketches extension](../development/extensions-core/datasketches-extension.html) must be loaded to use this function.|
|`APPROX_COUNT_DISTINCT_DS_THETA(expr, [size])`|Counts distinct values of expr, which can be a regular column or a [Theta sketch](../development/extensions-core/datasketches-theta.html) column. The `size` parameter is described in the Theta sketch documentation. This is always approximate, regardless of the value of "useApproximateCountDistinct". See also `COUNT(DISTINCT expr)`. The [DataSketches extension](../development/extensions-core/datasketches-extension.html) must be loaded to use this function.|
|`DS_HLL(expr, [lgK, tgtHllType])`|Creates an [HLL sketch](../development/extensions-core/datasketches-hll.html) on the values of expr, which can be a regular column or a column containing HLL sketches. The `lgK` and `tgtHllType` parameters are described in the HLL sketch documentation. The [DataSketches extension](../development/extensions-core/datasketches-extension.html) must be loaded to use this function.|
|`DS_THETA(expr, [size])`|Creates a [Theta sketch](../development/extensions-core/datasketches-theta.html) on the values of expr, which can be a regular column or a column containing Theta sketches. The `size` parameter is described in the Theta sketch documentation. The [DataSketches extension](../development/extensions-core/datasketches-extension.html) must be loaded to use this function.|
|`APPROX_QUANTILE(expr, probability, [resolution])`|Computes approximate quantiles on numeric or [approxHistogram](../development/extensions-core/approximate-histograms.html#approximate-histogram-aggregator) exprs. The "probability" should be between 0 and 1 (exclusive). The "resolution" is the number of centroids to use for the computation. Higher resolutions will give more precise results but also have higher overhead. If not provided, the default resolution is 50. The [approximate histogram extension](../development/extensions-core/approximate-histograms.html) must be loaded to use this function.|
|`APPROX_QUANTILE_DS(expr, probability, [k])`|Computes approximate quantiles on numeric or [Quantiles sketch](../development/extensions-core/datasketches-quantiles.html) exprs. The "probability" should be between 0 and 1 (exclusive). The `k` parameter is described in the Quantiles sketch documentation. The [DataSketches extension](../development/extensions-core/datasketches-extension.html) must be loaded to use this function.|
|`APPROX_QUANTILE_FIXED_BUCKETS(expr, probability, numBuckets, lowerLimit, upperLimit, [outlierHandlingMode])`|Computes approximate quantiles on numeric or [fixed buckets histogram](../development/extensions-core/approximate-histograms.html#fixed-buckets-histogram) exprs. The "probability" should be between 0 and 1 (exclusive). The `numBuckets`, `lowerLimit`, `upperLimit`, and `outlierHandlingMode` parameters are described in the fixed buckets histogram documentation. The [approximate histogram extension](../development/extensions-core/approximate-histograms.html) must be loaded to use this function.|
|`DS_QUANTILES_SKETCH(expr, [k])`|Creates a [Quantiles sketch](../development/extensions-core/datasketches-quantiles.html) on the values of expr, which can be a regular column or a column containing quantiles sketches. The `k` parameter is described in the Quantiles sketch documentation. The [DataSketches extension](../development/extensions-core/datasketches-extension.html) must be loaded to use this function.|
|`BLOOM_FILTER(expr, numEntries)`|Computes a bloom filter from values produced by `expr`, with `numEntries` maximum number of distinct values before false positive rate increases. See [bloom filter extension](../development/extensions-core/bloom-filter.html) documentation for additional details.|
|`TDIGEST_QUANTILE(expr, quantileFraction, [compression])`|Builds a T-Digest sketch on values produced by `expr` and returns the value for the quantile. Compression parameter (default value 100) determines the accuracy and size of the sketch. Higher compression means higher accuracy but more space to store sketches. See [t-digest extension](../development/extensions-contrib/tdigestsketch-quantiles.html) documentation for additional details.|
|`TDIGEST_GENERATE_SKETCH(expr, [compression])`|Builds a T-Digest sketch on values produced by `expr`. Compression parameter (default value 100) determines the accuracy and size of the sketch Higher compression means higher accuracy but more space to store sketches. See [t-digest extension](../development/extensions-contrib/tdigestsketch-quantiles.html) documentation for additional details.|
|`VAR_POP(expr)`|Computes variance population of `expr`. See [stats extension](../development/extensions-core/stats.html) documentation for additional details.|
|`VAR_SAMP(expr)`|Computes variance sample of `expr`. See [stats extension](../development/extensions-core/stats.html) documentation for additional details.|
|`VARIANCE(expr)`|Computes variance sample of `expr`. See [stats extension](../development/extensions-core/stats.html) documentation for additional details.|
|`STDDEV_POP(expr)`|Computes standard deviation population of `expr`. See [stats extension](../development/extensions-core/stats.html) documentation for additional details.|
|`STDDEV_SAMP(expr)`|Computes standard deviation sample of `expr`. See [stats extension](../development/extensions-core/stats.html) documentation for additional details.|
|`STDDEV(expr)`|Computes standard deviation sample of `expr`. See [stats extension](../development/extensions-core/stats.html) documentation for additional details.|
|`EARLIEST(expr)`|Returns the earliest value of `expr`, which must be numeric. If `expr` comes from a relation with a timestamp column (like a Druid datasource) then "earliest" is the value first encountered with the minimum overall timestamp of all values being aggregated. If `expr` does not come from a relation with a timestamp, then it is simply the first value encountered.|
|`EARLIEST(expr, maxBytesPerString)`|Like `EARLIEST(expr)`, but for strings. The `maxBytesPerString` parameter determines how much aggregation space to allocate per string. Strings longer than this limit will be truncated. This parameter should be set as low as possible, since high values will lead to wasted memory.|
|`LATEST(expr)`|Returns the latest value of `expr`, which must be numeric. If `expr` comes from a relation with a timestamp column (like a Druid datasource) then "latest" is the value last encountered with the maximum overall timestamp of all values being aggregated. If `expr` does not come from a relation with a timestamp, then it is simply the last value encountered.|
|`LATEST(expr, maxBytesPerString)`|Like `LATEST(expr)`, but for strings. The `maxBytesPerString` parameter determines how much aggregation space to allocate per string. Strings longer than this limit will be truncated. This parameter should be set as low as possible, since high values will lead to wasted memory.|
|`ANY_VALUE(expr)`|Returns any value of `expr` including null. `expr` must be numeric. This aggregator can simplify and optimize the performance by returning the first encountered value (including null)|
|`ANY_VALUE(expr, maxBytesPerString)`|Like `ANY_VALUE(expr)`, but for strings. The `maxBytesPerString` parameter determines how much aggregation space to allocate per string. Strings longer than this limit will be truncated. This parameter should be set as low as possible, since high values will lead to wasted memory.|

For advice on choosing approximate aggregation functions, check out our [approximate aggregations documentation](aggregations.html#approx).

## Scalar functions

### Numeric functions

For mathematical operations, Druid SQL will use integer math if all operands involved in an expression are integers.
Otherwise, Druid will switch to floating point math. You can force this to happen by casting one of your operands
to FLOAT. At runtime, Druid will widen 32-bit floats to 64-bit for most expressions.

|Function|Notes|
|--------|-----|
|`ABS(expr)`|Absolute value.|
|`CEIL(expr)`|Ceiling.|
|`EXP(expr)`|e to the power of expr.|
|`FLOOR(expr)`|Floor.|
|`LN(expr)`|Logarithm (base e).|
|`LOG10(expr)`|Logarithm (base 10).|
|`POWER(expr, power)`|expr to a power.|
|`SQRT(expr)`|Square root.|
|`TRUNCATE(expr[, digits])`|Truncate expr to a specific number of decimal digits. If digits is negative, then this truncates that many places to the left of the decimal point. Digits defaults to zero if not specified.|
|`TRUNC(expr[, digits])`|Synonym for `TRUNCATE`.|
|`ROUND(expr[, digits])`|`ROUND(x, y)` would return the value of the x rounded to the y decimal places. While x can be an integer or floating-point number, y must be an integer. The type of the return value is specified by that of x. y defaults to 0 if omitted. When y is negative, x is rounded on the left side of the y decimal points. If `expr` evaluates to either `NaN`, `expr` will be converted to 0. If `expr` is infinity, `expr` will be converted to the nearest finite double. |
|`x + y`|Addition.|
|`x - y`|Subtraction.|
|`x * y`|Multiplication.|
|`x / y`|Division.|
|`MOD(x, y)`|Modulo (remainder of x divided by y).|
|`SIN(expr)`|Trigonometric sine of an angle expr.|
|`COS(expr)`|Trigonometric cosine of an angle expr.|
|`TAN(expr)`|Trigonometric tangent of an angle expr.|
|`COT(expr)`|Trigonometric cotangent of an angle expr.|
|`ASIN(expr)`|Arc sine of expr.|
|`ACOS(expr)`|Arc cosine of expr.|
|`ATAN(expr)`|Arc tangent of expr.|
|`ATAN2(y, x)`|Angle theta from the conversion of rectangular coordinates (x, y) to polar * coordinates (r, theta).|
|`DEGREES(expr)`|Converts an angle measured in radians to an approximately equivalent angle measured in degrees|
|`RADIANS(expr)`|Converts an angle measured in degrees to an approximately equivalent angle measured in radians|

### String functions

String functions accept strings, and return a type appropriate to the function.

|Function|Notes|
|--------|-----|
|<code>x &#124;&#124; y</code>|Concat strings x and y.|
|`CONCAT(expr, expr...)`|Concats a list of expressions.|
|`TEXTCAT(expr, expr)`|Two argument version of CONCAT.|
|`STRING_FORMAT(pattern[, args...])`|Returns a string formatted in the manner of Java's [String.format](https://docs.oracle.com/javase/8/docs/api/java/lang/String.html#format-java.lang.String-java.lang.Object...-).|
|`LENGTH(expr)`|Length of expr in UTF-16 code units.|
|`CHAR_LENGTH(expr)`|Synonym for `LENGTH`.|
|`CHARACTER_LENGTH(expr)`|Synonym for `LENGTH`.|
|`STRLEN(expr)`|Synonym for `LENGTH`.|
|`LOOKUP(expr, lookupName)`|Look up expr in a registered [query-time lookup table](lookups.html). Note that lookups can also be queried directly using the [`lookup` schema](#from).|
|`LOWER(expr)`|Returns expr in all lowercase.|
|`PARSE_LONG(string[, radix])`|Parses a string into a long (BIGINT) with the given radix, or 10 (decimal) if a radix is not provided.|
|`POSITION(needle IN haystack [FROM fromIndex])`|Returns the index of needle within haystack, with indexes starting from 1. The search will begin at fromIndex, or 1 if fromIndex is not specified. If the needle is not found, returns 0.|
|`REGEXP_EXTRACT(expr, pattern, [index])`|Apply regular expression `pattern` to `expr` and extract a capture group, or `NULL` if there is no match. If index is unspecified or zero, returns the first substring that matched the pattern. The pattern may match anywhere inside `expr`; if you want to match the entire string instead, use the `^` and `$` markers at the start and end of your pattern. Note: when `druid.generic.useDefaultValueForNull = true`, it is not possible to differentiate an empty-string match from a non-match (both will return `NULL`).|
|`REGEXP_LIKE(expr, pattern)`|Returns whether `expr` matches regular expression `pattern`. The pattern may match anywhere inside `expr`; if you want to match the entire string instead, use the `^` and `$` markers at the start and end of your pattern. Similar to [`LIKE`](#comparison-operators), but uses regexps instead of LIKE patterns. Especially useful in WHERE clauses.|
|`REPLACE(expr, pattern, replacement)`|Replaces pattern with replacement in expr, and returns the result.|
|`STRPOS(haystack, needle)`|Returns the index of needle within haystack, with indexes starting from 1. If the needle is not found, returns 0.|
|`SUBSTRING(expr, index, [length])`|Returns a substring of expr starting at index, with a max length, both measured in UTF-16 code units.|
|`RIGHT(expr, [length])`|Returns the rightmost length characters from expr.|
|`LEFT(expr, [length])`|Returns the leftmost length characters from expr.|
|`SUBSTR(expr, index, [length])`|Synonym for SUBSTRING.|
|<code>TRIM([BOTH &#124; LEADING &#124; TRAILING] [<chars> FROM] expr)</code>|Returns expr with characters removed from the leading, trailing, or both ends of "expr" if they are in "chars". If "chars" is not provided, it defaults to " " (a space). If the directional argument is not provided, it defaults to "BOTH".|
|`BTRIM(expr[, chars])`|Alternate form of `TRIM(BOTH <chars> FROM <expr>)`.|
|`LTRIM(expr[, chars])`|Alternate form of `TRIM(LEADING <chars> FROM <expr>)`.|
|`RTRIM(expr[, chars])`|Alternate form of `TRIM(TRAILING <chars> FROM <expr>)`.|
|`UPPER(expr)`|Returns expr in all uppercase.|
|`REVERSE(expr)`|Reverses expr.|
|`REPEAT(expr, [N])`|Repeats expr N times|
|`LPAD(expr, length[, chars])`|Returns a string of `length` from `expr` left-padded with `chars`. If `length` is shorter than the length of `expr`, the result is `expr` which is truncated to `length`. The result will be null if either `expr` or `chars` is null. If `chars` is an empty string, no padding is added, however `expr` may be trimmed if necessary.|
|`RPAD(expr, length[, chars])`|Returns a string of `length` from `expr` right-padded with `chars`. If `length` is shorter than the length of `expr`, the result is `expr` which is truncated to `length`. The result will be null if either `expr` or `chars` is null. If `chars` is an empty string, no padding is added, however `expr` may be trimmed if necessary.|


### Time functions

Time functions can be used with Druid's `__time` column, with any column storing millisecond timestamps through use
of the `MILLIS_TO_TIMESTAMP` function, or with any column storing string timestamps through use of the `TIME_PARSE`
function. By default, time operations use the UTC time zone. You can change the time zone by setting the connection
context parameter "sqlTimeZone" to the name of another time zone, like "America/Los_Angeles", or to an offset like
"-08:00". If you need to mix multiple time zones in the same query, or if you need to use a time zone other than
the connection time zone, some functions also accept time zones as parameters. These parameters always take precedence
over the connection time zone.

Literal timestamps in the connection time zone can be written using `TIMESTAMP '2000-01-01 00:00:00'` syntax. The
simplest way to write literal timestamps in other time zones is to use TIME_PARSE, like
`TIME_PARSE('2000-02-01 00:00:00', NULL, 'America/Los_Angeles')`.

|Function|Notes|
|--------|-----|
|`CURRENT_TIMESTAMP`|Current timestamp in the connection's time zone.|
|`CURRENT_DATE`|Current date in the connection's time zone.|
|`DATE_TRUNC(<unit>, <timestamp_expr>)`|Rounds down a timestamp, returning it as a new timestamp. Unit can be 'milliseconds', 'second', 'minute', 'hour', 'day', 'week', 'month', 'quarter', 'year', 'decade', 'century', or 'millennium'.|
|`TIME_CEIL(<timestamp_expr>, <period>, [<origin>, [<timezone>]])`|Rounds up a timestamp, returning it as a new timestamp. Period can be any ISO8601 period, like P3M (quarters) or PT12H (half-days). The time zone, if provided, should be a time zone name like "America/Los_Angeles" or offset like "-08:00". This function is similar to `CEIL` but is more flexible.|
|`TIME_FLOOR(<timestamp_expr>, <period>, [<origin>, [<timezone>]])`|Rounds down a timestamp, returning it as a new timestamp. Period can be any ISO8601 period, like P3M (quarters) or PT12H (half-days). The time zone, if provided, should be a time zone name like "America/Los_Angeles" or offset like "-08:00". This function is similar to `FLOOR` but is more flexible.|
|`TIME_SHIFT(<timestamp_expr>, <period>, <step>, [<timezone>])`|Shifts a timestamp by a period (step times), returning it as a new timestamp. Period can be any ISO8601 period. Step may be negative. The time zone, if provided, should be a time zone name like "America/Los_Angeles" or offset like "-08:00".|
|`TIME_EXTRACT(<timestamp_expr>, [<unit>, [<timezone>]])`|Extracts a time part from expr, returning it as a number. Unit can be EPOCH, SECOND, MINUTE, HOUR, DAY (day of month), DOW (day of week), DOY (day of year), WEEK (week of [week year](https://en.wikipedia.org/wiki/ISO_week_date)), MONTH (1 through 12), QUARTER (1 through 4), or YEAR. The time zone, if provided, should be a time zone name like "America/Los_Angeles" or offset like "-08:00". This function is similar to `EXTRACT` but is more flexible. Unit and time zone must be literals, and must be provided quoted, like `TIME_EXTRACT(__time, 'HOUR')` or `TIME_EXTRACT(__time, 'HOUR', 'America/Los_Angeles')`.|
|`TIME_PARSE(<string_expr>, [<pattern>, [<timezone>]])`|Parses a string into a timestamp using a given [Joda DateTimeFormat pattern](http://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html), or ISO8601 (e.g. `2000-01-02T03:04:05Z`) if the pattern is not provided. The time zone, if provided, should be a time zone name like "America/Los_Angeles" or offset like "-08:00", and will be used as the time zone for strings that do not include a time zone offset. Pattern and time zone must be literals. Strings that cannot be parsed as timestamps will be returned as NULL.|
|`TIME_FORMAT(<timestamp_expr>, [<pattern>, [<timezone>]])`|Formats a timestamp as a string with a given [Joda DateTimeFormat pattern](http://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html), or ISO8601 (e.g. `2000-01-02T03:04:05Z`) if the pattern is not provided. The time zone, if provided, should be a time zone name like "America/Los_Angeles" or offset like "-08:00". Pattern and time zone must be literals.|
|`MILLIS_TO_TIMESTAMP(millis_expr)`|Converts a number of milliseconds since the epoch into a timestamp.|
|`TIMESTAMP_TO_MILLIS(timestamp_expr)`|Converts a timestamp into a number of milliseconds since the epoch.|
|`EXTRACT(<unit> FROM timestamp_expr)`|Extracts a time part from expr, returning it as a number. Unit can be EPOCH, MICROSECOND, MILLISECOND, SECOND, MINUTE, HOUR, DAY (day of month), DOW (day of week), ISODOW (ISO day of week), DOY (day of year), WEEK (week of year), MONTH, QUARTER, YEAR, ISOYEAR, DECADE, CENTURY or MILLENNIUM. Units must be provided unquoted, like `EXTRACT(HOUR FROM __time)`.|
|`FLOOR(timestamp_expr TO <unit>)`|Rounds down a timestamp, returning it as a new timestamp. Unit can be SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, or YEAR.|
|`CEIL(timestamp_expr TO <unit>)`|Rounds up a timestamp, returning it as a new timestamp. Unit can be SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, or YEAR.|
|`TIMESTAMPADD(<unit>, <count>, <timestamp>)`|Equivalent to `timestamp + count * INTERVAL '1' UNIT`.|
|`TIMESTAMPDIFF(<unit>, <timestamp1>, <timestamp2>)`|Returns the (signed) number of `unit` between `timestamp1` and `timestamp2`. Unit can be SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, or YEAR.|
|<code>timestamp_expr { + &#124; - } <interval_expr><code>|Add or subtract an amount of time from a timestamp. interval_expr can include interval literals like `INTERVAL '2' HOUR`, and may include interval arithmetic as well. This operator treats days as uniformly 86400 seconds long, and does not take into account daylight savings time. To account for daylight savings time, use TIME_SHIFT instead.|


### Reduction functions

Reduction functions operate on zero or more expressions and return a single expression. If no expressions are passed as
arguments, then the result is `NULL`. The expressions must all be convertible to a common data type, which will be the
type of the result:
*  If all argument are `NULL`, the result is `NULL`. Otherwise, `NULL` arguments are ignored.
*  If the arguments comprise a mix of numbers and strings, the arguments are interpreted as strings.
*  If all arguments are integer numbers, the arguments are interpreted as longs.
*  If all arguments are numbers and at least one argument is a double, the arguments are interpreted as doubles. 

|Function|Notes|
|--------|-----|
|`GREATEST([expr1, ...])`|Evaluates zero or more expressions and returns the maximum value based on comparisons as described above.|
|`LEAST([expr1, ...])`|Evaluates zero or more expressions and returns the minimum value based on comparisons as described above.|


### IP address functions

For the IPv4 address functions, the `address` argument can either be an IPv4 dotted-decimal string
(e.g., '192.168.0.1') or an IP address represented as an integer (e.g., 3232235521). The `subnet`
argument should be a string formatted as an IPv4 address subnet in CIDR notation (e.g.,
'192.168.0.0/16').

|Function|Notes|
|---|---|
|`IPV4_MATCH(address, subnet)`|Returns true if the `address` belongs to the `subnet` literal, else false. If `address` is not a valid IPv4 address, then false is returned. This function is more efficient if `address` is an integer instead of a string.|
|`IPV4_PARSE(address)`|Parses `address` into an IPv4 address stored as an integer . If `address` is an integer that is a valid IPv4 address, then it is passed through. Returns null if `address` cannot be represented as an IPv4 address.|
|`IPV4_STRINGIFY(address)`|Converts `address` into an IPv4 address dotted-decimal string. If `address` is a string that is a valid IPv4 address, then it is passed through. Returns null if `address` cannot be represented as an IPv4 address.|


### Comparison operators

|Function|Notes|
|--------|-----|
|`x = y`|Equals.|
|`x <> y`|Not-equals.|
|`x > y`|Greater than.|
|`x >= y`|Greater than or equal to.|
|`x < y`|Less than.|
|`x <= y`|Less than or equal to.|
|`x BETWEEN y AND z`|Equivalent to `x >= y AND x <= z`.|
|`x NOT BETWEEN y AND z`|Equivalent to `x < y OR x > z`.|
|`x LIKE pattern [ESCAPE esc]`|True if x matches a SQL LIKE pattern (with an optional escape).|
|`x NOT LIKE pattern [ESCAPE esc]`|True if x does not match a SQL LIKE pattern (with an optional escape).|
|`x IS NULL`|True if x is NULL or empty string.|
|`x IS NOT NULL`|True if x is neither NULL nor empty string.|
|`x IS TRUE`|True if x is true.|
|`x IS NOT TRUE`|True if x is not true.|
|`x IS FALSE`|True if x is false.|
|`x IS NOT FALSE`|True if x is not false.|
|`x IN (values)`|True if x is one of the listed values.|
|`x NOT IN (values)`|True if x is not one of the listed values.|
|`x IN (subquery)`|True if x is returned by the subquery. This will be translated into a join; see [Query translation](#query-translation) for details.|
|`x NOT IN (subquery)`|True if x is not returned by the subquery. This will be translated into a join; see [Query translation](#query-translation) for details.|
|`x AND y`|Boolean AND.|
|`x OR y`|Boolean OR.|
|`NOT x`|Boolean NOT.|

### Sketch functions

These functions operate on expressions or columns that return sketch objects.

#### HLL sketch functions

The following functions operate on [DataSketches HLL sketches](../development/extensions-core/datasketches-hll.html).
The [DataSketches extension](../development/extensions-core/datasketches-extension.html) must be loaded to use the following functions.

|Function|Notes|
|--------|-----|
|`HLL_SKETCH_ESTIMATE(expr, [round])`|Returns the distinct count estimate from an HLL sketch. `expr` must return an HLL sketch. The optional `round` boolean parameter will round the estimate if set to `true`, with a default of `false`.|
|`HLL_SKETCH_ESTIMATE_WITH_ERROR_BOUNDS(expr, [numStdDev])`|Returns the distinct count estimate and error bounds from an HLL sketch. `expr` must return an HLL sketch. An optional `numStdDev` argument can be provided.|
|`HLL_SKETCH_UNION([lgK, tgtHllType], expr0, expr1, ...)`|Returns a union of HLL sketches, where each input expression must return an HLL sketch. The `lgK` and `tgtHllType` can be optionally specified as the first parameter; if provided, both optional parameters must be specified.|
|`HLL_SKETCH_TO_STRING(expr)`|Returns a human-readable string representation of an HLL sketch for debugging. `expr` must return an HLL sketch.|

#### Theta sketch functions

The following functions operate on [theta sketches](../development/extensions-core/datasketches-theta.html).
The [DataSketches extension](../development/extensions-core/datasketches-extension.html) must be loaded to use the following functions.

|Function|Notes|
|--------|-----|
|`THETA_SKETCH_ESTIMATE(expr)`|Returns the distinct count estimate from a theta sketch. `expr` must return a theta sketch.|
|`THETA_SKETCH_ESTIMATE_WITH_ERROR_BOUNDS(expr, errorBoundsStdDev)`|Returns the distinct count estimate and error bounds from a theta sketch. `expr` must return a theta sketch.|
|`THETA_SKETCH_UNION([size], expr0, expr1, ...)`|Returns a union of theta sketches, where each input expression must return a theta sketch. The `size` can be optionally specified as the first parameter.|
|`THETA_SKETCH_INTERSECT([size], expr0, expr1, ...)`|Returns an intersection of theta sketches, where each input expression must return a theta sketch. The `size` can be optionally specified as the first parameter.|
|`THETA_SKETCH_NOT([size], expr0, expr1, ...)`|Returns a set difference of theta sketches, where each input expression must return a theta sketch. The `size` can be optionally specified as the first parameter.|

#### Quantiles sketch functions

The following functions operate on [quantiles sketches](../development/extensions-core/datasketches-quantiles.html).
The [DataSketches extension](../development/extensions-core/datasketches-extension.html) must be loaded to use the following functions.

|Function|Notes|
|--------|-----|
|`DS_GET_QUANTILE(expr, fraction)`|Returns the quantile estimate corresponding to `fraction` from a quantiles sketch. `expr` must return a quantiles sketch.|
|`DS_GET_QUANTILES(expr, fraction0, fraction1, ...)`|Returns a string representing an array of quantile estimates corresponding to a list of fractions from a quantiles sketch. `expr` must return a quantiles sketch.|
|`DS_HISTOGRAM(expr, splitPoint0, splitPoint1, ...)`|Returns a string representing an approximation to the histogram given a list of split points that define the histogram bins from a quantiles sketch. `expr` must return a quantiles sketch.|
|`DS_CDF(expr, splitPoint0, splitPoint1, ...)`|Returns a string representing approximation to the Cumulative Distribution Function given a list of split points that define the edges of the bins from a quantiles sketch. `expr` must return a quantiles sketch.|
|`DS_RANK(expr, value)`|Returns an approximation to the rank of a given value that is the fraction of the distribution less than that value from a quantiles sketch. `expr` must return a quantiles sketch.|
|`DS_QUANTILE_SUMMARY(expr)`|Returns a string summary of a quantiles sketch, useful for debugging. `expr` must return a quantiles sketch.|

### Other scalar functions

|Function|Notes|
|--------|-----|
|`CAST(value AS TYPE)`|Cast value to another type. See [Data types](#data-types) for details about how Druid SQL handles CAST.|
|`CASE expr WHEN value1 THEN result1 \[ WHEN value2 THEN result2 ... \] \[ ELSE resultN \] END`|Simple CASE.|
|`CASE WHEN boolean_expr1 THEN result1 \[ WHEN boolean_expr2 THEN result2 ... \] \[ ELSE resultN \] END`|Searched CASE.|
|`NULLIF(value1, value2)`|Returns NULL if value1 and value2 match, else returns value1.|
|`COALESCE(value1, value2, ...)`|Returns the first value that is neither NULL nor empty string.|
|`NVL(expr,expr-for-null)`|Returns 'expr-for-null' if 'expr' is null (or empty string for string type).|
|`BLOOM_FILTER_TEST(<expr>, <serialized-filter>)`|Returns true if the value is contained in a Base64-serialized bloom filter. See the [Bloom filter extension](../development/extensions-core/bloom-filter.html) documentation for additional details.|

## Multi-value string functions

All 'array' references in the multi-value string function documentation can refer to multi-value string columns or
`ARRAY` literals.

|Function|Notes|
|--------|-----|
| `ARRAY(expr1,expr ...)` | constructs a SQL ARRAY literal from the expression arguments, using the type of the first argument as the output array type |
| `MV_LENGTH(arr)` | returns length of array expression |
| `MV_OFFSET(arr,long)` | returns the array element at the 0 based index supplied, or null for an out of range index|
| `MV_ORDINAL(arr,long)` | returns the array element at the 1 based index supplied, or null for an out of range index |
| `MV_CONTAINS(arr,expr)` | returns 1 if the array contains the element specified by expr, or contains all elements specified by expr if expr is an array, else 0 |
| `MV_OVERLAP(arr1,arr2)` | returns 1 if arr1 and arr2 have any elements in common, else 0 |
| `MV_OFFSET_OF(arr,expr)` | returns the 0 based index of the first occurrence of expr in the array, or `-1` or `null` if `druid.generic.useDefaultValueForNull=false` if no matching elements exist in the array. |
| `MV_ORDINAL_OF(arr,expr)` | returns the 1 based index of the first occurrence of expr in the array, or `-1` or `null` if `druid.generic.useDefaultValueForNull=false` if no matching elements exist in the array. |
| `MV_PREPEND(expr,arr)` | adds expr to arr at the beginning, the resulting array type determined by the type of the array |
| `MV_APPEND(arr1,expr)` | appends expr to arr, the resulting array type determined by the type of the first array |
| `MV_CONCAT(arr1,arr2)` | concatenates 2 arrays, the resulting array type determined by the type of the first array |
| `MV_SLICE(arr,start,end)` | return the subarray of arr from the 0 based index start(inclusive) to end(exclusive), or `null`, if start is less than 0, greater than length of arr or less than end|
| `MV_TO_STRING(arr,str)` | joins all elements of arr by the delimiter specified by str |
| `STRING_TO_MV(str1,str2)` | splits str1 into an array on the delimiter specified by str2 |

## Query translation

Druid SQL translates SQL queries to [native queries](querying.md) before running them, and understanding how this
translation works is key to getting good performance.

### Best practices

Consider this (non-exhaustive) list of things to look out for when looking into the performance implications of
how your SQL queries are translated to native queries.

1. If you wrote a filter on the primary time column `__time`, make sure it is being correctly translated to an
`"intervals"` filter, as described in the [Time filters](#time-filters) section below. If not, you may need to change
the way you write the filter.

2. Try to avoid subqueries underneath joins: they affect both performance and scalability. This includes implicit
subqueries generated by conditions on mismatched types, and implicit subqueries generated by conditions that use
expressions to refer to the right-hand side.

3. Currently, Druid does not support pushing down predicates (condition and filter) past a Join (i.e. into 
Join's children). Druid only supports pushing predicates into the join if they originated from 
above the join. Hence, the location of predicates and filters in your Druid SQL is very important. 
Also, as a result of this, comma joins should be avoided.

4. Read through the [Query execution](query-execution.md) page to understand how various types of native queries
will be executed.

5. Be careful when interpreting EXPLAIN PLAN output, and use request logging if in doubt. Request logs will show the
exact native query that was run. See the [next section](#interpreting-explain-plan-output) for more details.

6. If you encounter a query that could be planned better, feel free to
[raise an issue on GitHub](https://github.com/apache/druid/issues/new/choose). A reproducible test case is always
appreciated.

### Interpreting EXPLAIN PLAN output

The [EXPLAIN PLAN](#explain-plan) functionality can help you understand how a given SQL query will
be translated to native. For simple queries that do not involve subqueries or joins, the output of EXPLAIN PLAN
is easy to interpret. The native query that will run is embedded as JSON inside a "DruidQueryRel" line:

```
> EXPLAIN PLAN FOR SELECT COUNT(*) FROM wikipedia

DruidQueryRel(query=[{"queryType":"timeseries","dataSource":"wikipedia","intervals":"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z","granularity":"all","aggregations":[{"type":"count","name":"a0"}]}], signature=[{a0:LONG}])
```

For more complex queries that do involve subqueries or joins, EXPLAIN PLAN is somewhat more difficult to interpret.
For example, consider this query:

```
> EXPLAIN PLAN FOR
> SELECT
>     channel,
>     COUNT(*)
> FROM wikipedia
> WHERE channel IN (SELECT page FROM wikipedia GROUP BY page ORDER BY COUNT(*) DESC LIMIT 10)
> GROUP BY channel

DruidJoinQueryRel(condition=[=($1, $3)], joinType=[inner], query=[{"queryType":"groupBy","dataSource":{"type":"table","name":"__join__"},"intervals":{"type":"intervals","intervals":["-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z"]},"granularity":"all","dimensions":["channel"],"aggregations":[{"type":"count","name":"a0"}]}], signature=[{d0:STRING, a0:LONG}])
  DruidQueryRel(query=[{"queryType":"scan","dataSource":{"type":"table","name":"wikipedia"},"intervals":{"type":"intervals","intervals":["-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z"]},"resultFormat":"compactedList","columns":["__time","channel","page"],"granularity":"all"}], signature=[{__time:LONG, channel:STRING, page:STRING}])
  DruidQueryRel(query=[{"queryType":"topN","dataSource":{"type":"table","name":"wikipedia"},"dimension":"page","metric":{"type":"numeric","metric":"a0"},"threshold":10,"intervals":{"type":"intervals","intervals":["-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z"]},"granularity":"all","aggregations":[{"type":"count","name":"a0"}]}], signature=[{d0:STRING}])
```

Here, there is a join with two inputs. The way to read this is to consider each line of the EXPLAIN PLAN output as
something that might become a query, or might just become a simple datasource. The `query` field they all have is
called a "partial query" and represents what query would be run on the datasource represented by that line, if that
line ran by itself. In some cases — like the "scan" query in the second line of this example — the query does not
actually run, and it ends up being translated to a simple table datasource. See the [Join translation](#joins) section
for more details about how this works.

We can see this for ourselves using Druid's [request logging](../configuration/index.md#request-logging) feature. After
enabling logging and running this query, we can see that it actually runs as the following native query.

```json
{
  "queryType": "groupBy",
  "dataSource": {
    "type": "join",
    "left": "wikipedia",
    "right": {
      "type": "query",
      "query": {
        "queryType": "topN",
        "dataSource": "wikipedia",
        "dimension": {"type": "default", "dimension": "page", "outputName": "d0"},
        "metric": {"type": "numeric", "metric": "a0"},
        "threshold": 10,
        "intervals": "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z",
        "granularity": "all",
        "aggregations": [
          { "type": "count", "name": "a0"}
        ]
      }
    },
    "rightPrefix": "j0.",
    "condition": "(\"page\" == \"j0.d0\")",
    "joinType": "INNER"
  },
  "intervals": "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z",
  "granularity": "all",
  "dimensions": [
    {"type": "default", "dimension": "channel", "outputName": "d0"}
  ],
  "aggregations": [
    { "type": "count", "name": "a0"}
  ]
}
```

### Query types

Druid SQL uses four different native query types.

- [Scan](scan-query.html) is used for queries that do not aggregate (no GROUP BY, no DISTINCT).

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

### Time filters

For all native query types, filters on the `__time` column will be translated into top-level query "intervals" whenever
possible, which allows Druid to use its global time index to quickly prune the set of data that must be scanned.
Consider this (non-exhaustive) list of time filters that will be recognized and translated to "intervals":

- `__time >= TIMESTAMP '2000-01-01 00:00:00'` (comparison to absolute time)
- `__time >= CURRENT_TIMESTAMP - INTERVAL '8' HOUR` (comparison to relative time)
- `FLOOR(__time TO DAY) = TIMESTAMP '2000-01-01 00:00:00'` (specific day)

Refer to the [Interpreting EXPLAIN PLAN output](#interpreting-explain-plan-output) section for details on confirming
that time filters are being translated as you expect.

### Joins

SQL join operators are translated to native join datasources as follows:

1. Joins that the native layer can handle directly are translated literally, to a [join datasource](datasource.md#join)
whose `left`, `right`, and `condition` are faithful translations of the original SQL. This includes any SQL join where
the right-hand side is a lookup or subquery, and where the condition is an equality where one side is an expression based
on the left-hand table, the other side is a simple column reference to the right-hand table, and both sides of the
equality are the same data type.

2. If a join cannot be handled directly by a native [join datasource](datasource.md#join) as written, Druid SQL
will insert subqueries to make it runnable. For example, `foo INNER JOIN bar ON foo.abc = LOWER(bar.def)` cannot be
directly translated, because there is an expression on the right-hand side instead of a simple column access. A subquery
will be inserted that effectively transforms this clause to
`foo INNER JOIN (SELECT LOWER(def) AS def FROM bar) t ON foo.abc = t.def`.

3. Druid SQL does not currently reorder joins to optimize queries.

Refer to the [Interpreting EXPLAIN PLAN output](#interpreting-explain-plan-output) section for details on confirming
that joins are being translated as you expect.

Refer to the [Query execution](query-execution.md#join) page for information about how joins are executed.

### Subqueries

Subqueries in SQL are generally translated to native query datasources. Refer to the
[Query execution](query-execution.md#query) page for information about how subqueries are executed.

> Note: Subqueries in the WHERE clause, like `WHERE col1 IN (SELECT foo FROM ...)` are translated to inner joins.

### Approximations

Druid SQL will use approximate algorithms in some situations:

- The `COUNT(DISTINCT col)` aggregation functions by default uses a variant of
[HyperLogLog](http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf), a fast approximate distinct counting
algorithm. Druid SQL will switch to exact distinct counts if you set "useApproximateCountDistinct" to "false", either
through query context or through Broker configuration.

- GROUP BY queries over a single column with ORDER BY and LIMIT may be executed using the TopN engine, which uses an
approximate algorithm. Druid SQL will switch to an exact grouping algorithm if you set "useApproximateTopN" to "false",
either through query context or through Broker configuration.

- Aggregation functions that are labeled as using sketches or approximations, such as APPROX_COUNT_DISTINCT, are always
approximate, regardless of configuration.

### Unsupported features

Druid does not support all SQL features. In particular, the following features are not supported.

- JOIN between native datasources (table, lookup, subquery) and system tables.
- JOIN conditions that are not an equality between expressions from the left- and right-hand sides.
- JOIN conditions containing a constant value inside the condition.
- JOIN conditions on a column which contains a multi-value dimension.
- OVER clauses, and analytic functions such as `LAG` and `LEAD`.
- OFFSET clauses.
- DDL and DML.
- Using Druid-specific functions like `TIME_PARSE` and `APPROX_QUANTILE_DS` on [metadata tables](#metadata-tables).

Additionally, some Druid native query features are not supported by the SQL language. Some unsupported Druid features
include:

- [Union datasources](datasource.html#union)
- [Inline datasources](datasource.html#inline)
- [Spatial filters](../development/geo.html).
- [Query cancellation](querying.html#query-cancellation).

## Client APIs

<a name="json-over-http"></a>

### HTTP POST

You can make Druid SQL queries using HTTP via POST to the endpoint `/druid/v2/sql/`. The request should
be a JSON object with a "query" field, like `{"query" : "SELECT COUNT(*) FROM data_source WHERE foo = 'bar'"}`.

##### Request
      
|Property|Description|Default|
|--------|----|-----------|
|`query`|SQL query string.| none (required)|
|`resultFormat`|Format of query results. See [Responses](#responses) for details.|`"object"`|
|`header`|Whether or not to include a header. See [Responses] for details.|`false`|
|`context`|JSON object containing [connection context parameters](#connection-context).|`{}` (empty)|
|`parameters`|List of query parameters for parameterized queries. Each parameter in the list should be a JSON object like `{"type": "VARCHAR", "value": "foo"}`. The type should be a SQL type; see [Data types](#data-types) for a list of supported SQL types.|`[]` (empty)|

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

Metadata is available over HTTP POST by querying [metadata tables](#metadata-tables).

#### Responses

Druid SQL's HTTP POST API supports a variety of result formats. You can specify these by adding a "resultFormat"
parameter, like:

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

Note that the non-JDBC [JSON over HTTP](#http-post) API is stateless and does not require stickiness.

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

> Note: INFORMATION_SCHEMA tables do not currently support Druid-specific functions like `TIME_PARSE` and
> `APPROX_QUANTILE_DS`. Only standard SQL functions can be used.

#### SCHEMATA table
`INFORMATION_SCHEMA.SCHEMATA` provides a list of all known schemas, which include `druid` for standard [Druid Table datasources](datasource.md#table), `lookup` for [Lookups](datasource.md#lookup), `sys` for the virtual [System metadata tables](#system-schema), and `INFORMATION_SCHEMA` for these virtual tables. Tables are allowed to have the same name across different schemas, so the schema may be included in an SQL statement to distinguish them, e.g. `lookup.table` vs `druid.table`.

|Column|Notes|
|------|-----|
|CATALOG_NAME|Always set as `druid`|
|SCHEMA_NAME|`druid`, `lookup`, `sys`, or `INFORMATION_SCHEMA`|
|SCHEMA_OWNER|Unused|
|DEFAULT_CHARACTER_SET_CATALOG|Unused|
|DEFAULT_CHARACTER_SET_SCHEMA|Unused|
|DEFAULT_CHARACTER_SET_NAME|Unused|
|SQL_PATH|Unused|

#### TABLES table
`INFORMATION_SCHEMA.TABLES` provides a list of all known tables and schemas.

|Column|Notes|
|------|-----|
|TABLE_CATALOG|Always set as `druid`|
|TABLE_SCHEMA|The 'schema' which the table falls under, see [SCHEMATA table for details](#schemata-table)|
|TABLE_NAME|Table name. For the `druid` schema, this is the `dataSource`.|
|TABLE_TYPE|"TABLE" or "SYSTEM_TABLE"|
|IS_JOINABLE|If a table is directly joinable if on the right hand side of a `JOIN` statement, without performing a subquery, this value will be set to `YES`, otherwise `NO`. Lookups are always joinable because they are globally distributed among Druid query processing nodes, but Druid datasources are not, and will use a less efficient subquery join.|
|IS_BROADCAST|If a table is 'broadcast' and distributed among all Druid query processing nodes, this value will be set to `YES`, such as lookups and Druid datasources which have a 'broadcast' load rule, else `NO`.|

#### COLUMNS table
`INFORMATION_SCHEMA.COLUMNS` provides a list of all known columns across all tables and schema.

|Column|Notes|
|------|-----|
|TABLE_CATALOG|Always set as `druid`|
|TABLE_SCHEMA|The 'schema' which the table column falls under, see [SCHEMATA table for details](#schemata-table)|
|TABLE_NAME|The 'table' which the column belongs to, see [TABLES table for details](#tables-table)|
|COLUMN_NAME|The column name|
|ORDINAL_POSITION|The order in which the column is stored in a table|
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

> Note: "sys" tables do not currently support Druid-specific functions like `TIME_PARSE` and
> `APPROX_QUANTILE_DS`. Only standard SQL functions can be used.

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
|shardSpec|STRING|The toString of specific `ShardSpec`|
|dimensions|STRING|The dimensions of the segment|
|metrics|STRING|The metrics of the segment|

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

## Server configuration

Druid SQL planning occurs on the Broker and is configured by
[Broker runtime properties](../configuration/index.html#sql).

## Security

Please see [Defining SQL permissions](../development/extensions-core/druid-basic-security.html#sql-permissions) in the
basic security documentation for information on what permissions are needed for making SQL queries.
