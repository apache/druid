---
layout: doc_page
title: "SQL"
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

# SQL

<div class="note caution">
Built-in SQL is an <a href="../development/experimental.html">experimental</a> feature. The API described here is
subject to change.
</div>

Druid SQL is a built-in SQL layer and an alternative to Druid's native JSON-based query language, and is powered by a
parser and planner based on [Apache Calcite](https://calcite.apache.org/). Druid SQL translates SQL into native Druid
queries on the query Broker (the first node you query), which are then passed down to data nodes as native Druid
queries. Other than the (slight) overhead of translating SQL on the Broker, there isn't an additional performance
penalty versus native queries.

To enable Druid SQL, make sure you have set `druid.sql.enable = true` either in your common.runtime.properties or your
Broker's runtime.properties.

## Query syntax

Each Druid datasource appears as a table in the "druid" schema. This is also the default schema, so Druid datasources
can be referenced as either `druid.dataSourceName` or simply `dataSourceName`.

Identifiers like datasource and column names can optionally be quoted using double quotes. To escape a double quote
inside an identifier, use another double quote, like `"My ""very own"" identifier"`. All identifiers are case-sensitive
and no implicit case conversions are performed.

Literal strings should be quoted with single quotes, like `'foo'`. Literal strings with Unicode escapes can be written
like `U&'fo\00F6'`, where character codes in hex are prefixed by a backslash. Literal numbers can be written in forms
like `100` (denoting an integer), `100.0` (denoting a floating point value), or `1.0e5` (scientific notation). Literal
timestamps can be written like `TIMESTAMP '2000-01-01 00:00:00'`. Literal intervals, used for time arithmetic, can be
written like `INTERVAL '1' HOUR`, `INTERVAL '1 02:03' DAY TO MINUTE`, `INTERVAL '1-2' YEAR TO MONTH`, and so on.

Druid SQL supports SELECT queries with the following structure:

```
[ EXPLAIN PLAN FOR ]
[ WITH tableName [ ( column1, column2, ... ) ] AS ( query ) ]
SELECT [ ALL | DISTINCT ] { * | exprs }
FROM table
[ WHERE expr ]
[ GROUP BY exprs ]
[ HAVING expr ]
[ ORDER BY expr [ ASC | DESC ], expr [ ASC | DESC ], ... ]
[ LIMIT limit ]
[ UNION ALL <another query> ]
```

The FROM clause refers to either a Druid datasource, like `druid.foo`, an [INFORMATION_SCHEMA table](#retrieving-metadata), a
subquery, or a common-table-expression provided in the WITH clause. If the FROM clause references a subquery or a
common-table-expression, and both levels of queries are aggregations and they cannot be combined into a single level of
aggregation, the overall query will be executed as a [nested GroupBy](groupbyquery.html#nested-groupbys).

The WHERE clause refers to columns in the FROM table, and will be translated to [native filters](filters.html). The
WHERE clause can also reference a subquery, like `WHERE col1 IN (SELECT foo FROM ...)`. Queries like this are executed
as [semi-joins](#query-execution), described below.

The GROUP BY clause refers to columns in the FROM table. Using GROUP BY, DISTINCT, or any aggregation functions will
trigger an aggregation query using one of Druid's [three native aggregation query types](#query-execution). GROUP BY
can refer to an expression or a select clause ordinal position (like `GROUP BY 2` to group by the second selected
column).

The HAVING clause refers to columns that are present after execution of GROUP BY. It can be used to filter on either
grouping expressions or aggregated values. It can only be used together with GROUP BY.

The ORDER BY clause refers to columns that are present after execution of GROUP BY. It can be used to order the results
based on either grouping expressions or aggregated values. ORDER BY can refer to an expression or a select clause
ordinal position (like `ORDER BY 2` to order by the second selected column). For non-aggregation queries, ORDER BY
can only order by the `__time` column. For aggregation queries, ORDER BY can order by any column.

The LIMIT clause can be used to limit the number of rows returned. It can be used with any query type. It is pushed down
to data nodes for queries that run with the native TopN query type, but not the native GroupBy query type. Future
versions of Druid will support pushing down limits using the native GroupBy query type as well. If you notice that
adding a limit doesn't change performance very much, then it's likely that Druid didn't push down the limit for your
query.

The "UNION ALL" operator can be used to fuse multiple queries together. Their results will be concatenated, and each
query will run separately, back to back (not in parallel). Druid does not currently support "UNION" without "ALL".

Add "EXPLAIN PLAN FOR" to the beginning of any query to see how it would be run as a native Druid query. In this case,
the query will not actually be executed.

### Aggregation functions

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
|`APPROX_COUNT_DISTINCT(expr)`|Counts distinct values of expr, which can be a regular column or a hyperUnique column. This is always approximate, regardless of the value of "useApproximateCountDistinct". See also `COUNT(DISTINCT expr)`.|
|`APPROX_QUANTILE(expr, probability, [resolution])`|Computes approximate quantiles on numeric or approxHistogram exprs. The "probability" should be between 0 and 1 (exclusive). The "resolution" is the number of centroids to use for the computation. Higher resolutions will give more precise results but also have higher overhead. If not provided, the default resolution is 50. The [approximate histogram extension](../development/extensions-core/approximate-histograms.html) must be loaded to use this function.|
|`BLOOM_FILTER(expr, numEntries)`|Computes a bloom filter from values produced by `expr`, with `numEntries` maximum number of distinct values before false positve rate increases. See [bloom filter extension](../development/extensions-core/bloom-filter.html) documentation for additional details.|

### Numeric functions

Numeric functions will return 64 bit integers or 64 bit floats, depending on their inputs.

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
|`x + y`|Addition.|
|`x - y`|Subtraction.|
|`x * y`|Multiplication.|
|`x / y`|Division.|
|`MOD(x, y)`|Modulo (remainder of x divided by y).|

### String functions

String functions accept strings, and return a type appropriate to the function.

|Function|Notes|
|--------|-----|
|`x \|\| y`|Concat strings x and y.|
|`CONCAT(expr, expr...)`|Concats a list of expressions.|
|`TEXTCAT(expr, expr)`|Two argument version of CONCAT.|
|`LENGTH(expr)`|Length of expr in UTF-16 code units.|
|`CHAR_LENGTH(expr)`|Synonym for `LENGTH`.|
|`CHARACTER_LENGTH(expr)`|Synonym for `LENGTH`.|
|`STRLEN(expr)`|Synonym for `LENGTH`.|
|`LOOKUP(expr, lookupName)`|Look up expr in a registered [query-time lookup table](lookups.html).|
|`LOWER(expr)`|Returns expr in all lowercase.|
|`POSITION(needle IN haystack [FROM fromIndex])`|Returns the index of needle within haystack, with indexes starting from 1. The search will begin at fromIndex, or 1 if fromIndex is not specified. If the needle is not found, returns 0.|
|`REGEXP_EXTRACT(expr, pattern, [index])`|Apply regular expression pattern and extract a capture group, or null if there is no match. If index is unspecified or zero, returns the substring that matched the pattern.|
|`REPLACE(expr, pattern, replacement)`|Replaces pattern with replacement in expr, and returns the result.|
|`STRPOS(haystack, needle)`|Returns the index of needle within haystack, with indexes starting from 1. If the needle is not found, returns 0.|
|`SUBSTRING(expr, index, [length])`|Returns a substring of expr starting at index, with a max length, both measured in UTF-16 code units.|
|`SUBSTR(expr, index, [length])`|Synonym for SUBSTRING.|
|`TRIM([BOTH \| LEADING \| TRAILING] [<chars> FROM] expr)`|Returns expr with characters removed from the leading, trailing, or both ends of "expr" if they are in "chars". If "chars" is not provided, it defaults to " " (a space). If the directional argument is not provided, it defaults to "BOTH".|
|`BTRIM(expr[, chars])`|Alternate form of `TRIM(BOTH <chars> FROM <expr>`).|
|`LTRIM(expr[, chars])`|Alternate form of `TRIM(LEADING <chars> FROM <expr>`).|
|`RTRIM(expr[, chars])`|Alternate form of `TRIM(TRAILING <chars> FROM <expr>`).|
|`UPPER(expr)`|Returns expr in all uppercase.|

### Time functions

Time functions can be used with Druid's `__time` column, with any column storing millisecond timestamps through use
of the `MILLIS_TO_TIMESTAMP` function, or with any column storing string timestamps through use of the `TIME_PARSE`
function. By default, time operations use the UTC time zone. You can change the time zone by setting the connection
context parameter "sqlTimeZone" to the name of another time zone, like "America/Los_Angeles", or to an offset like
"-08:00". If you need to mix multiple time zones in the same query, or if you need to use a time zone other than
the connection time zone, some functions also accept time zones as parameters. These parameters always take precedence
over the connection time zone.

|Function|Notes|
|--------|-----|
|`CURRENT_TIMESTAMP`|Current timestamp in the connection's time zone.|
|`CURRENT_DATE`|Current date in the connection's time zone.|
|`DATE_TRUNC(<unit>, <timestamp_expr>)`|Rounds down a timestamp, returning it as a new timestamp. Unit can be 'milliseconds', 'second', 'minute', 'hour', 'day', 'week', 'month', 'quarter', 'year', 'decade', 'century', or 'millenium'.|
|`TIME_FLOOR(<timestamp_expr>, <period>, [<origin>, [<timezone>]])`|Rounds down a timestamp, returning it as a new timestamp. Period can be any ISO8601 period, like P3M (quarters) or PT12H (half-days). The time zone, if provided, should be a time zone name like "America/Los_Angeles" or offset like "-08:00". This function is similar to `FLOOR` but is more flexible.|
|`TIME_SHIFT(<timestamp_expr>, <period>, <step>, [<timezone>])`|Shifts a timestamp by a period (step times), returning it as a new timestamp. Period can be any ISO8601 period. Step may be negative. The time zone, if provided, should be a time zone name like "America/Los_Angeles" or offset like "-08:00".|
|`TIME_EXTRACT(<timestamp_expr>, [<unit>, [<timezone>]])`|Extracts a time part from expr, returning it as a number. Unit can be EPOCH, SECOND, MINUTE, HOUR, DAY (day of month), DOW (day of week), DOY (day of year), WEEK (week of [week year](https://en.wikipedia.org/wiki/ISO_week_date)), MONTH (1 through 12), QUARTER (1 through 4), or YEAR. The time zone, if provided, should be a time zone name like "America/Los_Angeles" or offset like "-08:00". This function is similar to `EXTRACT` but is more flexible. Unit and time zone must be literals, and must be provided quoted, like `TIME_EXTRACT(__time, 'HOUR')` or `TIME_EXTRACT(__time, 'HOUR', 'America/Los_Angeles')`.|
|`TIME_PARSE(<string_expr>, [<pattern>, [<timezone>]])`|Parses a string into a timestamp using a given [Joda DateTimeFormat pattern](http://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html), or ISO8601 (e.g. `2000-01-02T03:04:05Z`) if the pattern is not provided. The time zone, if provided, should be a time zone name like "America/Los_Angeles" or offset like "-08:00", and will be used as the time zone for strings that do not include a time zone offset. Pattern and time zone must be literals. Strings that cannot be parsed as timestamps will be returned as NULL.|
|`TIME_FORMAT(<timestamp_expr>, [<pattern>, [<timezone>]])`|Formats a timestamp as a string with a given [Joda DateTimeFormat pattern](http://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html), or ISO8601 (e.g. `2000-01-02T03:04:05Z`) if the pattern is not provided. The time zone, if provided, should be a time zone name like "America/Los_Angeles" or offset like "-08:00". Pattern and time zone must be literals.|
|`MILLIS_TO_TIMESTAMP(millis_expr)`|Converts a number of milliseconds since the epoch into a timestamp.|
|`TIMESTAMP_TO_MILLIS(timestamp_expr)`|Converts a timestamp into a number of milliseconds since the epoch.|
|`EXTRACT(<unit> FROM timestamp_expr)`|Extracts a time part from expr, returning it as a number. Unit can be EPOCH, SECOND, MINUTE, HOUR, DAY (day of month), DOW (day of week), DOY (day of year), WEEK (week of year), MONTH, QUARTER, or YEAR. Units must be provided unquoted, like `EXTRACT(HOUR FROM __time)`.|
|`FLOOR(timestamp_expr TO <unit>)`|Rounds down a timestamp, returning it as a new timestamp. Unit can be SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, or YEAR.|
|`CEIL(timestamp_expr TO <unit>)`|Rounds up a timestamp, returning it as a new timestamp. Unit can be SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, or YEAR.|
|`TIMESTAMPADD(<unit>, <count>, <timestamp>)`|Equivalent to `timestamp + count * INTERVAL '1' UNIT`.|
|`timestamp_expr { + \| - } <interval_expr>`|Add or subtract an amount of time from a timestamp. interval_expr can include interval literals like `INTERVAL '2' HOUR`, and may include interval arithmetic as well. This operator treats days as uniformly 86400 seconds long, and does not take into account daylight savings time. To account for daylight savings time, use TIME_SHIFT instead.|

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
|`x IN (subquery)`|True if x is returned by the subquery. See [Syntax and execution](#syntax-and-execution) above for details about how Druid SQL handles `IN (subquery)`.|
|`x NOT IN (subquery)`|True if x is not returned by the subquery. See [Syntax and execution](#syntax-and-execution) for details about how Druid SQL handles `IN (subquery)`.|
|`x AND y`|Boolean AND.|
|`x OR y`|Boolean OR.|
|`NOT x`|Boolean NOT.|

### Other functions

|Function|Notes|
|--------|-----|
|`CAST(value AS TYPE)`|Cast value to another type. See [Data types and casts](#data-types-and-casts) for details about how Druid SQL handles CAST.|
|`CASE expr WHEN value1 THEN result1 \[ WHEN value2 THEN result2 ... \] \[ ELSE resultN \] END`|Simple CASE.|
|`CASE WHEN boolean_expr1 THEN result1 \[ WHEN boolean_expr2 THEN result2 ... \] \[ ELSE resultN \] END`|Searched CASE.|
|`NULLIF(value1, value2)`|Returns NULL if value1 and value2 match, else returns value1.|
|`COALESCE(value1, value2, ...)`|Returns the first value that is neither NULL nor empty string.|
|`BLOOM_FILTER_TEST(<expr>, <serialized-filter>)`|Returns true if the value is contained in the base64 serialized bloom filter. See [bloom filter extension](../development/extensions-core/bloom-filter.html) documentation for additional details.
### Unsupported features

Druid does not support all SQL features, including:

- OVER clauses, and analytic functions such as `LAG` and `LEAD`.
- JOIN clauses, other than semi-joins as described above.
- OFFSET clauses.
- DDL and DML.

Additionally, some Druid features are not supported by the SQL language. Some unsupported Druid features include:

- [Multi-value dimensions](multi-value-dimensions.html).
- [DataSketches aggregators](../development/extensions-core/datasketches-extension.html).
- [Spatial filters](../development/geo.html).
- [Query cancellation](querying.html#query-cancellation).

## Data types and casts

Druid natively supports five basic column types: "long" (64 bit signed int), "float" (32 bit float), "double" (64 bit
float) "string" (UTF-8 encoded strings), and "complex" (catch-all for more exotic data types like hyperUnique and
approxHistogram columns). Timestamps (including the `__time` column) are stored as longs, with the value being the
number of milliseconds since 1 January 1970 UTC.

At runtime, Druid may widen 32-bit floats to 64-bit for certain operators, like SUM aggregators. The reverse will not
happen: 64-bit floats are not be narrowed to 32-bit.

Druid generally treats NULLs and empty strings interchangeably, rather than according to the SQL standard. As such,
Druid SQL only has partial support for NULLs. For example, the expressions `col IS NULL` and `col = ''` are equivalent,
and both will evaluate to true if `col` contains an empty string. Similarly, the expression `COALESCE(col1, col2)` will
return `col2` if `col1` is an empty string. While the `COUNT(*)` aggregator counts all rows, the `COUNT(expr)`
aggregator will count the number of rows where expr is neither null nor the empty string. String columns in Druid are
NULLable. Numeric columns are NOT NULL; if you query a numeric column that is not present in all segments of your Druid
datasource, then it will be treated as zero for rows from those segments.

For mathematical operations, Druid SQL will use integer math if all operands involved in an expression are integers.
Otherwise, Druid will switch to floating point math. You can force this to happen by casting one of your operands
to FLOAT.

The following table describes how SQL types map onto Druid types during query runtime. Casts between two SQL types
that have the same Druid runtime type will have no effect, other than exceptions noted in the table. Casts between two
SQL types that have different Druid runtime types will generate a runtime cast in Druid. If a value cannot be properly
cast to another value, as in `CAST('foo' AS BIGINT)`, the runtime will substitute a default value. NULL values cast
to non-nullable types will also be substitued with a default value (for example, nulls cast to numbers will be
converted to zeroes).

|SQL type|Druid runtime type|Default value|Notes|
|--------|------------------|-------------|-----|
|CHAR|STRING|`''`||
|VARCHAR|STRING|`''`|Druid STRING columns are reported as VARCHAR|
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

## Query execution

Queries without aggregations will use Druid's [Scan](scan-query.html) or [Select](select-query.html) native query types.
Scan is used whenever possible, as it is generally higher performance and more efficient than Select. However, Select
is used in one case: when the query includes an `ORDER BY __time`, since Scan does not have a sorting feature.

Aggregation queries (using GROUP BY, DISTINCT, or any aggregation functions) will use one of Druid's three native
aggregation query types. Two (Timeseries and TopN) are specialized for specific types of aggregations, whereas the other
(GroupBy) is general-purpose.

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
your query. Results are streamed back from data nodes through the Broker if you ORDER BY the same expressions in your
GROUP BY clause, or if you don't have an ORDER BY at all. If your query has an ORDER BY referencing expressions that
don't appear in the GROUP BY clause (like aggregation functions) then the Broker will materialize a list of results in
memory, up to a max of your LIMIT, if any. See the GroupBy documentation for details about tuning performance and memory
use.

If your query does nested aggregations (an aggregation subquery in your FROM clause) then Druid will execute it as a
[nested GroupBy](groupbyquery.html#nested-groupbys). In nested GroupBys, the innermost aggregation is distributed, but
all outer aggregations beyond that take place locally on the query Broker.

Semi-join queries containing WHERE clauses like `col IN (SELECT expr FROM ...)` are executed with a special process. The
Broker will first translate the subquery into a GroupBy to find distinct values of `expr`. Then, the broker will rewrite
the subquery to a literal filter, like `col IN (val1, val2, ...)` and run the outer query. The configuration parameter
druid.sql.planner.maxSemiJoinRowsInMemory controls the maximum number of values that will be materialized for this kind
of plan.

For all native query types, filters on the `__time` column will be translated into top-level query "intervals" whenever
possible, which allows Druid to use its global time index to quickly prune the set of data that must be scanned. In
addition, Druid will use indexes local to each data node to further speed up WHERE evaluation. This can typically be
done for filters that involve boolean combinations of references to and functions of single columns, like
`WHERE col1 = 'a' AND col2 = 'b'`, but not `WHERE col1 = col2`.

### Approximate algorithms

Druid SQL will use approximate algorithms in some situations:

- The `COUNT(DISTINCT col)` aggregation functions by default uses a variant of
[HyperLogLog](http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf), a fast approximate distinct counting
algorithm. Druid SQL will switch to exact distinct counts if you set "useApproximateCountDistinct" to "false", either
through query context or through Broker configuration.
- GROUP BY queries over a single column with ORDER BY and LIMIT may be executed using the TopN engine, which uses an
approximate algorithm. Druid SQL will switch to an exact grouping algorithm if you set "useApproximateTopN" to "false",
either through query context or through Broker configuration.
- The APPROX_COUNT_DISTINCT and APPROX_QUANTILE aggregation functions always use approximate algorithms, regardless
of configuration.

## Client APIs

### JSON over HTTP

You can make Druid SQL queries using JSON over HTTP by posting to the endpoint `/druid/v2/sql/`. The request should
be a JSON object with a "query" field, like `{"query" : "SELECT COUNT(*) FROM data_source WHERE foo = 'bar'"}`.

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

Metadata is available over the HTTP API by querying [system tables](#retrieving-metadata).

#### Responses

Druid SQL supports a variety of result formats. You can specify these by adding a "resultFormat" parameter, like:

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
["INFORMATION_SCHEMA" tables](#retrieving-metadata). Parameterized queries (using `?` or other placeholders) don't work properly,
so avoid those.

#### Connection stickiness

Druid's JDBC server does not share connection state between Brokers. This means that if you're using JDBC and have
multiple Druid Brokers, you should either connect to a specific Broker, or use a load balancer with sticky sessions
enabled. The Druid Router node provides connection stickiness when balancing JDBC requests, and can be used to achieve
the necessary stickiness even with a normal non-sticky load balancer. Please see the
[Router](../development/router.html) documentation for more details.

Note that the non-JDBC [JSON over HTTP](#json-over-http) API is stateless and does not require stickiness.

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
|`useApproximateCountDistinct`|Whether to use an approximate cardinalty algorithm for `COUNT(DISTINCT foo)`.|druid.sql.planner.useApproximateCountDistinct on the Broker (default: true)|
|`useApproximateTopN`|Whether to use approximate [TopN queries](topnquery.html) when a SQL query could be expressed as such. If false, exact [GroupBy queries](groupbyquery.html) will be used instead.|druid.sql.planner.useApproximateTopN on the Broker (default: true)|
|`useFallback`|Whether to evaluate operations on the Broker when they cannot be expressed as Druid queries. This option is not recommended for production since it can generate unscalable query plans. If false, SQL queries that cannot be translated to Druid queries will fail.|druid.sql.planner.useFallback on the Broker (default: false)|

### Retrieving metadata

Druid Brokers infer table and column metadata for each dataSource from segments loaded in the cluster, and use this to
plan SQL queries. This metadata is cached on Broker startup and also updated periodically in the background through
[SegmentMetadata queries](segmentmetadataquery.html). Background metadata refreshing is triggered by
segments entering and exiting the cluster, and can also be throttled through configuration.

Druid exposes system information through special system tables. There are two such schemas available: Information Schema and Sys Schema.
Information schema provides details about table and column types. The "sys" schema provides information about Druid internals like segments/tasks/servers.

## INFORMATION SCHEMA

You can access table and column metadata through JDBC using `connection.getMetaData()`, or through the
INFORMATION_SCHEMA tables described below. For example, to retrieve metadata for the Druid
datasource "foo", use the query:

```sql
SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'druid' AND TABLE_NAME = 'foo'
```

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

## SYSTEM SCHEMA

The "sys" schema provides visibility into Druid segments, servers and tasks.

### SEGMENTS table
Segments table provides details on all Druid segments, whether they are published yet or not.


|Column|Notes|
|------|-----|
|segment_id|Unique segment identifier|
|datasource|Name of datasource|
|start|Interval start time (in ISO 8601 format)|
|end|Interval end time (in ISO 8601 format)|
|size|Size of segment in bytes|
|version|Version string (generally an ISO8601 timestamp corresponding to when the segment set was first started). Higher version means the more recently created segment. Version comparing is based on string comparison.|
|partition_num|Partition number (an integer, unique within a datasource+interval+version; may not necessarily be contiguous)|
|num_replicas|Number of replicas of this segment currently being served|
|num_rows|Number of rows in current segment, this value could be null if unkown to Broker at query time|
|is_published|Boolean is represented as long type where 1 = true, 0 = false. 1 represents this segment has been published to the metadata store|
|is_available|Boolean is represented as long type where 1 = true, 0 = false. 1 if this segment is currently being served by any server(Historical or realtime)|
|is_realtime|Boolean is represented as long type where 1 = true, 0 = false. 1 if this segment is being served on any type of realtime tasks|
|payload|JSON-serialized data segment payload|

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

### SERVERS table
Servers table lists all data servers(any server that hosts a segment). It includes both Historicals and Peons.

|Column|Notes|
|------|-----|
|server|Server name in the form host:port|
|host|Hostname of the server|
|plaintext_port|Unsecured port of the server, or -1 if plaintext traffic is disabled|
|tls_port|TLS port of the server, or -1 if TLS is disabled|
|server_type|Type of Druid service. Possible values include: Historical, realtime and indexer_executor(Peon).|
|tier|Distribution tier see [druid.server.tier](#../configuration/index.html#Historical-General-Configuration)|
|current_size|Current size of segments in bytes on this server|
|max_size|Max size in bytes this server recommends to assign to segments see [druid.server.maxSize](#../configuration/index.html#Historical-General-Configuration)|

To retrieve information about all servers, use the query:

```sql
SELECT * FROM sys.servers;
```

### SERVER_SEGMENTS table

SERVER_SEGMENTS is used to join servers with segments table

|Column|Notes|
|------|-----|
|server|Server name in format host:port (Primary key of [servers table](#SERVERS-table))|
|segment_id|Segment identifier (Primary key of [segments table](#SEGMENTS-table))|

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

### TASKS table

The tasks table provides information about active and recently-completed indexing tasks. For more information 
check out [ingestion tasks](#../ingestion/tasks.html)

|Column|Notes|
|------|-----|
|task_id|Unique task identifier|
|type|Task type, for example this value is "index" for indexing tasks. See [tasks-overview](../ingestion/tasks.md)|
|datasource|Datasource name being indexed|
|created_time|Timestamp in ISO8601 format corresponding to when the ingestion task was created. Note that this value is populated for completed and waiting tasks. For running and pending tasks this value is set to 1970-01-01T00:00:00Z|
|queue_insertion_time|Timestamp in ISO8601 format corresponding to when this task was added to the queue on the Overlord|
|status|Status of a task can be RUNNING, FAILED, SUCCESS|
|runner_status|Runner status of a completed task would be NONE, for in-progress tasks this can be RUNNING, WAITING, PENDING|
|duration|Time it took to finish the task in milliseconds, this value is present only for completed tasks|
|location|Server name where this task is running in the format host:port, this information is present only for RUNNING tasks|
|host|Hostname of the server where task is running|
|plaintext_port|Unsecured port of the server, or -1 if plaintext traffic is disabled|
|tls_port|TLS port of the server, or -1 if TLS is disabled|
|error_msg|Detailed error message in case of FAILED tasks|

For example, to retrieve tasks information filtered by status, use the query

```sql
SELECT * FROM sys.tasks WHERE status='FAILED';
```

Note that sys tables may not support all the Druid SQL Functions.

## Server configuration

The Druid SQL server is configured through the following properties on the Broker.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.sql.enable`|Whether to enable SQL at all, including background metadata fetching. If false, this overrides all other SQL-related properties and disables SQL metadata, serving, and planning completely.|false|
|`druid.sql.avatica.enable`|Whether to enable JDBC querying at `/druid/v2/sql/avatica/`.|true|
|`druid.sql.avatica.maxConnections`|Maximum number of open connections for the Avatica server. These are not HTTP connections, but are logical client connections that may span multiple HTTP connections.|25|
|`druid.sql.avatica.maxRowsPerFrame`|Maximum number of rows to return in a single JDBC frame. Setting this property to -1 indicates that no row limit should be applied. Clients can optionally specify a row limit in their requests; if a client specifies a row limit, the lesser value of the client-provided limit and `maxRowsPerFrame` will be used.|5,000|
|`druid.sql.avatica.maxStatementsPerConnection`|Maximum number of simultaneous open statements per Avatica client connection.|4|
|`druid.sql.avatica.connectionIdleTimeout`|Avatica client connection idle timeout.|PT5M|
|`druid.sql.http.enable`|Whether to enable JSON over HTTP querying at `/druid/v2/sql/`.|true|
|`druid.sql.planner.maxQueryCount`|Maximum number of queries to issue, including nested queries. Set to 1 to disable sub-queries, or set to 0 for unlimited.|8|
|`druid.sql.planner.maxSemiJoinRowsInMemory`|Maximum number of rows to keep in memory for executing two-stage semi-join queries like `SELECT * FROM Employee WHERE DeptName IN (SELECT DeptName FROM Dept)`.|100000|
|`druid.sql.planner.maxTopNLimit`|Maximum threshold for a [TopN query](../querying/topnquery.html). Higher limits will be planned as [GroupBy queries](../querying/groupbyquery.html) instead.|100000|
|`druid.sql.planner.metadataRefreshPeriod`|Throttle for metadata refreshes.|PT1M|
|`druid.sql.planner.selectThreshold`|Page size threshold for [Select queries](../querying/select-query.html). Select queries for larger resultsets will be issued back-to-back using pagination.|1000|
|`druid.sql.planner.useApproximateCountDistinct`|Whether to use an approximate cardinalty algorithm for `COUNT(DISTINCT foo)`.|true|
|`druid.sql.planner.useApproximateTopN`|Whether to use approximate [TopN queries](../querying/topnquery.html) when a SQL query could be expressed as such. If false, exact [GroupBy queries](../querying/groupbyquery.html) will be used instead.|true|
|`druid.sql.planner.useFallback`|Whether to evaluate operations on the Broker when they cannot be expressed as Druid queries. This option is not recommended for production since it can generate unscalable query plans. If false, SQL queries that cannot be translated to Druid queries will fail.|false|
|`druid.sql.planner.requireTimeCondition`|Whether to require SQL to have filter conditions on __time column so that all generated native queries will have user specified intervals. If true, all queries wihout filter condition on __time column will fail|false|
|`druid.sql.planner.sqlTimeZone`|Sets the default time zone for the server, which will affect how time functions and timestamp literals behave. Should be a time zone name like "America/Los_Angeles" or offset like "-08:00".|UTC|

## SQL Metrics

Broker will emit the following metrics for SQL.

|Metric|Description|Dimensions|Normal Value|
|------|-----------|----------|------------|
|`sqlQuery/time`|Milliseconds taken to complete a SQL.|id, nativeQueryIds, dataSource, remoteAddress, success.|< 1s|
|`sqlQuery/bytes`|number of bytes returned in SQL response.|id, nativeQueryIds, dataSource, remoteAddress, success.| |

