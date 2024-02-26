---
id: sql-scalar
title: "SQL scalar functions"
sidebar_label: "Scalar functions"
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

[Druid SQL](./sql.md) includes scalar functions that include numeric and string functions, IP address functions, Sketch functions, and more, as described on this page. 


## Numeric functions

For mathematical operations, Druid SQL will use integer math if all operands involved in an expression are integers.
Otherwise, Druid will switch to floating point math. You can force this to happen by casting one of your operands
to FLOAT. At runtime, Druid will widen 32-bit floats to 64-bit for most expressions.

|Function|Notes|
|--------|-----|
|`PI`|Constant Pi.|
|`ABS(expr)`|Absolute value.|
|`CEIL(expr)`|Ceiling.|
|`EXP(expr)`|e to the power of `expr`.|
|`FLOOR(expr)`|Floor.|
|`LN(expr)`|Logarithm (base e).|
|`LOG10(expr)`|Logarithm (base 10).|
|`POWER(expr, power)`|`expr` raised to the power of `power`.|
|`SQRT(expr)`|Square root.|
|`TRUNCATE(expr, [digits])`|Truncate `expr` to a specific number of decimal digits. If digits is negative, then this truncates that many places to the left of the decimal point. Digits defaults to zero if not specified.|
|`TRUNC(expr, [digits])`|Alias for `TRUNCATE`.|
|`ROUND(expr, [digits])`|`ROUND(x, y)` would return the value of the x rounded to the y decimal places. While x can be an integer or floating-point number, y must be an integer. The type of the return value is specified by that of x. y defaults to 0 if omitted. When y is negative, x is rounded on the left side of the y decimal points. If `expr` evaluates to either `NaN`, `expr` will be converted to 0. If `expr` is infinity, `expr` will be converted to the nearest finite double. |
|`MOD(x, y)`|Modulo (remainder of x divided by y).|
|`SIN(expr)`|Trigonometric sine of an angle `expr`.|
|`COS(expr)`|Trigonometric cosine of an angle `expr`.|
|`TAN(expr)`|Trigonometric tangent of an angle `expr`.|
|`COT(expr)`|Trigonometric cotangent of an angle `expr`.|
|`ASIN(expr)`|Arc sine of `expr`.|
|`ACOS(expr)`|Arc cosine of `expr`.|
|`ATAN(expr)`|Arc tangent of `expr`.|
|`ATAN2(y, x)`|Angle theta from the conversion of rectangular coordinates (x, y) to polar * coordinates (r, theta).|
|`DEGREES(expr)`|Converts an angle measured in radians to an approximately equivalent angle measured in degrees.|
|`RADIANS(expr)`|Converts an angle measured in degrees to an approximately equivalent angle measured in radians.|
|`BITWISE_AND(expr1, expr2)`|Returns the result of `expr1 & expr2`. Double values will be implicitly cast to longs, use `BITWISE_CONVERT_DOUBLE_TO_LONG_BITS` to perform bitwise operations directly with doubles.|
|`BITWISE_COMPLEMENT(expr)`|Returns the result of `~expr`. Double values will be implicitly cast to longs, use `BITWISE_CONVERT_DOUBLE_TO_LONG_BITS` to perform bitwise operations directly with doubles.|
|`BITWISE_CONVERT_DOUBLE_TO_LONG_BITS(expr)`|Converts the bits of an IEEE 754 floating-point double value to a long. If the input is not a double, it is implicitly cast to a double prior to conversion.|
|`BITWISE_CONVERT_LONG_BITS_TO_DOUBLE(expr)`|Converts a long to the IEEE 754 floating-point double specified by the bits stored in the long. If the input is not a long, it is implicitly cast to a long prior to conversion.|
|`BITWISE_OR(expr1, expr2)`|Returns the result of `expr1 [PIPE] expr2`. Double values will be implicitly cast to longs, use `BITWISE_CONVERT_DOUBLE_TO_LONG_BITS` to perform bitwise operations directly with doubles.|
|`BITWISE_SHIFT_LEFT(expr1, expr2)`|Returns the result of `expr1 << expr2`. Double values will be implicitly cast to longs, use `BITWISE_CONVERT_DOUBLE_TO_LONG_BITS` to perform bitwise operations directly with doubles.|
|`BITWISE_SHIFT_RIGHT(expr1, expr2)`|Returns the result of `expr1 >> expr2`. Double values will be implicitly cast to longs, use `BITWISE_CONVERT_DOUBLE_TO_LONG_BITS` to perform bitwise operations directly with doubles.|
|`BITWISE_XOR(expr1, expr2)`|Returns the result of `expr1 ^ expr2`. Double values will be implicitly cast to longs, use `BITWISE_CONVERT_DOUBLE_TO_LONG_BITS` to perform bitwise operations directly with doubles.|
|`DIV(x, y)`|Returns the result of integer division of x by y |
|`HUMAN_READABLE_BINARY_BYTE_FORMAT(value, [precision])`| Format a number in human-readable [IEC](https://en.wikipedia.org/wiki/Binary_prefix) format. For example, HUMAN_READABLE_BINARY_BYTE_FORMAT(1048576) returns `1.00 MiB`. `precision` must be in the range of `[0, 3]` (default: 2). |
|`HUMAN_READABLE_DECIMAL_BYTE_FORMAT(value, [precision])`| Format a number in human-readable [SI](https://en.wikipedia.org/wiki/Binary_prefix) format. HUMAN_READABLE_DECIMAL_BYTE_FORMAT(1048576) returns `1.04 MB`. `precision` must be in the range of `[0, 3]` (default: 2). `precision` must be in the range of `[0, 3]` (default: 2). |
|`HUMAN_READABLE_DECIMAL_FORMAT(value, [precision])`| Format a number in human-readable SI format. For example, HUMAN_READABLE_DECIMAL_FORMAT(1048576) returns `1.04 M`. `precision` must be in the range of `[0, 3]` (default: 2). |
|`SAFE_DIVIDE(x, y)`|Returns the division of x by y guarded on division by 0. In case y is 0 it returns 0, or `null` if `druid.generic.useDefaultValueForNull=false` |


## String functions

String functions accept strings, and return a type appropriate to the function.

|Function|Notes|
|--------|-----|
|`CONCAT(expr, expr...)`|Concats a list of expressions. Also see the [concatenation operator](sql-operators.md#concatenation-operator).|
|`TEXTCAT(expr, expr)`|Two argument version of `CONCAT`.|
|`CONTAINS_STRING(expr, str)`|Returns true if the `str` is a substring of `expr`.|
|`ICONTAINS_STRING(expr, str)`|Returns true if the `str` is a substring of `expr`. The match is case-insensitive.|
|`DECODE_BASE64_UTF8(expr)`|Decodes a Base64-encoded string into a UTF-8 encoded string.|
|`LEFT(expr, [length])`|Returns the leftmost length characters from `expr`.|
|`RIGHT(expr, [length])`|Returns the rightmost length characters from `expr`.|
|`LENGTH(expr)`|Length of `expr` in UTF-16 code units.|
|`CHAR_LENGTH(expr)`|Alias for `LENGTH`.|
|`CHARACTER_LENGTH(expr)`|Alias for `LENGTH`.|
|`STRLEN(expr)`|Alias for `LENGTH`.|
|`LOOKUP(expr, lookupName, [replaceMissingValueWith])`|Look up `expr` in a registered [query-time lookup table](lookups.md) named `lookupName`. The optional constant `replaceMissingValueWith`, if provided, is returned when the `expr` is null or when the lookup does not contain a value for `expr`.<br /><br />Lookups can also be queried directly using the [`lookup` schema](sql.md#from).|
|`LOWER(expr)`|Returns `expr` in all lowercase.|
|`UPPER(expr)`|Returns `expr` in all uppercase.|
|`LPAD(expr, length, [chars])`|Returns a string of `length` from `expr` left-padded with `chars`. If `length` is shorter than the length of `expr`, the result is `expr` which is truncated to `length`. The result will be null if either `expr` or `chars` is null. If `chars` is an empty string, no padding is added, however `expr` may be trimmed if necessary.|
|`RPAD(expr, length, [chars])`|Returns a string of `length` from `expr` right-padded with `chars`. If `length` is shorter than the length of `expr`, the result is `expr` which is truncated to `length`. The result will be null if either `expr` or `chars` is null. If `chars` is an empty string, no padding is added, however `expr` may be trimmed if necessary.|
|`PARSE_LONG(string, [radix])`|Parses a string into a long (BIGINT) with the given radix, or 10 (decimal) if a radix is not provided.|
|`POSITION(needle IN haystack [FROM fromIndex])`|Returns the index of `needle` within `haystack`, with indexes starting from 1. The search will begin at `fromIndex`, or 1 if `fromIndex` is not specified. If `needle` is not found, returns 0.|
|`REGEXP_EXTRACT(expr, pattern, [index])`|Apply regular expression `pattern` to `expr` and extract a capture group, or `NULL` if there is no match. If index is unspecified or zero, returns the first substring that matched the pattern. The pattern may match anywhere inside `expr`; if you want to match the entire string instead, use the `^` and `$` markers at the start and end of your pattern. Note: when `druid.generic.useDefaultValueForNull = true`, it is not possible to differentiate an empty-string match from a non-match (both will return `NULL`).|
|`REGEXP_LIKE(expr, pattern)`|Returns whether `expr` matches regular expression `pattern`. The pattern may match anywhere inside `expr`; if you want to match the entire string instead, use the `^` and `$` markers at the start and end of your pattern. Similar to [`LIKE`](sql-operators.md#logical-operators), but uses regexps instead of LIKE patterns. Especially useful in WHERE clauses.|
|`REGEXP_REPLACE(expr, pattern, replacement)`|Replaces all occurrences of regular expression `pattern` within `expr` with `replacement`. The replacement string may refer to capture groups using `$1`, `$2`, etc. The pattern may match anywhere inside `expr`; if you want to match the entire string instead, use the `^` and `$` markers at the start and end of your pattern.|
|`REPLACE(expr, pattern, replacement)`|Replaces pattern with replacement in `expr`, and returns the result.|
|`REPEAT(expr, [N])`|Repeats `expr` N times.|
|`REVERSE(expr)`|Reverses `expr`.|
|`STRING_FORMAT(pattern, [args...])`|Returns a string formatted in the manner of Java's [String.format](https://docs.oracle.com/javase/8/docs/api/java/lang/String.html#format-java.lang.String-java.lang.Object...-).|
|`STRPOS(haystack, needle)`|Returns the index of `needle` within `haystack`, with indexes starting from 1. If `needle` is not found, returns 0.|
|`SUBSTRING(expr, index, [length])`|Returns a substring of `expr` starting at index, with a max length, both measured in UTF-16 code units.|
|`SUBSTR(expr, index, [length])`|Alias for `SUBSTRING`.|
|`TRIM([BOTH `<code>&#124;</code>` LEADING `<code>&#124;</code>` TRAILING] [chars FROM] expr)`|Returns `expr` with characters removed from the leading, trailing, or both ends of `expr` if they are in `chars`. If `chars` is not provided, it defaults to `''` (a space). If the directional argument is not provided, it defaults to `BOTH`.|
|`BTRIM(expr, [chars])`|Alternate form of `TRIM(BOTH chars FROM expr)`.|
|`LTRIM(expr, [chars])`|Alternate form of `TRIM(LEADING chars FROM expr)`.|
|`RTRIM(expr, [chars])`|Alternate form of `TRIM(TRAILING chars FROM expr)`.|


## Date and time functions 

Time functions can be used with:

- Druid's primary timestamp column, `__time`;
- Numeric values representing milliseconds since the epoch, through the MILLIS_TO_TIMESTAMP function; and
- String timestamps, through the TIME_PARSE function.

By default, time operations use the UTC time zone. You can change the time zone by setting the connection
context parameter `sqlTimeZone` to the name of another time zone, like `America/Los_Angeles`, or to an offset like
`-08:00`. If you need to mix multiple time zones in the same query, or if you need to use a time zone other than
the connection time zone, some functions also accept time zones as parameters. These parameters always take precedence
over the connection time zone.

Literal timestamps in the connection time zone can be written using `TIMESTAMP '2000-01-01 00:00:00'` syntax. The
simplest way to write literal timestamps in other time zones is to use TIME_PARSE, like
`TIME_PARSE('2000-02-01 00:00:00', NULL, 'America/Los_Angeles')`.

The best ways to filter based on time are by using ISO8601 intervals, like
`TIME_IN_INTERVAL(__time, '2000-01-01/2000-02-01')`, or by using literal timestamps with the `>=` and `<` operators, like
`__time >= TIMESTAMP '2000-01-01 00:00:00' AND __time < TIMESTAMP '2000-02-01 00:00:00'`.

Druid supports the standard SQL BETWEEN operator, but we recommend avoiding it for time filters. BETWEEN is inclusive
of its upper bound, which makes it awkward to write time filters correctly. For example, the equivalent of
`TIME_IN_INTERVAL(__time, '2000-01-01/2000-02-01')` is
`__time BETWEEN TIMESTAMP '2000-01-01 00:00:00' AND TIMESTAMP '2000-01-31 23:59:59.999'`.

Druid processes timestamps internally as longs (64-bit integers) representing milliseconds since the epoch. Therefore,
time functions perform best when used with the primary timestamp column, or with timestamps stored in long columns as
milliseconds and accessed with MILLIS_TO_TIMESTAMP. Other timestamp representations, include string timestamps and
POSIX timestamps (seconds since the epoch) require query-time conversion to Druid's internal form, which adds additional
overhead.

|Function|Notes|
|--------|-----|
|`CURRENT_TIMESTAMP`|Current timestamp in the connection's time zone.|
|`CURRENT_DATE`|Current date in the connection's time zone.|
|`DATE_TRUNC(unit, timestamp_expr)`|Rounds down a timestamp, returning it as a new timestamp. Unit can be 'milliseconds', 'second', 'minute', 'hour', 'day', 'week', 'month', 'quarter', 'year', 'decade', 'century', or 'millennium'.|
|`TIME_CEIL(timestamp_expr, period, [origin, [timezone]])`|Rounds up a timestamp, returning it as a new timestamp. Period can be any ISO8601 period, like P3M (quarters) or PT12H (half-days). Specify `origin` as a timestamp to set the reference time for rounding. For example, `TIME_CEIL(__time, 'PT1H', TIMESTAMP '2016-06-27 00:30:00')` measures an hourly period from 00:30-01:30 instead of 00:00-01:00. See [Period granularities](granularities.md) for details on the default starting boundaries. The time zone, if provided, should be a time zone name like "America/Los_Angeles" or offset like "-08:00". This function is similar to `CEIL` but is more flexible.|
|`TIME_FLOOR(timestamp_expr, period, [origin, [timezone]])`|Rounds down a timestamp, returning it as a new timestamp. Period can be any ISO8601 period, like P3M (quarters) or PT12H (half-days). Specify `origin` as a timestamp to set the reference time for rounding. For example, `TIME_FLOOR(__time, 'PT1H', TIMESTAMP '2016-06-27 00:30:00')` measures an hourly period from 00:30-01:30 instead of 00:00-01:00. See [Period granularities](granularities.md) for details on the default starting boundaries. The time zone, if provided, should be a time zone name like "America/Los_Angeles" or offset like "-08:00". This function is similar to `FLOOR` but is more flexible.|
|`TIME_SHIFT(timestamp_expr, period, step, [timezone])`|Shifts a timestamp by a period (step times), returning it as a new timestamp. Period can be any ISO8601 period. Step may be negative. The time zone, if provided, should be a time zone name like "America/Los_Angeles" or offset like "-08:00".|
|`TIME_EXTRACT(timestamp_expr, [unit, [timezone]])`|Extracts a time part from `expr`, returning it as a number. Unit can be EPOCH, SECOND, MINUTE, HOUR, DAY (day of month), DOW (day of week), DOY (day of year), WEEK (week of [week year](https://en.wikipedia.org/wiki/ISO_week_date)), MONTH (1 through 12), QUARTER (1 through 4), or YEAR. The time zone, if provided, should be a time zone name like "America/Los_Angeles" or offset like "-08:00". This function is similar to `EXTRACT` but is more flexible. Unit and time zone must be literals, and must be provided quoted, like `TIME_EXTRACT(__time, 'HOUR')` or `TIME_EXTRACT(__time, 'HOUR', 'America/Los_Angeles')`.|
|`TIME_PARSE(string_expr, [pattern, [timezone]])`|Parses a string into a timestamp using a given [Joda DateTimeFormat pattern](http://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html), or ISO8601 (e.g. `2000-01-02T03:04:05Z`) if the pattern is not provided. The time zone, if provided, should be a time zone name like "America/Los_Angeles" or offset like "-08:00", and will be used as the time zone for strings that do not include a time zone offset. Pattern and time zone must be literals. Strings that cannot be parsed as timestamps will be returned as NULL.|
|`TIME_FORMAT(timestamp_expr, [pattern, [timezone]])`|Formats a timestamp as a string with a given [Joda DateTimeFormat pattern](http://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html), or ISO8601 (e.g. `2000-01-02T03:04:05Z`) if the pattern is not provided. The time zone, if provided, should be a time zone name like "America/Los_Angeles" or offset like "-08:00". Pattern and time zone must be literals.|
|`TIME_IN_INTERVAL(timestamp_expr, interval)`|Returns whether a timestamp is contained within a particular interval. The interval must be a literal string containing any ISO8601 interval, such as `'2001-01-01/P1D'` or `'2001-01-01T01:00:00/2001-01-02T01:00:00'`. The start instant of the interval is inclusive and the end instant is exclusive.|
|`MILLIS_TO_TIMESTAMP(millis_expr)`|Converts a number of milliseconds since the epoch (1970-01-01 00:00:00 UTC) into a timestamp.|
|`TIMESTAMP_TO_MILLIS(timestamp_expr)`|Converts a timestamp into a number of milliseconds since the epoch.|
|`EXTRACT(unit FROM timestamp_expr)`|Extracts a time part from `expr`, returning it as a number. Unit can be EPOCH, MICROSECOND, MILLISECOND, SECOND, MINUTE, HOUR, DAY (day of month), DOW (day of week), ISODOW (ISO day of week), DOY (day of year), WEEK (week of year), MONTH, QUARTER, YEAR, ISOYEAR, DECADE, CENTURY or MILLENNIUM. Units must be provided unquoted, like `EXTRACT(HOUR FROM __time)`.|
|`FLOOR(timestamp_expr TO unit)`|Rounds down a timestamp, returning it as a new timestamp. Unit can be SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, or YEAR.|
|`CEIL(timestamp_expr TO unit)`|Rounds up a timestamp, returning it as a new timestamp. Unit can be SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, or YEAR.|
|`TIMESTAMPADD(unit, count, timestamp)`|Equivalent to `timestamp + count * INTERVAL '1' UNIT`.|
|`TIMESTAMPDIFF(unit, timestamp1, timestamp2)`|Returns the (signed) number of `unit` between `timestamp1` and `timestamp2`. Unit can be SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, or YEAR.|


## Reduction functions

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


## IP address functions

For the IPv4 address functions, the `address` argument can either be an IPv4 dotted-decimal string
(e.g., "192.168.0.1") or an IP address represented as an integer (e.g., 3232235521). The `subnet`
argument should be a string formatted as an IPv4 address subnet in CIDR notation (e.g., "192.168.0.0/16").

For the IPv6 address function, the `address` argument accepts a semicolon separated string (e.g. "75e9:efa4:29c6:85f6::232c"). The format of the `subnet` argument should be an IPv6 address subnet in CIDR notation (e.g. "75e9:efa4:29c6:85f6::/64").

|Function|Notes|
|---|---|
|`IPV4_MATCH(address, subnet)`|Returns true if the `address` belongs to the `subnet` literal, else false. If `address` is not a valid IPv4 address, then false is returned. This function is more efficient if `address` is an integer instead of a string.|
|`IPV4_PARSE(address)`|Parses `address` into an IPv4 address stored as an integer . If `address` is an integer that is a valid IPv4 address, then it is passed through. Returns null if `address` cannot be represented as an IPv4 address.|
|`IPV4_STRINGIFY(address)`|Converts `address` into an IPv4 address dotted-decimal string. If `address` is a string that is a valid IPv4 address, then it is passed through. Returns null if `address` cannot be represented as an IPv4 address.|
| IPV6_MATCH(address, subnet) | Returns 1 if the IPv6 `address` belongs to the `subnet` literal, else 0. If `address` is not a valid IPv6 address, then 0 is returned.|

## Sketch functions

These functions operate on expressions or columns that return sketch objects.
To create sketch objects, see the [DataSketches aggregators](sql-aggregations.md#sketch-functions).


### HLL sketch functions

The following functions operate on [DataSketches HLL sketches](../development/extensions-core/datasketches-hll.md).
The [DataSketches extension](../development/extensions-core/datasketches-extension.md) must be loaded to use the following functions.

|Function|Notes|
|--------|-----|
|`HLL_SKETCH_ESTIMATE(expr, [round])`|Returns the distinct count estimate from an HLL sketch. `expr` must return an HLL sketch. The optional `round` boolean parameter will round the estimate if set to `true`, with a default of `false`.|
|`HLL_SKETCH_ESTIMATE_WITH_ERROR_BOUNDS(expr, [numStdDev])`|Returns the distinct count estimate and error bounds from an HLL sketch. `expr` must return an HLL sketch. An optional `numStdDev` argument can be provided.|
|`HLL_SKETCH_UNION([lgK, tgtHllType], expr0, expr1, ...)`|Returns a union of HLL sketches, where each input expression must return an HLL sketch. The `lgK` and `tgtHllType` can be optionally specified as the first parameter; if provided, both optional parameters must be specified.|
|`HLL_SKETCH_TO_STRING(expr)`|Returns a human-readable string representation of an HLL sketch for debugging. `expr` must return an HLL sketch.|

### Theta sketch functions

The following functions operate on [theta sketches](../development/extensions-core/datasketches-theta.md).
The [DataSketches extension](../development/extensions-core/datasketches-extension.md) must be loaded to use the following functions.

|Function|Notes|
|--------|-----|
|`THETA_SKETCH_ESTIMATE(expr)`|Returns the distinct count estimate from a theta sketch. `expr` must return a theta sketch.|
|`THETA_SKETCH_ESTIMATE_WITH_ERROR_BOUNDS(expr, errorBoundsStdDev)`|Returns the distinct count estimate and error bounds from a theta sketch. `expr` must return a theta sketch.|
|`THETA_SKETCH_UNION([size], expr0, expr1, ...)`|Returns a union of theta sketches, where each input expression must return a theta sketch. The `size` can be optionally specified as the first parameter.|
|`THETA_SKETCH_INTERSECT([size], expr0, expr1, ...)`|Returns an intersection of theta sketches, where each input expression must return a theta sketch. The `size` can be optionally specified as the first parameter.|
|`THETA_SKETCH_NOT([size], expr0, expr1, ...)`|Returns a set difference of theta sketches, where each input expression must return a theta sketch. The `size` can be optionally specified as the first parameter.|

### Quantiles sketch functions

The following functions operate on [quantiles sketches](../development/extensions-core/datasketches-quantiles.md).
The [DataSketches extension](../development/extensions-core/datasketches-extension.md) must be loaded to use the following functions.

|Function|Notes|
|--------|-----|
|`DS_GET_QUANTILE(expr, fraction)`|Returns the quantile estimate corresponding to `fraction` from a quantiles sketch. `expr` must return a quantiles sketch.|
|`DS_GET_QUANTILES(expr, fraction0, fraction1, ...)`|Returns a string representing an array of quantile estimates corresponding to a list of fractions from a quantiles sketch. `expr` must return a quantiles sketch.|
|`DS_HISTOGRAM(expr, splitPoint0, splitPoint1, ...)`|Returns a string representing an approximation to the histogram given a list of split points that define the histogram bins from a quantiles sketch. `expr` must return a quantiles sketch.|
|`DS_CDF(expr, splitPoint0, splitPoint1, ...)`|Returns a string representing approximation to the Cumulative Distribution Function given a list of split points that define the edges of the bins from a quantiles sketch. `expr` must return a quantiles sketch.|
|`DS_RANK(expr, value)`|Returns an approximation to the rank of a given value that is the fraction of the distribution less than that value from a quantiles sketch. `expr` must return a quantiles sketch.|
|`DS_QUANTILE_SUMMARY(expr)`|Returns a string summary of a quantiles sketch, useful for debugging. `expr` must return a quantiles sketch.|

### Tuple sketch functions

The following functions operate on [tuple sketches](../development/extensions-core/datasketches-tuple.md).
The [DataSketches extension](../development/extensions-core/datasketches-extension.md) must be loaded to use the following functions.

|Function|Notes|Default|
|--------|-----|-------|
|`DS_TUPLE_DOUBLES_METRICS_SUM_ESTIMATE(expr)`|Computes approximate sums of the values contained within a [Tuple sketch](../development/extensions-core/datasketches-tuple.md#estimated-metrics-values-for-each-column-of-arrayofdoublessketch) column which contains an array of double values as its Summary Object.
|`DS_TUPLE_DOUBLES_INTERSECT(expr, ..., [nominalEntries])`|Returns an intersection of tuple sketches, where each input expression must return a tuple sketch which contains an array of double values as its Summary Object. The values contained in the Summary Objects are summed when combined. If the last value of the array is a numeric literal, Druid assumes that the value is an override parameter for [nominal entries](../development/extensions-core/datasketches-tuple.md).|
|`DS_TUPLE_DOUBLES_NOT(expr, ..., [nominalEntries])`|Returns a set difference of tuple sketches, where each input expression must return a tuple sketch which contains an array of double values as its Summary Object. The values contained in the Summary Object are preserved as is. If the last value of the array is a numeric literal, Druid assumes that the value is an override parameter for [nominal entries](../development/extensions-core/datasketches-tuple.md).|
|`DS_TUPLE_DOUBLES_UNION(expr, ..., [nominalEntries])`|Returns a union of tuple sketches, where each input expression must return a tuple sketch which contains an array of double values as its Summary Object. The values contained in the Summary Objects are summed when combined. If the last value of the array is a numeric literal, Druid assumes that the value is an override parameter for [nominal entries](../development/extensions-core/datasketches-tuple.md).|


## Other scalar functions

|Function|Notes|
|--------|-----|
|`BLOOM_FILTER_TEST(expr, serialized-filter)`|Returns true if the value of `expr` is contained in the base64-serialized Bloom filter. See the [Bloom filter extension](../development/extensions-core/bloom-filter.md) documentation for additional details. See the [`BLOOM_FILTER` function](sql-aggregations.md) for computing Bloom filters.|
|`CASE expr WHEN value1 THEN result1 \[ WHEN value2 THEN result2 ... \] \[ ELSE resultN \] END`|Simple CASE.|
|`CASE WHEN boolean_expr1 THEN result1 \[ WHEN boolean_expr2 THEN result2 ... \] \[ ELSE resultN \] END`|Searched CASE.|
|`CAST(value AS TYPE)`|Cast value to another type. See [Data types](sql-data-types.md) for details about how Druid SQL handles CAST.|
|`COALESCE(value1, value2, ...)`|Returns the first value that is neither NULL nor empty string.|
|`DECODE_BASE64_COMPLEX(dataType, expr)`| Decodes a Base64-encoded string into a complex data type, where `dataType` is the complex data type and `expr` is the Base64-encoded string to decode. The `hyperUnique` and `serializablePairLongString` data types are supported by default. You can enable support for the following complex data types by loading their extensions:<br/><ul><li>`druid-bloom-filter`: `bloom`</li><li>`druid-datasketches`: `arrayOfDoublesSketch`, `HLLSketch`, `KllDoublesSketch`, `KllFloatsSketch`, `quantilesDoublesSketch`, `thetaSketch`</li><li>`druid-histogram`: `approximateHistogram`, `fixedBucketsHistogram`</li><li>`druid-stats`: `variance`</li><li>`druid-compressed-big-decimal`: `compressedBigDecimal`</li><li>`druid-momentsketch`: `momentSketch`</li><li>`druid-tdigestsketch`: `tDigestSketch`</li></ul>|
|`NULLIF(value1, value2)`|Returns NULL if `value1` and `value2` match, else returns `value1`.|
|`NVL(value1, value2)`|Returns `value1` if `value1` is not null, otherwise `value2`.|