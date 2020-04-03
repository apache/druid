---
id: queryfunctions
title: "Druid query functions"
sidebar_label: "Query functions"
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


For aggregation functions, see [Aggregation functions](aggregationfunctions) 


## Numeric functions

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
|`ROUND(expr[, digits])`|`ROUND(x, y)` would return the value of the x rounded to the y decimal places. While x can be an integer or floating-point number, y must be an integer. The type of the return value is specified by that of x. y defaults to 0 if omitted. When y is negative, x is rounded on the left side of the y decimal points.|
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

## String functions

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
|`LOOKUP(expr, lookupName)`|Look up expr in a registered [query-time lookup table](lookups.html).|
|`LOWER(expr)`|Returns expr in all lowercase.|
|`PARSE_LONG(string[, radix])`|Parses a string into a long (BIGINT) with the given radix, or 10 (decimal) if a radix is not provided.|
|`POSITION(needle IN haystack [FROM fromIndex])`|Returns the index of needle within haystack, with indexes starting from 1. The search will begin at fromIndex, or 1 if fromIndex is not specified. If the needle is not found, returns 0.|
|`REGEXP_EXTRACT(expr, pattern, [index])`|Apply regular expression pattern and extract a capture group, or null if there is no match. If index is unspecified or zero, returns the substring that matched the pattern.|
|`REPLACE(expr, pattern, replacement)`|Replaces pattern with replacement in expr, and returns the result.|
|`STRPOS(haystack, needle)`|Returns the index of needle within haystack, with indexes starting from 1. If the needle is not found, returns 0.|
|`SUBSTRING(expr, index, [length])`|Returns a substring of expr starting at index, with a max length, both measured in UTF-16 code units.|
|`RIGHT(expr, [length])`|Returns the rightmost length characters from expr.|
|`LEFT(expr, [length])`|Returns the leftmost length characters from expr.|
|`SUBSTR(expr, index, [length])`|Synonym for SUBSTRING.|
|<code>TRIM([BOTH &#124; LEADING &#124; TRAILING] [<chars> FROM] expr)</code>|Returns expr with characters removed from the leading, trailing, or both ends of "expr" if they are in "chars". If "chars" is not provided, it defaults to " " (a space). If the directional argument is not provided, it defaults to "BOTH".|
|`BTRIM(expr[, chars])`|Alternate form of `TRIM(BOTH <chars> FROM <expr>`).|
|`LTRIM(expr[, chars])`|Alternate form of `TRIM(LEADING <chars> FROM <expr>`).|
|`RTRIM(expr[, chars])`|Alternate form of `TRIM(TRAILING <chars> FROM <expr>`).|
|`UPPER(expr)`|Returns expr in all uppercase.|
|`REVERSE(expr)`|Reverses expr.|
|`REPEAT(expr, [N])`|Repeats expr N times|
|`LPAD(expr, length[, chars])`|Returns a string of "length" from "expr" left-padded with "chars". If "length" is shorter than the length of "expr", the result is "expr" which is truncated to "length". If either "expr" or "chars" are null, the result will be null.|
|`RPAD(expr, length[, chars])`|Returns a string of "length" from "expr" right-padded with "chars". If "length" is shorter than the length of "expr", the result is "expr" which is truncated to "length". If either "expr" or "chars" are null, the result will be null.|


## Time functions

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
(e.g., '192.168.0.1') or an IP address represented as an integer (e.g., 3232235521). The `subnet`
argument should be a string formatted as an IPv4 address subnet in CIDR notation (e.g.,
'192.168.0.0/16').

|Function|Notes|
|---|---|
|`IPV4_MATCH(address, subnet)`|Returns true if the `address` belongs to the `subnet` literal, else false. If `address` is not a valid IPv4 address, then false is returned. This function is more efficient if `address` is an integer instead of a string.|
|`IPV4_PARSE(address)`|Parses `address` into an IPv4 address stored as an integer . If `address` is an integer that is a valid IPv4 address, then it is passed through. Returns null if `address` cannot be represented as an IPv4 address.|
|`IPV4_STRINGIFY(address)`|Converts `address` into an IPv4 address dotted-decimal string. If `address` is a string that is a valid IPv4 address, then it is passed through. Returns null if `address` cannot be represented as an IPv4 address.|


## Comparison operators

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
|`x IN (subquery)`|True if x is returned by the subquery. See [Query execution](#query-execution) above for details about how Druid SQL handles `IN (subquery)`.|
|`x NOT IN (subquery)`|True if x is not returned by the subquery. See [Query execution](#query-execution) for details about how Druid SQL handles `IN (subquery)`.|
|`x AND y`|Boolean AND.|
|`x OR y`|Boolean OR.|
|`NOT x`|Boolean NOT.|

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

## Sketch operators

These functions operate on expressions or columns that return sketch objects.

### HLL sketch operators

The following functions operate on [DataSketches HLL sketches](../development/extensions-core/datasketches-hll.html).
The [DataSketches extension](../development/extensions-core/datasketches-extension.html) must be loaded to use the following functions.

|Function|Notes|
|--------|-----|
|`HLL_SKETCH_ESTIMATE(expr, [round])`|Returns the distinct count estimate from an HLL sketch. `expr` must return an HLL sketch. The optional `round` boolean parameter will round the estimate if set to `true`, with a default of `false`.|
|`HLL_SKETCH_ESTIMATE_WITH_ERROR_BOUNDS(expr, [numStdDev])`|Returns the distinct count estimate and error bounds from an HLL sketch. `expr` must return an HLL sketch. An optional `numStdDev` argument can be provided.|
|`HLL_SKETCH_UNION([lgK, tgtHllType], expr0, expr1, ...)`|Returns a union of HLL sketches, where each input expression must return an HLL sketch. The `lgK` and `tgtHllType` can be optionally specified as the first parameter; if provided, both optional parameters must be specified.|
|`HLL_SKETCH_TO_STRING(expr)`|Returns a human-readable string representation of an HLL sketch for debugging. `expr` must return an HLL sketch.|

### Theta sketch operators

The following functions operate on [theta sketches](../development/extensions-core/datasketches-theta.html).
The [DataSketches extension](../development/extensions-core/datasketches-extension.html) must be loaded to use the following functions.

|Function|Notes|
|--------|-----|
|`THETA_SKETCH_ESTIMATE(expr)`|Returns the distinct count estimate from a theta sketch. `expr` must return a theta sketch.|
|`THETA_SKETCH_ESTIMATE_WITH_ERROR_BOUNDS(expr, errorBoundsStdDev)`|Returns the distinct count estimate and error bounds from a theta sketch. `expr` must return a theta sketch.|
|`THETA_SKETCH_UNION([size], expr0, expr1, ...)`|Returns a union of theta sketches, where each input expression must return a theta sketch. The `size` can be optionally specified as the first parameter.|
|`THETA_SKETCH_INTERSECT([size], expr0, expr1, ...)`|Returns an intersection of theta sketches, where each input expression must return a theta sketch. The `size` can be optionally specified as the first parameter.|
|`THETA_SKETCH_NOT([size], expr0, expr1, ...)`|Returns a set difference of theta sketches, where each input expression must return a theta sketch. The `size` can be optionally specified as the first parameter.|

### Quantiles sketch operators

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

## Other functions

|Function|Notes|
|--------|-----|
|`CAST(value AS TYPE)`|Cast value to another type. See [Data types and casts](#data-types-and-casts) for details about how Druid SQL handles CAST.|
|`CASE expr WHEN value1 THEN result1 \[ WHEN value2 THEN result2 ... \] \[ ELSE resultN \] END`|Simple CASE.|
|`CASE WHEN boolean_expr1 THEN result1 \[ WHEN boolean_expr2 THEN result2 ... \] \[ ELSE resultN \] END`|Searched CASE.|
|`NULLIF(value1, value2)`|Returns NULL if value1 and value2 match, else returns value1.|
|`COALESCE(value1, value2, ...)`|Returns the first value that is neither NULL nor empty string.|
|`NVL(expr,expr-for-null)`|Returns 'expr-for-null' if 'expr' is null (or empty string for string type).|
|`BLOOM_FILTER_TEST(<expr>, <serialized-filter>)`|Returns true if the value is contained in the base64 serialized bloom filter. See [bloom filter extension](../development/extensions-core/bloom-filter.html) documentation for additional details.|

