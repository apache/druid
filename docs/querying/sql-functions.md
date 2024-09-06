---
id: sql-functions
title: "All Druid SQL functions"
sidebar_label: "All functions"
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


This page provides a reference of all Druid SQL functions in alphabetical order.
Click the linked function type for documentation on a particular function.

## ABS

`ABS(<NUMERIC>)`

**Function type:** [Scalar, numeric](sql-scalar.md#numeric-functions)

Calculates the absolute value of a numeric expression.

## ACOS

`ACOS(<NUMERIC>)`

**Function type:** [Scalar, numeric](sql-scalar.md#numeric-functions)

Calculates the arc cosine of a numeric expression.

## ANY_VALUE

`ANY_VALUE(expr, [maxBytesPerValue, [aggregateMultipleValues]])`

**Function type:** [Aggregation](sql-aggregations.md)

Returns any value of the specified expression.

## APPROX_COUNT_DISTINCT

`APPROX_COUNT_DISTINCT(expr)`

**Function type:** [Aggregation](sql-aggregations.md)

Counts distinct values of a regular column or a prebuilt sketch column.

`APPROX_COUNT_DISTINCT_BUILTIN(expr)`

**Function type:** [Aggregation](sql-aggregations.md)

Counts distinct values of a string, numeric, or `hyperUnique` column using Druid's built-in `cardinality` or `hyperUnique` aggregators.

## APPROX_COUNT_DISTINCT_DS_HLL

`APPROX_COUNT_DISTINCT_DS_HLL(expr, [<NUMERIC>, <CHARACTER>])`

**Function type:** [Aggregation](sql-aggregations.md)

Counts distinct values of an HLL sketch column or a regular column.

## APPROX_COUNT_DISTINCT_DS_THETA

`APPROX_COUNT_DISTINCT_DS_THETA(expr, [<NUMERIC>])`

**Function type:** [Aggregation](sql-aggregations.md)

Counts distinct values of a Theta sketch column or a regular column.

## APPROX_QUANTILE

`APPROX_QUANTILE(expr, <NUMERIC>, [<NUMERIC>])`

**Function type:** [Aggregation](sql-aggregations.md)

Deprecated in favor of `APPROX_QUANTILE_DS`.

## APPROX_QUANTILE_DS

`APPROX_QUANTILE_DS(expr, <NUMERIC>, [<NUMERIC>])`

**Function type:** [Aggregation](sql-aggregations.md)

Computes approximate quantiles on a Quantiles sketch column or a regular numeric column.

## APPROX_QUANTILE_FIXED_BUCKETS

`APPROX_QUANTILE_FIXED_BUCKETS(expr, <NUMERIC>, <NUMERIC>, <NUMERIC>, <NUMERIC>, [<CHARACTER>])`

**Function type:** [Aggregation](sql-aggregations.md)

Computes approximate quantiles on fixed buckets histogram column or a regular numeric column.

## ARRAY[]

`ARRAY[expr1, expr2, ...]`

**Function type:** [Array](sql-array-functions.md)

Constructs a SQL ARRAY literal from the expression arguments. The arguments must be of the same type.

## ARRAY_AGG

`ARRAY_AGG([DISTINCT] expr, [<NUMERIC>])`

**Function type:** [Aggregation](sql-aggregations.md)

Returns an array of all values of the specified expression.

## ARRAY_APPEND

`ARRAY_APPEND(arr1, expr)`

**Function type:** [Array](./sql-array-functions.md)

Appends `expr` to `arr`, the resulting array type determined by the type of `arr1`.

## ARRAY_CONCAT

`ARRAY_CONCAT(arr1, arr2)`

**Function type:** [Array](./sql-array-functions.md)

Concatenates `arr2` to `arr1`. The resulting array type is determined by the type of `arr1`.|

## ARRAY_CONCAT_AGG

`ARRAY_CONCAT_AGG([DISTINCT] expr, [<NUMERIC>])`

**Function type:** [Aggregation](sql-aggregations.md)

Concatenates array inputs into a single array.

## ARRAY_CONTAINS

`ARRAY_CONTAINS(arr, expr)`

**Function type:** [Array](./sql-array-functions.md)

If `expr` is a scalar type, returns true if `arr` contains `expr`. If `expr` is an array, returns 1 if `arr` contains all elements of `expr`. Otherwise returns false.


## ARRAY_LENGTH

`ARRAY_LENGTH(arr)`

**Function type:** [Array](./sql-array-functions.md)

Returns length of the array expression.

## ARRAY_OFFSET

`ARRAY_OFFSET(arr, long)`

**Function type:** [Array](./sql-array-functions.md)

Returns the array element at the 0-based index supplied, or null for an out of range index.

## ARRAY_OFFSET_OF

`ARRAY_OFFSET_OF(arr, expr)`

**Function type:** [Array](./sql-array-functions.md)

Returns the 0-based index of the first occurrence of `expr` in the array. If no matching elements exist in the array, returns `null` or `-1` if `druid.generic.useDefaultValueForNull=true` (deprecated legacy mode).

## ARRAY_ORDINAL

**Function type:** [Array](./sql-array-functions.md)

`ARRAY_ORDINAL(arr, long)`

Returns the array element at the 1-based index supplied, or null for an out of range index.
## ARRAY_ORDINAL_OF

`ARRAY_ORDINAL_OF(arr, expr)`

**Function type:** [Array](./sql-array-functions.md)

Returns the 1-based index of the first occurrence of `expr` in the array. If no matching elements exist in the array, returns `null` or `-1` if `druid.generic.useDefaultValueForNull=true` (deprecated legacy mode).

## ARRAY_OVERLAP

`ARRAY_OVERLAP(arr1, arr2)`

**Function type:** [Array](./sql-array-functions.md)

Returns true if `arr1` and `arr2` have any elements in common, else false.

## SCALAR_IN_ARRAY

`SCALAR_IN_ARRAY(expr, arr)`

**Function type:** [Array](./sql-array-functions.md)

Returns true if the scalar `expr` is present in `arr`. Otherwise, returns false if the scalar `expr` is non-null or
`UNKNOWN` if the scalar `expr` is `NULL`.

Returns `UNKNOWN` if `arr` is `NULL`.

## ARRAY_PREPEND

`ARRAY_PREPEND(expr, arr)`

**Function type:** [Array](./sql-array-functions.md)

Prepends `expr` to `arr` at the beginning, the resulting array type determined by the type of `arr`.

## ARRAY_SLICE

`ARRAY_SLICE(arr, start, end)`

**Function type:** [Array](./sql-array-functions.md)

Returns the subarray of `arr` from the 0-based index `start` (inclusive) to `end` (exclusive). Returns `null`, if `start` is less than 0, greater than length of `arr`, or greater than `end`.

## ARRAY_TO_MV

`ARRAY_TO_MV(arr)`

**Function type:** [Array](./sql-array-functions.md)

Converts an `ARRAY` of any type into a multi-value string `VARCHAR`.

## ARRAY_TO_STRING

`ARRAY_TO_STRING(arr, str)`

**Function type:** [Array](./sql-array-functions.md)

Joins all elements of `arr` by the delimiter specified by `str`.

## ASIN

`ASIN(<NUMERIC>)`

**Function type:** [Scalar, numeric](sql-scalar.md#numeric-functions)

Calculates the arc sine of a numeric expression.

## ATAN

`ATAN(<NUMERIC>)`

**Function type:** [Scalar, numeric](sql-scalar.md#numeric-functions)

Calculates the arc tangent of a numeric expression.

## ATAN2

`ATAN2(<NUMERIC>, <NUMERIC>)`

**Function type:** [Scalar, numeric](sql-scalar.md#numeric-functions)

Calculates the arc tangent of the two arguments.

## AVG

`AVG(<NUMERIC>)`

**Function type:** [Aggregation](sql-aggregations.md)

Calculates the average of a set of values.

## BIT_AND

`BIT_AND(expr)`

**Function type:** [Aggregation](sql-aggregations.md)

Performs a bitwise AND operation on all input values.

## BIT_OR

`BIT_OR(expr)`

**Function type:** [Aggregation](sql-aggregations.md)

Performs a bitwise OR operation on all input values.

## BIT_XOR

`BIT_XOR(expr)`

**Function type:** [Aggregation](sql-aggregations.md)

Performs a bitwise XOR operation on all input values.

## BITWISE_AND

`BITWISE_AND(expr1, expr2)`

**Function type:** [Scalar, numeric](sql-scalar.md#numeric-functions)

Returns the bitwise AND between the two expressions, that is, `expr1 & expr2`.

## BITWISE_COMPLEMENT

`BITWISE_COMPLEMENT(expr)`

**Function type:** [Scalar, numeric](sql-scalar.md#numeric-functions)

Returns the bitwise NOT for the expression, that is, `~expr`.

## BITWISE_CONVERT_DOUBLE_TO_LONG_BITS

`BITWISE_CONVERT_DOUBLE_TO_LONG_BITS(expr)`

**Function type:** [Scalar, numeric](sql-scalar.md#numeric-functions)

Converts the bits of an IEEE 754 floating-point double value to a long.

## BITWISE_CONVERT_LONG_BITS_TO_DOUBLE

`BITWISE_CONVERT_LONG_BITS_TO_DOUBLE(expr)`

**Function type:** [Scalar, numeric](sql-scalar.md#numeric-functions)

Converts a long to the IEEE 754 floating-point double specified by the bits stored in the long.

## BITWISE_OR

`BITWISE_OR(expr1, expr2)`

**Function type:** [Scalar, numeric](sql-scalar.md#numeric-functions)

Returns the bitwise OR between the two expressions, that is, `expr1 | expr2`.

## BITWISE_SHIFT_LEFT

`BITWISE_SHIFT_LEFT(expr1, expr2)`

**Function type:** [Scalar, numeric](sql-scalar.md#numeric-functions)

Returns a bitwise left shift of expr1, that is, `expr1 << expr2`.

## BITWISE_SHIFT_RIGHT

`BITWISE_SHIFT_RIGHT(expr1, expr2)`

**Function type:** [Scalar, numeric](sql-scalar.md#numeric-functions)

Returns a bitwise right shift of expr1, that is, `expr1 >> expr2`.

## BITWISE_XOR

`BITWISE_XOR(expr1, expr2)`

**Function type:** [Scalar, numeric](sql-scalar.md#numeric-functions)

Returns the bitwise exclusive OR between the two expressions, that is, `expr1 ^ expr2`.

## BLOOM_FILTER

`BLOOM_FILTER(expr, <NUMERIC>)`

**Function type:** [Aggregation](sql-aggregations.md)

Computes a Bloom filter from values produced by the specified expression.

## BLOOM_FILTER_TEST

`BLOOM_FILTER_TEST(expr, <STRING>)`

**Function type:** [Scalar, other](sql-scalar.md#other-scalar-functions)

Returns true if the expression is contained in a Base64-serialized Bloom filter.

## BTRIM

`BTRIM(<CHARACTER>, [<CHARACTER>])`

**Function type:** [Scalar, string](sql-scalar.md#string-functions)

Trims characters from both the leading and trailing ends of an expression.

## CASE

`CASE expr WHEN value1 THEN result1 \[ WHEN value2 THEN result2 ... \] \[ ELSE resultN \] END`

**Function type:** [Scalar, other](sql-scalar.md#other-scalar-functions)

Returns a result based on a given condition.

## CAST

`CAST(value AS TYPE)`

**Function type:** [Scalar, other](sql-scalar.md#other-scalar-functions)

Converts a value into the specified data type.

## CEIL (date and time)

Rounds up a timestamp by a given time unit.

* **Syntax:** `CEIL(timestamp_expr TO unit>)`
* **Function type:** Scalar, date and time

<details><summary>Example</summary>

The following example rounds up the `__time` column from the `taxi-trips` datasource to the nearest year.

```sql
SELECT
  "__time" AS "original_time",
  CEIL("__time" TO YEAR) AS "ceiling"
FROM "taxi-trips"
LIMIT 1
```

Returns the following:

| `original_time` | `ceiling` |
| -- | -- |
| `2013-08-01T08:14:37.000Z` | `2014-01-01T00:00:00.000Z` |

</details>

[Learn more](sql-scalar.md#date-and-time-functions)

## CEIL (numeric)

`CEIL(<NUMERIC>)`

**Function type:** [Scalar, numeric](sql-scalar.md#numeric-functions)

Calculates the smallest integer value greater than or equal to the numeric expression.

## CHAR_LENGTH

`CHAR_LENGTH(expr)`

**Function type:** [Scalar, string](sql-scalar.md#string-functions)

Alias for [`LENGTH`](#length).

## CHARACTER_LENGTH

`CHARACTER_LENGTH(expr)`

**Function type:** [Scalar, string](sql-scalar.md#string-functions)

Alias for [`LENGTH`](#length).

## COALESCE

`COALESCE(expr, expr, ...)`

**Function type:** [Scalar, other](sql-scalar.md#other-scalar-functions)

Returns the first non-null value.

## CONCAT

`CONCAT(expr, expr...)`

**Function type:** [Scalar, string](sql-scalar.md#string-functions)

Concatenates a list of expressions.

## CONTAINS_STRING

`CONTAINS_STRING(<CHARACTER>, <CHARACTER>)`

**Function type:** [Scalar, string](sql-scalar.md#string-functions)

Finds whether a string is in a given expression, case-sensitive.

## COS

`COS(<NUMERIC>)`

**Function type:** [Scalar, numeric](sql-scalar.md#numeric-functions)

Calculates the trigonometric cosine of an angle expressed in radians.

## COT

`COT(<NUMERIC>)`

**Function type:** [Scalar, numeric](sql-scalar.md#numeric-functions)

Calculates the trigonometric cotangent of an angle expressed in radians.

## COUNT

`COUNT([DISTINCT] expr)`

`COUNT(*)`

**Function type:** [Aggregation](sql-aggregations.md)

Counts the number of rows.

## CUME_DIST

`CUME_DIST()`

**Function type:** [Window](sql-window-functions.md#window-function-reference)

Returns the cumulative distribution of the current row within the window calculated as `number of window rows at the same rank or higher than current row` / `total window rows`. The return value ranges between `1/number of rows` and 1.

## CURRENT_DATE

`CURRENT_DATE`

**Function type:** [Scalar, date and time](sql-scalar.md#date-and-time-functions)

Returns the current date in the connection's time zone.

## CURRENT_TIMESTAMP

`CURRENT_TIMESTAMP`

**Function type:** [Scalar, date and time](sql-scalar.md#date-and-time-functions)

Returns the current timestamp in the connection's time zone.

## DATE_TRUNC

`DATE_TRUNC(<CHARACTER>, <TIMESTAMP>)`

**Function type:** [Scalar, date and time](sql-scalar.md#date-and-time-functions)

Rounds down a timestamp by a given time unit.

## DECODE_BASE64_COMPLEX

`DECODE_BASE64_COMPLEX(dataType, expr)`

**Function type:** [Scalar, other](sql-scalar.md#other-scalar-functions)

Decodes a Base64-encoded string into a complex data type, where `dataType` is the complex data type and `expr` is the Base64-encoded string to decode.

## DECODE_BASE64_UTF8

`DECODE_BASE64_UTF8(expr)`

**Function type:** [Scalar, string](sql-scalar.md#string-functions)


Decodes a Base64-encoded string into a UTF-8 encoded string.

## DEGREES

`DEGREES(<NUMERIC>)`

**Function type:** [Scalar, numeric](sql-scalar.md#numeric-functions)

Converts an angle from radians to degrees.

## DENSE_RANK

`DENSE_RANK()`

**Function type:** [Window](sql-window-functions.md#window-function-reference)

Returns the rank for a row within a window without gaps. For example, if two rows tie for a rank of 1, the subsequent row is ranked 2.

## DIV

`DIV(x, y)`

**Function type:** [Scalar, numeric](sql-scalar.md#numeric-functions)

Returns the result of integer division of `x` by `y`.

## DS_CDF

`DS_CDF(expr, splitPoint0, splitPoint1, ...)`

**Function type:** [Scalar, sketch](sql-scalar.md#sketch-functions)

Returns a string representing an approximation to the Cumulative Distribution Function given the specified bin definition.

## DS_GET_QUANTILE

`DS_GET_QUANTILE(expr, fraction)`

**Function type:** [Scalar, sketch](sql-scalar.md#sketch-functions)

Returns the quantile estimate corresponding to `fraction` from a quantiles sketch.

## DS_GET_QUANTILES

`DS_GET_QUANTILES(expr, fraction0, fraction1, ...)`

**Function type:** [Scalar, sketch](sql-scalar.md#sketch-functions)

Returns a string representing an array of quantile estimates corresponding to a list of fractions from a quantiles sketch.

## DS_HISTOGRAM

`DS_HISTOGRAM(expr, splitPoint0, splitPoint1, ...)`

**Function type:** [Scalar, sketch](sql-scalar.md#sketch-functions)

Returns a string representing an approximation to the histogram given the specified bin definition.


## DS_HLL

`DS_HLL(expr, [lgK, tgtHllType])`

**Function type:** [Aggregation](sql-aggregations.md)

Creates an HLL sketch on a column containing HLL sketches or a regular column.

## DS_QUANTILE_SUMMARY

`DS_QUANTILE_SUMMARY(expr)`

**Function type:** [Scalar, sketch](sql-scalar.md#sketch-functions)

Returns a string summary of a quantiles sketch.

## DS_QUANTILES_SKETCH

`DS_QUANTILES_SKETCH(expr, [k])`

**Function type:** [Aggregation](sql-aggregations.md)

Creates a Quantiles sketch on a column containing Quantiles sketches or a regular column.

## DS_RANK

`DS_RANK(expr, value)`

**Function type:** [Scalar, sketch](sql-scalar.md#sketch-functions)

Returns an approximate rank between 0 and 1 of a given value, in which the rank signifies the fraction of the distribution less than the given value.

## DS_THETA

`DS_THETA(expr, [size])`

**Function type:** [Aggregation](sql-aggregations.md)

Creates a Theta sketch on a column containing Theta sketches or a regular column.

## DS_TUPLE_DOUBLES

`DS_TUPLE_DOUBLES(expr, [nominalEntries])`

`DS_TUPLE_DOUBLES(dimensionColumnExpr, metricColumnExpr, ..., [nominalEntries])`

**Function type:** [Aggregation](sql-aggregations.md)

Creates a Tuple sketch which contains an array of double values as the Summary Object. If the last value of the array is a numeric literal, Druid assumes that the value is an override parameter for [nominal entries](../development/extensions-core/datasketches-tuple.md).

## DS_TUPLE_DOUBLES_INTERSECT

`DS_TUPLE_DOUBLES_INTERSECT(expr, ..., [nominalEntries])`

**Function type:** [Scalar, sketch](sql-scalar.md#tuple-sketch-functions)

Returns an intersection of Tuple sketches which each contain an array of double values as their Summary Objects. The values contained in the Summary Objects are summed when combined. If the last value of the array is a numeric literal, Druid assumes that the value is an override parameter for [nominal entries](../development/extensions-core/datasketches-tuple.md).

## DS_TUPLE_DOUBLES_METRICS_SUM_ESTIMATE

`DS_TUPLE_DOUBLES_METRICS_SUM_ESTIMATE(expr)`

**Function type:** [Scalar, sketch](sql-scalar.md#tuple-sketch-functions)

Computes approximate sums of the values contained within a Tuple sketch which contains an array of double values as the Summary Object.

## DS_TUPLE_DOUBLES_NOT

`DS_TUPLE_DOUBLES_NOT(expr, ..., [nominalEntries])`

**Function type:** [Scalar, sketch](sql-scalar.md#tuple-sketch-functions)

Returns a set difference of Tuple sketches which each contain an array of double values as their Summary Objects. The values contained in the Summary Object are preserved as is. If the last value of the array is a numeric literal, Druid assumes that the value is an override parameter for [nominal entries](../development/extensions-core/datasketches-tuple.md).

## DS_TUPLE_DOUBLES_UNION

`DS_TUPLE_DOUBLES_UNION(expr, ..., [nominalEntries])`

**Function type:** [Scalar, sketch](sql-scalar.md#tuple-sketch-functions)

Returns a union of Tuple sketches which each contain an array of double values as their Summary Objects. The values contained in the Summary Objects are summed when combined. If the last value of the array is a numeric literal, Druid assumes that the value is an override parameter for [nominal entries](../development/extensions-core/datasketches-tuple.md).

## EARLIEST

`EARLIEST(expr, [maxBytesPerValue])`

**Function type:** [Aggregation](sql-aggregations.md)

Returns the value of a numeric or string expression corresponding to the earliest `__time` value.

## EARLIEST_BY

`EARLIEST_BY(expr, timestampExpr, [maxBytesPerValue])`

**Function type:** [Aggregation](sql-aggregations.md)

Returns the value of a numeric or string expression corresponding to the earliest time value from `timestampExpr`.

## EXP

`EXP(<NUMERIC>)`

**Function type:** [Scalar, numeric](sql-scalar.md#numeric-functions)

Calculates _e_ raised to the power of the numeric expression.

## EXTRACT

Extracts the value of some unit from the timestamp.

* **Syntax:** `EXTRACT(unit FROM timestamp_expr)`
* **Function type:** Scalar, date and time

<details><summary>Example</summary>

The following example extracts the year from the `__time` column from the `taxi-trips` datasource.

```sql
SELECT 
  "__time" AS "original_time",
  EXTRACT(YEAR FROM "__time" ) AS "year"
FROM "taxi-trips"
LIMIT 1
```

Returns the following:

| `original_time` | `year` |
| -- | -- |
| `2013-08-01T08:14:37.000Z` | `2013` |

</details>

[Learn more](sql-scalar.md#date-and-time-functions)

## FIRST_VALUE

`FIRST_VALUE(expr)`

**Function type:** [Window](sql-window-functions.md#window-function-reference)

Returns the value evaluated for the expression for the first row within the window.

## FLOOR (date and time)

Rounds down a timestamp by a given time unit. 

* **Syntax:** `FLOOR(timestamp_expr TO unit>)`
* **Function type:** Scalar, date and time

<details><summary>Example</summary>

The following example rounds down the `__time` column from the `taxi-trips` datasource to the nearest year.

```sql
SELECT
  "__time" AS "original_time",
  FLOOR("__time" TO YEAR) AS "floor"
FROM "taxi-trips"
LIMIT 1
```

Returns the following:

| `original_time` | `floor` |
| -- | -- |
| `2013-08-01T08:14:37.000Z` | `2013-01-01T00:00:00.000Z` |

</details>

[Learn more](sql-scalar.md#date-and-time-functions)

## FLOOR (numeric)

`FLOOR(<NUMERIC>)`

**Function type:** [Scalar, numeric](sql-scalar.md#numeric-functions)

Calculates the largest integer value less than or equal to the numeric expression.

## GREATEST

Returns the maximum value from the provided expressions. For information on how Druid interprets the arguments passed into the function, see [Reduction functions](sql-scalar.md#reduction-functions).

* **Syntax:** `GREATEST([expr1, ...])`
* **Function type:** Scalar, reduction

<details><summary>Example</summary>

The following example returns the greatest value between the numeric constant `PI`, the integer number `4`, and the double `-5.0`. Druid interprets these arguments as DOUBLE data type.

```sql
SELECT GREATEST(PI, 4, -5.0) AS "greatest"
```

Returns the following:

| `greatest` |
| -- | 
| `4` |

</details>

[Learn more](sql-scalar.md#reduction-functions)


## GROUPING

`GROUPING(expr, expr...)`

**Function type:** [Aggregation](sql-aggregations.md)

Returns a number for each output row of a groupBy query, indicating whether the specified dimension is included for that row.

## HLL_SKETCH_ESTIMATE

`HLL_SKETCH_ESTIMATE(expr, [round])`

**Function type:** [Scalar, sketch](sql-scalar.md#sketch-functions)

Returns the distinct count estimate from an HLL sketch.

## HLL_SKETCH_ESTIMATE_WITH_ERROR_BOUNDS

`HLL_SKETCH_ESTIMATE_WITH_ERROR_BOUNDS(expr, [numStdDev])`

**Function type:** [Scalar, sketch](sql-scalar.md#sketch-functions)

Returns the distinct count estimate and error bounds from an HLL sketch.

## HLL_SKETCH_TO_STRING

`HLL_SKETCH_TO_STRING(expr)`

**Function type:** [Scalar, sketch](sql-scalar.md#sketch-functions)

Returns a human-readable string representation of an HLL sketch.

## HLL_SKETCH_UNION

`HLL_SKETCH_UNION([lgK, tgtHllType], expr0, expr1, ...)`

**Function type:** [Scalar, sketch](sql-scalar.md#sketch-functions)

Returns a union of HLL sketches.

## HUMAN_READABLE_BINARY_BYTE_FORMAT

`HUMAN_READABLE_BINARY_BYTE_FORMAT(value[, precision])`

**Function type:** [Scalar, numeric](sql-scalar.md#numeric-functions)

Converts an integer byte size into human-readable IEC format.

## HUMAN_READABLE_DECIMAL_BYTE_FORMAT

`HUMAN_READABLE_DECIMAL_BYTE_FORMAT(value[, precision])`

**Function type:** [Scalar, numeric](sql-scalar.md#numeric-functions)

Converts a byte size into human-readable SI format.

## HUMAN_READABLE_DECIMAL_FORMAT

`HUMAN_READABLE_DECIMAL_FORMAT(value[, precision])`

**Function type:** [Scalar, numeric](sql-scalar.md#numeric-functions)

Converts a byte size into human-readable SI format with single-character units.

## ICONTAINS_STRING

`ICONTAINS_STRING(<expr>, str)`

**Function type:** [Scalar, string](sql-scalar.md#string-functions)

Finds whether a string is in a given expression, case-insensitive.

## IPV4_MATCH

Returns true if the IPv4 `address` belongs to the `subnet` literal, otherwise returns false.

* **Syntax:** `IPV4_MATCH(address, subnet)`
* **Function type:** Scalar, IP address

<details><summary>Example</summary>

The following example returns true if the IPv4 address in the `forward_for` column from the `kttm` datasource belongs to the subnet `181.13.41.0/24`.

```sql
SELECT 
  "forwarded_for" AS "ipv4_address",
  IPV4_MATCH("forwarded_for", '181.13.41.0/24') AS "belongs_in_subnet"
FROM "kttm"
LIMIT 2
```

Returns the following:

| `ipv4_address` | `belongs_in_subnet`|
| -- | -- | 
| `181.13.41.82` | `true`|
| `177.242.100.0` | `false`|

</details>


[Learn more](sql-scalar.md#ip-address-functions)


## IPV4_PARSE

Parses an IPv4 `address` into its integer notation.

* **Syntax:** `IPV4_PARSE(address)`
* **Function type:** Scalar, IP address

<details><summary>Example</summary>

The following example returns an integer that represents the IPv4 address `5.5.5.5`.

```sql
SELECT 
  '5.5.5.5' AS "ipv4_address",
  IPV4_PARSE('5.5.5.5') AS "integer"
```

Returns the following:

| `ipv4_address` | `integer` |
| -- | -- |
| `5.5.5.5` | `84215045` |

</details>

[Learn more](sql-scalar.md#ip-address-functions)

## IPV4_STRINGIFY

Converts an IPv4 `address` in integer notation into dot-decimal notation.

* **Syntax:** `IPV4_STRINGIFY(address)`
* **Function type:** Scalar, IP address

<details><summary>Example</summary>

The following example returns the integer `84215045` in IPv4 dot-decimal notation.

```sql
SELECT 
  '84215045' AS "integer",
  IPV4_STRINGIFY(84215045) AS "dot_decimal_notation"
```

Returns the following:

| `integer` | `dot_decimal_notation` |
| -- | -- |
| `84215045` | `5.5.5.5` |

</details>

[Learn more](sql-scalar.md#ip-address-functions)

## IPV6_MATCH

Returns true if the IPv6 `address` belongs to the `subnet` literal. Otherwise, returns false.

* **Syntax:** `IPV6_MATCH(address, subnet)`
* **Function type:** Scalar, IP address

<details><summary>Example</summary>

The following example returns true because `75e9:efa4:29c6:85f6::232c` is in the subnet of `75e9:efa4:29c6:85f6::/64`.

```sql
SELECT 
  '75e9:efa4:29c6:85f6::232c' AS "ipv6_address",
  IPV6_MATCH('75e9:efa4:29c6:85f6::232c', '75e9:efa4:29c6:85f6::/64') AS "belongs_in_subnet" 
```

Returns the following: 

| `ipv6_address` | `belongs_in_subnet` | 
| -- | -- |
| `75e9:efa4:29c6:85f6::232c` | `true` | 


</details>

[Learn more](sql-scalar.md#ip-address-functions)


## JSON_KEYS

**Function type:** [JSON](sql-json-functions.md)

`JSON_KEYS(expr, path)`

Returns an array of field names from `expr` at the specified `path`.

## JSON_OBJECT

**Function type:** [JSON](sql-json-functions.md)

`JSON_OBJECT(KEY expr1 VALUE expr2[, KEY expr3 VALUE expr4, ...])`

Constructs a new `COMPLEX<json>` object. The `KEY` expressions must evaluate to string types. The `VALUE` expressions can be composed of any input type, including other `COMPLEX<json>` values. `JSON_OBJECT` can accept colon-separated key-value pairs. The following syntax is equivalent: `JSON_OBJECT(expr1:expr2[, expr3:expr4, ...])`.

## JSON_PATHS

**Function type:** [JSON](sql-json-functions.md)

`JSON_PATHS(expr)`

Returns an array of all paths which refer to literal values in `expr` in JSONPath format.

## JSON_QUERY

**Function type:** [JSON](sql-json-functions.md)

`JSON_QUERY(expr, path)`

Extracts a `COMPLEX<json>` value from `expr`, at the specified `path`.

## JSON_QUERY_ARRAY

**Function type:** [JSON](sql-json-functions.md)

`JSON_QUERY_ARRAY(expr, path)`

Extracts an `ARRAY<COMPLEX<json>>` value from `expr` at the specified `path`. If value is not an `ARRAY`, it gets translated into a single element `ARRAY` containing the value at `path`. The primary use of this function is to extract arrays of objects to use as inputs to other [array functions](./sql-array-functions.md).

## JSON_VALUE

**Function type:** [JSON](sql-json-functions.md)

`JSON_VALUE(expr, path [RETURNING sqlType])`

Extracts a literal value from `expr` at the specified `path`. If you specify `RETURNING` and an SQL type name (such as `VARCHAR`, `BIGINT`, `DOUBLE`, etc) the function plans the query using the suggested type. Otherwise, it attempts to infer the type based on the context. If it can't infer the type, it defaults to `VARCHAR`.

## LAG

`LAG(expr[, offset])`

**Function type:** [Window](sql-window-functions.md#window-function-reference)

If you do not supply an `offset`, returns the value evaluated at the row preceding the current row. Specify an offset number `n` to return the value evaluated at `n` rows preceding the current one.

## LAST_VALUE

`LAST_VALUE(expr)`

**Function type:** [Window](sql-window-functions.md#window-function-reference)

Returns the value evaluated for the expression for the last row within the window.

## LATEST

`LATEST(expr, [maxBytesPerValue])`

**Function type:** [Aggregation](sql-aggregations.md)

Returns the value of a numeric or string expression corresponding to the latest `__time` value.

## LATEST_BY

`LATEST_BY(expr, timestampExpr, [maxBytesPerValue])`

**Function type:** [Aggregation](sql-aggregations.md)

Returns the value of a numeric or string expression corresponding to the latest time value from `timestampExpr`.

## LEAD

`LEAD(expr[, offset])`

**Function type:** [Window](sql-window-functions.md#window-function-reference)

If you do not supply an `offset`, returns the value evaluated at the row following the current row. Specify an offset number `n` to return the value evaluated at `n` rows following the current one; if there is no such row, returns the given default value.

## LEAST

Returns the minimum value from the provided expressions. For information on how Druid interprets the arguments passed into the function, see [Reduction functions](sql-scalar.md#reduction-functions).

* **Syntax:** `LEAST([expr1, ...])`
* **Function type:** Scalar, reduction

<details><summary>Example</summary>

The following example returns the minimum value between the strings `apple`, `orange`, and `pear`. Druid interprets these arguments as STRING data type. 

```sql
SELECT LEAST( 'apple', 'orange', 'pear') AS "least"
```

Returns the following:

| `least` |
| -- |
| `apple` |

</details>

[Learn more](sql-scalar.md#reduction-functions)


## LEFT

`LEFT(expr, [length])`

**Function type:** [Scalar, string](sql-scalar.md#string-functions)

Returns the leftmost number of characters from an expression.

## LENGTH

`LENGTH(expr)`

**Function type:** [Scalar, string](sql-scalar.md#string-functions)

Returns the length of the expression in UTF-16 encoding.

## LN

`LN(expr)`

**Function type:** [Scalar, numeric](sql-scalar.md#numeric-functions)

Calculates the natural logarithm of the numeric expression.

## LOG10

`LOG10(expr)`

**Function type:** [Scalar, numeric](sql-scalar.md#numeric-functions)

Calculates the base-10 of the numeric expression.

## LOOKUP

`LOOKUP(<CHARACTER>, <CHARACTER>[, <CHARACTER>])`

**Function type:** [Scalar, string](sql-scalar.md#string-functions)

Looks up the expression in a registered query-time lookup table.

## LOWER

`LOWER(expr)`

**Function type:** [Scalar, string](sql-scalar.md#string-functions)

Returns the expression in lowercase.

## LPAD

`LPAD(<CHARACTER>, <INTEGER>, [<CHARACTER>])`

**Function type:** [Scalar, string](sql-scalar.md#string-functions)

Returns the leftmost number of characters from an expression, optionally padded with the given characters.

## LTRIM

`LTRIM(<CHARACTER>, [<CHARACTER>])`

**Function type:** [Scalar, string](sql-scalar.md#string-functions)

Trims characters from the leading end of an expression.

## MAX

`MAX(expr)`

**Function type:** [Aggregation](sql-aggregations.md)

Returns the maximum value of a set of values.

## MILLIS_TO_TIMESTAMP

Converts a number of milliseconds since epoch into a timestamp.

* **Syntax:** `MILLIS_TO_TIMESTAMP(millis_expr)`
* **Function type:** Scalar, date and time

<details><summary>Example</summary>

The following example converts 1375344877000 milliseconds from epoch into a timestamp. 

```sql
SELECT MILLIS_TO_TIMESTAMP(1375344877000) AS "timestamp"
```

Returns the following:

| `timestamp` |
| -- |
| `2013-08-01T08:14:37.000Z` |

</details>

[Learn more](sql-scalar.md#date-and-time-functions)

## MIN

`MIN(expr)`

**Function type:** [Aggregation](sql-aggregations.md)

Returns the minimum value of a set of values.

## MOD

`MOD(x, y)`

**Function type:** [Scalar, numeric](sql-scalar.md#numeric-functions)

Calculates x modulo y, or the remainder of x divided by y.

## MV_APPEND

`MV_APPEND(arr1, expr)`

**Function type:** [Multi-value string](sql-multivalue-string-functions.md)

Adds the expression to the end of the array.

## MV_CONCAT

`MV_CONCAT(arr1, arr2)`

**Function type:** [Multi-value string](sql-multivalue-string-functions.md)

Concatenates two arrays.

## MV_CONTAINS

`MV_CONTAINS(arr, expr)`

**Function type:** [Multi-value string](sql-multivalue-string-functions.md)

Returns true if the expression is in the array, false otherwise.

## MV_FILTER_NONE

`MV_FILTER_NONE(expr, arr)`

**Function type:** [Multi-value string](sql-multivalue-string-functions.md)

Filters a multi-value expression to include no values contained in the array.

## MV_FILTER_ONLY

`MV_FILTER_ONLY(expr, arr)`

**Function type:** [Multi-value string](sql-multivalue-string-functions.md)

Filters a multi-value expression to include only values contained in the array.

## MV_LENGTH

`MV_LENGTH(arr)`

**Function type:** [Multi-value string](sql-multivalue-string-functions.md)

Returns the length of an array expression.

## MV_OFFSET

`MV_OFFSET(arr, long)`

**Function type:** [Multi-value string](sql-multivalue-string-functions.md)

Returns the array element at the given zero-based index.

## MV_OFFSET_OF

`MV_OFFSET_OF(arr, expr)`

**Function type:** [Multi-value string](sql-multivalue-string-functions.md)

Returns the zero-based index of the first occurrence of a given expression in the array.

## MV_ORDINAL

`MV_ORDINAL(arr, long)`

**Function type:** [Multi-value string](sql-multivalue-string-functions.md)

Returns the array element at the given one-based index.

## MV_ORDINAL_OF

`MV_ORDINAL_OF(arr, expr)`

**Function type:** [Multi-value string](sql-multivalue-string-functions.md)

Returns the one-based index of the first occurrence of a given expression.

## MV_OVERLAP

`MV_OVERLAP(arr1, arr2)`

**Function type:** [Multi-value string](sql-multivalue-string-functions.md)

Returns true if the two arrays have any elements in common, false otherwise.

## MV_PREPEND

`MV_PREPEND(expr, arr)`

**Function type:** [Multi-value string](sql-multivalue-string-functions.md)

Adds the expression to the beginning of the array.

## MV_SLICE

`MV_SLICE(arr, start, end)`

**Function type:** [Multi-value string](sql-multivalue-string-functions.md)

Returns a slice of the array from the zero-based start and end indexes.

## MV_TO_ARRAY

`MV_TO_ARRAY(str)`

**Function type:** [Multi-value string](sql-multivalue-string-functions.md)

Converts a multi-value string from a `VARCHAR` to a `VARCHAR ARRAY`.

## MV_TO_STRING

`MV_TO_STRING(arr, str)`

**Function type:** [Multi-value string](sql-multivalue-string-functions.md)

Joins all elements of the array together by the given delimiter.

## NTILE

`NTILE(tiles)`

**Function type:** [Window](sql-window-functions.md#window-function-reference)

Divides the rows within a window as evenly as possible into the number of tiles, also called buckets, and returns the value of the tile that the row falls into.

## NULLIF

`NULLIF(value1, value2)`

**Function type:** [Scalar, other](sql-scalar.md#other-scalar-functions)

Returns NULL if two values are equal, else returns the first value.

## NVL

`NVL(e1, e2)`

**Function type:** [Scalar, other](sql-scalar.md#other-scalar-functions)

Returns `e2` if `e1` is null, else returns `e1`.

## PARSE_JSON

**Function type:** [JSON](sql-json-functions.md)

`PARSE_JSON(expr)`

Parses `expr` into a `COMPLEX<json>` object. This operator deserializes JSON values when processing them, translating stringified JSON into a nested structure. If the input is not a `VARCHAR` or it is invalid JSON, this function will result in an error.

## PARSE_LONG

`PARSE_LONG(<CHARACTER>, [<INTEGER>])`

**Function type:** [Scalar, string](sql-scalar.md#string-functions)

Converts a string into a BIGINT with the given base or into a DECIMAL data type if the base is not specified.

## PERCENT_RANK

`PERCENT_RANK()`

**Function type:** [Window](sql-window-functions.md#window-function-reference)

Returns the relative rank of the row calculated as a percentage according to the formula: `RANK() OVER (window) / COUNT(1) OVER (window)`.

## POSITION

`POSITION(<CHARACTER> IN <CHARACTER> [FROM <INTEGER>])`

**Function type:** [Scalar, string](sql-scalar.md#string-functions)

Returns the one-based index position of a substring within an expression, optionally starting from a given one-based index.

## POWER

`POWER(expr, power)`

**Function type:** [Scalar, numeric](sql-scalar.md#numeric-functions)

Calculates a numerical expression raised to the specified power.

## RADIANS

`RADIANS(expr)`

**Function type:** [Scalar, numeric](sql-scalar.md#numeric-functions)

Converts an angle from degrees to radians.

## RANK

`RANK()`

**Function type:** [Window](sql-window-functions.md#window-function-reference)

Returns the rank with gaps for a row within a window. For example, if two rows tie for rank 1, the next rank is 3.

## REGEXP_EXTRACT

`REGEXP_EXTRACT(<CHARACTER>, <CHARACTER>, [<INTEGER>])`

**Function type:** [Scalar, string](sql-scalar.md#string-functions)

Applies a regular expression to the string expression and returns the _n_th match.

## REGEXP_LIKE

`REGEXP_LIKE(<CHARACTER>, <CHARACTER>)`

**Function type:** [Scalar, string](sql-scalar.md#string-functions)

Returns true or false signifying whether the regular expression finds a match in the string expression.

## REGEXP_REPLACE

`REGEXP_REPLACE(<CHARACTER>, <CHARACTER>, <CHARACTER>)`

**Function type:** [Scalar, string](sql-scalar.md#string-functions)

Replaces all occurrences of a regular expression in a string expression with a replacement string. The replacement
string may refer to capture groups using `$1`, `$2`, etc.

## REPEAT

`REPEAT(<CHARACTER>, [<INTEGER>])`

**Function type:** [Scalar, string](sql-scalar.md#string-functions)

Repeats the string expression an integer number of times.

## REPLACE

`REPLACE(expr, pattern, replacement)`

**Function type:** [Scalar, string](sql-scalar.md#string-functions)

Replaces a pattern with another string in the given expression.

## REVERSE

`REVERSE(expr)`

**Function type:** [Scalar, string](sql-scalar.md#string-functions)

Reverses the given expression.

## RIGHT

`RIGHT(expr, [length])`

**Function type:** [Scalar, string](sql-scalar.md#string-functions)

Returns the rightmost number of characters from an expression.

## ROUND

`ROUND(expr[, digits])`

**Function type:** [Scalar, numeric](sql-scalar.md#numeric-functions)

Calculates the rounded value for a numerical expression.

## ROW_NUMBER

`ROW_NUMBER()`

**Function type:** [Window](sql-window-functions.md#window-function-reference)

Returns the number of the row within the window starting from 1.

## RPAD

`RPAD(<CHARACTER>, <INTEGER>, [<CHARACTER>])`

**Function type:** [Scalar, string](sql-scalar.md#string-functions)

Returns the rightmost number of characters from an expression, optionally padded with the given characters.

## RTRIM

`RTRIM(<CHARACTER>, [<CHARACTER>])`

**Function type:** [Scalar, string](sql-scalar.md#string-functions)

Trims characters from the trailing end of an expression.

## SAFE_DIVIDE

`SAFE_DIVIDE(x, y)`

**Function type:** [Scalar, numeric](sql-scalar.md#numeric-functions)

Returns `x` divided by `y`, guarded on division by 0.

## SIN

`SIN(expr)`

**Function type:** [Scalar, numeric](sql-scalar.md#numeric-functions)

Calculates the trigonometric sine of an angle expressed in radians.

## SQRT

`SQRT(expr)`

**Function type:** [Scalar, numeric](sql-scalar.md#numeric-functions)

Calculates the square root of a numeric expression.

## STDDEV

`STDDEV(expr)`

**Function type:** [Aggregation](sql-aggregations.md)

Alias for [`STDDEV_SAMP`](#stddev_samp).

## STDDEV_POP

`STDDEV_POP(expr)`

**Function type:** [Aggregation](sql-aggregations.md)

Calculates the population standard deviation of a set of values.

## STDDEV_SAMP

`STDDEV_SAMP(expr)`

**Function type:** [Aggregation](sql-aggregations.md)

Calculates the sample standard deviation of a set of values.

## STRING_AGG

`STRING_AGG(expr, separator, [size])`

**Function type:** [Aggregation](sql-aggregations.md)

Collects all values of an expression into a single string.

## STRING_TO_ARRAY

`STRING_TO_ARRAY(str1, str2)`

**Function type:** [Array](sql-array-functions.md)

Splits `str1` into an array on the delimiter specified by `str2`, which is a regular expression.


## STRING_FORMAT

`STRING_FORMAT(pattern[, args...])`

**Function type:** [Scalar, string](sql-scalar.md#string-functions)

Returns a string formatted in accordance to Java's String.format method.

## STRING_TO_MV

`STRING_TO_MV(str1, str2)`

**Function type:** [Multi-value string](sql-multivalue-string-functions.md)

Splits `str1` into an multi-value string on the delimiter specified by `str2`, which is a regular expression.

## STRLEN

`STRLEN(expr)`

**Function type:** [Scalar, string](sql-scalar.md#string-functions)

Alias for [`LENGTH`](#length).

## STRPOS

`STRPOS(<CHARACTER>, <CHARACTER>)`

**Function type:** [Scalar, string](sql-scalar.md#string-functions)

Returns the one-based index position of a substring within an expression.

## SUBSTR

`SUBSTR(<CHARACTER>, <INTEGER>, [<INTEGER>])`

**Function type:** [Scalar, string](sql-scalar.md#string-functions)

Alias for [`SUBSTRING`](#substring).

## SUBSTRING

`SUBSTRING(<CHARACTER>, <INTEGER>, [<INTEGER>])`

**Function type:** [Scalar, string](sql-scalar.md#string-functions)

Returns a substring of the expression starting at a given one-based index.

## SUM

`SUM(expr)`

**Function type:** [Aggregation](sql-aggregations.md)

Calculates the sum of a set of values.

## TAN

`TAN(expr)`

**Function type:** [Scalar, numeric](sql-scalar.md#numeric-functions)

Calculates the trigonometric tangent of an angle expressed in radians.

## TDIGEST_GENERATE_SKETCH

`TDIGEST_GENERATE_SKETCH(expr, [compression])`

**Function type:** [Aggregation](sql-aggregations.md)

Generates a T-digest sketch from values of the specified expression.

## TDIGEST_QUANTILE

`TDIGEST_QUANTILE(expr, quantileFraction, [compression])`

**Function type:** [Aggregation](sql-aggregations.md)

Returns the quantile for the specified fraction from a T-Digest sketch constructed from values of the expression.

## TEXTCAT

`TEXTCAT(<CHARACTER>, <CHARACTER>)`

**Function type:** [Scalar, string](sql-scalar.md#string-functions)

Concatenates two string expressions.

## THETA_SKETCH_ESTIMATE

`THETA_SKETCH_ESTIMATE(expr)`

**Function type:** [Scalar, sketch](sql-scalar.md#sketch-functions)

Returns the distinct count estimate from a Theta sketch.

## THETA_SKETCH_ESTIMATE_WITH_ERROR_BOUNDS

`THETA_SKETCH_ESTIMATE_WITH_ERROR_BOUNDS(expr, errorBoundsStdDev)`

**Function type:** [Scalar, sketch](sql-scalar.md#sketch-functions)

Returns the distinct count estimate and error bounds from a Theta sketch.

## THETA_SKETCH_INTERSECT

`THETA_SKETCH_INTERSECT([size], expr0, expr1, ...)`

**Function type:** [Scalar, sketch](sql-scalar.md#sketch-functions)

Returns an intersection of Theta sketches.

## THETA_SKETCH_NOT

`THETA_SKETCH_NOT([size], expr0, expr1, ...)`

**Function type:** [Scalar, sketch](sql-scalar.md#sketch-functions)

Returns a set difference of Theta sketches.

## THETA_SKETCH_UNION

`THETA_SKETCH_UNION([size], expr0, expr1, ...)`

**Function type:** [Scalar, sketch](sql-scalar.md#sketch-functions)

Returns a union of Theta sketches.

## TIME_CEIL

`TIME_CEIL(<TIMESTAMP>, <period>, [<origin>, [<timezone>]])`

**Function type:** [Scalar, date and time](sql-scalar.md#date-and-time-functions)

Rounds up a timestamp by a given time period, optionally from some reference time or timezone.

## TIME_EXTRACT

`TIME_EXTRACT(<TIMESTAMP>, [<unit>, [<timezone>]])`

**Function type:** [Scalar, date and time](sql-scalar.md#date-and-time-functions)

Extracts the value of some unit of the timestamp and returns the number.

## TIME_FLOOR

`TIME_FLOOR(<TIMESTAMP>, <period>, [<origin>, [<timezone>]])`

**Function type:** [Scalar, date and time](sql-scalar.md#date-and-time-functions)

Rounds down a timestamp by a given time period, optionally from some reference time or timezone.

## TIME_FORMAT

`TIME_FORMAT(<TIMESTAMP>, [<pattern>, [<timezone>]])`

**Function type:** [Scalar, date and time](sql-scalar.md#date-and-time-functions)

Formats a timestamp as a string.

## TIME_IN_INTERVAL

Returns true if a timestamp is contained within a particular interval. Intervals must be formatted as a string literal containing any ISO 8601 interval. The start instant of an interval is inclusive, and the end instant is exclusive.

* **Syntax:** `TIME_IN_INTERVAL(timestamp_expr, interval)`
* **Function type:** Scalar, date and time

<details><summary>Example</summary>

The following example returns true when a timestamp in the `__time` column of the `taxi-trips` datasource is within a hour interval starting from `2013-08-01T08:00:00`.

```sql
SELECT 
  "__time" AS "original_time",
  TIME_IN_INTERVAL("__time", '2013-08-01T08:00:00/PT1H') AS "in_interval"
FROM "taxi-trips"
LIMIT 2
```

Returns the following:

| `original_time` | `in_interval` |
| -- | -- |
| `2013-08-01T08:14:37.000Z` | `true` | 
| `2013-08-01T09:13:00.000Z` | `false` | 

</details>

[Learn more](sql-scalar.md#date-and-time-functions)

## TIME_PARSE

`TIME_PARSE(<string_expr>, [<pattern>, [<timezone>]])`

**Function type:** [Scalar, date and time](sql-scalar.md#date-and-time-functions)

Parses a string into a timestamp.

## TIME_SHIFT

`TIME_SHIFT(<TIMESTAMP>, <period>, <step>, [<timezone>])`

**Function type:** [Scalar, date and time](sql-scalar.md#date-and-time-functions)

Shifts a timestamp forwards or backwards by a given number of time units.

## TIMESTAMP_TO_MILLIS

Returns the number of milliseconds since epoch for the given timestamp.

* **Syntax:** `TIMESTAMP_TO_MILLIS(timestamp_expr)`
* **Function type:** Scalar, date and time

<details><summary>Example</summary>

The following example converts the `__time` column from the `taxi-trips` datasource into milliseconds since epoch.

```sql
SELECT 
  "__time" AS "original_time",
  TIMESTAMP_TO_MILLIS("__time") AS "miliseconds"
FROM "taxi-trips"
LIMIT 1
```

Returns the following:

| `original_time` | `miliseconds` |
| -- | -- |
| `2013-08-01T08:14:37.000Z` | `1375344877000` |

</details>

[Learn more](sql-scalar.md#date-and-time-functions)

## TIMESTAMPADD

Add a `unit` of time multiplied by `count` to `timestamp`.

* **Syntax:** `TIMESTAMPADD(unit, count, timestamp)`
* **Function type:** Scalar, date and time

<details><summary>Example</summary>

The following example adds five months to the timestamp `2000-01-01 00:00:00`.

```sql
SELECT
  TIMESTAMP '2000-01-01 00:00:00' AS "original_time",
  TIMESTAMPADD (MONTH, 5, TIMESTAMP '2000-01-01 00:00:00') AS "new_time"
```

Returns the following:

| `original_time` | `new_time` |
| -- | -- |
| `2000-01-01T00:00:00.000Z` | `2000-06-01T00:00:00.000Z` |

</details>

[Learn more](sql-scalar.md#date-and-time-functions)

## TIMESTAMPDIFF

Returns the difference between two timestamps in a given unit.

* **Syntax:** `TIMESTAMPDIFF(unit, timestamp1, timestamp2)`
* **Function type:** Scalar, date and time

<details><summary>Example</summary>

The following example calculates the taxi trip length in minutes by subtracting the `__time` column from the `dropoff_datetime` column in the `taxi-trips` datasource.

```sql
SELECT
  "__time" AS "pickup_time",
  "dropoff_datetime" AS "dropoff_time",
  TIMESTAMPDIFF (MINUTE, "__time", TIME_PARSE("dropoff_datetime")) AS "trip_length"
FROM "taxi-trips"
LIMIT 1
```

Returns the following: 

| `pickup_time` | `dropoff_time` | `trip_length` |
| -- | -- | -- |
| `2013-08-01T08:14:37.000Z` | `2013-08-01 09:09:06` | `54` |

</details>

[Learn more](sql-scalar.md#date-and-time-functions)

## TO_JSON_STRING

**Function type:** [JSON](sql-json-functions.md)

`TO_JSON_STRING(expr)`

Serializes `expr` into a JSON string.


## TRIM

`TRIM([BOTH|LEADING|TRAILING] [<chars> FROM] expr)`

**Function type:** [Scalar, string](sql-scalar.md#string-functions)

Trims the leading or trailing characters of an expression.

## TRUNC

`TRUNC(expr[, digits])`

**Function type:** [Scalar, numeric](sql-scalar.md#numeric-functions)

Alias for [`TRUNCATE`](#truncate).

## TRUNCATE

`TRUNCATE(expr[, digits])`

**Function type:** [Scalar, numeric](sql-scalar.md#numeric-functions)

Truncates a numerical expression to a specific number of decimal digits.


## TRY_PARSE_JSON

**Function type:** [JSON](sql-json-functions.md)

`TRY_PARSE_JSON(expr)`

Parses `expr` into a `COMPLEX<json>` object. This operator deserializes JSON values when processing them, translating stringified JSON into a nested structure. If the input is not a `VARCHAR` or it is invalid JSON, this function will result in a `NULL` value.

## UNNEST

`UNNEST(source_expression) as table_alias_name(column_alias_name)`

Unnests a source expression that includes arrays into a target column with an aliased name. 

For more information, see [UNNEST](./sql.md#unnest).

## UPPER

`UPPER(expr)`

**Function type:** [Scalar, string](sql-scalar.md#string-functions)

Returns the expression in uppercase.

## VAR_POP

`VAR_POP(expr)`

**Function type:** [Aggregation](sql-aggregations.md)

Calculates the population variance of a set of values.

## VAR_SAMP

`VAR_SAMP(expr)`

**Function type:** [Aggregation](sql-aggregations.md)

Calculates the sample variance of a set of values.

## VARIANCE

`VARIANCE(expr)`

**Function type:** [Aggregation](sql-aggregations.md)

Alias for [`VAR_SAMP`](#var_samp).
