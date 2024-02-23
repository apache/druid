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

If `expr` is a scalar type, returns 1 if `arr` contains `expr`. If `expr` is an array, returns 1 if `arr` contains all elements of `expr`. Otherwise returns 0.


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

Returns 1 if `arr1` and `arr2` have any elements in common, else 0.|

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

`CEIL(<TIMESTAMP> TO <TIME_UNIT>)`

**Function type:** [Scalar, date and time](sql-scalar.md#date-and-time-functions)

Rounds up a timestamp by a given time unit.

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

`EXTRACT(<TIME_UNIT> FROM <TIMESTAMP>)`

**Function type:** [Scalar, date and time](sql-scalar.md#date-and-time-functions)

Extracts the value of some unit of the timestamp, optionally from a certain time zone, and returns the number.

## FLOOR (date and time)

`FLOOR(<TIMESTAMP> TO <TIME_UNIT>)`

**Function type:** [Scalar, date and time](sql-scalar.md#date-and-time-functions)

Rounds down a timestamp by a given time unit.

## FLOOR (numeric)

`FLOOR(<NUMERIC>)`

**Function type:** [Scalar, numeric](sql-scalar.md#numeric-functions)

Calculates the largest integer value less than or equal to the numeric expression.

## GREATEST

`GREATEST([expr1, ...])`

**Function type:** [Scalar, reduction](sql-scalar.md#reduction-functions)

Returns the maximum value from the provided arguments.

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

`IPV4_MATCH(address, subnet)`

**Function type:** [Scalar, IP address](sql-scalar.md#ip-address-functions)

Returns true if the IPv4 `address` belongs to the `subnet` literal, else false.

## IPV4_PARSE

`IPV4_PARSE(address)`

**Function type:** [Scalar, IP address](sql-scalar.md#ip-address-functions)

Parses `address` into an IPv4 address stored as an integer.

## IPV4_STRINGIFY

`IPV4_STRINGIFY(address)`

**Function type:** [Scalar, IP address](sql-scalar.md#ip-address-functions)

Converts `address` into an IPv4 address in dot-decimal notation.

## IPV6_MATCH

`IPV6_MATCH(address, subnet)`

**Function type:** [Scalar, IP address](sql-scalar.md#ip-address-functions)

Returns true if the IPv6 `address` belongs to the `subnet` literal, else false.

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

## LATEST

`LATEST(expr, [maxBytesPerValue])`

**Function type:** [Aggregation](sql-aggregations.md)

Returns the value of a numeric or string expression corresponding to the latest `__time` value.

## LATEST_BY

`LATEST_BY(expr, timestampExpr, [maxBytesPerValue])`

**Function type:** [Aggregation](sql-aggregations.md)

Returns the value of a numeric or string expression corresponding to the latest time value from `timestampExpr`.

## LEAST

`LEAST([expr1, ...])`

**Function type:** [Scalar, reduction](sql-scalar.md#reduction-functions)

Returns the minimum value from the provided arguments.

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

`MILLIS_TO_TIMESTAMP(millis_expr)`

**Function type:** [Scalar, date and time](sql-scalar.md#date-and-time-functions)

Converts a number of milliseconds since epoch into a timestamp.

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

## MV_TO_STRING

`MV_TO_STRING(arr, str)`

**Function type:** [Multi-value string](sql-multivalue-string-functions.md)

Joins all elements of the array together by the given delimiter.

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

`TIME_IN_INTERVAL(<TIMESTAMP>, <CHARACTER>)`

**Function type:** [Scalar, date and time](sql-scalar.md#date-and-time-functions)

Returns whether a timestamp is contained within a particular interval, formatted as a string.

## TIME_PARSE

`TIME_PARSE(<string_expr>, [<pattern>, [<timezone>]])`

**Function type:** [Scalar, date and time](sql-scalar.md#date-and-time-functions)

Parses a string into a timestamp.

## TIME_SHIFT

`TIME_SHIFT(<TIMESTAMP>, <period>, <step>, [<timezone>])`

**Function type:** [Scalar, date and time](sql-scalar.md#date-and-time-functions)

Shifts a timestamp forwards or backwards by a given number of time units.

## TIMESTAMP_TO_MILLIS

`TIMESTAMP_TO_MILLIS(<TIMESTAMP>)`

**Function type:** [Scalar, date and time](sql-scalar.md#date-and-time-functions)

Returns the number of milliseconds since epoch for the given timestamp.

## TIMESTAMPADD

`TIMESTAMPADD(<unit>, <count>, <TIMESTAMP>)`

**Function type:** [Scalar, date and time](sql-scalar.md#date-and-time-functions)

Adds a certain amount of time to a given timestamp.

## TIMESTAMPDIFF

`TIMESTAMPDIFF(<unit>, <TIMESTAMP>, <TIMESTAMP>)`

**Function type:** [Scalar, date and time](sql-scalar.md#date-and-time-functions)

Takes the difference between two timestamps, returning the results in the given units.

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

