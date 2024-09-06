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
<!-- The **Learn More** at the end of each function section provides further documentation. -->
This page provides a reference of Apache Druid&circledR; SQL functions in alphabetical order. For more details on a function, refer to the following:
* [Aggregation functions](sql-aggregations.md)
* [Array functions](sql-array-functions.md)
* [JSON functions](sql-json-functions.md)
* [Multi-value string functions](sql-multivalue-string-functions.md)
* [Scalar functions](sql-scalar.md)
* [Window functions](sql-window-functions.md)

The examples on this page use the following example datasources:
* `flight-carriers` using `FlightCarrierOnTime (1 month)` 
* `taxi-trips` using `NYC Taxi cabs (3 files)`

## ABS

Calculates the absolute value of a numeric expression.

* **Syntax:** `ABS(<NUMERIC>)`
* **Function type:** Scalar, numeric

<details><summary>Example</summary>

The following example applies the ABS function to the `ArrDelay` column from the `flight-carriers` datasource.

```sql
SELECT
  "ArrDelay" AS "arrival_delay",
  ABS("ArrDelay") AS "absolute_arrival_delay"
FROM "flight-carriers"
WHERE "ArrDelay" < 0
LIMIT 1
```
Returns the following:

| `arrival_delay` | `absolute_arrival_delay` | 
| -- | -- | 
| `-27` | `27` | 
</details>

[Learn more](sql-scalar.md#numeric-functions)

## ACOS

Calculates the arc cosine (arccosine) of a numeric expression.

* **Syntax:** `ACOS(expr)`
* **Function type:** Scalar, numeric

<details><summary>Example</summary>

The following example calculates the arc cosine  of `0`.

```sql
SELECT ACOS(0) AS "arc_cosine"
```
Returns the following:

| `arc_cosine` |  
| -- | 
| `1.5707963267948966` |
</details>

[Learn more](sql-scalar.md#numeric-functions)

## ANY_VALUE

`ANY_VALUE(expr, [maxBytesPerValue, [aggregateMultipleValues]])`

**Function type:** [Aggregation](sql-aggregations.md)

Returns any value of the specified expression.

## APPROX_COUNT_DISTINCT

`APPROX_COUNT_DISTINCT(expr)`

**Function type:** [Aggregation](sql-aggregations.md)

Counts distinct values of a regular column or a prebuilt sketch column.

## APPROX_COUNT_DISTINCT_BUILTIN
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

Calculates the arc sine (arcsine) of a numeric expression.

* **Syntax:** `ASIN(expr)`
* **Function type:** Scalar, numeric

<details><summary>Example</summary>

The following example calculates the arc sine of `1`.

```sql
SELECT ASIN(1) AS "arc_sine"
```
Returns the following:

| `arc_sine` |  
| -- | 
| `1.5707963267948966` |
</details>

[Learn more](sql-scalar.md#numeric-functions)

## ATAN

Calculates the arc tangent (arctangent) of a numeric expression.

* **Syntax:** `ATAN(expr)`
* **Function type:** Scalar, numeric

<details><summary>Example</summary>

The following example calculates the arc tangent of `1`.

```sql
SELECT ATAN(1) AS "arc_tangent"
```
Returns the following:

| `arc_tangent` |  
| -- | 
| `0.7853981633974483` |
</details>

[Learn more](sql-scalar.md#numeric-functions)

## ATAN2

Calculates the arc tangent (arctangent) of a specified x and y coordinate.

* **Syntax:** `ATAN2(x, y)`
* **Function type:** Scalar, numeric

<details><summary>Example</summary>

The following example calculates the arc tangent of the coordinate `(1, -1)`

```sql
SELECT ATAN2(1,-1) AS "arc_tangent_2"
```
Returns the following:

| `arc_tangent_2` |  
| -- | 
| `2.356194490192345` |
</details>

[Learn more](sql-scalar.md#numeric-functions)

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

Returns the bitwise AND between two expressions: `expr1 & expr2`. 

* **Syntax:** `BITWISE_AND(expr1, expr2)`
* **Function type:** Scalar, numeric

<details><summary>Example</summary>

The following example performs the bitwise AND operation `12 & 10`.

```sql
SELECT BITWISE_AND(12, 10) AS "bitwise_and"
```
Returns the following:

| `bitwise_and` | 
| -- |
| 8 | 
</details>

[Learn more](sql-scalar.md#numeric-functions)

## BITWISE_COMPLEMENT

Returns the bitwise complement (bitwise not) for the expression: `~expr`.

* **Syntax:** `BITWISE_COMPLEMENT(expr)`
* **Function type:** Scalar, numeric

<details><summary>Example</summary>

The following example performs the bitwise complement operation `~12`.

```sql
SELECT BITWISE_COMPLEMENT(12) AS "bitwise_complement"
```
Returns the following:

| `bitwise_complement` | 
| -- |
| -13 | 
</details>

[Learn more](sql-scalar.md#numeric-functions)

## BITWISE_CONVERT_DOUBLE_TO_LONG_BITS

Converts the bits of an IEEE 754 floating-point double value to long.

* **Syntax:**`BITWISE_CONVERT_DOUBLE_TO_LONG_BITS(expr)`
* **Function type:** Scalar, numeric

<details><summary>Example</summary>

The following example returns the IEEE 754 floating-point double representation of `255` as a long. 

```sql
SELECT BITWISE_CONVERT_DOUBLE_TO_LONG_BITS(255) AS "ieee_754_double_to_long"
```
Returns the following:

| `ieee_754_double_to_long` | 
| -- |
| `4643176031446892544` | 
</details>

[Learn more](sql-scalar.md#numeric-functions)


## BITWISE_CONVERT_LONG_BITS_TO_DOUBLE

Converts a long to the IEEE 754 floating-point double specified by the bits stored in the long.

* **Syntax:**`BITWISE_CONVERT_LONG_BITS_TO_DOUBLE(expr)`
* **Function type:** Scalar, numeric

<details><summary>Example</summary>

The following example returns the long representation of `4643176031446892544` as an IEEE 754 floating-point double.

```sql
SELECT BITWISE_CONVERT_LONG_BITS_TO_DOUBLE(4643176031446892544) AS "long_to_ieee_754_double"
```
Returns the following:

| `long_to_ieee_754_double` | 
| -- |
| `255` | 
</details>

[Learn more](sql-scalar.md#numeric-functions)

## BITWISE_OR

Returns the bitwise OR between the two expressions: `expr1 | expr2`.

* **Syntax:** `BITWISE_OR(expr1, expr2)`
* **Function type:** Scalar, numeric

<details><summary>Example</summary>

The following example performs the bitwise OR operation `12 | 10`.

```sql
SELECT BITWISE_OR(12, 10) AS "bitwise_or"
```
Returns the following:

| `bitwise_or` | 
| -- |
| `14` | 
</details>

[Learn more](sql-scalar.md#numeric-functions)

## BITWISE_SHIFT_LEFT

Returns the bitwise left shift by x positions of an expr: `expr << x`.

* **Syntax:** `BITWISE_SHIFT_LEFT(expr, x)`
* **Function type:** Scalar, numeric

<details><summary>Example</summary>

The following example performs the bitwise SHIFT operation `2 << 3`.

```sql
SELECT BITWISE_SHIFT_LEFT(2, 3) AS "bitwise_shift_left"
```
Returns the following:

| `bitwise_shift_left` | 
| -- |
| `16` | 
</details>

[Learn more](sql-scalar.md#numeric-functions)

## BITWISE_SHIFT_RIGHT

Returns the bitwise right shift by x positions of an expr: `expr >> x`.

* **Syntax:** `BITWISE_SHIFT_RIGHT(expr, x)`
* **Function type:** Scalar, numeric

<details><summary>Example</summary>

The following example performs the bitwise SHIFT operation `16 >> 3`.

```sql
SELECT BITWISE_SHIFT_RIGHT(16, 3) AS "bitwise_shift_right"
```
Returns the following:

| `bitwise_shift_right` | 
| -- |
| `2` | 
</details>

[Learn more](sql-scalar.md#numeric-functions)

## BITWISE_XOR

Returns the bitwise exclusive OR between the two expressions: `expr1 ^ expr2`.

* **Syntax:** `BITWISE_XOR(expr1, expr2)`
* **Function type:** Scalar, numeric

<details><summary>Example</summary>

The following example performs the bitwise XOR operation `12 ^ 10`.

```sql
SELECT BITWISE_XOR(12, 10) AS "bitwise_xor"
```
Returns the following:

| `bitwise_xor` | 
| -- |
| `6` | 
</details>

[Learn more](sql-scalar.md#numeric-functions)

## BLOOM_FILTER

`BLOOM_FILTER(expr, <NUMERIC>)`

**Function type:** [Aggregation](sql-aggregations.md)

Computes a Bloom filter from values produced by the specified expression.

## BLOOM_FILTER_TEST

`BLOOM_FILTER_TEST(expr, <STRING>)`

**Function type:** [Scalar, other](sql-scalar.md#other-scalar-functions)

Returns true if the expression is contained in a Base64-serialized Bloom filter.

## BTRIM

Trims characters from both the leading and trailing ends of an expression. Defaults `chars` to a space if none is provided.

* **Syntax:** `BTRIM(expr[, chars])`
* **Function type:** Scalar, string

<details><summary>Example</summary>

The following example trims the `_` characters from both ends of the string expression.

```sql
SELECT 
  '___abc___' AS "original_string",
  BTRIM('___abc___', '_') AS "trim_both_ends"
```

Returns the following:

| `original_string` | `trim_both_ends` |
| -- | -- | 
| `___abc___` | `abc` |

</details>

[Learn more](sql-scalar.md#string-functions)

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

Calculates the smallest integer value greater than or equal to the numeric expression.
* **Syntax:** `CEIL(<NUMERIC>)`
* **Function type:** Scalar, numeric

<details><summary>Example</summary>

The following example applies the CEIL function to the `fare_amount` column from the `taxi-trips` datasource.

```sql
SELECT
  "fare_amount" AS "fare_amount",
  CEIL("fare_amount") AS "ceiling_fare_amount"
FROM "taxi-trips"
LIMIT 1
```
Returns the following:

| `fare_amount` | `ceiling_fare_amount` | 
| -- | -- | 
| `21.25` | `22` | 
</details>

[Learn more](sql-scalar.md#numeric-functions)

## CHAR_LENGTH

Alias for [`LENGTH`](#length).

* **Syntax:** `CHAR_LENGTH(expr)`
* **Function type:** Scalar, string 

[Learn more](sql-scalar.md#string-functions)

## CHARACTER_LENGTH

Alias for [`LENGTH`](#length).

* **Syntax:** `CHARACTER_LENGTH(expr)`
* **Function type:** Scalar, string 

[Learn more](sql-scalar.md#string-functions)


## COALESCE

`COALESCE(expr, expr, ...)`

**Function type:** [Scalar, other](sql-scalar.md#other-scalar-functions)

Returns the first non-null value.

## CONCAT

Concatenates a list of expressions.

* **Syntax:** `CONCAT(expr[, expr,...])`
* **Function type:** Scalar, string

<details><summary>Example</summary>

The following example concatenates the `OriginCityName` column from `flight-carriers`, the string ` to `, and the `DestCityName` column from `flight-carriers`. 

```sql
SELECT
  "OriginCityName" AS "origin_city",
  "DestCityName" AS "destination_city",
  CONCAT("OriginCityName", ' to ', "DestCityName") AS "concatenate_flight_details"
FROM "flight-carriers"
LIMIT 1
```

Returns the following:

| `origin_city` | `destination_city` | `concatenate_flight_details` |
| -- | -- | -- |
| `San Juan, PR` | `Washington, DC` | `San Juan, PR to Washington, DC` | 

</details>

[Learn more](sql-scalar.md#string-functions)

## CONTAINS_STRING

Returns `true` if `str` is a substring of `expr`, case-sensitive. Otherwise returns `false`.

* **Syntax:** `CONTAINS_STRING(expr, str)`
* **Function type:** Scalar, string

<details><summary>Example</summary>

The following example returns `true` if the `OriginCityName` column from the `flight-carriers` datasource contains the substring `San`. 

```sql
SELECT
  "OriginCityName" AS "origin_city",
  CONTAINS_STRING("OriginCityName", 'San') AS "contains_string"
FROM "flight-carriers"
LIMIT 2
```

Returns the following:

| `origin_city` | `contains_string` | 
| -- | -- |
| `San Juan, PR` | `true` |
| `Boston, MA` | `false` |

</details>


[Learn more](sql-scalar.md#string-functions)

## COS

Calculates the trigonometric cosine of an angle expressed in radians.

* **Syntax:** `COS(expr)`
* **Function type:** Scalar, numeric

<details><summary>Example</summary>

The following example calculates the cosine of angle `PI/3` radians.

```sql
SELECT COS(PI / 3) AS "cosine"
```
Returns the following:

| `cosine` |  
| -- | 
| `0.5000000000000001` |
</details>

[Learn more](sql-scalar.md#numeric-functions)

## COT

Calculates the trigonometric cotangent of an angle expressed in radians.

* **Syntax:** `COT(expr)`
* **Function type:** Scalar, numeric

<details><summary>Example</summary>

The following example calculates the cotangent of angle `PI/3` radians.

```sql
SELECT COT(PI / 3) AS "cotangent"
```
Returns the following:

| `cotangent` |  
| -- | 
| `0.577350269189626` |
</details>

[Learn more](sql-scalar.md#numeric-functions)

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

Returns the current date in UTC time, unless you specify a different timezone in the query context.

* **Syntax:** `CURRENT_DATE`
* **Function type:** Scalar, date and time

<details><summary>Example</summary>

The following example returns the current date.

```sql
SELECT CURRENT_DATE AS "current_date"
```

Returns the following:

| `current_date` | 
| -- |
| `2024-08-14T00:00:00.000Z `|

</details>

[Learn more](sql-scalar.md#date-and-time-functions)

## CURRENT_TIMESTAMP

Returns the current timestamp in UTC time, unless you specify a different timezone in the query context.


* **Syntax:** `CURRENT_TIMESTAMP`
* **Function type:** Scalar, date and time

<details><summary>Example</summary>

The following example returns the current timestamp.

```sql
SELECT CURRENT_TIMESTAMP AS "current_timestamp"
```

Returns the following:

| `current_timestamp` |
| -- |
| `2024-08-14T21:30:13.793Z` |

</details>

[Learn more](sql-scalar.md#date-and-time-functions)

## DATE_TRUNC

Rounds down a timestamp by a given time unit.

* **Syntax:** `DATE_TRUNC(unit, timestamp_expr)` 
* **Function type:** Scalar, date and time

<details><summary>Example</summary>

The following example truncates a timestamp from the `__time` column from the `taxi-trips` datasource to the most recent `decade`.

```sql
SELECT 
  "__time" AS "original_timestamp",
  DATE_TRUNC('decade', "__time") AS "truncate_timestamp"
FROM "taxi-trips"
LIMIT 1
```

Returns the following:

| `original_timestamp` | `truncate_time` |
| -- | -- |
| `2013-08-01T08:14:37.000Z` | `2010-01-01T00:00:00.000Z` |

</details>

[Learn more](sql-scalar.md#date-and-time-functions)


## DECODE_BASE64_COMPLEX

`DECODE_BASE64_COMPLEX(dataType, expr)`

**Function type:** [Scalar, other](sql-scalar.md#other-scalar-functions)

Decodes a Base64-encoded string into a complex data type, where `dataType` is the complex data type and `expr` is the Base64-encoded string to decode.

## DECODE_BASE64_UTF8

Decodes a Base64-encoded string into a UTF-8 encoded string.

* **Syntax:** `DECODE_BASE64_UTF8(expr)`
* **Function type:** Scalar, string

<details><summary>Example</summary>

The following example converts the base64 encoded string `SGVsbG8gV29ybGQhCg==` into an UTF-8 encoded string.

```sql
SELECT
  'SGVsbG8gV29ybGQhCg==' AS "base64_encoding",
  DECODE_BASE64_UTF8('SGVsbG8gV29ybGQhCg==') AS "convert_to_UTF8_encoding"
```

Returns the following:

| `base64_encoding` | `convert_to_UTF8_encoding` |
| -- | -- |
| `SGVsbG8gV29ybGQhCg==` | `Hello World!` |

</details>

[Learn more](sql-scalar.md#string-functions)

## DEGREES

Converts an angle from radians to degrees.

* **Syntax:** `DEGREES(expr)`
* **Function type:** Scalar, numeric

<details><summary>Example</summary>

The following example converts an angle of `PI` radians to degrees

```sql
SELECT DEGREES(PI) AS "degrees"
```
Returns the following:

| `degrees` |  
| -- | 
| `180` |
</details>

[Learn more](sql-scalar.md#numeric-functions)

## DENSE_RANK

`DENSE_RANK()`

**Function type:** [Window](sql-window-functions.md#window-function-reference)

Returns the rank for a row within a window without gaps. For example, if two rows tie for a rank of 1, the subsequent row is ranked 2.

## DIV

Returns the result of integer division of `x` by `y`.

:::info
The `DIV` function is not implemented in Druid versions 30.0.0 or earlier. Consider using [`SAFE_DIVIDE`](./sql-functions.md#safe_divide) instead. 
:::

* **Syntax:** `DIV(x, y)`
* **Function type:** Scalar, numeric

<details><summary>Example</summary>

  The following calculates integer divisions of `78` by `10`.

  ```sql
  SELECT DIV(78, 10) as "division"
  ``` 

  Returns the following:

  | `division` |
  | -- |
  | `7` |

</details>


[Learn more](sql-scalar.md#numeric-functions)

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

Calculates _e_ raised to the power of the numeric expression.

* **Syntax:** `EXP(<NUMERIC>)`
* **Function type:** Scalar, numeric

<details><summary>Example</summary>

The following example calculates _e_ to the power of 1.

```sql
SELECT EXP(1) AS "exponential" 
```
Returns the following:

| `exponential` |
| -- |
| `2.7182818284590455` |
</details>

[Learn more](sql-scalar.md#numeric-functions)

## EXTRACT

`EXTRACT(<TIME_UNIT> FROM <TIMESTAMP>)`

**Function type:** [Scalar, date and time](sql-scalar.md#date-and-time-functions)

Extracts the value of some unit of the timestamp, optionally from a certain time zone, and returns the number.

## FIRST_VALUE

`FIRST_VALUE(expr)`

**Function type:** [Window](sql-window-functions.md#window-function-reference)

Returns the value evaluated for the expression for the first row within the window.

## FLOOR (date and time)

`FLOOR(<TIMESTAMP> TO <TIME_UNIT>)`

**Function type:** [Scalar, date and time](sql-scalar.md#date-and-time-functions)

Rounds down a timestamp by a given time unit.

## FLOOR (numeric)

Calculates the largest integer less than or equal to the numeric expression.

* **Syntax:** `FLOOR(<NUMERIC>)`
* **Function type:** Scalar, numeric

<details><summary>Example</summary>

The following example applies the FLOOR function to the `fare_amount` column from the `taxi-trips` datasource.

```sql
SELECT
  "fare_amount" AS "fare_amount",
  FLOOR("fare_amount") AS "floor_fare_amount"
FROM "taxi-trips"
LIMIT 1
```
Returns the following:

| `fare_amount` | `floor_fare_amount` | 
| -- | -- | 
| `21.25` | `21` | 
</details>

[Learn more](sql-scalar.md#numeric-functions)

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

Converts an integer byte size into human-readable [IEC](https://en.wikipedia.org/wiki/Binary_prefix) format.

* **Syntax:** `HUMAN_READABLE_BINARY_BYTE_FORMAT(value[, precision])`
* **Function type:** Scalar, numeric

<details><summary>Example</summary>

  The following example converts `1000000` into IEC format.

  ```sql
    SELECT HUMAN_READABLE_BINARY_BYTE_FORMAT(1000000, 2) AS "iec_format"
  ```
  
  Returns the following:

  | `iec_format` |
  | -- |
  | `976.56 KiB` |
 
</details>

[Learn more](sql-scalar.md#numeric-functions)

## HUMAN_READABLE_DECIMAL_BYTE_FORMAT

Converts a byte size into human-readable [SI](https://en.wikipedia.org/wiki/Binary_prefix) format.

* **Syntax:** `HUMAN_READABLE_DECIMAL_BYTE_FORMAT(value[, precision])`
* **Function type:** Scalar, numeric

<details><summary>Example</summary>

The following example converts `1000000` into SI format.

```sql
SELECT HUMAN_READABLE_DECIMAL_BYTE_FORMAT(1000000, 2) AS "si_format"
```

Returns the following:

|`si_format`|
|--|
|`1.00 MB`|

</details>

[Learn more](sql-scalar.md#numeric-functions)

## HUMAN_READABLE_DECIMAL_FORMAT

Converts a byte size into human-readable SI format with single-character units.

* **Syntax:** `HUMAN_READABLE_DECIMAL_FORMAT(value[, precision])`
* **Function type:** Scalar, numeric

<details><summary>Example</summary>

  The following example converts `1000000` into single character SI format.

```sql
SELECT HUMAN_READABLE_DECIMAL_FORMAT(1000000, 2) AS "single_character_si_format"
```

Returns the following:

|`single_character_si_format`|
|--|
|`1.00 M`|
</details>

[Learn more](sql-scalar.md#numeric-functions)

## ICONTAINS_STRING

Returns `true` if `str` is a substring of `expr`, case-insensitive. Otherwise returns `false`.

* **Syntax:** `ICONTAINS_STRING(expr, str)`
* **Function type:** Scalar, string

<details><summary>Example</summary>

The following example returns `true` if the `OriginCityName` column from the `flight-carriers` datasource contains the case-insensitive substring `san`.  

```sql
SELECT
  "OriginCityName" AS "origin_city",
  ICONTAINS_STRING("OriginCityName", 'san') AS "contains_case_insensitive_string"
FROM "flight-carriers"
LIMIT 2
```

Returns the following:

| `origin_city` | `contains_case_insensitive_string` |
| -- | -- |
| `San Juan, PR` | `true` |
| `Boston, MA` | `false` |

</details>

[Learn more](sql-scalar.md#string-functions)

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

## JSON_MERGE

**Function type:** [JSON](sql-json-functions.md)

`JSON_MERGE(expr1, expr2[, expr3 ...])`
Merges two or more JSON `STRING` or `COMPLEX<json>` into one. Preserves the rightmost value when there are key overlaps. Returning always a `COMPLEX<json>` type.

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

Returns the `N` leftmost characters of an expression, where `N` is an integer value.

* **Syntax:** `LEFT(expr, N)`
* **Function type:** Scalar, string 

<details><summary>Example</summary>

The following example returns the `3` leftmost characters of the expression `ABCDEFG`.

```sql
SELECT
  'ABCDEFG' AS "expression",
  LEFT('ABCDEFG', 3) AS "leftmost_characters"
```

Returns the following:

| `expression` | `leftmost_characters` |
| -- | -- |
| `ABCDEFG` | `ABC` |

</details>

[Learn more](sql-scalar.md#string-functions)

## LENGTH

Returns the length of the expression in UTF-16 code units.

* **Syntax:** `LENGTH(expr)`
* **Function type:** Scalar, string

<details><summary>Example</summary>

The following example returns the character length of the `OriginCityName` column from the `flight-carriers` datasource.

```sql
SELECT 
  "OriginCityName" AS "origin_city_name",
  LENGTH("OriginCityName") AS "city_name_length"
FROM "flight-carriers"
LIMIT 1
```

Returns the following:

| `origin_city_name` | `city_name_length` | 
| -- | -- |
| `San Juan, PR` | `12` |

</details>

[Learn more](sql-scalar.md#string-functions)

## LN

Calculates the natural logarithm of the numeric expression.

* **Syntax:** `LN(<NUMERIC>)`
* **Function type:** Scalar, numeric

<details><summary>Example</summary>

The following example applies the LN function to the `max_temperature` column from the `taxi-trips` datasource.

```sql
SELECT
  "max_temperature" AS "max_temperature",
  LN("max_temperature") AS "natural_log_max_temp"
FROM "taxi-trips"
LIMIT 1
```

Returns the following:

| `max_temperature` | `natural_log_max_temp` | 
| -- | -- | 
| `76` | `4.330733340286331` | 

</details>

[Learn more](sql-scalar.md#numeric-functions)

## LOG10

Calculates the base-10 logarithm of the numeric expression.

* **Syntax:** `LOG10(<NUMERIC>)`
* **Function type:** Scalar, numeric

<details><summary>Example</summary>

The following example applies the LOG10 function to the `max_temperature` column from the `taxi-trips` datasource.

```sql
SELECT
  "max_temperature" AS "max_temperature",
  LOG10("max_temperature") AS "log10_max_temp"
FROM "taxi-trips"
LIMIT 1
```
Returns the following:

| `max_temperature` | `log10_max_temp` | 
| -- | -- | 
| `76` | `1.8808135922807914` | 
</details>

[Learn more](sql-scalar.md#numeric-functions)

## LOOKUP

Searches for `expr` in a registered [query-time lookup table](lookups.md) named `lookupName` and returns the mapped value. If `expr` is null or not contained in the lookup, returns `defaultValue` if supplied, otherwise returns null.

* **Syntax:** `LOOKUP(expr, lookupName[, defaultValue])`
* **Function type:** Scalar, string

<details><summary>Example</summary>

The following example uses a `map` type lookup table named `code_to_name`, which contains the following key-value pairs:

```json
{
  "SJU": "Luis Munoz Marin International Airport",
  "IAD": "Dulles International Airport"
}
```

The example uses `code_to_name` to map the `Origin` column from the `flight-carriers` datasource to the corresponding full airport name. Returns `key not found` if no matching key exists in the lookup table.

```sql
SELECT 
  "Origin" AS "origin_airport",
  LOOKUP("Origin", 'code_to_name','key not found') AS "full_airport_name"
FROM "flight-carriers"
LIMIT 2
```

Returns the following:

| `origin_airport` | `full_airport_name` | 
| -- | -- |
| `SJU` | `Luis Munoz Marin International Airport` |
| `BOS` | `key not found` |

</details> 

[Learn more](sql-scalar.md#string-functions)

## LOWER

Returns the expression in lowercase.

* **Syntax:** `LOWER(expr)`
* **Function type:** Scalar, string

<details><summary>Example</summary>

The following example converts the `OriginCityName` column from the `flight-carriers` datasource to lowercase.

```sql
SELECT 
  "OriginCityName" AS "origin_city",
  LOWER("OriginCityName") AS "lowercase"
FROM "flight-carriers"
LIMIT 1
```

Returns the following:

| `origin_city` | `lowercase` |
| -- | -- |
`San Juan, PR` | `san juan, pr` |

</details>

[Learn more](sql-scalar.md#string-functions)


## LPAD

Returns a string of size `length` from `expr`. When the length of `expr` is less than `length`, left pads `expr` with `chars`, which defaults to the space character. Truncates `expr` to `length` if `length` is shorter than the length of `expr`.

* **Syntax:** `LPAD(expr, length[, chars])`
* **Function type:** Scalar, string

<details><summary>Example</summary>

The following example left pads the value of `OriginStateName` from the `flight-carriers` datasource to return a total of 11 characters.

```sql
SELECT 
  "OriginStateName" AS "origin_state",
  LPAD("OriginStateName", 11, '+') AS "add_left_padding"
FROM "flight-carriers"
LIMIT 3
```

Returns the following:

| `origin_state` | `add_left_padding` |
| -- | -- |
| `Puerto Rico` | `Puerto Rico` |
| `Massachusetts` | `Massachuset` |
| `Florida` | `++++Florida` |

</details>

[Learn more](sql-scalar.md#string-functions)


## LTRIM

Trims characters from the leading end of an expression. Defaults `chars` to a space if none is provided.

* **Syntax:** `LTRIM(expr[, chars])`
* **Function type:** Scalar, string

<details><summary>Example</summary>

The following example trims the `_` characters from the leading end of the string expression.

```sql
SELECT 
  '___abc___' AS "original_string",
  LTRIM('___abc___', '_') AS "trim_leading_end_of_expression"
```

Returns the following:

| `original_string` | `trim_leading_end_of_expression` |
| -- | -- | 
| `___abc___` | `abc___` |

</details>

[Learn more](sql-scalar.md#string-functions)

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

Calculates x modulo y, or the remainder of x divided by y. Where x and y are numeric expressions.

* **Syntax:** `MOD(x, y)`
* **Function type:** Scalar, numeric

<details><summary>Example</summary>

The following calculates 78 MOD 10.

```sql
SELECT MOD(78, 10) as "modulo"
```
Returns the following:

| `modulo` | 
| -- | 
| `8` | 
</details>

[Learn more](sql-scalar.md#numeric-functions)

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

Converts a string into a long(BIGINT) with the given radix, or into DECIMAL(base 10) if a radix is not provided.

* **Syntax:**`PARSE_LONG(string[, radix])`
* **Function type:** Scalar, string

<details><summary>Example</summary>

The following example converts the string representation of the binary, radix 2, number `1100` into its long (BIGINT) equivalent.

```sql
SELECT 
  '1100' AS "binary_as_string", 
  PARSE_LONG('1110', 2) AS "bigint_value"
```

Returns the following:

| `binary_as_string` | `bigint_value` |
| -- | -- |
| `1100` | `14` |

</details>

[Learn more](sql-scalar.md#string-functions)


## PERCENT_RANK

`PERCENT_RANK()`

**Function type:** [Window](sql-window-functions.md#window-function-reference)

Returns the relative rank of the row calculated as a percentage according to the formula: `RANK() OVER (window) / COUNT(1) OVER (window)`.

## POSITION

Returns the one-based index position of a substring within an expression, optionally starting from a given one-based index. If `substring` is not found, returns 0.

* **Syntax**: `POSITION(substring IN expr [FROM startingIndex])`
* **Function type:** Scalar, string

<details><summary>Example</summary>

The following example returns the one-based index of the substring `PR` in the `OriginCityName` column from the `flight-carriers` datasource starting from index 5.

```sql
SELECT 
  "OriginCityName" AS "origin_city",
  POSITION('PR' IN "OriginCityName" FROM 5) AS "index"
FROM "flight-carriers"
LIMIT 2
```

Returns the following:

| `origin_city` | `index` |
| -- | -- |
| `San Juan, PR` | `11` |
| `Boston, MA` | `0` |

</details>

[Learn more](sql-scalar.md#string-functions)

## POWER

Calculates a numerical expression raised to the specified power.

* **Syntax:** `POWER(base, exponent)`
* **Function type:** Scalar, numeric

<details><summary>Example</summary>

The following example raises 5 to the power of 2.

```sql
SELECT POWER(5, 2) AS "power"
```
Returns the following:

| `power` |
| -- |
| `25` | 
</details>

[Learn more](sql-scalar.md#numeric-functions)

## RADIANS

Converts an angle from degrees to radians.

* **Syntax:** `RADIANS(expr)`
* **Function type:** Scalar, numeric

<details><summary>Example</summary>

The following example converts an angle of `180` degrees to radians

```sql
SELECT RADIANS(180) AS "radians"
```
Returns the following:

| `radians` |  
| -- | 
| `3.141592653589793` |
</details>

[Learn more](sql-scalar.md#numeric-functions)

## RANK

`RANK()`

**Function type:** [Window](sql-window-functions.md#window-function-reference)

Returns the rank with gaps for a row within a window. For example, if two rows tie for rank 1, the next rank is 3.

## REGEXP_EXTRACT

Apply regular expression `pattern` to `expr` and extract the Nth capture group. If `N` is unspecified or zero, returns the first substring that matches the pattern. Returns `null` if there is no matching pattern.

* **Syntax:** `REGEXP_EXTRACT(expr, pattern[, N])`
* **Function type:** Scalar, string 

<details><summary>Example</summary>

The following example uses regular expressions to find city names inside the `OriginCityName` column from the `flight-carriers` datasource by matching what comes before the comma.

```sql
SELECT 
  "OriginCityName" AS "origin_city",
  REGEXP_EXTRACT("OriginCityName", '([^,]+)', 0) AS "pattern_match"
FROM "flight-carriers"
LIMIT 1
```

Returns the following:

| `origin_city` | `pattern_match` |
| -- | -- |
| `San Juan, PR` | `San Juan`|

</details>

[Learn more](sql-scalar.md#string-functions)

## REGEXP_LIKE

Returns `true` if the regular expression `pattern` finds a match in `expr`. Returns `false` otherwise.

* **Syntax:** `REGEXP_LIKE(expr, pattern)`
* **Function type:** Scalar, string

<details><summary>Example</summary>

The following example returns `true` when the `OriginCityName` column from `flight-carriers` has a city name containing a space.

```sql
SELECT 
  "OriginCityName" AS "origin_city",
  REGEXP_LIKE("OriginCityName", '[A-Za-z]+\s[A-Za-z]+') AS "pattern_found"
FROM "flight-carriers"
LIMIT 2
```

Returns the following:

| `origin_city` | `pattern_found` |
| -- | -- |
| `San Juan, PR` | `true` |
| `Boston, MA` | `false` |

</details>

[Learn more](sql-scalar.md#string-functions)

## REGEXP_REPLACE

Replaces all occurrences of a regular expression in a string expression with a replacement string. Refer to capture groups in the replacement string using `$group` syntax. For example: `$1` or `$2`.

* **Syntax:** `REGEXP_REPLACE(expr, pattern, replacement)`
* **Function type:** Scalar, string

<details><summary>Example</summary>

The following example matches three consecutive words, where each word is its own capture group, and replaces the matched words with the word in the second capture group punctuated with exclamation marks.

```sql
SELECT 
  'foo bar baz' AS "original_string",
  REGEXP_REPLACE('foo bar baz', '([A-Za-z]+) ([A-Za-z]+) ([A-Za-z]+)' , '$2!') AS "modified_string"
```

Returns the following:

| `original_string` | `modified_string` |
| -- | -- |
| `foo bar baz` | `bar!` |

</details>

[Learn more](sql-scalar.md#string-functions)


## REPEAT

Repeats the string expression `N` times, where `N` is an integer.

* **Syntax:** `REPEAT(expr, N)`
* **Function type:** Scalar, string

<details><summary>Example</summary>

The following example returns the string expression `abc` repeated `3` times.

```sql
SELECT 
  'abc' AS "original_string",
  REPEAT('abc', 3) AS "with_repetition"
```

Returns the following: 

| `original_string` | `with_repetition` |
| -- | -- |
| `abc` | `abcabcabc` |

</details>

[Learn more](sql-scalar.md#string-functions)

## REPLACE

Replaces instances of a substring with a replacement string in the given expression.

* **Syntax:** `REPLACE(expr, substring, replacement)`
* **Function type:** Scalar, string 

<details><summary>Example</summary>

The following example replaces instances of the substring `abc` with `XYZ`.

```sql
SELECT 
  'abc 123 abc 123' AS "original_string",
   REPLACE('abc 123 abc 123', 'abc', 'XYZ') AS "modified_string"
```

Returns the following:

| `original_string` | `modified_string` | 
| -- | -- |
| `abc 123 abc 123` | `XYZ 123 XYZ 123` |

</details>

[Learn more](sql-scalar.md#string-functions)

## REVERSE

Reverses the given expression.

* **Syntax:** `REVERSE(expr)`
* **Function type:** Scalar, string

<details><summary>Example</summary>

The following example reverses the string expression `abc`.

```sql
SELECT 
  'abc' AS "original_string",
  REVERSE('abc') AS "reversal"
```

Returns the following:

| `original_string` | `reversal` |
| -- | -- |
| `abc` | `cba` |

</details>

[Learn more](sql-scalar.md#string-functions)

## RIGHT

Returns the `N` rightmost characters of an expression, where `N` is an integer value.

* **Syntax:** `RIGHT(expr, N)`
* **Function type:** Scalar, string 

<details><summary>Example</summary>

The following example returns the `3` rightmost characters of the expression `ABCDEFG`.

```sql
SELECT
  'ABCDEFG' AS "expression",
  RIGHT('ABCDEFG', 3) AS "rightmost_characters"
```

Returns the following:

| `expression` | `rightmost_characters` |
| -- | -- |
| `ABCDEFG` | `EFG` |

</details>

[Learn more](sql-scalar.md#string-functions)

## ROUND

Calculates the rounded value for a numerical expression.

* **Syntax:** `ROUND(expr[, digits])`
* **Function type:** Scalar, numeric

<details><summary>Example</summary>

The following applies the ROUND function to 0 decimal points on the `pickup_longitude` column from the `taxi-trips` datasource.

```sql
SELECT
  "pickup_longitude" AS "pickup_longitude",
  ROUND("pickup_longitude", 0) as "rounded_pickup_longitude"
FROM "taxi-trips"
WHERE "pickup_longitude" IS NOT NULL
LIMIT 1
```
Returns the following:

| `pickup_longitude` | `rounded_pickup_longitude` | 
| -- | -- | 
| `-73.9377670288086` | `-74` | 
</details>

[Learn more](sql-scalar.md#numeric-functions)

## ROW_NUMBER

`ROW_NUMBER()`

**Function type:** [Window](sql-window-functions.md#window-function-reference)

Returns the number of the row within the window starting from 1.

## RPAD

Returns a string of size `length` from `expr`. When the length of `expr` is less than `length`, right pads `expr` with `chars`, which defaults to the space character. Truncates `expr` to `length` if `length` is shorter than the length of `expr`.

* **Syntax:** `RPAD(expr, length[, chars])`
* **Function type:** Scalar, string

<details><summary>Example</summary>

The following example right pads the value of `OriginStateName` from the `flight-carriers` datasource to return a total of 11 characters.

```sql
SELECT 
  "OriginStateName" AS "origin_state",
  RPAD("OriginStateName", 11, '+') AS "add_right_padding"
FROM "flight-carriers"
LIMIT 3
```

Returns the following:

| `origin_state` | `add_right_padding` |
| -- | -- |
| `Puerto Rico` | `Puerto Rico` |
| `Massachusetts` | `Massachuset` |
| `Florida` | `Florida++++` |

</details>

[Learn more](sql-scalar.md#string-functions)

## RTRIM

Trims characters from the trailing end of an expression. Defaults `chars` to a space if none is provided.

* **Syntax:** `RTRIM(expr[, chars])`
* **Function type:** Scalar, string

<details><summary>Example</summary>

The following example trims the `_` characters from the trailing end of the string expression.

```sql
SELECT 
  '___abc___' AS "original_string",
  RTRIM('___abc___', '_') AS "trim_end"
```

Returns the following:

| `original_string` | `trim_end` |
| -- | -- | 
| `___abc___` | `___abc` |

</details>

[Learn more](sql-scalar.md#string-functions)

## SAFE_DIVIDE

Returns `x` divided by `y`, guarded on division by 0.

* **Syntax:** `SAFE_DIVIDE(x, y)`
* **Function type:** Scalar, numeric 

<details><summary>Example</summary>

The following example calculates divisions of integer `78` by integer `10`.

```sql
SELECT SAFE_DIVIDE(78, 10) AS "safe_division"
```

Returns the following:

|`safe_division`|
|--|
| `7` |

</details>

[Learn more](sql-scalar.md#numeric-functions)

## SIN

Calculates the trigonometric sine of an angle expressed in radians.

* **Syntax:** `SIN(expr)`
* **Function type:** Scalar, numeric

<details><summary>Example</summary>

The following example calculates the sine of angle `PI/3` radians.

```sql
SELECT SIN(PI / 3) AS "sine"
```
Returns the following:

| `sine` |  
| -- | 
| `0.8660254037844386` |
</details>

[Learn more](sql-scalar.md#numeric-functions)

## SQRT

Calculates the square root of a numeric expression.

* **Syntax:** `SQRT(<NUMERIC>)`
* **Function type:** Scalar, numeric

<details><summary>Example</summary>

The following example calculates the square root of 25.

```sql
SELECT SQRT(25) AS "square_root"
```
Returns the following:

| `square_root` |  
| -- | 
| `5` |
</details>

[Learn more](sql-scalar.md#numeric-functions)

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

Returns a string formatted in the manner of Java's [String.format](https://docs.oracle.com/javase/8/docs/api/java/lang/String.html#format-java.lang.String-java.lang.Object...-).

* **Syntax:** `STRING_FORMAT(pattern[, args...])`
* **Function type:** Scalar, string 

<details><summary>Example</summary>

The following example uses Java String format to pass in `Flight_Number_Reporting_Airline` and `origin_airport` columns, from the `flight-carriers` datasource, as arguments into the string. 

```sql
SELECT 
  "Flight_Number_Reporting_Airline" AS "flight_number",
  "Origin" AS "origin_airport",
  STRING_FORMAT('Flight No.%d departing from %s', "Flight_Number_Reporting_Airline", "Origin") AS "departure_announcement"
FROM "flight-carriers"
LIMIT 1
```

Returns the following:

| `flight_number` | `origin_airport` | `departure_announcement` |
| -- | -- | -- |
| `314` | `SJU` | `Flight No.314 departing from SJU` |

</details>

[Learn more](sql-scalar.md#string-functions)

## STRING_TO_MV

`STRING_TO_MV(str1, str2)`

**Function type:** [Multi-value string](sql-multivalue-string-functions.md)

Splits `str1` into an multi-value string on the delimiter specified by `str2`, which is a regular expression.

## STRLEN

Alias for [`LENGTH`](#length).

* **Syntax:** `STRLEN(expr)`
* **Function type:** Scalar, string 

[Learn more](sql-scalar.md#string-functions)

## STRPOS

Returns the one-based index position of a substring within an expression. If `substring` is not found, returns 0.

* **Syntax:** `STRPOS(expr, substring)`
* **Function type:** Scalar, string

<details><summary>Example</summary>

The following example returns the one-based index position of `World`.

```sql
SELECT 
  'Hello World!' AS "original_string",
  STRPOS('Hello World!', 'World') AS "index"
```

Returns the following:

| `original_string` | `index` |
| -- | -- |
| `Hello World!` | `7` |

</details>

[Learn more](sql-scalar.md#string-functions)

## SUBSTR

Alias for [`SUBSTRING`](#substring).

* **Syntax:** `SUBSTR(expr, index[, length])`
* **Function type:** Scalar, string

[Learn more](sql-scalar.md#string-functions)


## SUBSTRING

Returns a substring of the expression starting at a given one-based index. If `length` is omitted, extracts characters to the end of the string, otherwise returns a substring of `length` characters.

* **Syntax:** `SUBSTRING(expr, index[, length])`
* **Function type:** Scalar, string

<details><summary>Example</summary>

The following example extracts a substring from the string expression `abcdefghi` of length `3` starting at index `4`

```sql
SELECT 
  'abcdefghi' AS "original_string",
  SUBSTRING('abcdefghi', 4, 3) AS "substring"
```

Returns the following:

| `original_string` | `substring` |
| -- | -- |
| `abcdefghi` | `def` |

</details>



[Learn more](sql-scalar.md#string-functions)

## SUM

`SUM(expr)`

**Function type:** [Aggregation](sql-aggregations.md)

Calculates the sum of a set of values.

## TAN

Calculates the trigonometric tangent of an angle expressed in radians.

* **Syntax:** `TAN(expr)`
* **Function type:** Scalar, numeric

<details><summary>Example</summary>

The following example calculates the tangent of angle `PI/3` radians.

```sql
SELECT TAN(PI / 3) AS "tangent"
```
Returns the following:

| `tangent` |  
| -- | 
| `1.7320508075688767` |
</details>

[Learn more](sql-scalar.md#numeric-functions)

## TDIGEST_GENERATE_SKETCH

`TDIGEST_GENERATE_SKETCH(expr, [compression])`

**Function type:** [Aggregation](sql-aggregations.md)

Generates a T-digest sketch from values of the specified expression.

## TDIGEST_QUANTILE

`TDIGEST_QUANTILE(expr, quantileFraction, [compression])`

**Function type:** [Aggregation](sql-aggregations.md)

Returns the quantile for the specified fraction from a T-Digest sketch constructed from values of the expression.

## TEXTCAT

Concatenates two string expressions.

* **Syntax:** `TEXTCAT(expr, expr)`
* **Function type:** Scalar, string
  
<details><summary>Example</summary>

The following example concatenates the `OriginState` column from the `flight-carriers` datasource to `, USA`.

```sql
SELECT
  "OriginState" AS "origin_state",
  TEXTCAT("OriginState", ', USA') AS "concatenate_state_with_USA"
FROM "flight-carriers"
LIMIT 1
```

Returns the following:

| `origin_state` | `concatenate_state_with_USA` | 
| -- | -- | 
| `PR` | `PR, USA` | 

</details>

[Learn more](sql-scalar.md#string-functions)

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

Rounds up a timestamp to a given ISO 8601 time period. You can specify `origin` to provide a reference timestamp from which to start rounding. If provided, `timezone` should be a time zone name like `America/Los_Angeles` or an offset like `-08:00`.

* **Syntax:** `TIME_CEIL(timestamp_expr, period[, origin[, timezone]])`
* **Function type:** Scalar, date and time

<details><summary>Example</summary>

The following example rounds up the `__time` column from the `taxi-trips` datasource to the nearest 45th minute in reference to the timestamp `2013-08-01 08:0:00`.

```sql
SELECT 
  "__time" AS "original_timestamp",
  TIME_CEIL("__time", 'PT45M', TIMESTAMP '2013-08-01 08:0:00') AS "time_ceiling"
FROM "taxi-trips"
LIMIT 2
```

Returns the following:

| `original_timestamp` | `time_ceiling` |
| -- | -- |
| `2013-08-01T08:14:37.000Z` | `2013-08-01T08:45:00.000Z` |
| `2013-08-01T09:13:00.000Z` | `2013-08-01T09:30:00.000Z` |
</details>

[Learn more](sql-scalar.md#date-and-time-functions)

## TIME_EXTRACT

Extracts the value of `unit` from the timestamp and returns it as a number. If provided, `timezone` should be a time zone name like `America/Los_Angeles` or an offset like `-08:00`.

* **Syntax:** `TIME_EXTRACT(timestamp_expr[, unit[, timezone]])`
* **Function type:** Scalar, date and time

<details><summary>Example</summary>

The following example extracts the hour from the `__time` column in the `taxi-trips` datasource and offsets its timezone by `-04:00` hours.

```sql
SELECT 
  "__time" AS "original_timestamp",
  TIME_EXTRACT("__time", 'hour', '-04:00') AS "extract_hour"
FROM "taxi-trips"
LIMIT 2
```

Returns the following:

| `original_timestamp` | `extract_hour` | 
| -- | -- | 
| `2013-08-01T08:14:37.000Z` | `4` |
| `2013-08-01T09:13:00.000Z` | `5` |

</details>

[Learn more](sql-scalar.md#date-and-time-functions)

## TIME_FLOOR

Rounds down a timestamp to a given ISO 8601 time period. You can specify `origin` to provide a reference timestamp from which to start rounding. If provided, `timezone` should be a time zone name like `America/Los_Angeles` or an offset like `-08:00`.

* **Syntax:** `TIME_FLOOR(timestamp_expr, period[, origin[, timezone]])`
* **Function type:** Scalar, date and time

<details><summary>Example</summary>

The following example rounds down the `__time` column from the `taxi-trips` datasource to the nearest 45th minute in reference to the timestamp `2013-08-01 08:0:00`.

```sql
SELECT 
  "__time" AS "original_timestamp",
  TIME_FLOOR("__time", 'PT45M', TIMESTAMP '2013-08-01 08:0:00') AS "time_floor"
FROM "taxi-trips"
LIMIT 2
```

Returns the following:

| `original_timestamp` | `time_floor` |
| -- | -- |
| `2013-08-01T08:14:37.000Z` | `2013-08-01T08:00:00.000Z` |
| `2013-08-01T09:13:00.000Z` | `2013-08-01T08:45:00.000Z` |

</details>

[Learn more](sql-scalar.md#date-and-time-functions)

## TIME_FORMAT

Formats a timestamp as a string in a provided [Joda DateTimeFormat pattern](http://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html). If no pattern is provided, `pattern` defaults to ISO 8601. If provided, `timezone` should be a time zone name like `America/Los_Angeles` or an offset like `-08:00`.

* **Syntax:** `TIME_FORMAT(timestamp_expr[, pattern[, timezone]])`
* **Function type:** Scalar, date and time

<details><summary>Example</summary>

The following example formats the `__time` column from the `flight-carriers` datasource into a string format and offsets the result's timezone by `-05:00` hours.

```sql
SELECT
  "__time" AS "original_time",
TIME_FORMAT( "__time", 'dd-MM-YYYY hh:mm aa zzz', '-05:00') AS "string"
FROM "taxi-trips"
LIMIT 1
```

Returns the following:

| `original_time` | `string` |
| -- | -- |
| `2013-08-01T08:14:37.000Z` | `01-08-2013 03:14 AM -05:00` |

</details>

[Learn more](sql-scalar.md#date-and-time-functions)

## TIME_IN_INTERVAL

`TIME_IN_INTERVAL(<TIMESTAMP>, <CHARACTER>)`

**Function type:** [Scalar, date and time](sql-scalar.md#date-and-time-functions)

Returns whether a timestamp is contained within a particular interval, formatted as a string.

## TIME_PARSE

Parses a string into a timestamp using a given [Joda DateTimeFormat pattern](http://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html). If no pattern is provided, `pattern` defaults to ISO 8601. Returns NULL if string cannot be parsed. If provided, `timezone` should be a time zone name like `America/Los_Angeles` or an offset like `-08:00`.

* **Syntax:** `TIME_PARSE(string_expr[, pattern[, timezone]])`
* **Function type:** Scalar, date and time

<details><summary>Example</summary>

The following example parses the `FlightDate` STRING column from the `flight-carriers` datasource into a valid timestamp with an offset of `-05:00` hours.

```sql
SELECT
  "FlightDate" AS "original_string",
  TIME_PARSE("FlightDate", 'YYYY-MM-dd', '-05:00') AS "timestamp"
FROM "flight-carriers"
LIMIT 1
```

Returns the following:

| `original_string` | `timestamp` | 
| -- | -- |
| `2005-11-01` | `2005-11-01T05:00:00.000Z` |

</details>

[Learn more](sql-scalar.md#date-and-time-functions)

## TIME_SHIFT

Shifts a timestamp by a given number of time units. The `period` parameter can be any ISO 8601 period. The `step` parameter can be negative. If provided, `timezone` should be a time zone name like `America/Los_Angeles` or an offset like `-08:00`.

* **Syntax:** `TIME_SHIFT(timestamp_expr, period, step[, timezone])`
* **Function type:** Scalar, date and time

<details><summary>Example</summary>

The following example shifts the `__time` column from the `taxi-trips` datasource back by 24 hours.

```sql
SELECT
  "__time" AS "original_timestamp",
  TIME_SHIFT("__time", 'PT1H', -24) AS "shift_back"
FROM "taxi-trips"
LIMIT 1
```

Returns the following:

| `original_timestamp` | `shift_back` |
| -- | -- |
| `2013-08-01T08:14:37.000Z` | `2013-07-31T08:14:37.000Z` | 

</details>

[Learn more](sql-scalar.md#date-and-time-functions)


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

Trims the leading and/or trailing characters of an expression. Defaults `chars` to a space if none is provided. Defaults to `BOTH` if no directional argument is provided.

* **Syntax:** `TRIM([BOTH|LEADING|TRAILING] [chars FROM] expr)`
* **Function type:** Scalar, string

<details><summary>Example</summary>

The following example trims `_` characters from both ends of the string expression.

```sql
SELECT 
  '___abc___' AS "original_string",
  TRIM( BOTH '_' FROM '___abc___') AS "trim_expression"
```

Returns the following:

| `original_string` | `trim_expression` |
| -- | -- |
| `___abc___` | `abc` |

</details>

[Learn more](sql-scalar.md#string-functions)

## TRUNC

Alias for [`TRUNCATE`](#truncate).

* **Syntax:** `TRUNC(expr[, digits])`
* **Function type:** Scalar, numeric

[Learn more](sql-scalar.md#numeric-functions)

## TRUNCATE

Truncates a numerical expression to a specific number of decimal digits.

* **Syntax:** `TRUNCATE(expr[, digits])`
* **Function type:** Scalar, numeric

<details><summary>Example</summary>

The following applies the TRUNCATE function to 1 decimal place on the `pickup_longitude` column from the `taxi-trips` datasource.

```sql
SELECT
  "pickup_longitude" as "pickup_longitude",
  TRUNCATE("pickup_longitude", 1) as "truncate_pickup_longitude"
FROM "taxi-trips"
WHERE "pickup_longitude" IS NOT NULL
LIMIT 1
```
Returns the following:

| `pickup_longitude` | `truncate_pickup_longitude` | 
| -- | -- | 
| `-73.9377670288086` | `-73.9` | 
</details>


[Learn more](sql-scalar.md#numeric-functions)

## TRY_PARSE_JSON

**Function type:** [JSON](sql-json-functions.md)

`TRY_PARSE_JSON(expr)`

Parses `expr` into a `COMPLEX<json>` object. This operator deserializes JSON values when processing them, translating stringified JSON into a nested structure. If the input is not a `VARCHAR` or it is invalid JSON, this function will result in a `NULL` value.

## UNNEST

`UNNEST(source_expression) as table_alias_name(column_alias_name)`

Unnests a source expression that includes arrays into a target column with an aliased name. 

For more information, see [UNNEST](./sql.md#unnest).

## UPPER

Returns the expression in uppercase.

* **Syntax:** `UPPER(expr)`
* **Function type:** Scalar, string

<details><summary>Example</summary>

The following example converts the `OriginCityName` column from the `flight-carriers` datasource to uppercase.

```sql
SELECT 
  "OriginCityName" AS "origin_city",
  UPPER("OriginCityName") AS "uppercase"
FROM "flight-carriers"
LIMIT 1
```

Returns the following:

| `origin_city` | `uppercase` |
| -- | -- |
`San Juan, PR` | `SAN JUAN, PR` |

</details>

[Learn more](sql-scalar.md#string-functions)

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

