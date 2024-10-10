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

Approximates distinct values of a HLL sketch column or a regular column. See [DataSketches HLL Sketch module](../development/extensions-core/datasketches-hll.md) for a description of optional parameters.

* **Syntax:** `APPROX_COUNT_DISTINCT_DS_HLL(expr, [lgK, tgtHllType])`
* **Function type:** Aggregation

<details><summary>Example</summary>

The following example returns the approximate number of distinct tail numbers in the `flight_carriers` datasource.

```sql
SELECT APPROX_COUNT_DISTINCT_DS_HLL("Tail_Number") AS "estimate"
FROM "flight-carriers"
```

Returns the following:

| `estimate` |
| -- |
| `4686` |

</details>

[Learn more](sql-aggregations.md)

## APPROX_COUNT_DISTINCT_DS_THETA

Estimates distinct values of a Theta sketch column or a regular column. The [Theta sketch documentation](../development/extensions-core/datasketches-theta#aggregator) describes the optional `size` parameter. 

* **Syntax:** `APPROX_COUNT_DISTINCT_DS_THETA(expr, [size])`
* **Function type:** Aggregation

<details><summary>Example</summary>

The following example estimates the number of distinct tail numbers in the `Tail_Number` column from the `flight-carriers` datasource. 

```sql 
SELECT APPROX_COUNT_DISTINCT_DS_THETA("Tail_Number") AS "estimate"
FROM "flight-carriers"
```

Returns the following:

| `estimate` |
| -- |
| `4667` |

</details>

[Learn more](sql-aggregations.md)

## APPROX_QUANTILE

:::info
Deprecated in favor of [`APPROX_QUANTILE_DS`](#approx_quantile_ds).
:::

* **Syntax:** `APPROX_QUANTILE(expr, fraction, [k])`
* **Function type:** Aggregation

[Learn more](sql-aggregations.md)

## APPROX_QUANTILE_DS

Computes approximate quantiles on a Quantiles sketch column or a regular numeric column. The parameter `k` determines the accuracy and size of the sketch. The [DataSketches Quantiles Sketch module](../development/extensions-core/datasketches-quantiles.md) provides more information on the parameter `k`.

* **Syntax:** `APPROX_QUANTILE_DS(expr, fraction, [k])`
* **Function type:** Aggregation


<details><summary>Example</summary>

The following example approximates the median of the `Distance` column from the `flight-carriers` datasource using a `k` value of 128. The query may return a different approximation on each execution.

```sql
SELECT APPROX_QUANTILE_DS("Distance", 0.5, 128)  AS "estimate_median"
FROM "flight-carriers"
```

May return the following:

| `estimate_median` |
| -- | 
| `569` |

</details>

[Learn more](sql-aggregations.md)

## APPROX_QUANTILE_FIXED_BUCKETS

Computes approximate quantiles on fixed buckets histogram column or a regular numeric column. The [fixed buckets histogram](../development/extensions-core/approximate-histograms.md#fixed-buckets-histogram) documentation describes the parameters.

* **Syntax:** `APPROX_QUANTILE_FIXED_BUCKETS(expr, probability, numBuckets, lowerLimit, upperLimit, [outlierHandlingMode])`
* **Function type:** Aggregation


<details><summary>Example</summary>

The following example approximates the median of a histogram on the `Distance` column from the `flight-carriers` datasource. The histogram has 10 buckets, a lower limit of zero, an upper limit of 2500, and outliers are ignored. 

```sql
SELECT APPROX_QUANTILE_FIXED_BUCKETS("Distance", 0.5, 10, 0, 2500, 'ignore')  AS "estimate_median"
FROM "flight-carriers"
```

Returns the following:

| `estimate_median` |
| -- | 
| `571.6983032226562` |

</details>

[Learn more](sql-aggregations.md)

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

Returns a result based on given conditions.

* **Syntax:** `CASE expr WHEN value1 THEN result1 \[ WHEN value2 THEN result2 ... \] \[ ELSE resultN \] END`
* **Syntax:** `CASE WHEN boolean_expr1 THEN result1 \[ WHEN boolean_expr2 THEN result2 ... \] \[ ELSE resultN \] END`
* **Function type:** Scalar, other

<details><summary>Examples</summary>

The following example returns a string representing the UI type based on the value of `agent_category` from the `kttm` datasource.

```sql
SELECT "agent_category" AS "device_type",
CASE "agent_category"
    WHEN 'Personal computer' THEN 'Large UI'
    WHEN 'Smartphone' THEN 'Mobile UI'
    ELSE 'other'
END AS "UI_type"
FROM "kttm"
LIMIT 2
```

Returns the following:

| `device_type` | `UI_type` |
| -- | -- |
| `Personal computer` | `Large UI` |
| `Smartphone` | `Mobile UI` |

The following example returns a string based on the value of the `OriginStateName` column from the `flight-carriers` datasource.

```sql
SELECT "OriginStateName" AS "flight_origin",
CASE
    WHEN "OriginStateName" = 'Puerto Rico' THEN 'U.S. Territory'
    WHEN "OriginStateName" = 'U.S. Virgin Islands' THEN 'U.S. Territory'
    ELSE 'U.S. State'
END AS "state_status"
FROM "flight-carriers"
LIMIT 2
```

Returns the following:

| `flight_origin` | `departure_location` |
| -- | -- |
| `Puerto Rico` | `U.S. Territory` |
| `Massachusetts` | `U.S. State` |

</details>

[Lean more](sql-scalar.md#other-scalar-functions)


## CAST

Converts a value into the specified data type.

* **Syntax:** `CAST(value AS TYPE)`
* **Function type:** Scalar, other

<details><summary>Example</summary>

The following example converts the values in the `Distance` column from the `flight-carriers` datasource from `DOUBLE` into type `VARCHAR`

```sql
SELECT "Distance" AS "original_column",
      CAST("Distance" AS VARCHAR) "cast_to_string" 
FROM "flight-carriers"
LIMIT 1
```

Returns the following:

| `original_column` | `cast_to_string` |
| -- | -- | 
| `1571` | `1571.0` |

</details>

[Learn more](sql-scalar.md#other-scalar-functions)

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

Returns the first non-null value.
* **Syntax:** `COALESCE(expr, expr, ...)`
* **Function type:** Scalar, other

<details><summary>Example</summary>

The following example returns the first non-null value from `null`, `null`, `5`, and `abc`.

```sql
SELECT COALESCE(null, null, 5, 'abc') AS "first_non_null"
```

Returns the following:

| `first_non_null` |
| -- |
| `5` |

</details>

[Learn more](sql-scalar.md#other-scalar-functions)

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

:::info
The `DIV` function is not implemented in Druid versions 30.0.0 or earlier. Consider using [`SAFE_DIVIDE`](./sql-functions.md#safe_divide) instead. 
:::

## DS_CDF

Returns a string representing an approximation to the cumulative distribution function given a list of split points that define the edges of the bins from a Quantiles sketch.  

* **Syntax:** `DS_CDF(expr, splitPoint0, splitPoint1, ...)`
* **Function type:** Scalar, sketch

<details><summary>Example</summary>

The following example specifies three split points to return cumulative distribution function approximations on the `Distance` column from the `flight-carriers` datasource. The query may return a different approximation for each bin on each execution.

```sql 
SELECT DS_CDF( DS_QUANTILES_SKETCH("Distance"), 750, 1500, 2250) AS "estimate_cdf"
FROM "flight-carriers"
```

May return the following:

| `estimate_cdf` | 
| -- |
| `[0.6332237016416492,0.8908411023460711,0.9612303007393957,1.0]` |

</details>

[Learn more](sql-scalar.md#sketch-functions)

## DS_GET_QUANTILE

Returns the quantile estimate corresponding to `fraction` from a Quantiles sketch. 

* **Syntax:** `DS_GET_QUANTILE(expr, fraction)`
* **Function type:** Scalar, sketch

<details><summary>Example</summary>

The following example approximates the median of the `Distance` column from the `flight-carriers` datasource. The query may return a different approximation with each execution.

```sql
SELECT DS_GET_QUANTILE( DS_QUANTILES_SKETCH("Distance"), 0.5) AS "estimate_median"
FROM "flight-carriers"
```

May return the following:

| `estimate_median` |
| -- | 
| `569` |

</details>

[Learn more](sql-scalar.md#sketch-functions)

## DS_GET_QUANTILES

Returns a string representing an array of quantile estimates corresponding to a list of fractions from a Quantiles sketch. 

* **Syntax:** `DS_GET_QUANTILES(expr, fraction0, fraction1, ...)`
* **Function type:** Scalar, sketch

<details><summary>Example</summary>

The following example approximates the 25th, 50th, and 75th percentiles of the `Distance` column from the `flight-carriers` datasource. The query may return a different approximation for each percentile on each execution.

```sql 
SELECT DS_GET_QUANTILES( DS_QUANTILES_SKETCH("Distance"), 0.25, 0.5, 0.75) AS "estimate_fractions"
FROM "flight-carriers"
```

May returns the following:

| `estimate_fractions` |
| -- | 
| `[316.0,571.0,951.0]` |

</details>

[Learn more](sql-scalar.md#sketch-functions)

## DS_HISTOGRAM

Returns a string representing an approximation to the histogram given a list of split points that define the histogram bins from a Quantiles sketch. 

* **Syntax:** `DS_HISTOGRAM(expr, splitPoint0, splitPoint1, ...)`
* **Function type:** Scalar, sketch

<details><summary>Example</summary>

The following example specifies three split points to approximate a histogram on the `Distance` column from the `flight-carriers` datasource. The query may return a different approximation for each bin on each execution.

```sql
SELECT DS_HISTOGRAM( DS_QUANTILES_SKETCH("Distance"), 750, 1500, 2250) AS "estimate_histogram"
FROM "flight-carriers"

```

May return the following:

| `estimate_histogram` | 
| -- |
| `[354396.0,150199.00000000003,39848.000000000015,21694.99999999997]` |

</details>

[Learn more](sql-scalar.md#sketch-functions)

## DS_HLL

Creates a HLL sketch on a column containing HLL sketches or a regular column. See [DataSketches HLL Sketch module](../development/extensions-core/datasketches-hll.md) for a description of optional parameters.

* **Syntax:**`DS_HLL(expr, [lgK, tgtHllType])`
* **Function type:** Aggregation

<details><summary>Example</summary>

The following example creates a HLL sketch on the `Tail_number` column from the `flight-carriers` dataset, grouping by `OriginState` and `DestState`.

```sql
SELECT
  "OriginState" AS "origin_state",
  "DestState" AS "destination_state",
  DS_HLL("Tail_Number") AS "hll_tail_number"
FROM "flight-carriers"
GROUP BY 1,2
LIMIT 1
```

Returns the following:

| `origin_state` | `destination_state` | `hll_tail_number` | 
| -- | -- | -- | 
| `AK` | `AK` | `"AwEHDAcIAAFBAAAAfY..."` |

</details>


[Learn more](sql-aggregations.md)

## DS_QUANTILE_SUMMARY

Returns a string summary of a Quantiles sketch. 
* **Syntax:** `DS_QUANTILE_SUMMARY(expr)`
* **Function type:** Scalar, sketch

<details><summary>Example</summary>

The following example returns a summary of a Quantiles sketch on the `Distance` column from the `flight-carriers`.

```sql
SELECT DS_QUANTILE_SUMMARY( DS_QUANTILES_SKETCH("Distance") ) AS "summary"
FROM "flight-carriers"
```

Returns the following:

<table>
<tr>
<td><code>summary</code></td>
</tr>
<tr>
<td>

```
### Quantiles DirectCompactDoublesSketch SUMMARY: 
   Empty                        : false
   Memory, Capacity bytes       : true, 6128
   Estimation Mode              : true
   K                            : 128
   N                            : 566,138
   Levels (Needed, Total, Valid): 12, 12, 5
   Level Bit Pattern            : 100010100011
   BaseBufferCount              : 122
   Combined Buffer Capacity     : 762
   Retained Items               : 762
   Compact Storage Bytes        : 6,128
   Updatable Storage Bytes      : 14,368
   Normalized Rank Error        : 1.406%
   Normalized Rank Error (PMF)  : 1.711%
   Min Item                     : 2.400000e+01
   Max Item                     : 4.962000e+03
### END SKETCH SUMMARY
```

</td>
</tr>
</table>

</details>

[Learn more](sql-scalar.md#sketch-functions)

## DS_QUANTILES_SKETCH

Creates a Quantiles sketch on a column containing Quantiles sketches or a regular column. The parameter `k` determines the accuracy and size of the sketch. The [DataSketches Quantiles Sketch module](../development/extensions-core/datasketches-quantiles.md) provides more information on the parameter `k`.

* **Syntax:** `DS_QUANTILES_SKETCH(expr, [k])`
* **Function type:** Aggregation

<details><summary>Example</summary>

The following example creates a Quantile sketch on the `Distance` column from the `flight-carriers` datasource.

```sql
SELECT DS_QUANTILES_SKETCH("Distance") AS "quantile_sketch"
FROM "flight-carriers"
```

Returns the following:

| `quantile_sketch` | 
| -- | 
| `AgMIGoAAAAB6owgAA...` |

</details>

[Learn more](sql-aggregations.md)

## DS_RANK

Returns an approximate rank for `value` between zero and one, in which the rank signifies the fraction of the distribution less than `value`.

* **Syntax:** `DS_RANK(expr, value)`
* **Function type:** Scalar, sketch

<details><summary>Example</summary>

The following example approximates what fraction of records have a lesser value than `500` in the `Distance` column from the `flight-carries` datasource. The query may return a different approximation on each execution.

```sql
SELECT DS_RANK( DS_QUANTILES_SKETCH("Distance"), 500) AS "estimate_rank"
FROM "flight-carriers"
```

May return the following:

| `estimate_rank` |
| -- |
| `0.43791089804959216` |


</details>

[Learn more](sql-scalar.md#sketch-functions)

## DS_THETA

Creates a Theta sketch on a column containing Theta sketches or a regular column. The [Theta sketch documentation](../development/extensions-core/datasketches-theta#aggregator) describes the optional `size` parameter

* **Syntax:** `DS_THETA(expr, [size])`
* **Function type:** Aggregation


<details><summary>Example</summary>

The following example creates a Theta sketch on the `Tail_number` column from the `flight-carriers` dataset grouping by `originState` and `DestState`.

```sql
SELECT
  "OriginState" AS "origin_state",
  "DestState" AS "destination_state",
  DS_THETA("Tail_Number") AS "theta_tail_number"
FROM "flight-carriers"
GROUP BY 1,2
LIMIT 1
```

Returns the following:

| `origin_state` | `destination_state` | `theta_tail_number` |
| -- | -- | -- |
| `AK` | `AK` | `AgMDAAAazJNBAAAAA...` |

</details>

[Learn more](sql-aggregations.md)

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

## FIRST_VALUE

`FIRST_VALUE(expr)`

**Function type:** [Window](sql-window-functions.md#window-function-reference)

Returns the value evaluated for the expression for the first row within the window.

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

Returns a distinct count estimate from a HLL sketch. To round the distinct count estimate, set `round` to true. Otherwise, `round` defaults to false.

* **Syntax:** `HLL_SKETCH_ESTIMATE(expr, [round])`
* **Function type:** Scalar, sketch


<details><summary>Example</summary>

The following example estimates the number of unique tail numbers in the `flight-carriers` datasource.

```sql
SELECT
  HLL_SKETCH_ESTIMATE(DS_HLL("Tail_Number")) AS "estimate"
FROM "flight-carriers"
```

Returns the following:

| `estimate` | 
| -- |
| `4685.8815405960595` |

</details>

[Learn more](sql-scalar.md#sketch-functions)

## HLL_SKETCH_ESTIMATE_WITH_ERROR_BOUNDS

Returns the distinct count estimate and error bounds from a HLL sketch. To specify the number of standard deviations of the bounds use `numStdDev`.

* **Syntax:** `HLL_SKETCH_ESTIMATE_WITH_ERROR_BOUNDS(expr, [numStdDev])`
* **Function type:** Scalar, sketch

<details><summary>Example</summary>

The following example estimates the number of unique tail numbers in the `flight-carriers` datasource with error bounds at plus or minus one standard deviation.

```sql
SELECT
  HLL_SKETCH_ESTIMATE_WITH_ERROR_BOUNDS(DS_HLL("Tail_Number"), 1) AS "estimate_with_errors"
FROM "flight-carriers"
```

Returns the following:

| `estimate_with_errors` |
| -- |
| `[4685.8815405960595,4611.381540678335,4762.978259800803]` |


</details>

[Learn more](sql-scalar.md#sketch-functions)

## HLL_SKETCH_TO_STRING

Returns a human-readable string representation of a HLL sketch for debugging.

* **Syntax:** `HLL_SKETCH_TO_STRING(expr)`
* **Function type:** Scalar, sketch

<details><summary>Example</summary>

The following example returns the HLL sketch on column `Tail_Number` from the `flight-carriers` as a human-readable string.

```sql
SELECT
  HLL_SKETCH_TO_STRING( DS_HLL("Tail_Number") ) AS "summary"
FROM "flight-carriers"
```

Returns the following:

<table>
<tr>
<td><code>summary</code></td>
</tr>
<tr>
<td>

```
### HLL SKETCH SUMMARY: 
  Log Config K   : 12
  Hll Target     : HLL_4
  Current Mode   : HLL
  Memory         : false
  LB             : 4611.381540678335
  Estimate       : 4685.8815405960595
  UB             : 4762.978259800803
  OutOfOrder Flag: true
  CurMin         : 0
  NumAtCurMin    : 1316
  HipAccum       : 0.0
  KxQ0           : 2080.7755126953125
  KxQ1           : 0.0
  Rebuild KxQ Flg: false
```

</td>
</tr>
</table>

</details>

[Learn more](sql-scalar.md#sketch-functions)

## HLL_SKETCH_UNION

Returns a union of HLL sketches. See [DataSketches HLL Sketch module](../development/extensions-core/datasketches-hll.md) for a description of optional parameters.

* **Syntax:** `HLL_SKETCH_UNION([lgK, tgtHllType], expr0, expr1, ...)`
* **Function type:** Scalar, sketch


<details><summary>Example</summary>

The following example estimates the union of the HLL sketch of tail numbers that took off from `CA` and the HLL sketch of tail numbers that took off from `TX`. The examples uses the `Tail_Number` and `OriginState` columns from the `flight-carriers` datasource. 

```sql
SELECT
  HLL_SKETCH_ESTIMATE(
    HLL_SKETCH_UNION( 
      DS_HLL("Tail_Number") FILTER(WHERE "OriginState" = 'CA'),
      DS_HLL("Tail_Number") FILTER(WHERE "OriginState" = 'TX')
    )
  ) AS "estimate_union"
FROM "flight-carriers"
```

Returns the following:

| `estimate_union` |
| -- |
| `4204.798431046455` |

</details>

[Learn more](sql-scalar.md#sketch-functions)

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

Returns null if two values are equal, else returns the first value.
* **Syntax:** `NULLIF(value1, value2)`
* **Function type:** Scalar, other

<details><summary>Example</summary>

The following example returns null if the `OriginState` column from the `flight-carriers` datasource is `PR`.

```sql
SELECT "OriginState" AS "origin_state",
  NULLIF("OriginState", 'PR') AS "remove_pr"
FROM "flight-carriers"
LIMIT 2
```

Returns the following:

| `origin_state` | `remove_pr` |
| -- | -- |
| `PR` | `null` |
| `MA` | `MA` |

</details>

[Learn more](sql-scalar.md#other-scalar-functions)


## NVL

Returns `value1` if `value1` is not null, otherwise returns `value2`.

* **Syntax:** `NVL(value1, value1)`
* **Function type:** Scalar, other

<details><summary>Example</summary>

The following example returns "No tail number" if the `Tail_Number` column from the `flight-carriers` datasource is null.

```sql
SELECT "Tail_Number" AS "original_column",
  NVL("Tail_Number", 'No tail number') AS "remove_null"
FROM "flight-carriers"
WHERE "OriginState" = 'CT'
LIMIT 2
```

Returns the following:

| `original_column` | `remove_null`
| -- | -- |
| `N951DL` | `N951DL` |
| `null` | `No tail number` |

</details>

[Learn more](sql-scalar.md#other-scalar-functions)

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

Returns the distinct count estimate from a Theta sketch. The `expr` argument must return a Theta sketch.

* **Syntax:** `THETA_SKETCH_ESTIMATE(expr)`
* **Function type:** Scalar, sketch

<details><summary>Example</summary>

The following example estimates the distinct number of tail numbers in the `Tail_Number` column from the `flight-carriers` datasource.

```sql
SELECT THETA_SKETCH_ESTIMATE( DS_THETA("Tail_Number") ) AS "estimate"
FROM "flight-carriers"
```

Returns the following:

| `estimate` |
| -- | 
| `4667` |

</details>

[Learn more](sql-scalar.md#sketch-functions)

## THETA_SKETCH_ESTIMATE_WITH_ERROR_BOUNDS

Returns the distinct count estimate and error bounds from a Theta sketch. The `expr` argument must return a Theta sketch. The optional `errorBoundsStdDev` argument represents the standard deviation of the error bars and  must be 1, 2, or 3.

* **Syntax:** `THETA_SKETCH_ESTIMATE_WITH_ERROR_BOUNDS(expr, errorBoundsStdDev)`
* **Function type:** Scalar, sketch

<details><summary>Details</summary>

The following example returns the estimate with error bars for the number of distinct tail numbers in the `Tail_Number` column from the `flight-carriers`.

```sql
SELECT THETA_SKETCH_ESTIMATE_WITH_ERROR_BOUNDS(DS_THETA("Tail_Number", 4096), 1) AS "estimate_with_error"
FROM "flight-carriers"
```

Returns the following:

| `estimate_with_error` |
| -- |
| `{"estimate":4691.201541339628,"highBound":4718.4577807143205,"lowBound":4664.093801991001,"numStdDev":1}` |

</details>

[Learn more](sql-scalar.md#sketch-functions)

## THETA_SKETCH_INTERSECT

Returns an intersection of Theta sketches. Each input expression must return a Theta sketch. The [Theta sketch documentation](../development/extensions-core/datasketches-theta#aggregator) describes the optional `size` parameter. 

* **Syntax:** `THETA_SKETCH_INTERSECT([size], expr0, expr1, ...)`
* **Function type:** Scalar, sketch

<details><summary>Example</summary>

The following example estimates the intersection of distinct tail numbers that have flights that originate in `CA`, `TX` and `NY` based on the `flight-carriers` datasource.

```sql
SELECT
  THETA_SKETCH_ESTIMATE(
    THETA_SKETCH_INTERSECT( 
      DS_THETA("Tail_Number") FILTER(WHERE "OriginState" = 'CA'),
      DS_THETA("Tail_Number") FILTER(WHERE "OriginState" = 'TX'),
      DS_THETA("Tail_Number") FILTER(WHERE "OriginState" = 'NY')
    )
  ) AS "estimate_intersection"
FROM "flight-carriers"
```

Returns the following:

| `estimate_intersection` |
| -- |
| `1701` |

</details>

[Learn more](sql-scalar.md#sketch-functions)

## THETA_SKETCH_NOT

Returns a set difference of Theta sketches. Each input expression must return a Theta sketch. The [Theta sketch documentation](../development/extensions-core/datasketches-theta#aggregator) describes the optional `size` parameter.

* **Syntax:** `THETA_SKETCH_NOT([size], expr0, expr1, ...)`
* **Function type:** Scalar, sketch

<details><summary>Example</summary>

The following example estimates the number of distinct tail numbers that have no departures from the states of `CA`, `TX`, or `NY` based on the `flight-carriers` datasource.

```sql
SELECT
  THETA_SKETCH_ESTIMATE(
    THETA_SKETCH_NOT( 
      DS_THETA("Tail_Number"),
      DS_THETA("Tail_Number") FILTER(WHERE "OriginState" = 'CA'),
      DS_THETA("Tail_Number") FILTER(WHERE "OriginState" = 'TX'),
      DS_THETA("Tail_Number") FILTER(WHERE "OriginState" = 'NY')
    )
  ) AS "estimate_not"
FROM "flight-carriers"
```

Returns the following:

| `estimate_not` |
| -- | 
| `145` |

</details>

[Learn more](sql-scalar.md#sketch-functions)

## THETA_SKETCH_UNION

Returns a union of Theta sketches. Each input expression must return a Theta sketch. The [Theta sketch documentation](../development/extensions-core/datasketches-theta#aggregator) describes the optional `size` parameter.

* **Syntax:**`THETA_SKETCH_UNION([size], expr0, expr1, ...)`
* **Function type:** Scalar, sketch

<details><summary>Example</summary>

The following example estimates the number of distinct tail numbers that have departures from `CA`, `TX`, or `NY`.

```sql
SELECT
  THETA_SKETCH_ESTIMATE(
    THETA_SKETCH_UNION( 
      DS_THETA("Tail_Number") FILTER(WHERE "OriginState" = 'CA'),
      DS_THETA("Tail_Number") FILTER(WHERE "OriginState" = 'TX'),
      DS_THETA("Tail_Number") FILTER(WHERE "OriginState" = 'NY')
    )
  ) AS "estimate_union"
FROM "flight-carriers"
```
Returns the following:

| `estimate_union` |
| -- |
| `4522` |

</details>

[Learn more](sql-scalar.md#sketch-functions)

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
