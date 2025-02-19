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

This page provides a reference of Apache Druid&circledR; SQL functions in alphabetical order. For more details on a function, refer to the following:
* [Aggregation functions](sql-aggregations.md)
* [Array functions](sql-array-functions.md)
* [JSON functions](sql-json-functions.md)
* [Multi-value string functions](sql-multivalue-string-functions.md)
* [Scalar functions](sql-scalar.md)
* [Window functions](sql-window-functions.md)

## Example data

The examples on this page use the following example datasources:
* `array-example` created with [SQL-based ingestion](../multi-stage-query/index.md)
* `flight-carriers` using `FlightCarrierOnTime (1 month)` included with Druid
* `kttm` using `KoalasToTheMax one day` included with Druid
* `mvd-example` using [SQL-based ingestion](multi-value-dimensions.md#sql-based-ingestion)
* `taxi-trips` using `NYC Taxi cabs (3 files)` included with Druid

To load a datasource included with Druid,
access the [web console](../operations/web-console.md)
and go to **Load data > Batch - SQL > Example data**.
Select **Connect data**, and parse using the default settings.
On the page to configure the schema, select the datasource label
and enter the name of the datasource listed above.

Use the following query to create the `array-example` datasource:

<details>
<summary>Datasource for arrays</summary>

```sql
REPLACE INTO "array-example" OVERWRITE ALL
WITH "ext" AS (
  SELECT *
  FROM TABLE(
    EXTERN(
      '{"type":"inline","data":"{\"timestamp\": \"2023-01-01T00:00:00\", \"label\": \"row1\", \"arrayString\": [\"a\", \"b\"],  \"arrayLong\":[1, null,3], \"arrayDouble\":[1.1, 2.2, null]}\n{\"timestamp\": \"2023-01-01T00:00:00\", \"label\": \"row2\", \"arrayString\": [null, \"b\"], \"arrayLong\":null,        \"arrayDouble\":[999, null, 5.5]}\n{\"timestamp\": \"2023-01-01T00:00:00\", \"label\": \"row3\", \"arrayString\": [],          \"arrayLong\":[1, 2, 3],   \"arrayDouble\":[null, 2.2, 1.1]} \n{\"timestamp\": \"2023-01-01T00:00:00\", \"label\": \"row4\", \"arrayString\": [\"a\", \"b\"],  \"arrayLong\":[1, 2, 3],   \"arrayDouble\":[]}\n{\"timestamp\": \"2023-01-01T00:00:00\", \"label\": \"row5\", \"arrayString\": null,        \"arrayLong\":[],          \"arrayDouble\":null}"}',
      '{"type":"json"}'
    )
  ) EXTEND (
    "timestamp" VARCHAR,
    "label" VARCHAR,
    "arrayString" VARCHAR ARRAY,
    "arrayLong" BIGINT ARRAY,
    "arrayDouble" DOUBLE ARRAY
  )
)
SELECT
    TIME_PARSE("timestamp") AS "__time",
    "label",
    "arrayString",
    "arrayLong",
    "arrayDouble"
FROM "ext"
PARTITIONED BY DAY
```

</details>

Use the following query to create the `mvd-example` datasource:

<details>
<summary>Datasource for multi-value string dimensions</summary>

```sql
REPLACE INTO "mvd-example" OVERWRITE ALL
WITH "ext" AS (
  SELECT *
  FROM TABLE(
    EXTERN(
      '{"type":"inline","data":"{\"timestamp\": \"2011-01-12T00:00:00.000Z\", \"label\": \"row1\", \"tags\": [\"t1\",\"t2\",\"t3\"]}\n{\"timestamp\": \"2011-01-13T00:00:00.000Z\", \"label\": \"row2\", \"tags\": [\"t3\",\"t4\",\"t5\"]}\n{\"timestamp\": \"2011-01-14T00:00:00.000Z\", \"label\": \"row3\", \"tags\": [\"t5\",\"t6\",\"t7\"]}\n{\"timestamp\": \"2011-01-14T00:00:00.000Z\", \"label\": \"row4\", \"tags\": []}"}',
      '{"type":"json"}',
      '[{"name":"timestamp", "type":"STRING"},{"name":"label", "type":"STRING"},{"name":"tags", "type":"ARRAY<STRING>"}]'
    )
  )
)
SELECT
  TIME_PARSE("timestamp") AS "__time",
  "label",
  ARRAY_TO_MV("tags") AS "tags"
FROM "ext"
PARTITIONED BY DAY
```

</details>

## ABS

Calculates the absolute value of a numeric expression.

* **Syntax:** `ABS(<NUMERIC>)`
* **Function type:** Scalar, numeric

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

The following example calculates the arc cosine of `0`.

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

Returns any value of the specified expression.

* **Syntax**: `ANY_VALUE(expr, [maxBytesPerValue, [aggregateMultipleValues]])`
* **Function type:** Aggregation

<details>
<summary>Example</summary>

The following example returns the state abbreviation, state name, and average flight time grouped by each state in `flight-carriers`:

```sql
SELECT
  "OriginState",
  ANY_VALUE("OriginStateName") AS "OriginStateName",
  AVG("ActualElapsedTime") AS "AverageFlightTime"
FROM "flight-carriers"
GROUP BY 1
LIMIT 3
```

Returns the following:

|`OriginState`|`OriginStateName`|`AverageFlightTime`|
|-------------|-----------------|-------------------|
|`AK`|`Alaska`|`113.2777967841259`|
|`AL`|`Alabama`|`92.28766697732215`|
|`AR`|`Arkansas`|`95.0391382405745`|

</details>

[Learn more](sql-aggregations.md)

## APPROX_COUNT_DISTINCT

Counts distinct values of a regular column or a prebuilt sketch column using an approximate algorithm.

* **Syntax**: `APPROX_COUNT_DISTINCT(expr)`
* **Function type:** Aggregation

<details>
<summary>Example</summary>

The following example counts the number of distinct airlines reported in `flight-carriers`:

```sql
SELECT APPROX_COUNT_DISTINCT("Reporting_Airline") AS "num_airlines"
FROM "flight-carriers"
```

Returns the following:

| `num_airlines` |
| -- |
| `20` |

</details>

[Learn more](sql-aggregations.md)

## APPROX_COUNT_DISTINCT_BUILTIN

Counts distinct values of a string, numeric, or `hyperUnique` column using Druid's built-in `cardinality` or `hyperUnique` aggregators.
Consider using `APPROX_COUNT_DISTINCT_DS_HLL` instead, which offers better accuracy in many cases.

* **Syntax**: `APPROX_COUNT_DISTINCT_BUILTIN(expr)`
* **Function type:** Aggregation

<details>
<summary>Example</summary>

The following example counts the number of distinct airlines reported in `flight-carriers`:

```sql
SELECT APPROX_COUNT_DISTINCT_BUILTIN("Reporting_Airline") AS "num_airlines"
FROM "flight-carriers"
```

Returns the following:

| `num_airlines` |
| -- |
| `20` |

</details>

[Learn more](sql-aggregations.md)

## APPROX_COUNT_DISTINCT_DS_HLL

Returns the approximate number of distinct values in a HLL sketch column or a regular column. See [DataSketches HLL Sketch module](../development/extensions-core/datasketches-hll.md) for a description of optional parameters.

* **Syntax:** `APPROX_COUNT_DISTINCT_DS_HLL(expr, [lgK, tgtHllType])`
* **Function type:** Aggregation

<details>
<summary>Example</summary>

The following example returns the approximate number of distinct tail numbers in the `flight-carriers` datasource.

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

Returns the approximate number of distinct values in a Theta sketch column or a regular column. See [DataSketches Theta Sketch module](../development/extensions-core/datasketches-theta#aggregator) for a description of optional parameters.

* **Syntax:** `APPROX_COUNT_DISTINCT_DS_THETA(expr, [size])`
* **Function type:** Aggregation

<details>
<summary>Example</summary>

The following example returns the approximate number of distinct tail numbers in the `Tail_Number` column of the `flight-carriers` datasource.

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

* **Syntax:** `APPROX_QUANTILE(expr, probability, [k])`
* **Function type:** Aggregation

[Learn more](sql-aggregations.md)

## APPROX_QUANTILE_DS

Computes approximate quantiles on a Quantiles sketch column or a regular numeric column. See [DataSketches Quantiles Sketch module](../development/extensions-core/datasketches-quantiles.md) for a description of parameters.

* **Syntax:** `APPROX_QUANTILE_DS(expr, probability, [k])`
* **Function type:** Aggregation

<details>
<summary>Example</summary>

The following example approximates the median of the `Distance` column from the `flight-carriers` datasource. The query may return a different approximation on each execution.

```sql
SELECT APPROX_QUANTILE_DS("Distance", 0.5, 128)  AS "estimate_median"
FROM "flight-carriers"
```

Returns a result similar to the following:

| `estimate_median` |
| -- |
| `569` |

</details>

[Learn more](sql-aggregations.md)

## APPROX_QUANTILE_FIXED_BUCKETS

Computes approximate quantiles on fixed buckets histogram column or a regular numeric column. See [Fixed buckets histogram](../development/extensions-core/approximate-histograms.md#fixed-buckets-histogram) for a description of parameters.

* **Syntax:** `APPROX_QUANTILE_FIXED_BUCKETS(expr, probability, numBuckets, lowerLimit, upperLimit, [outlierHandlingMode])`
* **Function type:** Aggregation

<details>
<summary>Example</summary>

The following example approximates the median of a histogram on the `Distance` column from the `flight-carriers` datasource. The histogram has 10 buckets, a lower limit of zero, an upper limit of 2500, and ignores outlier values. 

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

## ARRAY

Constructs a SQL `ARRAY` literal from the provided expression arguments. All arguments must be of the same type.

* **Syntax**: `ARRAY[expr1, expr2, ...]`
* **Function type:** Array

<details>
<summary>Example</summary>

The following example constructs arrays from the values of the `agent_category`, `browser`, and `browser_version` columns in the `kttm` datasource.

```sql
SELECT ARRAY["agent_category", "browser", "browser_version"] AS "user_agent_details"
FROM "kttm"
LIMIT 5
```

Returns the following:

| `user_agent_details` |
| -- |
| `["Personal computer","Chrome","76.0.3809.100"]` |
| `["Smartphone","Chrome Mobile","50.0.2661.89"]` |
| `["Personal computer","Chrome","76.0.3809.100"]` |
| `["Personal computer","Opera","62.0.3331.116"]` |
| `["Smartphone","Mobile Safari","12.0"]` |

</details>

[Learn more](sql-array-functions.md)

## ARRAY_AGG

Returns an array of all values of the specified expression. To include only unique values, specify `DISTINCT`.

* **Syntax**: `ARRAY_AGG([DISTINCT] expr, [size])`
* **Function type:** Aggregation

<details>
<summary>Example</summary>

The following example returns arrays of unique values from the `OriginState` column in the `flight-carriers` datasource, grouped by `Reporting_Airline`.

```sql
SELECT "Reporting_Airline", ARRAY_AGG(DISTINCT "OriginState", 50000) AS "Origin"
FROM "flight-carriers"
GROUP BY "Reporting_Airline"
LIMIT 5
```

Returns the following:

| `Reporting_Airline` | `Origin` |
| -- | -- |
| `AA` |`["AL","AR","AZ","CA","CO","CT","FL","GA","HI","IL","IN","KS","KY","LA","MA","MD","MI","MN","MO","NC","NE","NJ","NM","NV","NY","OH","OK","OR","PA","PR","RI","TN","TX","UT","VA","VI","WA"]`|
| `AS` |`["AK","AZ","CA","CO","FL","ID","IL","MA","NJ","NV","OR","TX","VA","WA"]`|
| `B6` |`["AZ","CA","CO","FL","LA","MA","NJ","NV","NY","OR","PR","UT","VA","VT","WA"]`|
| `CO` |`["AK","AL","AZ","CA","CO","CT","FL","GA","HI","IL","IN","LA","MA","MD","MI","MN","MO","MS","NC","NE","NH","NJ","NM","NV","NY","OH","OK","OR","PA","PR","RI","SC","TN","TX","UT","VA","VI","WA"]`|
| `DH` |`["AL","CA","CT","FL","GA","IL","MA","ME","MI","NC","NH","NJ","NV","NY","OH","PA","RI","SC","TN","VA","VT","WA","WV"]`|

</details>

[Learn more](sql-aggregations.md)

## ARRAY_APPEND

Appends the expression to the array. The source array type determines the resulting array type.

* **Syntax**: `ARRAY_APPEND(arr, expr)`
* **Function type:** Array

<details>
<summary>Example</summary>

The following example appends `c` to the values in the `arrayString` column from the `array-example` datasource.

```sql
SELECT ARRAY_APPEND("arrayString",'c') AS "array_appended"
FROM "array-example"
```

Returns the following:

| `array_appended` |
| -- |
| `[a, b, c]` |
| `[null,"b","c"]`|
| `[c]` |
| `[a, b, c]`|
| `null` |

</details>

[Learn more](sql-array-functions.md)

## ARRAY_CONCAT

Concatenates two arrays. The type of `arr1` determines the resulting array type.

* **Syntax**: `ARRAY_CONCAT(arr1, arr2)`
* **Function type:** Array

<details>
<summary>Example</summary>

The following example concatenates the arrays in the `arrayLong` and `arrayDouble` columns from the `array-example` datasource.

```sql
SELECT ARRAY_CONCAT("arrayLong", "arrayDouble") AS "arrayConcatenated" 
FROM "array-example"
```

Returns the following:

| `arrayConcatenated` |
| -- |
| `[1,null,3,1.1,2.2,null]` |
| `null`|
| `[1,2,3,null,2.2,1.1]` |
| `[1,2,3]`|
| `null` |

</details>

[Learn more](sql-array-functions.md)

## ARRAY_CONCAT_AGG

Concatenates array inputs into a single array. To include only unique values, specify `DISTINCT`.

* **Syntax**: `ARRAY_CONCAT_AGG([DISTINCT] expr, [size])`
* **Function type:** Aggregation

<details>
<summary>Example</summary>

The following example concatenates the array inputs from the `arrayDouble` column of the `array-example` datasource into a single array.

```sql
SELECT ARRAY_CONCAT_AGG( DISTINCT "arrayDouble") AS "array_concat_agg_distinct"
FROM "array-example"
```

Returns the following:

| `array_concat_agg_distinct` |
| -- |
| `[null,1.1,2.2,5.5,999]` |

</details>

[Learn more](sql-aggregations.md)

## ARRAY_CONTAINS

Checks if the array contains the specified expression.

### Scalar

If the specified expression is a scalar value, returns true if the source array contains the value.

* **Syntax**: `ARRAY_CONTAINS(arr, expr)`
* **Function type:** Array

<details>
<summary>Example</summary>

The following example returns true if the `arraySring` column from the `array-example` datasource contains `2`.

```sql
SELECT "arrayLong", ARRAY_CONTAINS("arrayLong", 2) AS "arrayContains"
FROM "array-example"
```

Returns the following:

| `arrayLong` | `arrayContains` |
| -- | --|
| `[1,null,3]` | `false` |
| `null` | `null` |
| `[1,2,3]` |  `true` |
| `[1,2,3]` | `true` |
| `[]` | `false` |

</details>

[Learn more](sql-array-functions.md)

### Array

If the specified expression is an array, returns true if the source array contains all elements of the expression.

* **Syntax**: `ARRAY_CONTAINS(arr, expr)`
* **Function type:** Array

<details>
<summary>Example</summary>

The following example returns true if the `arrayLong` column from the `array-example` datasource contains all elements of the provided expression.

```sql
SELECT "label", "arrayLong", ARRAY_CONTAINS("arrayLong", ARRAY[1,2,3]) AS "arrayContains"
FROM "array-example"
```

Returns the following:

| `label` | `arrayLong` | `arrayContains` |
| -- | -- | -- |
| `row1` | `[1,null,3]` | `false` |
| `row2`| `null` | `null` |
| `row3`| `[1,2,3]` | `true` |
| `row4`| `[1,2,3]` | `true` |
| `row5`| `[]` | `false` |

</details>

[Learn more](sql-array-functions.md)

## ARRAY_LENGTH

Returns the length of the array.

* **Syntax**: `ARRAY_LENGTH(arr)`
* **Function type:** Array

<details>
<summary>Example</summary>

The following example returns the length of array expressions in the `arrayDouble` column from the `array-example` datasource.

```sql
SELECT "arrayDouble" AS "array", ARRAY_LENGTH("arrayDouble") AS "arrayLength"
FROM "array-example"
```

Returns the following:

| `larray` | `arrayLength` |
| -- | -- |
| `row1` | 3 |
| `row2`| 3 |
| `row3`| 3 |
| `row4`| 0 |
| `row5`| `null` |

</details>

[Learn more](sql-array-functions.md)

## ARRAY_OFFSET

Returns the array element at the specified zero-based index. Returns null if the index is out of bounds.

* **Syntax**: `ARRAY_OFFSET(arr, long)`
* **Function type:** Array

<details>
<summary>Example</summary>

The following example returns the element at the specified zero-based index from the arrays in the `arrayLong` column of the `array-example` datasource.

```sql
SELECT "arrayLong" as "array", ARRAY_OFFSET("arrayLong", 2) AS "elementAtIndex"
FROM "array-example"
```

Returns the following:

| `array` | `elementAtIndex` |
| -- | -- |
| `[1,null,3]` | 3 |
| `null`| `null` |
| `[1,2,3]`| 3 |
| `[1,2,3]`| 3 |
| `[]`| `null` |

</details>

[Learn more](sql-array-functions.md)

## ARRAY_OFFSET_OF

Returns the zero-based index of the first occurrence of the expression in the array. Returns null if the value isn't present, or `-1` if `druid.generic.useDefaultValueForNull=true` (deprecated legacy mode).

* **Syntax**: `ARRAY_OFFSET_OF(arr, expr)`
* **Function type:** Array

<details>
<summary>Example</summary>

The following example returns the zero-based index of the fist occurrence of `3` in the arrays in the `arrayLong` column of the `array-example` datasource.

```sql
SELECT "arrayLong" as "array", ARRAY_OFFSET_OF("arrayLong", 3) AS "offset"
FROM "array-example"
```

Returns the following:

| `array` | `offset` |
| -- | -- |
| `[1,null,3]` | 2 |
| `null`| `null` |
| `[1,2,3]`| 2 |
| `[1,2,3]`| 2 |
| `[]`| `null` |

</details>

[Learn more](sql-array-functions.md)

## ARRAY_ORDINAL

Returns the array element at the specified one-based index. Returns null if the index is out of bounds.

* **Syntax**: `ARRAY_ORDINAL(arr, long)`
* **Function type:** Array

<details>
<summary>Example</summary>

The following example returns the element at the specified one-based index from the arrays in the `arrayLong` column of the `array-example` datasource.

```sql
SELECT "arrayLong" as "array", ARRAY_ORDINAL("arrayLong", 2) AS "elementAtIndex"
FROM "array-example"
```

Returns the following:

| `array` | `elementAtIndex` |
| -- | -- |
| `[1,null,3]` | `null` |
| `null`| `null` |
| `[1,2,3]`| 2 |
| `[1,2,3]`| 2 |
| `[]`| `null` |

</details>

[Learn more](sql-array-functions.md)

## ARRAY_ORDINAL_OF

Returns the one-based index of the first occurrence of the expression in the array. Returns null if the value isn't present, or `-1` if `druid.generic.useDefaultValueForNull=true` (deprecated legacy mode).

* **Syntax**: `ARRAY_ORDINAL_OF(arr, expr)`
* **Function type:** Array

<details>
<summary>Example</summary>

The following example returns the one-based index of the fist occurrence of `3` in the arrays in the `arrayLong` column of the `array-example` datasource.

```sql
SELECT "arrayLong" as "array", ARRAY_ORDINAL_OF("arrayLong", 3) AS "ordinal"
FROM "array-example"
```

Returns the following:

| `array` | `ordinal` |
| -- | -- |
| `[1,null,3]` | 3 |
| `null`| `null` |
| `[1,2,3]`| 3 |
| `[1,2,3]`| 3 |
| `[]`| `null` |

</details>

[Learn more](sql-array-functions.md)

## ARRAY_OVERLAP

Returns true if two arrays have any elements in common. Treats `NULL` values as known elements.

* **Syntax**: `ARRAY_OVERLAP(arr1, arr2)`
* **Function type:** Array

<details>
<summary>Example</summary>

The following example returns true if columns `arrayString` and `arrayDouble` from the `array-example` datasource have common elements.

```sql
SELECT "arrayString", "arrayDouble",  ARRAY_OVERLAP("arrayString", "arrayDouble") AS "overlap"
FROM "array-example"
```

Returns the following:

| `arrayString` | `arrayDouble` | `overlap`|
| -- | -- | -- |
| `["a","b"]` | `[1.1,2.2,null]` | false |
| `[null,"b"]`| `[999,null,5.5]` | true |
| `[]`| `[null,2.2,1.1]` | false |
| `["a","b"]`| `[]` | false |
| `null`| `null` | `null` |

</details>

[Learn more](sql-array-functions.md)

## SCALAR_IN_ARRAY

Checks if the scalar value is present in the array. Returns false if the value is non-null, or `UNKNOWN` if the value is `NULL`. Returns `UNKNOWN` if the array is `NULL`.

* **Syntax**: `SCALAR_IN_ARRAY(expr, arr)`
* **Function type:** Array

<details>
<summary>Example</summary>

The following example returns true if the value `36` is present in the array generated from the elements in the `DestStateFips` column from the `flight-carriers` datasource.

```sql
SELECT "Reporting_Airline", ARRAY_AGG(DISTINCT "DestStateFips") AS "StateFipsArray", SCALAR_IN_ARRAY(36, ARRAY_AGG(DISTINCT "DestStateFips")) AS "ValueInArray"
FROM "flight-carriers"
GROUP BY "Reporting_Airline"
LIMIT 5
```

Returns the following:

| `Reporting_Airline` | `StateFipsArray` | `ValueInArray`|
| -- | -- | -- |
| `AA` | `[1,4,5,6,8,9,12,13,15,17,18,20,21,22,24,25,26,27,29,31,32,34,35,36,37,39,40,41,42,44,47,48,49,51,53,72,78]` | true |
| `AS`| `[2,4,6,8,12,16,17,25,32,34,41,48,51,53]` | false |
| `B6`| `[4,6,8,12,22,25,32,34,36,41,49,50,51,53,72]` | true |
| `CO`| `[1,2,4,6,8,9,12,13,15,17,18,22,24,25,26,27,28,29,31,32,33,34,35,36,37,39,40,41,42,44,45,47,48,49,51,53,72,78]` | true |
| `DH`| `[1,6,9,12,13,17,23,25,26,32,33,34,36,37,39,42,44,45,47,50,51,53,54]` | true |

</details>

[Learn more](sql-array-functions.md)

## ARRAY_PREPEND

Prepends the expression to the array. The source array type determines the resulting array type.

* **Syntax**: `ARRAY_PREPEND(expr, arr)`
* **Function type:** Array

<details>
<summary>Example</summary>

The following example prepends `c` to the arrays in the `arrayString` column from the `array-example` datasource.

```sql
SELECT ARRAY_PREPEND('c', "arrayString") AS "arrayPrepended"
FROM "array-example"
```

Returns the following:

| `arrayPrepended` |
| -- |
| `[c, a, b]` |
| `["c",null,"b"]`|
| `[c]`|
| `[c,a,b]`|
| `null`|

</details>

[Learn more](sql-array-functions.md)

## ARRAY_SLICE

Returns a subset of the array from the zero-based index `start` (inclusive) to `end` (exclusive). Returns null if `start` is less than 0, greater than the length of the array, or greater than `end`.

* **Syntax**: `ARRAY_SLICE(arr, start, end)`
* **Function type:** Array

<details>
<summary>Example</summary>

The following example constructs a new array from the elements of arrays in the `arrayDouble` column from the `array-example` datasource.

```sql
SELECT "arrayDouble", ARRAY_SLICE("arrayDouble", 0, 2) AS "arrayNew"
FROM "array-example"
```

Returns the following:

| `arrayDouble` | `arrayNew` |
| -- | -- |
| `[1.1,2.2,null]` | `[1.1,2.2]` |
| `[999,null,5.5]`| `[999,null]` |
| `[null,2.2,1.1]`| `[null,2.2]` |
| `[]`| `[null,null]` |
| `null`| `null` |

</details>

[Learn more](sql-array-functions.md)

## ARRAY_TO_MV

Converts an array of any type into a [multi-value string](sql-data-types.md#multi-value-strings).

* **Syntax**: `ARRAY_TO_MV(arr)`
* **Function type:** Array

<details>
<summary>Example</summary>

The following example converts the arrays in the `arrayDouble` column from the `array-example` datasource into multi-value strings.

```sql
SELECT ARRAY_TO_MV("arrayDouble") AS "multiValueString"
FROM "array-example"
```

Returns the following:

| `multiValueString` |
| -- |
| `["1.1","2.2",null]` |
| `["999.0",null,"5.5"]`|
| `[null,"2.2","1.1"]`|
| `[]`|
| `null`|

</details>

[Learn more](sql-array-functions.md)

## ARRAY_TO_STRING

Joins all elements of the array into a string using the specified delimiter.

* **Syntax**: `ARRAY_TO_STRING(arr, delimiter)`
* **Function type:** Array

<details>
<summary>Example</summary>

The following example converts the arrays in the `arrayDouble` column of the `array-example` datasource into concatenated strings.

```sql
SELECT ARRAY_TO_STRING("arrayDouble", '') AS "notSeparated"
FROM "array-example"
```

Returns the following:

| `multiValueString` |
| -- |
| `1.12.2null` |
| `999.0null5.5` |
| `null2.21.1` |
| ` ` |
| `null`|

</details>

[Learn more](sql-array-functions.md)

## ASIN

Calculates the arc sine (arcsine) of a numeric expression.

* **Syntax:** `ASIN(expr)`
* **Function type:** Scalar, numeric

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

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

Calculates the average of a set of values.

* **Syntax**: `AVG(<NUMERIC>)`
* **Function type:** Aggregation


<details>
<summary>Example</summary>

The following example calculates the average minutes of delay for a particular airlines in `flight-carriers`:

```sql
SELECT AVG("DepDelayMinutes") AS avg_delay
FROM "flight-carriers"
WHERE "Reporting_Airline" = 'AA'
```

Returns the following:

| `avg_delay` |
| -- |
| `8.936` |

</details>

[Learn more](sql-aggregations.md)

## BIT_AND

Performs a bitwise AND operation on all input values.

* **Syntax**: `BIT_AND(expr)`
* **Function type:** Aggregation

<details>
<summary>Example</summary>

The following example returns the bitwise AND operation for all values in `passenger-count` from `taxi-trips`:

```sql
SELECT
  BIT_AND("passenger_count") AS "bit_and"
FROM "taxi-trips"
```

Returns the following:

| `bit_and` |
| -- |
| `0` |

</details>

[Learn more](sql-aggregations.md)

## BIT_OR

Performs a bitwise OR operation on all input values.

* **Syntax**: `BIT_OR(expr)`
* **Function type:** Aggregation

<details>
<summary>Example</summary>

The following example returns the bitwise OR operation for all values in `passenger-count` from `taxi-trips`:

```sql
SELECT
  BIT_OR("passenger_count") AS "bit_or"
FROM "taxi-trips"
```

Returns the following:

| `bit_or` |
| -- |
| `15` |

</details>

[Learn more](sql-aggregations.md)

## BIT_XOR

Performs a bitwise XOR operation on all input values.

* **Syntax**: `BIT_XOR(expr)`
* **Function type:** Aggregation

<details>
<summary>Example</summary>

The following example returns the bitwise XOR operation for all values in `passenger-count` from `taxi-trips`:

```sql
SELECT
  BIT_OR("passenger_count") AS "bit_xor"
FROM "taxi-trips"
```

Returns the following:

| `bit_xor` |
| -- |
| `6` |

</details>

[Learn more](sql-aggregations.md)

## BITWISE_AND

Returns the bitwise AND between two expressions: `expr1 & expr2`. 

* **Syntax:** `BITWISE_AND(expr1, expr2)`
* **Function type:** Scalar, numeric

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

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

Computes a Bloom filter from values produced by the specified expression.

* **Syntax**: `BLOOM_FILTER(expr, <NUMERIC>)`
* **Function type:** Aggregation

[Learn more](sql-aggregations.md)

## BLOOM_FILTER_TEST

Returns true if the expression is contained in a Base64-serialized Bloom filter.

* **Syntax**: `BLOOM_FILTER_TEST(expr, <STRING>)`
* **Function type:** Scalar, other

[Learn more](sql-scalar.md#other-scalar-functions)

## BTRIM

Trims characters from both the leading and trailing ends of an expression. Defaults `chars` to a space if none is provided.

* **Syntax:** `BTRIM(expr[, chars])`
* **Function type:** Scalar, string

<details>
<summary>Example</summary>

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

Returns a result based on given conditions.

### Simple CASE

Compares an expression to a set of values or expressions.

* **Syntax:** `CASE expr WHEN value1 THEN result1 \[ WHEN value2 THEN result2 ... \] \[ ELSE resultN \] END`
* **Function type:** Scalar, other

<details>
<summary>Example</summary>

The following example returns a UI type based on the value of `agent_category` from the `kttm` datasource.

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

</details>

[Lean more](sql-scalar.md#other-scalar-functions)

### Searched CASE

Evaluates a set of Boolean expressions.

* **Syntax:** `CASE WHEN boolean_expr1 THEN result1 \[ WHEN boolean_expr2 THEN result2 ... \] \[ ELSE resultN \] END`
* **Function type:** Scalar, other

<details>
<summary>Example</summary>

The following example returns the departure location corresponding to the value of the `OriginStateName` column from the `flight-carriers` datasource.

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

<details>
<summary>Example</summary>

The following example converts the values in the `Distance` column from the `flight-carriers` datasource from `DOUBLE` to `VARCHAR`.

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

## CEIL

### Date and time

Rounds up a timestamp by a given time unit.

* **Syntax:** `CEIL(timestamp_expr TO unit>)`
* **Function type:** Scalar, date and time

<details>
<summary>Example</summary>

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

### Numeric

Calculates the smallest integer value greater than or equal to the numeric expression.
* **Syntax:** `CEIL(<NUMERIC>)`
* **Function type:** Scalar, numeric

<details>
<summary>Example</summary>

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

Returns the first non-null value.
* **Syntax:** `COALESCE(expr, expr, ...)`
* **Function type:** Scalar, other

<details>
<summary>Example</summary>

The following example returns the first non-null value from the list of parameters.

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

Concatenates a list of expressions.

* **Syntax:** `CONCAT(expr[, expr,...])`
* **Function type:** Scalar, string

<details>
<summary>Example</summary>

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

Returns true if `str` is a substring of `expr`, case-sensitive. Otherwise, returns false.

* **Syntax:** `CONTAINS_STRING(expr, str)`
* **Function type:** Scalar, string

<details>
<summary>Example</summary>

The following example returns true if the `OriginCityName` column from the `flight-carriers` datasource contains the substring `San`. 

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

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

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

Counts the number of rows.

* **Syntax**: `COUNT([DISTINCT] expr)` `COUNT(*)`  
COUNT DISTINCT is an alias for [`APPROX_COUNT_DISTINCT`](#approx_count_distinct).
* **Function type:** Aggregation

<details>
<summary>Example</summary>

The following example counts the number of distinct flights per day after `'2005-01-01 00:00:00'` in `flight-carriers`:

```sql
SELECT
  TIME_FLOOR(__time, 'P1D') AS "flight_day",
  COUNT(*) AS "num_flights"
FROM "flight-carriers"
WHERE __time > '2005-01-01 00:00:00'
GROUP BY 1
LIMIT 3
```

Returns the following:

|`flight_day`|`num_flights`|
|------------|------------|
|`2005-11-01T00:00:00.000Z`|`18961`|
|`2005-11-02T00:00:00.000Z`|`19434`|
|`2005-11-03T00:00:00.000Z`|`19745`|

</details>

[Learn more](sql-aggregations.md)

## CUME_DIST

Returns the cumulative distribution of the current row within the window calculated as `number of window rows at the same rank or higher than current row` / `total window rows`. The return value ranges between `1/number of rows` and 1.

* **Syntax**: `CUME_DIST()`
* **Function type:** Window

<details>
<summary>Example</summary>

The following example returns the cumulative distribution of number of flights by airline from two airports on a single day.

```sql
SELECT FLOOR("__time" TO DAY)  AS "flight_day",
    "Origin" AS "airport",
    "Reporting_Airline" as "airline",
    COUNT("Flight_Number_Reporting_Airline") as "num_flights",
    CUME_DIST() OVER (PARTITION BY "Origin" ORDER BY COUNT("Flight_Number_Reporting_Airline") DESC) AS "cume_dist"
FROM "flight-carriers"
WHERE FLOOR("__time" TO DAY) = '2005-11-01'
   AND "Origin" IN ('KOA', 'LIH')
GROUP BY 1, 2, 3
```

Returns the following:

| `flight_day` | `airport` | `airline` | `num_flights` | `cume_dist` |
| --- | --- | --- | --- | ---|
| `2005-11-01T00:00:00.000Z` | `KOA` | `HA` | `11` | `0.25` |
| `2005-11-01T00:00:00.000Z` | `KOA` | `UA` | `4` |  `0.5` |
| `2005-11-01T00:00:00.000Z` | `KOA` | `AA` | `1` |  `1` |
| `2005-11-01T00:00:00.000Z` | `KOA` | `NW` | `1` | `1` |
| `2005-11-01T00:00:00.000Z` | `LIH` | `HA` | `15` |  `0.3333333333333333` |
| `2005-11-01T00:00:00.000Z` | `LIH` | `AA` | `2` | `1`|
| `2005-11-01T00:00:00.000Z` | `LIH` | `UA` | `2` | `1` |
 
</details>


[Learn more](sql-window-functions.md#window-function-reference)

## CURRENT_DATE

Returns the current date in UTC time, unless you specify a different timezone in the query context.

* **Syntax:** `CURRENT_DATE`
* **Function type:** Scalar, date and time

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

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

Decodes a Base64-encoded string into a complex data type, where `dataType` is the complex data type and `expr` is the Base64-encoded string to decode.

* **Syntax**: `DECODE_BASE64_COMPLEX(dataType, expr)`
* **Function type:** Scalar, other

[Learn more](sql-scalar.md#other-scalar-functions)

## DECODE_BASE64_UTF8

Decodes a Base64-encoded string into a UTF-8 encoded string.

* **Syntax:** `DECODE_BASE64_UTF8(expr)`
* **Function type:** Scalar, string

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

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

Returns the rank for a row within a window without gaps. For example, if two rows tie for a rank of 1, the subsequent row is ranked 2.

* **Syntax**: `DENSE_RANK()`
* **Function type:** Window

<details>
<summary>Example</summary>

The following example returns the dense rank by airline for flights from two airports on a single day.

```sql
SELECT FLOOR("__time" TO DAY)  AS "flight_day",
    "Origin" AS "airport",
    "Reporting_Airline" as "airline",
    COUNT("Flight_Number_Reporting_Airline") as "num_flights",
    DENSE_RANK() OVER (PARTITION BY "Origin" ORDER BY COUNT("Flight_Number_Reporting_Airline") DESC) AS "dense_rank"
FROM "flight-carriers"
WHERE FLOOR("__time" TO DAY) = '2005-11-01'
    AND "Origin" IN ('KOA', 'LIH')
GROUP BY 1, 2, 3
```

Returns the following:

| `flight_day` | `airport` | `airline` | `num_flights` | `dense_rank` |
| --- | --- | --- | --- | ---|
| `2005-11-01T00:00:00.000Z` | `KOA` | `HA` | `11` | `1` |
| `2005-11-01T00:00:00.000Z` | `KOA` | `UA` | `4` | `2` |
| `2005-11-01T00:00:00.000Z` | `KOA` | `AA` | `1` | `3` |
| `2005-11-01T00:00:00.000Z` | `KOA` | `NW` | `1` | `3` |
| `2005-11-01T00:00:00.000Z` | `LIH` | `HA` | `15` | `1` |
| `2005-11-01T00:00:00.000Z` | `LIH` | `AA` | `2` | `2`|
| `2005-11-01T00:00:00.000Z` | `LIH` | `UA` | `2` | `2` |
 
</details>

[Learn more](sql-window-functions.md#window-function-reference)

## DIV

Returns the result of integer division of `x` by `y`.

:::info
The `DIV` function is not implemented in Druid versions 30.0.0 or earlier. Consider using [`SAFE_DIVIDE`](./sql-functions.md#safe_divide) instead. 
:::

* **Syntax:** `DIV(x, y)`
* **Function type:** Scalar, numeric


<details>
<summary>Example</summary>

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

Returns a string representing an approximation to the cumulative distribution function given a list of split points that define the edges of the bins from a Quantiles sketch.  

* **Syntax:** `DS_CDF(expr, splitPoint0, splitPoint1, ...)`
* **Function type:** Scalar, sketch

<details>
<summary>Example</summary>

The following example specifies three split points to return cumulative distribution function approximations on the `Distance` column from the `flight-carriers` datasource. The query may return a different approximation for each bin on each execution.

```sql 
SELECT DS_CDF( DS_QUANTILES_SKETCH("Distance"), 750, 1500, 2250) AS "estimate_cdf"
FROM "flight-carriers"
```

Returns a result similar to the following:

| `estimate_cdf` |
| -- |
| `[0.6332237016416492,0.8908411023460711,0.9612303007393957,1.0]` |

</details>

[Learn more](sql-scalar.md#sketch-functions)

## DS_GET_QUANTILE

Returns the quantile estimate corresponding to the fraction from a Quantiles sketch. 

* **Syntax:** `DS_GET_QUANTILE(expr, fraction)`
* **Function type:** Scalar, sketch

<details>
<summary>Example</summary>

The following example approximates the median of the `Distance` column from the `flight-carriers` datasource. The query may return a different approximation with each execution.

```sql
SELECT DS_GET_QUANTILE( DS_QUANTILES_SKETCH("Distance"), 0.5) AS "estimate_median"
FROM "flight-carriers"
```

Returns a result similar to the following:

| `estimate_median` |
| -- |
| `569` |

</details>

[Learn more](sql-scalar.md#sketch-functions)

## DS_GET_QUANTILES

Returns a string representing an array of quantile estimates corresponding to a list of fractions from a Quantiles sketch. 

* **Syntax:** `DS_GET_QUANTILES(expr, fraction0, fraction1, ...)`
* **Function type:** Scalar, sketch

<details>
<summary>Example</summary>

The following example approximates the 25th, 50th, and 75th percentiles of the `Distance` column from the `flight-carriers` datasource. The query may return a different approximation for each percentile on each execution.

```sql 
SELECT DS_GET_QUANTILES( DS_QUANTILES_SKETCH("Distance"), 0.25, 0.5, 0.75) AS "estimate_fractions"
FROM "flight-carriers"
```

Returns a result similar to the following:

| `estimate_fractions` |
| -- |
| `[316.0,571.0,951.0]` |

</details>

[Learn more](sql-scalar.md#sketch-functions)

## DS_HISTOGRAM

Returns an approximation to the histogram from a Quantiles sketch. The split points define the histogram bins. 

* **Syntax:** `DS_HISTOGRAM(expr, splitPoint0, splitPoint1, ...)`
* **Function type:** Scalar, sketch

<details>
<summary>Example</summary>

The following example specifies three split points to approximate a histogram on the `Distance` column from the `flight-carriers` datasource. The query may return a different approximation for each bin on each execution.

```sql
SELECT DS_HISTOGRAM( DS_QUANTILES_SKETCH("Distance"), 750, 1500, 2250) AS "estimate_histogram"
FROM "flight-carriers"

```

Returns a result similar to the following:

| `estimate_histogram` |
| -- |
| `[358496.0,153974.99999999997,39909.99999999999,13757.000000000005]` |

</details>

[Learn more](sql-scalar.md#sketch-functions)

## DS_HLL

Creates a HLL sketch on a column containing HLL sketches or a regular column. See [DataSketches HLL Sketch module](../development/extensions-core/datasketches-hll.md) for a description of optional parameters.

* **Syntax:**`DS_HLL(expr, [lgK, tgtHllType])`
* **Function type:** Aggregation

<details>
<summary>Example</summary>

The following example creates a HLL sketch on the `Tail_number` column of the `flight-carriers` datasource grouping by `OriginState` and `DestState`.

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

<details>
<summary>Example</summary>

The following example returns a summary of a Quantiles sketch on the `Distance` column from the `flight-carriers` datasource.

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

Creates a Quantiles sketch on a Quantiles sketch column or a regular column. See [DataSketches Quantiles Sketch module](../development/extensions-core/datasketches-quantiles.md) for a description of parameters.

* **Syntax:** `DS_QUANTILES_SKETCH(expr, [k])`
* **Function type:** Aggregation

<details>
<summary>Example</summary>

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

Returns an approximate rank of a given value in a distribution. The rank represents the fraction of the distribution less than the given value.

* **Syntax:** `DS_RANK(expr, value)`
* **Function type:** Scalar, sketch

<details>
<summary>Example</summary>

The following example estimates the fraction of records in the `flight-carriers` datasource where the value in the `Distance` column is less than 500. The query may return a different approximation on each execution.

```sql
SELECT DS_RANK( DS_QUANTILES_SKETCH("Distance"), 500) AS "estimate_rank"
FROM "flight-carriers"
```

Returns a result similar to the following:

| `estimate_rank` |
| -- |
| `0.43837721544923675 ` |


</details>

[Learn more](sql-scalar.md#sketch-functions)

## DS_THETA

Creates a Theta sketch on a column containing Theta sketches or a regular column. See [DataSketches Theta Sketch module](../development/extensions-core/datasketches-theta#aggregator) for a description of optional parameters.

* **Syntax:** `DS_THETA(expr, [size])`
* **Function type:** Aggregation

<details>
<summary>Example</summary>

The following example creates a Theta sketch on the `Tail_number` column of the `flight-carriers` datasource grouping by `OriginState` and `DestState`.

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

Creates a Tuple sketch on raw data or a precomputed sketch column. See [DataSketches Tuple Sketch module](../development/extensions-core/datasketches-tuple.md) for a description of parameters.

* **Syntax**: `DS_TUPLE_DOUBLES(expr[, nominalEntries])`  
              `DS_TUPLE_DOUBLES(dimensionColumnExpr, metricColumnExpr1[, metricColumnExpr2, ...], [nominalEntries])`
* **Function type:** Aggregation

<details>
<summary>Example</summary>

The following example creates a Tuples sketch column that stores the arrival and departure delay minutes for each airline in `flight-carriers`:

```sql
SELECT
  "Reporting_Airline",
  DS_TUPLE_DOUBLES("Reporting_Airline", "ArrDelayMinutes", "DepDelayMinutes") AS tuples_delay
FROM "flight-carriers"
GROUP BY 1
LIMIT 2
```

Returns the following:

|`Reporting_Airline`|`tuples_delay`|
|-------------------|--------------|
|`AA`|`1.0`|
|`AS`|`1.0`|

</details>

[Learn more](sql-aggregations.md)

## DS_TUPLE_DOUBLES_INTERSECT

Returns an intersection of Tuple sketches which each contain an array of double values as their Summary Objects. The values contained in the Summary Objects are summed when combined. If the last value of the array is a numeric literal, Druid assumes that the value is an override parameter for [nominal entries](../development/extensions-core/datasketches-tuple.md).

* **Syntax**: `DS_TUPLE_DOUBLES_INTERSECT(expr, ..., [nominalEntries])`
* **Function type:** Scalar, sketch

<details>
<summary>Example</summary>

The following example calculates the total minutes of arrival delay for airlines flying out of `SFO` or `LAX`.
An airline that doesn't fly out of both airports returns a value of 0.

```sql
SELECT
  "Reporting_Airline",
  DS_TUPLE_DOUBLES_METRICS_SUM_ESTIMATE(
    DS_TUPLE_DOUBLES_INTERSECT(
      DS_TUPLE_DOUBLES("Reporting_Airline", "ArrDelayMinutes") FILTER(WHERE "Origin" = 'SFO'),
      DS_TUPLE_DOUBLES("Reporting_Airline", "ArrDelayMinutes") FILTER(WHERE "Origin" = 'LAX')
    )
  ) AS arrival_delay_sfo_lax
FROM "flight-carriers"
GROUP BY 1
LIMIT 5
```

Returns the following:

|`Reporting_Airline`|`arrival_delay_sfo_lax`|
|----|---------|
|`AA`|`[33296]`|
|`AS`|`[13694]`|
|`B6`|`[0]`|
|`CO`|`[13582]`|
|`DH`|`[0]`|

</details>

[Learn more](sql-scalar.md#tuple-sketch-functions)

## DS_TUPLE_DOUBLES_METRICS_SUM_ESTIMATE

Computes approximate sums of the values contained within a Tuple sketch which contains an array of double values as the Summary Object.

* **Syntax**: `DS_TUPLE_DOUBLES_METRICS_SUM_ESTIMATE(expr)`
* **Function type:** Scalar, sketch

<details>
<summary>Example</summary>

The following example calculates the sum of arrival and departure delay minutes for each airline in `flight-carriers`:

```sql
SELECT
  "Reporting_Airline",
  DS_TUPLE_DOUBLES_METRICS_SUM_ESTIMATE(DS_TUPLE_DOUBLES("Reporting_Airline", "ArrDelayMinutes", "DepDelayMinutes")) AS sum_delays
FROM "flight-carriers"
GROUP BY 1
LIMIT 2
```

Returns the following:

|`Reporting_Airline`|`sum_delays`|
|----|-----------------|
|`AA`|`[612831,474309]`|
|`AS`|`[157340,141462]`|

Compare this example with an analogous SQL statement that doesn't use approximations:

```sql
SELECT
  "Reporting_Airline",
  SUM("ArrDelayMinutes") AS sum_arrival_delay,
  SUM("DepDelayMinutes") AS sum_departure_delay
FROM "flight-carriers"
GROUP BY 1
LIMIT 2
```

Returns the following:

|`Reporting_Airline`|`sum_arrival_delay`|`sum_departure_delay`|
|----|--------|--------|
|`AA`|`612831`|`475735`|
|`AS`|`157340`|`143620`|

</details>

[Learn more](sql-scalar.md#tuple-sketch-functions)

## DS_TUPLE_DOUBLES_NOT

Returns a set difference of Tuple sketches which each contain an array of double values as their Summary Objects. The values contained in the Summary Object are preserved as is. If the last value of the array is a numeric literal, Druid assumes that the value is an override parameter for [nominal entries](../development/extensions-core/datasketches-tuple.md).

* **Syntax**: `DS_TUPLE_DOUBLES_NOT(expr, ..., [nominalEntries])`
* **Function type:** Scalar, sketch

<details>
<summary>Example</summary>

The following example calculates the total minutes of arrival delay for airlines that fly out of `SFO` but not `LAX`.

```sql
SELECT
  "Reporting_Airline",
  DS_TUPLE_DOUBLES_METRICS_SUM_ESTIMATE(
    DS_TUPLE_DOUBLES_NOT(
      DS_TUPLE_DOUBLES("Reporting_Airline", "ArrDelayMinutes") FILTER(WHERE "Origin" = 'SFO'),
      DS_TUPLE_DOUBLES("Reporting_Airline", "ArrDelayMinutes") FILTER(WHERE "Origin" = 'LAX')
    )
  ) AS arrival_delay_sfo_lax
FROM "flight-carriers"
GROUP BY 1
LIMIT 5
```

Returns the following:

|`Reporting_Airline`|`arrival_delay_sfo_lax`|
|----|---------|
|`AA`|`[0]`|
|`AS`|`[0]`|
|`B6`|`[0]`|
|`CO`|`[0]`|
|`DH`|`[93]`|

</details>

[Learn more](sql-scalar.md#tuple-sketch-functions)

## DS_TUPLE_DOUBLES_UNION

Returns a union of Tuple sketches which each contain an array of double values as their Summary Objects. The values contained in the Summary Objects are summed when combined. If the last value of the array is a numeric literal, Druid assumes that the value is an override parameter for [nominal entries](../development/extensions-core/datasketches-tuple.md).

* **Syntax**: `DS_TUPLE_DOUBLES_UNION(expr, ..., [nominalEntries])`
* **Function type:** Scalar, sketch

<details>
<summary>Example</summary>

The following example calculates the total minutes of arrival delay for airlines flying out of either `SFO` or `LAX`.

```sql
SELECT
  "Reporting_Airline",
  DS_TUPLE_DOUBLES_METRICS_SUM_ESTIMATE(
    DS_TUPLE_DOUBLES_UNION(
      DS_TUPLE_DOUBLES("Reporting_Airline", "ArrDelayMinutes") FILTER(WHERE "Origin" = 'SFO'),
      DS_TUPLE_DOUBLES("Reporting_Airline", "ArrDelayMinutes") FILTER(WHERE "Origin" = 'LAX')
    )
  ) AS arrival_delay_sfo_lax
FROM "flight-carriers"
GROUP BY 1
LIMIT 5
```

Returns the following:

|`Reporting_Airline`|`arrival_delay_sfo_lax`|
|----|---------|
|`AA`|`[33296]`|
|`AS`|`[13694]`|
|`B6`|`[0]`|
|`CO`|`[13582]`|
|`DH`|`[93]`|

</details>

[Learn more](sql-scalar.md#tuple-sketch-functions)

## EARLIEST

Returns the value of a numeric or string expression corresponding to the earliest `__time` value.

* **Syntax**: `EARLIEST(expr, [maxBytesPerValue])`
* **Function type:** Aggregation

<details>
<summary>Example</summary>

The following example returns the origin airport code associated with the earliest departing flight daily after `'2005-01-01 00:00:00'` in `flight-carriers`:

```sql
SELECT
  TIME_FLOOR(__time, 'P1D') AS "departure_day",
  EARLIEST("Origin") AS "origin"
FROM "flight-carriers"
WHERE __time >= TIMESTAMP '2005-01-01 00:00:00'
GROUP BY 1
LIMIT 2
```

Returns the following:

|`departure_day`|`origin`|
|------------|--------|
|`2005-11-01T00:00:00.000Z`|`LAS`|
|`2005-11-02T00:00:00.000Z`|`SDF`|

</details>

[Learn more](sql-aggregations.md)

## EARLIEST_BY

Returns the value of a numeric or string expression corresponding to the earliest time value from `timestampExpr`.

* **Syntax**: `EARLIEST_BY(expr, timestampExpr, [maxBytesPerValue])`
* **Function type:** Aggregation

<details>
<summary>Example</summary>

The following example returns the destination airport code associated with the earliest arriving flight daily after `'2005-01-01 00:00:00'` in `flight-carriers`:

```sql
SELECT
  TIME_FLOOR(TIME_PARSE("arrivalime"), 'P1D') AS "arrival_day",
  EARLIEST_BY("Dest", TIME_PARSE("arrivalime")) AS "dest"
FROM "flight-carriers"
WHERE TIME_PARSE("arrivalime") >= TIMESTAMP '2005-01-01 00:00:00'
GROUP BY 1
LIMIT 2
```

Returns the following:

|`arrival_day`|`origin`|
|-------------|--------|
|`2005-11-01T00:00:00.000Z`|`RSW`|
|`2005-11-02T00:00:00.000Z`|`CLE`|

</details>

[Learn more](sql-aggregations.md)

## EXP

Calculates _e_ raised to the power of the numeric expression.

* **Syntax:** `EXP(<NUMERIC>)`
* **Function type:** Scalar, numeric

<details>
<summary>Example</summary>

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

Extracts the value of some unit from the timestamp.

* **Syntax:** `EXTRACT(unit FROM timestamp_expr)`
* **Function type:** Scalar, date and time

<details>
<summary>Example</summary>

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

Returns the value evaluated for the expression for the first row within the window.

* **Syntax**: `FIRST_VALUE(expr)`
* **Function type:** Window

<details>
<summary>Example</summary>

The following example returns the name of the first airline in the window of flights by airline for two airports on a single day.

```sql
SELECT FLOOR("__time" TO DAY)  AS "flight_day",
    "Origin" AS "airport",
    "Reporting_Airline" as "airline",
    COUNT("Flight_Number_Reporting_Airline") as "num_flights",
    FIRST_VALUE("Reporting_Airline") OVER (PARTITION BY "Origin" ORDER BY COUNT("Flight_Number_Reporting_Airline") DESC) AS "first_val"
FROM "flight-carriers"
WHERE FLOOR("__time" TO DAY) = '2005-11-01'
    AND "Origin" IN ('KOA', 'LIH')
GROUP BY 1, 2, 3
```

Returns the following:

| `flight_day` | `airport` | `airline` | `num_flights` | `first_val` |
| --- | --- | --- | --- | ---|
| `2005-11-01T00:00:00.000Z` | `KOA` | `HA` | `11` | `HA` |
| `2005-11-01T00:00:00.000Z` | `KOA` | `UA` | `4` | `HA` |
| `2005-11-01T00:00:00.000Z` | `KOA` | `AA` | `1` | `HA` |
| `2005-11-01T00:00:00.000Z` | `KOA` | `NW` | `1` | `HA` |
| `2005-11-01T00:00:00.000Z` | `LIH` | `HA` | `15` | `HA` |
| `2005-11-01T00:00:00.000Z` | `LIH` | `AA` | `2` | `HA` |
| `2005-11-01T00:00:00.000Z` | `LIH` | `UA` | `2` | `HA` |
 
</details>

[Learn more](sql-window-functions.md#window-function-reference)

## FLOOR

### Date and time

Rounds down a timestamp by a given time unit. 

* **Syntax:** `FLOOR(timestamp_expr TO unit)`
* **Function type:** Scalar, date and time

<details>
<summary>Example</summary>

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

### Numeric

Calculates the largest integer less than or equal to the numeric expression.

* **Syntax:** `FLOOR(expr)`
* **Function type:** Scalar, numeric

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

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

Returns a number for each output row of a groupBy query, indicating whether the specified dimension is included for that row.

* **Syntax**: `GROUPING(expr, expr...)`
* **Function type:** Aggregation

<details>
<summary>Example</summary>

The following example returns the total minutes of flight delay for each day of the week in `flight-carriers`.
The GROUP BY clause creates two grouping sets, one for the day of the week and one for the grand total.

For more information, refer to [CASE](#case) and grouping sets with [SQL GROUP BY](sql.md#group-by).

```sql
SELECT
  CASE
     WHEN GROUPING("DayOfWeek") = 1 THEN 'Total'
     ELSE "DayOfWeek"
  END AS "DayOfWeek",
  GROUPING("DayOfWeek") AS Subgroup,
  SUM("DepDelayMinutes") AS "MinutesDelayed"
FROM "flight-carriers"
GROUP BY GROUPING SETS("DayOfWeek", ())
```

Returns the following:

|`DayOfWeek`|`Subgroup`|`MinutesDelayed`|
|-----------|-----------|----------------|
|`1`|`0`|`998505`|
|`2`|`0`|`1031599`|
|`3`|`0`|`884677`|
|`4`|`0`|`525351`|
|`5`|`0`|`519413`|
|`6`|`0`|`354601`|
|`7`|`0`|`848704`|
|`Total`|`1`|`5162850`|

</details>

[Learn more](sql-aggregations.md)

## HLL_SKETCH_ESTIMATE

Returns the distinct count estimate from a HLL sketch. To round the distinct count estimate, set `round` to true. `round` defaults to false.

* **Syntax:** `HLL_SKETCH_ESTIMATE(expr, [round])`
* **Function type:** Scalar, sketch


<details>
<summary>Example</summary>

The following example estimates the distinct number of unique tail numbers in the `flight-carriers` datasource.

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

Returns the distinct count estimate and error bounds from a HLL sketch. To specify the number of standard bound deviations, use `numStdDev`.

* **Syntax:** `HLL_SKETCH_ESTIMATE_WITH_ERROR_BOUNDS(expr, [numStdDev])`
* **Function type:** Scalar, sketch

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

The following example returns the HLL sketch on column `Tail_Number` from the `flight-carriers` datasource as a human-readable string.

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


<details>
<summary>Example</summary>

The following example estimates the union of the HLL sketch of tail numbers that took off from `CA` and the HLL sketch of tail numbers that took off from `TX`. The example uses the `Tail_Number` and `OriginState` columns from the `flight-carriers` datasource. 

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

Converts an integer byte size into human-readable [IEC](https://en.wikipedia.org/wiki/Binary_prefix) format.

* **Syntax:** `HUMAN_READABLE_BINARY_BYTE_FORMAT(value[, precision])`
* **Function type:** Scalar, numeric

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

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

Returns true if `str` is a substring of `expr`, case-insensitive. Otherwise, returns false.

* **Syntax:** `ICONTAINS_STRING(expr, str)`
* **Function type:** Scalar, string

<details>
<summary>Example</summary>

The following example returns true if the `OriginCityName` column from the `flight-carriers` datasource contains the case-insensitive substring `san`.  

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

Returns true if the IPv4 `address` belongs to the `subnet` literal, otherwise returns false.

* **Syntax:** `IPV4_MATCH(address, subnet)`
* **Function type:** Scalar, IP address

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

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

Returns an array of field names from an expression, at a specified path.

* **Syntax:** `JSON_KEYS(expr, path)`
* **Function type:** JSON

<details>
<summary>Example</summary>

The following example returns an array of field names from the nested column `agent`:

```sql
SELECT
  JSON_KEYS(agent, '$.') AS agent_keys
FROM "kttm_nested"
LIMIT 1
```

Returns the following:

| `agent_keys` |
| -- |
| `[type, category, browser, browser_version, os, platform]` |

</details>

[Learn more](sql-json-functions.md)

## JSON_MERGE

Merges two or more JSON `STRING` or `COMPLEX<json>` expressions into one, preserving the rightmost value when there are key overlaps.
The function always returns a `COMPLEX<json>` object.

* **Syntax:** `JSON_MERGE(expr1, expr2[, expr3 ...])`
* **Function type:** JSON

<details>
<summary>Example</summary>

The following example merges the `event` object with a static string `example_string`:

```sql
SELECT 
  event,
  JSON_MERGE(event, '{"example_string": 123}') as event_with_string
FROM "kttm_nested"
LIMIT 1
```

Returns the following:

| `event` | `event_with_string` |
| -- | -- |
| `{"type":"PercentClear","percentage":55}` | `{"type":"PercentClear","percentage":55,"example_string":123}` |

</details>

[Learn more](sql-json-functions.md)

## JSON_OBJECT

Constructs a new `COMPLEX<json>` object from one or more expressions. 
The `KEY` expressions must evaluate to string types.
The `VALUE` expressions can be composed of any input type, including other `COMPLEX<json>` objects.
The function can accept colon-separated key-value pairs.

* **Syntax:** `JSON_OBJECT(KEY expr1 VALUE expr2[, KEY expr3 VALUE expr4, ...])`  
  or  
  `JSON_OBJECT(expr1:expr2[, expr3:expr4, ...])`
* **Function type:** JSON

<details>
<summary>Example</summary>

The following example creates a new object `combinedJSON` from `continent` in `geo_ip` and `type` in `event`:

```sql
SELECT
  JSON_OBJECT(
     KEY 'geo_ip' VALUE JSON_QUERY(geo_ip, '$.continent'),
     KEY 'event' VALUE JSON_QUERY(event, '$.type')
     )
  as combined_JSON
FROM "kttm_nested"
LIMIT 1
```

Returns the following:

| `combined_JSON` |
| -- |
| `{"geo_ip": {"continent": "South America"},"event": {"type": "PercentClear"}}` |

</details>

[Learn more](sql-json-functions.md)

## JSON_PATHS

Returns an array of all paths which refer to literal values in an expression, in JSONPath format.

* **Syntax:** `JSON_PATHS(expr)`  
* **Function type:** JSON

<details>
<summary>Example</summary>

The following example returns an array of distinct paths in the `geo_ip` nested column:

```sql
SELECT
  ARRAY_CONCAT_AGG(DISTINCT JSON_PATHS(geo_ip)) AS geo_ip_paths
from "kttm_nested"
```

Returns the following:

| `geo_ip_paths` |
| -- |
| `[$.city, $.continent, $.country, $.region]` |

</details>

[Learn more](sql-json-functions.md)

## JSON_QUERY

Extracts a `COMPLEX<json>` value from an expression at a specified path.

* **Syntax:** `JSON_QUERY(expr, path)`  
* **Function type:** JSON

<details>
<summary>Example</summary>

The following example returns the values of `percentage` in the `event` nested column:

```sql
SELECT
   "event",
   JSON_QUERY("event", '$.percentage')
FROM "kttm_nested"
LIMIT 2
```

Returns the following:

| `event` | `percentage` |
| -- | -- |
| `{"type":"PercentClear","percentage":55}` | `55` |
| `{"type":"PercentClear","percentage":80}` | `80` |

</details>

[Learn more](sql-json-functions.md)

## JSON_QUERY_ARRAY

Extracts an `ARRAY<COMPLEX<json>>` value from an expression at a specified path.

If the value isn't an array, the function translates it into a single element `ARRAY` containing the value at `path`.
This function is mainly used to extract arrays of objects to use as inputs to other [array functions](./sql-array-functions.md).

* **Syntax:** `JSON_QUERY_ARRAY(expr, path)`
* **Function type:** JSON

<details>
<summary>Example</summary>

The following example returns an array of `percentage` values in the `event` nested column:

```sql
SELECT
   "event",
   JSON_QUERY_ARRAY("event", '$.percentage')
FROM "kttm_nested"
LIMIT 2
```

Returns the following:

| `event` | `percentage` |
| -- | -- |
| `{"type":"PercentClear","percentage":55}` | `[55]` |
| `{"type":"PercentClear","percentage":80}` | `[80]` |

</details>

[Learn more](sql-json-functions.md)

## JSON_VALUE

Extracts a literal value from an expression at a specified path.

If you include `RETURNING` and specify a SQL type (such as `VARCHAR`, `BIGINT`, `DOUBLE`) the function plans the query using the suggested type.
If `RETURNING` isn't included, the function attempts to infer the type based on the context.
If the function can't infer the type, it defaults to `VARCHAR`.

* **Syntax:** `JSON_VALUE(expr, path [RETURNING sqlType])`
* **Function type:** JSON

<details>
<summary>Example</summary>

The following example returns the value of `city` in the `geo_ip` nested column:

```sql
SELECT
  geo_ip,
  JSON_VALUE(geo_ip, '$.city' RETURNING VARCHAR) as city
FROM "kttm_nested"
WHERE JSON_VALUE(geo_ip, '$.continent') = 'Asia'
LIMIT 2
```

Returns the following:

| `geo_ip` | `city` |
| -- | -- |
| `{"continent":"Asia","country":"Taiwan","region":"Taipei City","city":"Taipei"}` | `Taipei` |
| `{"continent":"Asia","country":"Thailand","region":"Bangkok","city":"Bangkok"}` | `Bangkok` |

</details>

[Learn more](sql-json-functions.md)

## LAG

If you do not supply an `offset`, returns the value evaluated at the row preceding the current row. Specify an offset number `n` to return the value evaluated at `n` rows preceding the current one.

* **Syntax**: `LAG(expr[, offset])`
* **Function type:** Window

<details>
<summary>Example</summary>

The following example returns the preceding airline in the window for flights by airline from two airports on a single day.

```sql
SELECT FLOOR("__time" TO DAY)  AS "flight_day",
    "Origin" AS "airport",
    "Reporting_Airline" as "airline",
    COUNT("Flight_Number_Reporting_Airline") as "num_flights",
    LAG("Reporting_Airline") OVER (PARTITION BY "Origin" ORDER BY COUNT("Flight_Number_Reporting_Airline") DESC) AS "lag"
FROM "flight-carriers"
WHERE FLOOR("__time" TO DAY) = '2005-11-01'
    AND "Origin" IN ('KOA', 'LIH')
GROUP BY 1, 2, 3
```

Returns the following:

| `flight_day` | `airport` | `airline` | `num_flights` | `lag` |
| --- | --- | --- | --- | ---|
| `2005-11-01T00:00:00.000Z` | `KOA` | `HA` | `11` | `null` |
| `2005-11-01T00:00:00.000Z` | `KOA` | `UA` | `4` | `HA` |
| `2005-11-01T00:00:00.000Z` | `KOA` | `AA` | `1` | `UA` |
| `2005-11-01T00:00:00.000Z` | `KOA` | `NW` | `1` | `AA` |
| `2005-11-01T00:00:00.000Z` | `LIH` | `HA` | `15` | `null` |
| `2005-11-01T00:00:00.000Z` | `LIH` | `AA` | `2` | `HA` |
| `2005-11-01T00:00:00.000Z` | `LIH` | `UA` | `2` | `AA` |
 
</details>

[Learn more](sql-window-functions.md#window-function-reference)

## LAST_VALUE

Returns the value evaluated for the expression for the last row within the window.

* **Syntax**: `LAST_VALUE(expr)`
* **Function type:** Window

<details>
<summary>Example</summary>

The following example returns the last airline name in the window for flights for two airports on a single day.
Note that the RANGE BETWEEN clause defines the window frame between the current row and the final row in the window instead of the default of RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW when using ORDER BY.

```sql
SELECT FLOOR("__time" TO DAY)  AS "flight_day",
    "Origin" AS "airport",
    "Reporting_Airline" as "airline",
    COUNT("Flight_Number_Reporting_Airline") as "num_flights",
    LAST_VALUE("Reporting_Airline") OVER (PARTITION BY "Origin" ORDER BY COUNT("Flight_Number_Reporting_Airline") DESC
      RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS "last_value"
FROM "flight-carriers"
WHERE FLOOR("__time" TO DAY) = '2005-11-01'
    AND "Origin" IN ('KOA', 'LIH')
GROUP BY 1, 2, 3
```

Returns the following:

| `flight_day` | `airport` | `airline` | `num_flights` | `last_value` |
| --- | --- | --- | --- | ---|
| `2005-11-01T00:00:00.000Z` | `KOA` | `HA` | `11` | `NW` |
| `2005-11-01T00:00:00.000Z` | `KOA` | `UA` | `4` | `NW` |
| `2005-11-01T00:00:00.000Z` | `KOA` | `AA` | `1` | `NW` |
| `2005-11-01T00:00:00.000Z` | `KOA` | `NW` | `1` | `NW` |
| `2005-11-01T00:00:00.000Z` | `LIH` | `HA` | `15` | `UA` |
| `2005-11-01T00:00:00.000Z` | `LIH` | `AA` | `2` | `UA` |
| `2005-11-01T00:00:00.000Z` | `LIH` | `UA` | `2` | `UA` |
 
</details>

[Learn more](sql-window-functions.md#window-function-reference)

## LATEST

Returns the value of a numeric or string expression corresponding to the latest `__time` value.

* **Syntax**: `LATEST(expr, [maxBytesPerValue])`
* **Function type:** Aggregation

<details>
<summary>Example</summary>

The following example returns the origin airport code associated with the latest departing flight daily after `'2005-01-01 00:00:00'` in `flight-carriers`:

```sql
SELECT
  TIME_FLOOR(__time, 'P1D') AS "departure_day",
  LATEST("Origin") AS "origin"
FROM "flight-carriers"
WHERE __time >= TIMESTAMP '2005-01-01 00:00:00'
GROUP BY 1
LIMIT 2
```

Returns the following:

|`departure_day`|`origin`|
|------------|--------|
|`2005-11-01T00:00:00.000Z`|`LAS`|
|`2005-11-02T00:00:00.000Z`|`LAX`|

</details>

[Learn more](sql-aggregations.md)

## LATEST_BY

Returns the value of a numeric or string expression corresponding to the latest time value from `timestampExpr`.

* **Syntax**: `LATEST_BY(expr, timestampExpr, [maxBytesPerValue])`
* **Function type:** Aggregation

<details>
<summary>Example</summary>

The following example returns the destination airport code associated with the latest arriving flight daily after `'2005-01-01 00:00:00'` in `flight-carriers`:

```sql
SELECT
  TIME_FLOOR(TIME_PARSE("arrivalime"), 'P1D') AS "arrival_day",
  LATEST_BY("Dest", TIME_PARSE("arrivalime")) AS "dest"
FROM "flight-carriers"
WHERE TIME_PARSE("arrivalime") >= TIMESTAMP '2005-01-01 00:00:00'
GROUP BY 1
LIMIT 2
```

Returns the following:

|`arrival_day`|`origin`|
|-------------|--------|
|`2005-11-01T00:00:00.000Z`|`MCO`|
|`2005-11-02T00:00:00.000Z`|`BUF`|

</details>

[Learn more](sql-aggregations.md)

## LEAD

If you do not supply an `offset`, returns the value evaluated at the row following the current row. Specify an offset number `n` to return the value evaluated at `n` rows following the current one; if there is no such row, returns the given default value.

* **Syntax**: `LEAD(expr[, offset])`
* **Function type:** Window

<details>
<summary>Example</summary>

The following example returns the subsequent value for an airline in the window for flights from two airports on a single day.

```sql
SELECT FLOOR("__time" TO DAY)  AS "flight_day",
    "Origin" AS "airport",
    "Reporting_Airline" as "airline",
    COUNT("Flight_Number_Reporting_Airline") as "num_flights",
    LEAD("Reporting_Airline") OVER (PARTITION BY "Origin" ORDER BY COUNT("Flight_Number_Reporting_Airline") DESC) AS "lead"
FROM "flight-carriers"
WHERE FLOOR("__time" TO DAY) = '2005-11-01'
    AND "Origin" IN ('KOA', 'LIH')
GROUP BY 1, 2, 3
```

Returns the following:

| `flight_day` | `airport` | `airline` | `num_flights ` | `lead` |
| --- | --- | --- | --- | ---|
| `2005-11-01T00:00:00.000Z` | `KOA` | `HA` | `11` |`UA` |
| `2005-11-01T00:00:00.000Z` | `KOA` | `UA` | `4` | `AA` |
| `2005-11-01T00:00:00.000Z` | `KOA` | `AA` | `1` | `NW` |
| `2005-11-01T00:00:00.000Z` | `KOA` | `NW` | `1` | `null` |
| `2005-11-01T00:00:00.000Z` | `LIH` | `HA` | `15` | `AA` |
| `2005-11-01T00:00:00.000Z` | `LIH` | `AA` | `2` | `UA` |
| `2005-11-01T00:00:00.000Z` | `LIH` | `UA` | `2` | `null` |
 
</details>

[Learn more](sql-window-functions.md#window-function-reference)

## LEAST

Returns the minimum value from the provided expressions. For information on how Druid interprets the arguments passed into the function, see [Reduction functions](sql-scalar.md#reduction-functions).

* **Syntax:** `LEAST([expr1, ...])`
* **Function type:** Scalar, reduction

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

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

## LISTAGG

Alias for [`STRING_AGG`](#string_agg).

* **Syntax:** `LISTAGG([DISTINCT] expr, [separator, [size]])`
* **Function type:** Aggregation

[Learn more](sql-aggregations.md)

## LN

Calculates the natural logarithm of the numeric expression.

* **Syntax:** `LN(<NUMERIC>)`
* **Function type:** Scalar, numeric

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

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

Returns the maximum value of a set of values.

* **Syntax**: `MAX(expr)`
* **Function type:** Aggregation


<details>
<summary>Example</summary>

The following example calculates the maximum delay in minutes for an airline in `flight-carriers`:

```sql
SELECT MAX("DepDelayMinutes") AS max_delay
FROM "flight-carriers"
WHERE "Reporting_Airline" = 'AA'
```

Returns the following:

| `max_delay` |
| -- |
| `1210` |

</details>

[Learn more](sql-aggregations.md)

## MILLIS_TO_TIMESTAMP

Converts a number of milliseconds since epoch into a timestamp.

* **Syntax:** `MILLIS_TO_TIMESTAMP(millis_expr)`
* **Function type:** Scalar, date and time

<details>
<summary>Example</summary>

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

Returns the minimum value of a set of values.

* **Syntax**: `MIN(expr)`
* **Function type:** Aggregation

<details>
<summary>Example</summary>

The following example calculates the minimum delay in minutes for an airline in `flight-carriers`:

```sql
SELECT MIN("DepDelayMinutes") AS min_delay
FROM "flight-carriers"
WHERE "Reporting_Airline" = 'AA'
```

Returns the following:

| `min_delay` |
| -- |
| `0` |

</details>

[Learn more](sql-aggregations.md)

## MOD

Calculates x modulo y, or the remainder of x divided by y. Where x and y are numeric expressions.

* **Syntax:** `MOD(x, y)`
* **Function type:** Scalar, numeric

<details>
<summary>Example</summary>

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

Adds the expression to the end of the array.

* **Syntax:** `MV_APPEND(arr1, expr)`
* **Function type:** Multi-value string

<details>
<summary>Example</summary>

The following example appends the string `label` to the multi-value string `tags` from `mvd-example`:

```sql
SELECT MV_APPEND("tags", "label") AS append
FROM "mvd-example"
LIMIT 1
```

Returns the following:

| `append` |
| -- |
| `["t1","t2","t3","row1"]` |

</details>

[Learn more](sql-multivalue-string-functions.md)

## MV_CONCAT

Concatenates two arrays.

* **Syntax:** `MV_CONCAT(arr1, arr2)`
* **Function type:** Multi-value string

<details>
<summary>Example</summary>

The following example concatenates `tags` from `mvd-example` to itself:

```sql
SELECT MV_CONCAT("tags", "tags") AS cat
FROM "mvd-example"
LIMIT 1
```

Returns the following:

| `cat` |
| -- |
| `["t1","t2","t3","t1","t2","t3"]` |

</details>

[Learn more](sql-multivalue-string-functions.md)


## MV_CONTAINS

Returns true if the expression is in the array, false otherwise.

* **Syntax:** `MV_CONTAINS(arr, expr)`
* **Function type:** Multi-value string

<details>
<summary>Example</summary>

The following example checks if the string `t3` exists within `tags` from `mvd-example`:

```sql
SELECT "tags", MV_CONTAINS("tags", 't3') AS contained
FROM "mvd-example"
```

Returns the following:

|`tags`|`contained`|
|------|-----------|
|`["t1","t2","t3"]`|`true`|
|`["t3","t4","t5"]`|`true`|
|`["t5","t6","t7"]`|`false`|
|`null`|`false`|

</details>

[Learn more](sql-multivalue-string-functions.md)

## MV_FILTER_NONE

Filters a multi-value expression to exclude values from an array.

* **Syntax:** `MV_FILTER_NONE(expr, arr)`
* **Function type:** Multi-value string

<details>
<summary>Example</summary>

The following example filters `tags` from `mvd-example` to remove values `t1` or `t3`, if present:

```sql
SELECT MV_FILTER_NONE("tags", ARRAY['t1', 't3']) AS exclude
FROM "mvd-example"
LIMIT 3
```

Returns the following:

| `exclude` |
| -- |
| `t2` |
| `["t4", "t5"]` |
| `["t5","t6","t7"]` |

</details>

[Learn more](sql-multivalue-string-functions.md)

## MV_FILTER_ONLY

Filters a multi-value expression to include only values contained in the array.

* **Syntax:** `MV_FILTER_ONLY(expr, arr)`
* **Function type:** Multi-value string

<details>
<summary>Example</summary>

The following example filters `tags` from `mvd-example` to only contain the values `t1` or `t3`:

```sql
SELECT MV_FILTER_ONLY("tags", ARRAY['t1', 't3']) AS filt
FROM "mvd-example"
LIMIT 3
```

Returns the following:

| `filt` |
| -- |
| `["t1","t3"]` |
| `t3` |
| null |

</details>

[Learn more](sql-multivalue-string-functions.md)

## MV_LENGTH

Returns the length of an array expression.

* **Syntax:** `MV_LENGTH(arr)`
* **Function type:** Multi-value string

<details>
<summary>Example</summary>

The following example returns the length of the `tags` multi-value strings from `mvd-example`:

```sql
SELECT MV_LENGTH("tags") AS len
FROM "mvd-example"
LIMIT 1
```

Returns the following:

| `len` |
| -- |
| `3` |

</details>

[Learn more](sql-multivalue-string-functions.md)

## MV_OFFSET

Returns the array element at the given zero-based index.

* **Syntax:** `MV_OFFSET(arr, long)`
* **Function type:** Multi-value string

<details>
<summary>Example</summary>

The following example returns `tags` and the element at the third position of `tags` in `mvd-example`:

```sql
SELECT "tags", MV_OFFSET("tags", 2) AS elem
FROM "mvd-example"
```

Returns the following:

|`tags`|`elem`|
|------|------|
|`["t1","t2","t3"]`|`t3`|
|`["t3","t4","t5"]`|`t5`|
|`["t5","t6","t7"]`|`t7`|
|`null`|`null`|

</details>

[Learn more](sql-multivalue-string-functions.md)

## MV_OFFSET_OF

Returns the zero-based index of the first occurrence of a given expression in the array.

* **Syntax:** `MV_OFFSET_OF(arr, expr)`
* **Function type:** Multi-value string

<details>
<summary>Example</summary>

The following example returns `tags` and the zero-based index of the string `t3` from `tags` in `mvd-example`:

```sql
SELECT "tags", MV_OFFSET_OF("tags", 't3') AS index
FROM "mvd-example"
```

Returns the following:

|`tags`|`index`|
|------|-------|
|`["t1","t2","t3"]`|`2`|
|`["t3","t4","t5"]`|`0`|
|`["t5","t6","t7"]`|`null`|
|`null`|`null`|

</details>

[Learn more](sql-multivalue-string-functions.md)

## MV_ORDINAL

Returns the array element at the given one-based index.

* **Syntax:** `MV_ORDINAL(arr, long)`
* **Function type:** Multi-value string

<details>
<summary>Example</summary>

The following example returns `tags` and the element at the third position of `tags` in `mvd-example`:

```sql
SELECT "tags", MV_ORDINAL("tags", 3) AS elem
FROM "mvd-example"
```

Returns the following:

|`tags`|`elem`|
|------|------|
|`["t1","t2","t3"]`|`t3`|
|`["t3","t4","t5"]`|`t5`|
|`["t5","t6","t7"]`|`t7`|
|`null`|`null`|

</details>

[Learn more](sql-multivalue-string-functions.md)

## MV_ORDINAL_OF

Returns the one-based index of the first occurrence of a given expression.

* **Syntax:** `MV_ORDINAL_OF(arr, expr)`
* **Function type:** Multi-value string

<details>
<summary>Example</summary>

The following example returns `tags` and the one-based index of the string `t3` from `tags` in `mvd-example`:

```sql
SELECT "tags", MV_ORDINAL_OF("tags", 't3') AS index
FROM "mvd-example"
```

Returns the following:

|`tags`|`index`|
|------|-------|
|`["t1","t2","t3"]`|`3`|
|`["t3","t4","t5"]`|`1`|
|`["t5","t6","t7"]`|`null`|
|`null`|`null`|

</details>

[Learn more](sql-multivalue-string-functions.md)

## MV_OVERLAP

Returns true if the two arrays have any elements in common, false otherwise.

* **Syntax:** `MV_OVERLAP(arr1, arr2)`
* **Function type:** Multi-value string

<details>
<summary>Example</summary>

The following example identifies rows that contain `t1` or `t3` in `tags` from `mvd-example`:

```sql
SELECT "tags", MV_OVERLAP("tags", ARRAY['t1', 't3']) AS overlap
FROM "mvd_example"
```

Returns the following:

|`tags`|`overlap`|
|------|---------|
|`["t1","t2","t3"]`|`true`|
|`["t3","t4","t5"]`|`true`|
|`["t5","t6","t7"]`|`false`|
|`null`|`false`|

</details>

[Learn more](sql-multivalue-string-functions.md)

## MV_PREPEND

Adds the expression to the beginning of the array.

* **Syntax:** `MV_PREPEND(expr, arr)`
* **Function type:** Multi-value string

<details>
<summary>Example</summary>

The following example prepends the string dimension `label` to the multi-value string dimension `tags` from `mvd-example`:

```sql
SELECT MV_PREPEND("label", "tags") AS prepend
FROM "mvd-example"
LIMIT 1
```

Returns the following:

| `prepend` |
| -- |
| `["row1","t1","t2","t3"]` |


</details>

[Learn more](sql-multivalue-string-functions.md)

## MV_SLICE

Returns a slice of the array from the zero-based start and end indexes.

* **Syntax:** `MV_SLICE(arr, start, end)`
* **Function type:** Multi-value string

<details>
<summary>Example</summary>

The following example returns `tags` and the second and third values of `tags` from `mvd-example`:

```sql
SELECT "tags", MV_SLICE(tags, 1, 3) AS slice
FROM "mvd-example"
```

Returns the following:

|`tags`|`slice`|
|------|-------|
|`["t1"","t2","t3"]`|`["t2","t3"]`|
|`["t3"","t4","t5"]`|`["t4","t5"]`|
|`["t5"","t6","t7"]`|`["t6","t7"]`|
|`null`|`null`|

</details>

[Learn more](sql-multivalue-string-functions.md)

## MV_TO_ARRAY

Converts a multi-value string from a `VARCHAR` to a `VARCHAR ARRAY`.

* **Syntax:** `MV_TO_ARRAY(str)`
* **Function type:** Multi-value string

<details>
<summary>Example</summary>

The following example transforms the `tags` column from `mvd-example` to arrays:

```sql
SELECT MV_TO_ARRAY(tags) AS arr
FROM "mvd-example"
LIMIT 1
```

Returns the following:

| `arr` |
| -- |
| `[t1, t2, t3]` |

</details>

[Learn more](sql-multivalue-string-functions.md)

## MV_TO_STRING

Joins all elements of the array together by the given delimiter.

* **Syntax:** `MV_TO_STRING(arr, str)`
* **Function type:** Multi-value string

<details>
<summary>Example</summary>

The following example transforms the `tags` column from `mvd-example` to strings delimited by a space character:

```sql
SELECT MV_TO_STRING("tags", ' ') AS str
FROM mvd-example
LIMIT 1
```

Returns the following:

| `str` |
| -- |
| `t1 t2 t3` |

</details>

[Learn more](sql-multivalue-string-functions.md)

## NTILE

Divides the rows within a window as evenly as possible into the number of tiles, also called buckets, and returns the value of the tile that the row falls into.

* **Syntax**: `NTILE(tiles)`
* **Function type:** Window

<details>
<summary>Example</summary>

The following example returns the results for flights by airline from two airports on a single day divided into 3 tiles.

```sql
SELECT FLOOR("__time" TO DAY)  AS "flight_day",
    "Origin" AS "airport",
    "Reporting_Airline" as "airline",
    COUNT("Flight_Number_Reporting_Airline") as "num_flights",
    NTILE(3) OVER (PARTITION BY "Origin" ORDER BY COUNT("Flight_Number_Reporting_Airline") DESC) AS "ntile"
FROM "flight-carriers"
WHERE FLOOR("__time" TO DAY) = '2005-11-01'
    AND "Origin" IN ('KOA', 'LIH')
GROUP BY 1, 2, 3
```

Returns the following:

| `flight_day` | `airport` | `airline` | `lead` | `ntile` |
| --- | --- | --- | --- | ---|
| `2005-11-01T00:00:00.000Z` | `KOA` | `HA` | `11` | `1` |
| `2005-11-01T00:00:00.000Z` | `KOA` | `UA` | `4` | `1` |
| `2005-11-01T00:00:00.000Z` | `KOA` | `AA` | `1` | `2` |
| `2005-11-01T00:00:00.000Z` | `KOA` | `NW` | `1` | `3` |
| `2005-11-01T00:00:00.000Z` | `LIH` | `HA` | `15` | `1` |
| `2005-11-01T00:00:00.000Z` | `LIH` | `AA` | `2` | `2` |
| `2005-11-01T00:00:00.000Z` | `LIH` | `UA` | `2` | `3` |
 
</details>

[Learn more](sql-window-functions.md#window-function-reference)

## NULLIF

Returns null if two values are equal, else returns the first value.
* **Syntax:** `NULLIF(value1, value2)`
* **Function type:** Scalar, other

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

The following example replaces each null value in the `Tail_Number` column of the `flight-carriers` datasource with the string "No tail number."

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

Parses an expression into a `COMPLEX<json>` object. 

The function deserializes JSON values when processing them, translating stringified JSON into a nested structure.
If the input is invalid JSON or not a `VARCHAR`, it returns an error.

* **Syntax:** `PARSE_JSON(expr)`
* **Function type:** JSON

<details>
<summary>Example</summary>

The following example creates a `COMPLEX<json>` object `gus` from a string of fields:

```sql
SELECT
  PARSE_JSON('{"name":"Gus","email":"gus_cat@example.com","type":"Pet"}') as gus
```

Returns the following:

| `gus` |
| -- |
| `{"name":"Gus","email":"gus_cat@example.com","type":"Pet"}` |

</details>

[Learn more](sql-json-functions.md)
## PARSE_LONG

Converts a string into a long(BIGINT) with the given radix, or into DECIMAL(base 10) if a radix is not provided.

* **Syntax:**`PARSE_LONG(string[, radix])`
* **Function type:** Scalar, string

<details>
<summary>Example</summary>

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

Returns the relative rank of the row calculated as a percentage according to the formula: `RANK() OVER (window) / COUNT(1) OVER (window)`.

* **Syntax**: `PERCENT_RANK()`
* **Function type:** Window

<details>
<summary>Example</summary>

The following example returns the percent rank within the window for flights by airline from two airports on a single day.

```sql
SELECT FLOOR("__time" TO DAY)  AS "flight_day",
    "Origin" AS "airport",
    "Reporting_Airline" as "airline",
    COUNT("Flight_Number_Reporting_Airline") as "num_flights",
    PERCENT_RANK() OVER (PARTITION BY "Origin" ORDER BY COUNT("Flight_Number_Reporting_Airline") DESC) AS "pct_rank"
FROM "flight-carriers"
WHERE FLOOR("__time" TO DAY) = '2005-11-01'
    AND "Origin" IN ('KOA', 'LIH')
GROUP BY 1, 2, 3
```

Returns the following:

| `flight_day` | `airport` | `airline` | `num_flights` | `pct_rank` |
| --- | --- | --- | --- | ---|
| `2005-11-01T00:00:00.000Z` | `KOA` | `HA` | `11` | `0` |
| `2005-11-01T00:00:00.000Z` | `KOA` | `UA` | `4` | `0.3333333333333333` |
| `2005-11-01T00:00:00.000Z` | `KOA` | `AA` | `1` | `0.6666666666666666` |
| `2005-11-01T00:00:00.000Z` | `KOA` | `NW` | `1` | `0.6666666666666666` |
| `2005-11-01T00:00:00.000Z` | `LIH` | `HA` | `15` | `0` |
| `2005-11-01T00:00:00.000Z` | `LIH` | `AA` | `2` | `0.5` |
| `2005-11-01T00:00:00.000Z` | `LIH` | `UA` | `2` | `0.5` |
 
</details>


[Learn more](sql-window-functions.md#window-function-reference)

## POSITION

Returns the one-based index position of a substring within an expression, optionally starting from a given one-based index. If `substring` is not found, returns 0.

* **Syntax**: `POSITION(substring IN expr [FROM startingIndex])`
* **Function type:** Scalar, string

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

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

Returns the rank with gaps for a row within a window. For example, if two rows tie for rank 1, the next rank is 3.

* **Syntax**: `RANK()`
* **Function type:** Window

<details>
<summary>Example</summary>

The following example returns the rank within the window for flights by airline from two airports on a single day.

```sql
SELECT FLOOR("__time" TO DAY)  AS "flight_day",
    "Origin" AS "airport",
    "Reporting_Airline" as "airline",
    COUNT("Flight_Number_Reporting_Airline") as "num_flights",
    RANK() OVER (PARTITION BY "Origin" ORDER BY COUNT("Flight_Number_Reporting_Airline") DESC) AS "rank"
FROM "flight-carriers"
WHERE FLOOR("__time" TO DAY) = '2005-11-01'
    AND "Origin" IN ('KOA', 'LIH')
GROUP BY 1, 2, 3
```

Returns the following:

| `flight_day` | `airport` | `airline` | `num_flights` | `rank` |
| --- | --- | --- | --- | ---|
| `2005-11-01T00:00:00.000Z` | `KOA` | `HA` | `11` | `1` |
| `2005-11-01T00:00:00.000Z` | `KOA` | `UA` | `4` | `2` |
| `2005-11-01T00:00:00.000Z` | `KOA` | `AA` | `1` | `3` |
| `2005-11-01T00:00:00.000Z` | `KOA` | `NW` | `1` | `3` |
| `2005-11-01T00:00:00.000Z` | `LIH` | `HA` | `15` | `1` |
| `2005-11-01T00:00:00.000Z` | `LIH` | `AA` | `2` | `2` |
| `2005-11-01T00:00:00.000Z` | `LIH` | `UA` | `2` | `3` |
 
</details>

[Learn more](sql-window-functions.md#window-function-reference)

## REGEXP_EXTRACT

Apply regular expression `pattern` to `expr` and extract the Nth capture group. If `N` is unspecified or zero, returns the first substring that matches the pattern. Returns null if there is no matching pattern.

* **Syntax:** `REGEXP_EXTRACT(expr, pattern[, N])`
* **Function type:** Scalar, string 

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

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

Returns the number of the row within the window starting from 1.

* **Syntax**: `ROW_NUMBER()`
* **Function type:** Window

<details>
<summary>Example</summary>

The following example returns the row number within the window for flights by airline from two airports on a single day.

```sql
SELECT FLOOR("__time" TO DAY)  AS "flight_day",
    "Origin" AS "airport",
    "Reporting_Airline" as "airline",
    COUNT("Flight_Number_Reporting_Airline") as "num_flights",
    ROW_NUMBER() OVER (PARTITION BY "Origin" ORDER BY COUNT("Flight_Number_Reporting_Airline") DESC) AS "row_num"
FROM "flight-carriers"
WHERE FLOOR("__time" TO DAY) = '2005-11-01'
    AND "Origin" IN ('KOA', 'LIH')
GROUP BY 1, 2, 3
```

Returns the following:

| `flight_day` | `airport` | `airline` | `num_flights` | `row_num` |
| --- | --- | --- | --- | ---|
| `2005-11-01T00:00:00.000Z` | `KOA` | `HA` | `11` | `1` |
| `2005-11-01T00:00:00.000Z` | `KOA` | `UA` | `4` | `2` |
| `2005-11-01T00:00:00.000Z` | `KOA` | `AA` | `1` | `3` |
| `2005-11-01T00:00:00.000Z` | `KOA` | `NW` | `1` | `4` |
| `2005-11-01T00:00:00.000Z` | `LIH` | `HA` | `15` | `1` |
| `2005-11-01T00:00:00.000Z` | `LIH` | `AA` | `2` | `2` |
| `2005-11-01T00:00:00.000Z` | `LIH` | `UA` | `2` | `3` |
 
</details>

[Learn more](sql-window-functions.md#window-function-reference)

## RPAD

Returns a string of size `length` from `expr`. When the length of `expr` is less than `length`, right pads `expr` with `chars`, which defaults to the space character. Truncates `expr` to `length` if `length` is shorter than the length of `expr`.

* **Syntax:** `RPAD(expr, length[, chars])`
* **Function type:** Scalar, string

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

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

Alias for [`STDDEV_SAMP`](#stddev_samp).  
Requires the [`druid-stats` extension](../development/extensions-core/stats.md).

* **Syntax**: `STDDEV(expr)`
* **Function type:** Aggregation

[Learn more](sql-aggregations.md)

## STDDEV_POP

Calculates the population standard deviation of a set of values.  
Requires the [`druid-stats` extension](../development/extensions-core/stats.md).

* **Syntax**: `STDDEV_POP(expr)`
* **Function type:** Aggregation

<details>
<summary>Example</summary>

The following example calculates the population standard deviation for minutes of delay for an airline in `flight-carriers`:

```sql
SELECT STDDEV_POP("DepDelayMinutes") AS sd_delay
FROM "flight-carriers"
WHERE "Reporting_Airline" = 'AA'
```

Returns the following:

| `sd_delay` |
| -- |
| `27.083557` |

</details>

[Learn more](sql-aggregations.md)

## STDDEV_SAMP

Calculates the sample standard deviation of a set of values.  
Requires the [`druid-stats` extension](../development/extensions-core/stats.md).

* **Syntax**: `STDDEV_SAMP(expr)`
* **Function type:** Aggregation

<details>
<summary>Example</summary>

The following example calculates the sample standard deviation for minutes of delay for an airline in `flight-carriers`:

```sql
SELECT STDDEV_SAMP("DepDelayMinutes") AS sd_delay
FROM "flight-carriers"
WHERE "Reporting_Airline" = 'AA'
```

Returns the following:

| `sd_delay` |
| -- |
| `27.083811` |

</details>

[Learn more](sql-aggregations.md)

## STRING_AGG

Collects all values of an expression into a single string.

* **Syntax**: `STRING_AGG(expr, separator, [size])`
* **Function type:** Aggregation

<details>
<summary>Example</summary>

The following example returns all the distinct airlines from `flight-carriers` as a single space-delimited string:

```sql
SELECT
  STRING_AGG(DISTINCT "Reporting_Airline", ' ') AS "AllCarriers"
FROM "flight-carriers"
```

Returns the following:

|`AllCarriers`|
|-------------|
|`AA AS B6 CO DH DL EV F9 FL HA HP MQ NW OH OO TZ UA US WN XE`|

</details>

[Learn more](sql-aggregations.md)

## STRING_FORMAT

Returns a string formatted in the manner of Java's [String.format](https://docs.oracle.com/javase/8/docs/api/java/lang/String.html#format-java.lang.String-java.lang.Object...-).

* **Syntax:** `STRING_FORMAT(pattern[, args...])`
* **Function type:** Scalar, string 

<details>
<summary>Example</summary>

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

## STRING_TO_ARRAY

Splits the string into an array of substrings using the specified delimiter. The delimiter must be a valid regular expression.

* **Syntax**: `STRING_TO_ARRAY(string, delimiter)`
* **Function type:** Array

[Learn more](sql-array-functions.md)

## STRING_TO_MV

Splits `str1` into an multi-value string on the delimiter specified by `str2`, which is a regular expression.

* **Syntax:** `STRING_TO_MV(str1, str2)`
* **Function type:** Multi-value string

<details>
<summary>Example</summary>

The following example splits a street address by whitespace characters:

```sql
SELECT STRING_TO_MV('123 Rose Lane', '\s+') AS mv
```

Returns the following:

| `mv` |
| -- |
| `["123","Rose","Lane"]` |

</details>

[Learn more](sql-multivalue-string-functions.md)

## STRLEN

Alias for [`LENGTH`](#length).

* **Syntax:** `STRLEN(expr)`
* **Function type:** Scalar, string 

[Learn more](sql-scalar.md#string-functions)

## STRPOS

Returns the one-based index position of a substring within an expression. If `substring` is not found, returns 0.

* **Syntax:** `STRPOS(expr, substring)`
* **Function type:** Scalar, string

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

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

Calculates the sum of a set of values.

* **Syntax**: `SUM(expr)`
* **Function type:** Aggregation

<details>
<summary>Example</summary>

The following example calculates the total minutes of delay for an airline in `flight-carriers`:

```sql
SELECT SUM("DepDelayMinutes") AS tot_delay
FROM "flight-carriers"
WHERE "Reporting_Airline" = 'AA'
```

Returns the following:

| `tot_delay` |
| -- |
| `475735` |

</details>

[Learn more](sql-aggregations.md)

## TAN

Calculates the trigonometric tangent of an angle expressed in radians.

* **Syntax:** `TAN(expr)`
* **Function type:** Scalar, numeric

<details>
<summary>Example</summary>

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

Generates a T-digest sketch from values of the specified expression.

* **Syntax**: `TDIGEST_GENERATE_SKETCH(expr, [compression])`
* **Function type:** Aggregation

[Learn more](sql-aggregations.md)

## TDIGEST_QUANTILE

Returns the quantile for the specified fraction from a T-Digest sketch constructed from values of the expression.

* **Syntax**: `TDIGEST_QUANTILE(expr, quantileFraction, [compression])`
* **Function type:** Aggregation

[Learn more](sql-aggregations.md)

## TEXTCAT

Concatenates two string expressions.

* **Syntax:** `TEXTCAT(expr, expr)`
* **Function type:** Scalar, string
  
<details>
<summary>Example</summary>

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

Returns the distinct count estimate from a Theta sketch. The `expr` argument must return a Theta sketch.

* **Syntax:** `THETA_SKETCH_ESTIMATE(expr)`
* **Function type:** Scalar, sketch

<details>
<summary>Example</summary>

The following example estimates the distinct number of tail numbers in the `Tail_Number` column of the `flight-carriers` datasource.

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

Returns the distinct count estimate and error bounds from a Theta sketch. The `expr` argument must return a Theta sketch. Use `errorBoundsStdDev` to specify the number of standard error bound deviations.

* **Syntax:** `THETA_SKETCH_ESTIMATE_WITH_ERROR_BOUNDS(expr, errorBoundsStdDev)`
* **Function type:** Scalar, sketch

<details>
<summary>Details</summary>

The following example estimates the number of distinct tail numbers in the `Tail_Number` column of the `flight-carriers` datasource with error bounds at plus or minus one standard deviation.

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

Returns an intersection of Theta sketches. Each input expression must return a Theta sketch. See [DataSketches Theta Sketch module](../development/extensions-core/datasketches-theta#aggregator) for a description of optional parameters. 

* **Syntax:** `THETA_SKETCH_INTERSECT([size], expr0, expr1, ...)`
* **Function type:** Scalar, sketch

<details>
<summary>Example</summary>

The following example estimates the intersection of distinct tail numbers in the `flight-carriers` datasource for flights originating in CA, TX, and NY.

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

Returns a set difference of Theta sketches. Each input expression must return a Theta sketch. See [DataSketches Theta Sketch module](../development/extensions-core/datasketches-theta#aggregator) for a description of optional parameters.

* **Syntax:** `THETA_SKETCH_NOT([size], expr0, expr1, ...)`
* **Function type:** Scalar, sketch

<details>
<summary>Example</summary>

The following example estimates the number of distinct tail numbers in the `flight-carriers` datasource for flights not originating in CA, TX, or NY.

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

Returns a union of Theta sketches. Each input expression must return a Theta sketch. See [DataSketches Theta Sketch module](../development/extensions-core/datasketches-theta#aggregator) for a description of optional parameters.

* **Syntax:**`THETA_SKETCH_UNION([size], expr0, expr1, ...)`
* **Function type:** Scalar, sketch

<details>
<summary>Example</summary>

The following example estimates the number of distinct tail numbers that depart from CA, TX, or NY.

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

Rounds up a timestamp to a given ISO 8601 time period. You can specify `origin` to provide a reference timestamp from which to start rounding. If provided, `timezone` should be a time zone name like `America/Los_Angeles` or an offset like `-08:00`.

* **Syntax:** `TIME_CEIL(timestamp_expr, period[, origin[, timezone]])`
* **Function type:** Scalar, date and time

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

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

Returns true if a timestamp is contained within a particular interval. Intervals must be formatted as a string literal containing any ISO 8601 interval. The start instant of an interval is inclusive, and the end instant is exclusive.

* **Syntax:** `TIME_IN_INTERVAL(timestamp_expr, interval)`
* **Function type:** Scalar, date and time

<details>
<summary>Example</summary>

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

Parses a string into a timestamp using a given [Joda DateTimeFormat pattern](http://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html). If no pattern is provided, `pattern` defaults to ISO 8601. Returns NULL if string cannot be parsed. If provided, `timezone` should be a time zone name like `America/Los_Angeles` or an offset like `-08:00`.

* **Syntax:** `TIME_PARSE(string_expr[, pattern[, timezone]])`
* **Function type:** Scalar, date and time

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

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

Returns the number of milliseconds since epoch for the given timestamp.

* **Syntax:** `TIMESTAMP_TO_MILLIS(timestamp_expr)`
* **Function type:** Scalar, date and time

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

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

Serializes an expression into a JSON string.

* **Syntax:** `TO_JSON_STRING(expr)`
* **Function type:** JSON

<details>
<summary>Example</summary>

The following example writes the distinct column names in the `events` nested column to a JSON string:

```sql
SELECT
  TO_JSON_STRING(ARRAY_CONCAT_AGG(DISTINCT JSON_KEYS(event, '$.'))) as json_string
FROM "kttm_nested"
```

Returns the following:

| `json_string` |
| -- |
| `["error","layer","percentage","saveNumber","type","url","userAgent"]` |

</details>

[Learn more](sql-json-functions.md)


## TRIM

Trims the leading and/or trailing characters of an expression. Defaults `chars` to a space if none is provided. Defaults to `BOTH` if no directional argument is provided.

* **Syntax:** `TRIM([BOTH|LEADING|TRAILING] [chars FROM] expr)`
* **Function type:** Scalar, string

<details>
<summary>Example</summary>

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

<details>
<summary>Example</summary>

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

Parses an expression into a `COMPLEX<json>` object.

This function deserializes JSON values when processing them, translating stringified JSON into a nested structure.
If the input is invalid JSON or not a `VARCHAR`, it returns a `NULL` value.

You can use this function instead of [PARSE_JSON](#parse_json) to insert a null value when processing invalid data, instead of producing an error.

* **Syntax:** `TRY_PARSE_JSON(expr)`
* **Function type:** JSON

<details>
<summary>Example</summary>

The following example creates a `COMPLEX<json>` object `gus` from a string of fields:

```sql
SELECT
  TRY_PARSE_JSON('{"name":"Gus","email":"gus_cat@example.com","type":"Pet"}') as gus
```

Returns the following:

| `gus` |
| -- |
| `{"name":"Gus","email":"gus_cat@example.com","type":"Pet"}` |


The following example contains invalid data `x:x`:

```sql
SELECT
  TRY_PARSE_JSON('{"name":"Gus","email":"gus_cat@example.com","type":"Pet",x:x}') as gus
```

Returns the following:

| `gus` |
| -- |
| `null` |

</details>

[Learn more](sql-json-functions.md)


## UPPER

Returns the expression in uppercase.

* **Syntax:** `UPPER(expr)`
* **Function type:** Scalar, string

<details>
<summary>Example</summary>

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
| `San Juan, PR` | `SAN JUAN, PR` |

</details>

[Learn more](sql-scalar.md#string-functions)

## VAR_POP

Calculates the population variance of a set of values.  
Requires the [`druid-stats` extension](../development/extensions-core/stats.md).

* **Syntax**: `VAR_POP(expr)`
* **Function type:** Aggregation

<details>
<summary>Example</summary>

The following example calculates the population variance for minutes of delay by a particular airlines in `flight-carriers`:

```sql
SELECT VAR_POP("DepDelayMinutes") AS varpop_delay
FROM "flight-carriers"
WHERE "Reporting_Airline" = 'AA'
```

Returns the following:

| `varpop_delay` |
| -- |
| `733.51908` |

</details>

[Learn more](sql-aggregations.md)

## VAR_SAMP

Calculates the sample variance of a set of values.  
Requires the [`druid-stats` extension](../development/extensions-core/stats.md).

* **Syntax**: `VAR_SAMP(expr)`
* **Function type:** Aggregation

<details>
<summary>Example</summary>

The following example calculates the sample variance for minutes of delay for an airline in `flight-carriers`:

```sql
SELECT VAR_SAMP("DepDelayMinutes") AS varsamp_delay
FROM "flight-carriers"
WHERE "Reporting_Airline" = 'AA'
```

Returns the following:

| `varsamp_delay` |
| -- |
| `733.53286` |

</details>

[Learn more](sql-aggregations.md)

## VARIANCE

Alias for [`VAR_SAMP`](#var_samp).  
Requires the [`druid-stats` extension](../development/extensions-core/stats.md).

* **Syntax**: `VARIANCE(expr)`
* **Function type:** Aggregation

[Learn more](sql-aggregations.md)
