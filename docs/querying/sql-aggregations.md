---
id: sql-aggregations
title: "SQL aggregation functions"
sidebar_label: "Aggregation functions"
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

You can use aggregation functions in the SELECT clause of any [Druid SQL](./sql.md) query.

Filter any aggregator using the FILTER clause, for example:

```
SELECT 
  SUM(added) FILTER(WHERE channel = '#en.wikipedia')
FROM wikipedia
```

The FILTER clause limits an aggregation query to only the rows that match the filter.
Druid translates the FILTER clause to a native [filtered aggregator](aggregations.md#filtered-aggregator).
Two aggregators in the same SQL query may have different filters.

When no rows are selected, aggregation functions return their initial value. This can occur from the following:
* When no rows match the filter while aggregating values across an entire table without a grouping, or
* When using filtered aggregations within a grouping.

The initial value varies by aggregator. `COUNT` and the approximate count distinct sketch functions
always return 0 as the initial value.

In the aggregation functions supported by Druid, only `COUNT`, `ARRAY_AGG`, and `STRING_AGG` accept the DISTINCT keyword.

:::info
 The order of aggregation operations across segments is not deterministic. This means that non-commutative aggregation
 functions can produce inconsistent results across the same query.

 Functions that operate on an input type of "float" or "double" may also see these differences in aggregation
 results across multiple query runs because of this. If precisely the same value is desired across multiple query runs,
 consider using the `ROUND` function to smooth out the inconsistencies between queries.
:::

|Function|Notes|Default|
|--------|-----|-------|
|`COUNT(*)`|Counts the number of rows.|`0`|
|`COUNT([DISTINCT] expr)`|Counts the values of `expr`.<br /><br />By default, using DISTINCT serves as an alias for `APPROX_COUNT_DISTINCT` (`useApproximateCountDistinct=true`). The specific algorithm depends on the value of [`druid.sql.approxCountDistinct.function`](../configuration/index.md#sql). In this mode, you can use strings, numbers, or prebuilt sketches. If counting prebuilt sketches, the prebuilt sketch type must match the selected algorithm.<br /><br />When `useApproximateCountDistinct=false`, returns the exact computation. In this case, `expr` must be string or numeric, since exact counts are not possible using prebuilt sketches. In exact mode, only one distinct count per query is permitted unless `useGroupingSetForExactDistinct` is enabled.<br /><br />Counts each distinct value in a [`multi-value`](../querying/multi-value-dimensions.md)-row separately.|`0`|
|`SUM(expr)`|Sums numbers.|`null` or `0` if `druid.generic.useDefaultValueForNull=true` (deprecated legacy mode)|
|`MIN(expr)`|Takes the minimum of numbers.|`null` or `9223372036854775807` (maximum LONG value) if `druid.generic.useDefaultValueForNull=true` (deprecated legacy mode)|
|`MAX(expr)`|Takes the maximum of numbers.|`null` or `-9223372036854775808` (minimum LONG value) if `druid.generic.useDefaultValueForNull=true` (deprecated legacy mode)|
|`AVG(expr)`|Averages numbers.|`null` or `0` if `druid.generic.useDefaultValueForNull=true` (deprecated legacy mode)|
|`APPROX_COUNT_DISTINCT(expr)`|Counts distinct values of `expr` using an approximate algorithm. The `expr` can be a regular column or a prebuilt sketch column.<br /><br />The specific algorithm depends on the value of [`druid.sql.approxCountDistinct.function`](../configuration/index.md#sql). By default, this is `APPROX_COUNT_DISTINCT_BUILTIN`. If the [DataSketches extension](../development/extensions-core/datasketches-extension.md) is loaded, you can set it to `APPROX_COUNT_DISTINCT_DS_HLL` or `APPROX_COUNT_DISTINCT_DS_THETA`.<br /><br />When run on prebuilt sketch columns, the sketch column type must match the implementation of this function. For example: when `druid.sql.approxCountDistinct.function` is set to `APPROX_COUNT_DISTINCT_BUILTIN`, this function runs on prebuilt hyperUnique columns, but not on prebuilt HLLSketchBuild columns.|
|`APPROX_COUNT_DISTINCT_BUILTIN(expr)`|_Usage note:_ consider using `APPROX_COUNT_DISTINCT_DS_HLL` instead, which offers better accuracy in many cases.<br/><br/>Counts distinct values of `expr` using Druid's built-in "cardinality" or "hyperUnique" aggregators, which implement a variant of [HyperLogLog](http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf). The `expr` can be a string, a number, or a prebuilt hyperUnique column. Results are always approximate, regardless of the value of `useApproximateCountDistinct`.|
|`APPROX_QUANTILE(expr, probability, [resolution])`|_Deprecated._ Use `APPROX_QUANTILE_DS` instead, which provides a superior distribution-independent algorithm with formal error guarantees.<br/><br/>Computes approximate quantiles on numeric or [approxHistogram](../development/extensions-core/approximate-histograms.md#approximate-histogram-aggregator) expressions. `probability` should be between 0 and 1, exclusive. `resolution` is the number of centroids to use for the computation. Higher resolutions will give more precise results but also have higher overhead. If not provided, the default resolution is 50. Load the [approximate histogram extension](../development/extensions-core/approximate-histograms.md) to use this function.|`NaN`|
|`APPROX_QUANTILE_FIXED_BUCKETS(expr, probability, numBuckets, lowerLimit, upperLimit, [outlierHandlingMode])`|Computes approximate quantiles on numeric or [fixed buckets histogram](../development/extensions-core/approximate-histograms.md#fixed-buckets-histogram) expressions. `probability` should be between 0 and 1, exclusive. The `numBuckets`, `lowerLimit`, `upperLimit`, and `outlierHandlingMode` parameters are described in the fixed buckets histogram documentation. Load the [approximate histogram extension](../development/extensions-core/approximate-histograms.md) to use this function.|`0.0`|
|`BLOOM_FILTER(expr, numEntries)`|Computes a bloom filter from values produced by `expr`, with `numEntries` maximum number of distinct values before false positive rate increases. See [bloom filter extension](../development/extensions-core/bloom-filter.md) documentation for additional details.|Empty base64 encoded bloom filter STRING|
|`VAR_POP(expr)`|Computes variance population of `expr`. See [stats extension](../development/extensions-core/stats.md) documentation for additional details.|`null` or `0` if `druid.generic.useDefaultValueForNull=true` (deprecated legacy mode)|
|`VAR_SAMP(expr)`|Computes variance sample of `expr`. See [stats extension](../development/extensions-core/stats.md) documentation for additional details.|`null` or `0` if `druid.generic.useDefaultValueForNull=true` (deprecated legacy mode)|
|`VARIANCE(expr)`|Computes variance sample of `expr`. See [stats extension](../development/extensions-core/stats.md) documentation for additional details.|`null` or `0` if `druid.generic.useDefaultValueForNull=true` (deprecated legacy mode)|
|`STDDEV_POP(expr)`|Computes standard deviation population of `expr`. See [stats extension](../development/extensions-core/stats.md) documentation for additional details.|`null` or `0` if `druid.generic.useDefaultValueForNull=true` (deprecated legacy mode)|
|`STDDEV_SAMP(expr)`|Computes standard deviation sample of `expr`. See [stats extension](../development/extensions-core/stats.md) documentation for additional details.|`null` or `0` if `druid.generic.useDefaultValueForNull=true` (deprecated legacy mode)|
|`STDDEV(expr)`|Computes standard deviation sample of `expr`. See [stats extension](../development/extensions-core/stats.md) documentation for additional details.|`null` or `0` if `druid.generic.useDefaultValueForNull=true` (deprecated legacy mode)|
|`EARLIEST(expr, [maxBytesPerValue])`|Returns the earliest value of `expr`.<br />If `expr` comes from a relation with a timestamp column (like `__time` in a Druid datasource), the "earliest" is taken from the row with the overall earliest non-null value of the timestamp column.<br />If the earliest non-null value of the timestamp column appears in multiple rows, the `expr` may be taken from any of those rows. If `expr` does not come from a relation with a timestamp, then it is simply the first value encountered.<br /><br />If `expr` is a string or complex type `maxBytesPerValue` amount of space is allocated for the aggregation. Strings longer than this limit are truncated. The  `maxBytesPerValue` parameter should be set as low as possible, since high values will lead to wasted memory.<br/>If `maxBytesPerValue`is omitted; it defaults to `1024`. |`null` or `0`/`''` if `druid.generic.useDefaultValueForNull=true` (deprecated legacy mode)|
|`EARLIEST_BY(expr, timestampExpr, [maxBytesPerValue])`|Returns the earliest value of `expr`.<br />The earliest value of `expr` is taken from the row with the overall earliest non-null value of `timestampExpr`. <br />If the earliest non-null value of `timestampExpr` appears in multiple rows, the `expr` may be taken from any of those rows.<br /><br />If `expr` is a string or complex type `maxBytesPerValue` amount of space is allocated for the aggregation. Strings longer than this limit are truncated. The  `maxBytesPerValue` parameter should be set as low as possible, since high values will lead to wasted memory.<br/>If `maxBytesPerValue`is omitted; it defaults to `1024`.<br /><br />Use `EARLIEST` instead of `EARLIEST_BY` on a table that has rollup enabled and was created with any variant of `EARLIEST`, `LATEST`, `EARLIEST_BY`, or `LATEST_BY`. In these cases, the intermediate type already stores the timestamp, and Druid ignores the value passed in `timestampExpr`. |`null` or `0`/`''` if `druid.generic.useDefaultValueForNull=true` (deprecated legacy mode)|
|`LATEST(expr, [maxBytesPerValue])`|Returns the latest value of `expr`<br />The `expr` must come from a relation with a timestamp column (like `__time` in a Druid datasource) and the "latest" is taken from the row with the overall latest non-null value of the timestamp column.<br />If the latest non-null value of the timestamp column appears in multiple rows, the `expr` may be taken from any of those rows.<br /><br />If `expr` is a string or complex type `maxBytesPerValue` amount of space is allocated for the aggregation. Strings longer than this limit are truncated. The  `maxBytesPerValue` parameter should be set as low as possible, since high values will lead to wasted memory.<br/>If `maxBytesPerValue`is omitted; it defaults to `1024`. |`null` or `0`/`''` if `druid.generic.useDefaultValueForNull=true` (deprecated legacy mode)|
|`LATEST_BY(expr, timestampExpr, [maxBytesPerValue])`|Returns the latest value of `expr`.<br />The latest value of `expr` is taken from the row with the overall latest non-null value of `timestampExpr`.<br />If the overall latest non-null value of `timestampExpr` appears in multiple rows, the `expr` may be taken from any of those rows.<br /><br />If `expr` is a string or complex type `maxBytesPerValue` amount of space is allocated for the aggregation. Strings longer than this limit are truncated. The `maxBytesPerValue` parameter should be set as low as possible, since high values will lead to wasted memory.<br/>If `maxBytesPerValue`is omitted; it defaults to `1024`.<br /><br />Use `LATEST` instead of `LATEST_BY` on a table that has rollup enabled and was created with any variant of `EARLIEST`, `LATEST`, `EARLIEST_BY`, or `LATEST_BY`. In these cases, the intermediate type already stores the timestamp, and Druid ignores the value passed in `timestampExpr`. |`null` or `0`/`''` if `druid.generic.useDefaultValueForNull=true` (deprecated legacy mode)|
|`ANY_VALUE(expr, [maxBytesPerValue, [aggregateMultipleValues]])`|Returns any value of `expr` including null. This aggregator can simplify and optimize the performance by returning the first encountered value (including `null`).<br /><br />If `expr` is a string or complex type `maxBytesPerValue` amount of space is allocated for the aggregation. Strings longer than this limit are truncated. The `maxBytesPerValue` parameter should be set as low as possible, since high values will lead to wasted memory.<br/>If `maxBytesPerValue` is omitted; it defaults to `1024`. `aggregateMultipleValues` is an optional boolean flag controls the behavior of aggregating a [multi-value dimension](./multi-value-dimensions.md). `aggregateMultipleValues` is set as true by default and returns the stringified array in case of a multi-value dimension. By setting it to false, function will return first value instead. |`null` or `0`/`''` if `druid.generic.useDefaultValueForNull=true` (deprecated legacy mode)|
|`GROUPING(expr, expr...)`|Returns a number to indicate which groupBy dimension is included in a row, when using `GROUPING SETS`. Refer to [additional documentation](aggregations.md#grouping-aggregator) on how to infer this number.|N/A|
|`ARRAY_AGG(expr, [size])`|Collects all values of `expr` into an ARRAY, including null values, with `size` in bytes limit on aggregation size (default of 1024 bytes). If the aggregated array grows larger than the maximum size in bytes, the query will fail. Use of `ORDER BY` within the `ARRAY_AGG` expression is not currently supported, and the ordering of results within the output array may vary depending on processing order.|`null`|
|`ARRAY_AGG(DISTINCT expr, [size])`|Collects all distinct values of `expr` into an ARRAY, including null values, with `size` in bytes limit on aggregation size (default of 1024 bytes) per aggregate. If the aggregated array grows larger than the maximum size in bytes, the query will fail. Use of `ORDER BY` within the `ARRAY_AGG` expression is not currently supported, and the ordering of results will be based on the default for the element type.|`null`|
|`ARRAY_CONCAT_AGG(expr, [size])`|Concatenates all array `expr` into a single ARRAY, with `size` in bytes limit on aggregation size (default of 1024 bytes).   Input `expr` _must_ be an array. Null `expr` will be ignored, but any null values within an `expr` _will_ be included in the resulting array. If the aggregated array grows larger than the maximum size in bytes, the query will fail. Use of `ORDER BY` within the `ARRAY_CONCAT_AGG` expression is not currently supported, and the ordering of results within the output array may vary depending on processing order.|`null`|
|`ARRAY_CONCAT_AGG(DISTINCT expr, [size])`|Concatenates all distinct values of all array `expr` into a single ARRAY, with `size` in bytes limit on aggregation size (default of 1024 bytes) per aggregate. Input `expr` _must_ be an array. Null `expr` will be ignored, but any null values within an `expr` _will_ be included in the resulting array. If the aggregated array grows larger than the maximum size in bytes, the query will fail. Use of `ORDER BY` within the `ARRAY_CONCAT_AGG` expression is not currently supported, and the ordering of results will be based on the default for the element type.|`null`|
|`STRING_AGG([DISTINCT] expr, [separator, [size]])`|Collects all values (or all distinct values) of `expr` into a single STRING, ignoring null values. Each value is joined by an optional `separator`, which must be a literal STRING. If the `separator` is not provided, strings are concatenated without a separator.<br /><br />An optional `size` in bytes can be supplied to limit aggregation size (default of 1024 bytes). If the aggregated string grows larger than the maximum size in bytes, the query will fail. Use of `ORDER BY` within the `STRING_AGG` expression is not currently supported, and the ordering of results within the output string may vary depending on processing order.|`null` or `''` if `druid.generic.useDefaultValueForNull=true` (deprecated legacy mode)|
|`LISTAGG([DISTINCT] expr, [separator, [size]])`|Synonym for `STRING_AGG`.|`null` or `''` if `druid.generic.useDefaultValueForNull=true` (deprecated legacy mode)|
|`BIT_AND(expr)`|Performs a bitwise AND operation on all input values.|`null` or `0` if `druid.generic.useDefaultValueForNull=true` (deprecated legacy mode)|
|`BIT_OR(expr)`|Performs a bitwise OR operation on all input values.|`null` or `0` if `druid.generic.useDefaultValueForNull=true` (deprecated legacy mode)|
|`BIT_XOR(expr)`|Performs a bitwise XOR operation on all input values.|`null` or `0` if `druid.generic.useDefaultValueForNull=true` (deprecated legacy mode)|

## Sketch functions

These functions create sketch objects that you can use to perform fast, approximate analyses.
For advice on choosing approximate aggregation functions, check out our [approximate aggregations documentation](aggregations.md#approx).
To operate on sketch objects, also see the [DataSketches post aggregator functions](sql-scalar.md#sketch-functions).

### HLL sketch functions

Load the [DataSketches extension](../development/extensions-core/datasketches-extension.md) to use the following functions.

|Function|Notes|Default|
|--------|-----|-------|
|`APPROX_COUNT_DISTINCT_DS_HLL(expr, [lgK, tgtHllType])`|Counts distinct values of `expr`, which can be a regular column or an [HLL sketch](../development/extensions-core/datasketches-hll.md) column. Results are always approximate, regardless of the value of [`useApproximateCountDistinct`](sql-query-context.md). The `lgK` and `tgtHllType` parameters here are, like the equivalents in the [aggregator](../development/extensions-core/datasketches-hll.md#aggregators), described in the HLL sketch documentation. See also `COUNT(DISTINCT expr)`.|`0`|
|`DS_HLL(expr, [lgK, tgtHllType])`|Creates an [HLL sketch](../development/extensions-core/datasketches-hll.md) on the values of `expr`, which can be a regular column or a column containing HLL sketches. The `lgK` and `tgtHllType` parameters are described in the HLL sketch documentation.|`'0'` (STRING)|


### Theta sketch functions

Load the [DataSketches extension](../development/extensions-core/datasketches-extension.md) to use the following functions.

|Function|Notes|Default|
|--------|-----|-------|
|`APPROX_COUNT_DISTINCT_DS_THETA(expr, [size])`|Counts distinct values of `expr`, which can be a regular column or a [Theta sketch](../development/extensions-core/datasketches-theta.md) column. Results are always approximate, regardless of the value of [`useApproximateCountDistinct`](sql-query-context.md). The `size` parameter is described in the Theta sketch documentation. See also `COUNT(DISTINCT expr)`.|`0`|
|`DS_THETA(expr, [size])`|Creates a [Theta sketch](../development/extensions-core/datasketches-theta.md) on the values of `expr`, which can be a regular column or a column containing Theta sketches. The `size` parameter is described in the Theta sketch documentation.|`'0.0'` (STRING)|


### Quantiles sketch functions

Load the [DataSketches extension](../development/extensions-core/datasketches-extension.md) to use the following functions.

|Function|Notes|Default|
|--------|-----|-------|
|`APPROX_QUANTILE_DS(expr, probability, [k])`|Computes approximate quantiles on numeric or [Quantiles sketch](../development/extensions-core/datasketches-quantiles.md) expressions. The `probability` value should be between 0 and 1, exclusive. The `k` parameter is described in the Quantiles sketch documentation.<br/><br/>See the [known issue](sql-translation.md#approximations) with this function.|`NaN`|
|`DS_QUANTILES_SKETCH(expr, [k])`|Creates a [Quantiles sketch](../development/extensions-core/datasketches-quantiles.md) on the values of `expr`, which can be a regular column or a column containing quantiles sketches. The `k` parameter is described in the Quantiles sketch documentation.<br/><br/>See the [known issue](sql-translation.md#approximations) with this function.|`'0'` (STRING)|


### Tuple sketch functions

Load the [DataSketches extension](../development/extensions-core/datasketches-extension.md) to use the following functions.

|Function|Notes|Default|
|--------|-----|-------|
|`DS_TUPLE_DOUBLES(expr, [nominalEntries])`|Creates a [Tuple sketch](../development/extensions-core/datasketches-tuple.md) on the values of `expr` which is a column containing Tuple sketches which contain an array of double values as their Summary Objects. The `nominalEntries` override parameter is optional and described in the Tuple sketch documentation.
|`DS_TUPLE_DOUBLES(dimensionColumnExpr, metricColumnExpr, ..., [nominalEntries])`|Creates a [Tuple sketch](../development/extensions-core/datasketches-tuple.md) which contains an array of double values as its Summary Object based on the dimension value of `dimensionColumnExpr` and the numeric metric values contained in one or more `metricColumnExpr` columns. If the last value of the array is a numeric literal, Druid assumes that the value is an override parameter for [nominal entries](../development/extensions-core/datasketches-tuple.md).


### T-Digest sketch functions

Load the T-Digest extension to use the following functions. See the [T-Digest extension](../development/extensions-contrib/tdigestsketch-quantiles.md) for additional details and for more information on these functions.

|Function|Notes|Default|
|--------|-----|-------|
|`TDIGEST_QUANTILE(expr, quantileFraction, [compression])`|Builds a T-Digest sketch on values produced by `expr` and returns the value for the quantile. Compression parameter (default value 100) determines the accuracy and size of the sketch. Higher compression means higher accuracy but more space to store sketches.|`Double.NaN`|
|`TDIGEST_GENERATE_SKETCH(expr, [compression])`|Builds a T-Digest sketch on values produced by `expr`. Compression parameter (default value 100) determines the accuracy and size of the sketch Higher compression means higher accuracy but more space to store sketches.|Empty base64 encoded T-Digest sketch STRING|
