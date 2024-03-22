---
id: aggregations
title: "Aggregations"
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
Apache Druid supports two query languages: [Druid SQL](sql.md) and [native queries](querying.md).
This document describes the native
language. For information about aggregators available in SQL, refer to the
[SQL documentation](sql-aggregations.md).
:::

You can use aggregations:
-  in the ingestion spec during ingestion to summarize data before it enters Apache Druid.
-  at query time to summarize result data.

The following sections list the available aggregate functions. Unless otherwise noted, aggregations are available at both ingestion and query time.

## Exact aggregations

### Count aggregator

`count` computes the count of Druid rows that match the filters.

| Property | Description | Required |
| --- | --- | --- |
| `type` | Must be "count". | Yes |
| `name` | Output name of the aggregator | Yes |

Example:
```json
{ "type" : "count", "name" : "count" }
```

The `count` aggregator counts the number of Druid rows, which does not always reflect the number of raw events ingested.
This is because Druid can be configured to roll up data at ingestion time. To
count the number of ingested rows of data, include a `count` aggregator at ingestion time and a `longSum` aggregator at
query time.

### Sum aggregators

| Property | Description | Required |
| --- | --- | --- |
| `type` | Must be "longSum", "doubleSum", or "floatSum". | Yes |
| `name` | Output name for the summed value. | Yes |
| `fieldName` | Name of the input column to sum over. | No. You must specify `fieldName` or `expression`. |
| `expression` | You can specify an inline [expression](./math-expr.md) as an alternative to `fieldName`. | No. You must specify `fieldName` or `expression`. |

#### `longSum` aggregator

Computes the sum of values as a 64-bit, signed integer.

Example:
```json
{ "type" : "longSum", "name" : "sumLong", "fieldName" : "aLong" }
```

#### `doubleSum` aggregator

Computes and stores the sum of values as a 64-bit floating point value. Similar to `longSum`.

Example:
```json
{ "type" : "doubleSum", "name" : "sumDouble", "fieldName" : "aDouble" }
```

#### `floatSum` aggregator

Computes and stores the sum of values as a 32-bit floating point value. Similar to `longSum` and `doubleSum`.

Example:
```json
{ "type" : "floatSum", "name" : "sumFloat", "fieldName" : "aFloat" }
```

### Min and max aggregators

| Property | Description | Required |
| --- | --- | --- |
| `type` | Must be "doubleMin", "doubleMax", "floatMin", "floatMax", "longMin", or "longMax". | Yes |
| `name` | Output name for the min or max value. | Yes |
| `fieldName` | Name of the input column to compute the minimum or maximum value over. | No. You must specify `fieldName` or `expression`. |
| `expression` | You can specify an inline [expression](./math-expr.md) as an alternative to `fieldName`. | No. You must specify `fieldName` or `expression`. |

#### `doubleMin` aggregator

`doubleMin` computes the minimum of all input values and null if `druid.generic.useDefaultValueForNull` is false or Double.POSITIVE_INFINITY if true.

Example:
```json
{ "type" : "doubleMin", "name" : "maxDouble", "fieldName" : "aDouble" }
```

#### `doubleMax` aggregator

`doubleMax` computes the maximum of all input values and null if `druid.generic.useDefaultValueForNull` is false or Double.NEGATIVE_INFINITY if true.

Example:
```json
{ "type" : "doubleMax", "name" : "minDouble", "fieldName" : "aDouble" }
```

#### `floatMin` aggregator

`floatMin` computes the minimum of all input values and null if `druid.generic.useDefaultValueForNull` is false or Float.POSITIVE_INFINITY if true.

Example:
```json
{ "type" : "floatMin", "name" : "minFloat", "fieldName" : "aFloat" }
```

#### `floatMax` aggregator

`floatMax` computes the maximum of all input values and null if `druid.generic.useDefaultValueForNull` is false or Float.NEGATIVE_INFINITY if true.

Example:
```json
{ "type" : "floatMax", "name" : "maxFloat", "fieldName" : "aFloat" }
```

#### `longMin` aggregator

`longMin` computes the minimum of all input values and null if `druid.generic.useDefaultValueForNull` is false or Long.MAX_VALUE if true.

Example:
```json
{ "type" : "longMin", "name" : "minLong", "fieldName" : "aLong" }
```

#### `longMax` aggregator

`longMax` computes the maximum of all metric values and null if `druid.generic.useDefaultValueForNull` is false or Long.MIN_VALUE if true.

Example:
```json
{ "type" : "longMax", "name" : "maxLong", "fieldName" : "aLong" }
```

### `doubleMean` aggregator

Computes and returns the arithmetic mean of a column's values as a 64-bit floating point value. 

| Property | Description | Required |
| --- | --- | --- |
| `type` | Must be "doubleMean". | Yes |
| `name` | Output name for the mean value. | Yes |
| `fieldName` | Name of the input column to compute the arithmetic mean value over. | Yes |

Example:
```json
{ "type" : "doubleMean", "name" : "aMean", "fieldName" : "aDouble" }
```

`doubleMean` is a query time aggregator only. It is not available for indexing. To accomplish mean aggregation on ingestion, refer to the [Quantiles aggregator](../development/extensions-core/datasketches-quantiles.md#aggregator) from the DataSketches extension.


### First and last aggregators

The first and last aggregators determine the metric values that respectively correspond to the earliest and latest values of a time column.

Queries with first or last aggregators on a segment created with rollup return the rolled up value, not the first or last value from the 
raw ingested data. The `timeColumn` will get ignored in such cases, and the aggregation will use the original value of the time column
stored at the time the segment was created.

#### Numeric first and last aggregators

| Property | Description | Required |
| --- | --- | --- |
| `type` | Must be "doubleFirst", "doubleLast", "floatFirst", "floatLast", "longFirst", "longLast". | Yes |
| `name` | Output name for the first or last value. | Yes |
| `fieldName` | Name of the input column to compute the first or last value over. | Yes |
| `timeColumn` | Name of the input column to use for time values. Must be a LONG typed column. | No. Defaults to `__time`. |

##### `doubleFirst` aggregator

`doubleFirst` computes the input value with the minimum value for time column or 0 in default mode, or `null` in SQL-compatible mode if no row exists.

Example:
```json
{
  "type" : "doubleFirst",
  "name" : "firstDouble",
  "fieldName" : "aDouble"
}
```

##### `doubleLast` aggregator

`doubleLast` computes the input value with the maximum value for time column or 0 in default mode, or `null` in SQL-compatible mode if no row exists.

Example:
```json
{
  "type" : "doubleLast",
  "name" : "lastDouble",
  "fieldName" : "aDouble",
  "timeColumn" : "longTime"
}
```

##### `floatFirst` aggregator

`floatFirst` computes the input value with the minimum value for time column or 0 in default mode, or `null` in SQL-compatible mode if no row exists.

Example:
```json
{
  "type" : "floatFirst",
  "name" : "firstFloat",
  "fieldName" : "aFloat"
}
```

##### `floatLast` aggregator

`floatLast` computes the metric value with the maximum value for time column or 0 in default mode, or `null` in SQL-compatible mode if no row exists.

Example:
```json
{
  "type" : "floatLast",
  "name" : "lastFloat",
  "fieldName" : "aFloat"
}
```

##### `longFirst` aggregator

`longFirst` computes the metric value with the minimum value for time column or 0 in default mode, or `null` in SQL-compatible mode if no row exists.

Example:
```json
{
  "type" : "longFirst",
  "name" : "firstLong",
  "fieldName" : "aLong"
}
```

##### `longLast` aggregator

`longLast` computes the metric value with the maximum value for time column or 0 in default mode, or `null` in SQL-compatible mode if no row exists.

Example:
```json
{
  "type" : "longLast",
  "name" : "lastLong",
  "fieldName" : "aLong",
  "timeColumn" : "longTime"
}
```

#### String first and last aggregators

| Property | Description | Required |
| --- | --- | --- |
| `type` | Must be "stringFirst", "stringLast". | Yes |
| `name` | Output name for the first or last value. | Yes |
| `fieldName` | Name of the input column to compute the first or last value over. | Yes |
| `timeColumn` | Name of the input column to use for time values. Must be a LONG typed column. | No. Defaults to `__time`. |
| `maxStringBytes` | Maximum size of string values to accumulate when computing the first or last value per group. Values longer than this will be truncated. | No. Defaults to 1024. |


#### `stringFirst` aggregator

`stringFirst` computes the metric value with the minimum value for time column or `null` if no row exists.

Example:
```json
{
  "type" : "stringFirst",
  "name" : "firstString",
  "fieldName" : "aString",
  "maxStringBytes" : 2048,
  "timeColumn" : "longTime"
}
```

#### `stringLast` aggregator

`stringLast` computes the metric value with the maximum value for time column or `null` if no row exists.

Example:
```json
{
  "type" : "stringLast",
  "name" : "lastString",
  "fieldName" : "aString"
}
```

### ANY aggregators

(Double/Float/Long/String) ANY aggregator cannot be used in ingestion spec, and should only be specified as part of queries.

Returns any value including null. This aggregator can simplify and optimize the performance by returning the first encountered value (including null)

#### Numeric any aggregators
| Property | Description | Required |
| --- | --- | --- |
| `type` | Must be "doubleAny", "floatAny", or "longAny". | Yes |
| `name` | Output name for the value. | Yes |
| `fieldName` | Name of the input column to compute the value over. | Yes |

##### `doubleAny` aggregator

`doubleAny` returns any double metric value.

Example:
```json
{
  "type" : "doubleAny",
  "name" : "anyDouble",
  "fieldName" : "aDouble"
}
```

##### `floatAny` aggregator

`floatAny` returns any float metric value.

Example:
```json
{
  "type" : "floatAny",
  "name" : "anyFloat",
  "fieldName" : "aFloat"
}
```

##### `longAny` aggregator

`longAny` returns any long metric value.

Example:
```json
{
  "type" : "longAny",
  "name" : "anyLong",
  "fieldName" : "aLong"
}
```

#### `stringAny` aggregator

`stringAny` returns any string value present in the input.

| Property | Description | Required |
| --- | --- | --- |
| `type` | Must be "stringAny". | Yes |
| `name` | Output name for the value. | Yes |
| `fieldName` | Name of the input column to compute the value over. | Yes |
| `maxStringBytes` | Maximum size of string values to accumulate when computing the first or last value per group. Values longer than this will be truncated. | No. Defaults to 1024. |
| `aggregateMultipleValues` | `aggregateMultipleValues` is an optional boolean flag controls the behavior of aggregating a [multi-value dimension](./multi-value-dimensions.md). `aggregateMultipleValues` is set as true by default and returns the stringified array in case of a multi-value dimension. By setting it to false, function will return first value instead. | No. Defaults to true. |

Example:
```json
{
  "type" : "stringAny",
  "name" : "anyString",
  "fieldName" : "aString",
  "maxStringBytes" : 2048
}
```

<a name="approx"></a>

## Approximate aggregations

### Count distinct

#### Apache DataSketches Theta Sketch

The [DataSketches Theta Sketch](../development/extensions-core/datasketches-theta.md) extension-provided aggregator gives distinct count estimates with support for set union, intersection, and difference post-aggregators, using Theta sketches from the [Apache DataSketches](https://datasketches.apache.org/) library.

#### Apache DataSketches HLL Sketch

The [DataSketches HLL Sketch](../development/extensions-core/datasketches-hll.md) extension-provided aggregator gives distinct count estimates using the HyperLogLog algorithm.

Compared to the Theta sketch, the HLL sketch does not support set operations and has slightly slower update and merge speed, but requires significantly less space.

#### Cardinality, hyperUnique

:::info
For new use cases, we recommend evaluating [DataSketches Theta Sketch](../development/extensions-core/datasketches-theta.md) or [DataSketches HLL Sketch](../development/extensions-core/datasketches-hll.md) instead.
The DataSketches aggregators are generally able to offer more flexibility and better accuracy than the classic Druid `cardinality` and `hyperUnique` aggregators.
:::

The [Cardinality and HyperUnique](../querying/hll-old.md) aggregators are older aggregator implementations available by default in Druid that also provide distinct count estimates using the HyperLogLog algorithm. The newer DataSketches Theta and HLL extension-provided aggregators described above have superior accuracy and performance and are recommended instead.

The DataSketches team has published a [comparison study](https://datasketches.apache.org/docs/HLL/HllSketchVsDruidHyperLogLogCollector.html) between Druid's original HLL algorithm and the DataSketches HLL algorithm. Based on the demonstrated advantages of the DataSketches implementation, we are recommending using them in preference to Druid's original HLL-based aggregators.
However, to ensure backwards compatibility, we will continue to support the classic aggregators.

Please note that `hyperUnique` aggregators are not mutually compatible with Datasketches HLL or Theta sketches.

##### Multi-column handling

Note the DataSketches Theta and HLL aggregators currently only support single-column inputs. If you were previously using the Cardinality aggregator with multiple-column inputs, equivalent operations using Theta or HLL sketches are described below:

* Multi-column `byValue` Cardinality can be replaced with a union of Theta sketches on the individual input columns
* Multi-column `byRow` Cardinality can be replaced with a Theta or HLL sketch on a single [virtual column](../querying/virtual-columns.md) that combines the individual input columns.

### Histograms and quantiles

#### DataSketches Quantiles Sketch

The [DataSketches Quantiles Sketch](../development/extensions-core/datasketches-quantiles.md) extension-provided aggregator provides quantile estimates and histogram approximations using the numeric quantiles DoublesSketch from the [datasketches](https://datasketches.apache.org/) library.

We recommend this aggregator in general for quantiles/histogram use cases, as it provides formal error bounds and has distribution-independent accuracy.

#### Moments Sketch (Experimental)

The [Moments Sketch](../development/extensions-contrib/momentsketch-quantiles.md) extension-provided aggregator is an experimental aggregator that provides quantile estimates using the [Moments Sketch](https://github.com/stanford-futuredata/momentsketch).

The Moments Sketch aggregator is provided as an experimental option. It is optimized for merging speed and it can have higher aggregation performance compared to the DataSketches quantiles aggregator. However, the accuracy of the Moments Sketch is distribution-dependent, so users will need to empirically verify that the aggregator is suitable for their input data.

As a general guideline for experimentation, the [Moments Sketch paper](https://arxiv.org/pdf/1803.01969.pdf) points out that this algorithm works better on inputs with high entropy. In particular, the algorithm is not a good fit when the input data consists of a small number of clustered discrete values.

#### Fixed Buckets Histogram

Druid also provides a [simple histogram implementation](../development/extensions-core/approximate-histograms.md#fixed-buckets-histogram) that uses a fixed range and fixed number of buckets with support for quantile estimation, backed by an array of bucket count values.

The fixed buckets histogram can perform well when the distribution of the input data allows a small number of buckets to be used.

We do not recommend the fixed buckets histogram for general use, as its usefulness is extremely data dependent. However, it is made available for users that have already identified use cases where a fixed buckets histogram is suitable.

#### Approximate Histogram (deprecated)

:::info
The Approximate Histogram aggregator is deprecated.
There are a number of other quantile estimation algorithms that offer better performance, accuracy, and memory footprint.
We recommend using [DataSketches Quantiles](../development/extensions-core/datasketches-quantiles.md) instead.
:::

The [Approximate Histogram](../development/extensions-core/approximate-histograms.md) extension-provided aggregator also provides quantile estimates and histogram approximations, based on [http://jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf](http://jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf).

The algorithm used by this deprecated aggregator is highly distribution-dependent and its output is subject to serious distortions when the input does not fit within the algorithm's limitations.

A [study published by the DataSketches team](https://datasketches.apache.org/docs/QuantilesStudies/DruidApproxHistogramStudy.html) demonstrates some of the known failure modes of this algorithm:

- The algorithm's quantile calculations can fail to provide results for a large range of rank values (all ranks less than 0.89 in the example used in the study), returning all zeroes instead.
- The algorithm can completely fail to record spikes in the tail ends of the distribution
- In general, the histogram produced by the algorithm can deviate significantly from the true histogram, with no bounds on the errors.

It is not possible to determine a priori how well this aggregator will behave for a given input stream, nor does the aggregator provide any indication that serious distortions are present in the output.

For these reasons, we have deprecated this aggregator and recommend using the DataSketches Quantiles aggregator instead for new and existing use cases, although we will continue to support Approximate Histogram for backwards compatibility.


## Expression aggregations

### Expression aggregator

Aggregator applicable only at query time. Aggregates results using [Druid expressions](./math-expr.md) functions to facilitate building custom functions.

| Property | Description | Required |
| --- | --- | --- |
| `type` | Must be "expression". | Yes |
| `name` | The aggregator output name. | Yes |
| `fields` | The list of aggregator input columns. | Yes |
| `accumulatorIdentifier` | The variable which identifies the accumulator value in the `fold` and `combine` expressions. | No. Default `__acc`.|
| `fold` | The expression to accumulate values from `fields`. The result of the expression is stored in `accumulatorIdentifier` and available to the next computation. | Yes |
| `combine` | The expression to combine the results of various `fold` expressions of each segment when merging results. The input is available to the expression as a variable identified by the `name`. | No. Default to `fold` expression if the expression has a single input in `fields`.|
| `compare` | The comparator expression which can only refer to two input variables, `o1` and `o2`, where `o1` and `o2` are the output of `fold` or `combine` expressions, and must adhere to the Java comparator contract. If not set, the aggregator will try to fall back to an output type appropriate comparator. | No |
| `finalize` | The finalize expression which can only refer to a single input variable, `o`. This expression is used to perform any final transformation of the output of the `fold` or `combine` expressions. If not set, then the value is not transformed. | No |
| `initialValue` | The initial value of the accumulator for the `fold` (and `combine`, if `InitialCombineValue` is null) expression. | Yes |
| `initialCombineValue` | The initial value of the accumulator for the `combine` expression. | No. Default `initialValue`. |
| `isNullUnlessAggregated` | Indicates that the default output value should be `null` if the aggregator does not process any rows. If true, the value is `null`, if false, the result of running the expressions with initial values is used instead. | No. Defaults to the value of `druid.generic.useDefaultValueForNull`. |
| `shouldAggregateNullInputs` | Indicates if the `fold` expression should operate on any `null` input values. | No. Defaults to `true`. |
| `shouldCombineAggregateNullInputs` | Indicates if the `combine` expression should operate on any `null` input values. | No. Defaults to the value of `shouldAggregateNullInputs`. |
| `maxSizeBytes` | Maximum size in bytes that variably sized aggregator output types such as strings and arrays are allowed to grow to before the aggregation fails. | No. Default is 8192 bytes. |

#### Example: a "count" aggregator
The initial value is `0`. `fold` adds `1` for each row processed.

```json
{
  "type": "expression",
  "name": "expression_count",
  "fields": [],
  "initialValue": "0",
  "fold": "__acc + 1",
  "combine": "__acc + expression_count"
}
```

#### Example: a "sum" aggregator
The initial value is `0`. `fold` adds the numeric value `column_a` for each row processed.

```json
{
  "type": "expression",
  "name": "expression_sum",
  "fields": ["column_a"],
  "initialValue": "0",
  "fold": "__acc + column_a"
}
```

#### Example: a "distinct array element" aggregator, sorted by array_length
The initial value is an empty array. `fold` adds the elements of `column_a` to the accumulator using set semantics, `combine` merges the sets, and `compare` orders the values by `array_length`.

```json
{
  "type": "expression",
  "name": "expression_array_agg_distinct",
  "fields": ["column_a"],
  "initialValue": "[]",
  "fold": "array_set_add(__acc, column_a)",
  "combine": "array_set_add_all(__acc, expression_array_agg_distinct)",
  "compare": "if(array_length(o1) > array_length(o2), 1, if (array_length(o1) == array_length(o2), 0, -1))"
}
```

#### Example: an "approximate count" aggregator using the built-in hyper-unique
Similar to the cardinality aggregator, the default value is an empty hyper-unique sketch, `fold` adds the value of `column_a` to the sketch, `combine` merges the sketches, and `finalize` gets the estimated count from the accumulated sketch.

```json
{
  "type": "expression",
  "name": "expression_cardinality",
  "fields": ["column_a"],
  "initialValue": "hyper_unique()",
  "fold": "hyper_unique_add(column_a, __acc)",
  "combine": "hyper_unique_add(expression_cardinality, __acc)",
  "finalize": "hyper_unique_estimate(o)"
}
```

### JavaScript aggregator

Computes an arbitrary JavaScript function over a set of columns (both metrics and dimensions are allowed). Your
JavaScript functions are expected to return floating-point values.

| Property | Description | Required |
| --- | --- | --- |
| `type` | Must be "javascript". | Yes |
| `name` | The aggregator output name. | Yes |
| `fieldNames` | The list of aggregator input columns. | Yes |
| `fnAggregate` | JavaScript function that updates partial aggregate based on the current row values, and returns the updated partial aggregate. | Yes |
| `fnCombine` | JavaScript function to combine partial aggregates and return the combined result. | Yes |
| `fnReset` | JavaScript function that returns the 'initial' value. | Yes |

#### Example

```json
{
  "type": "javascript",
  "name": "sum(log(x)*y) + 10",
  "fieldNames": ["x", "y"],
  "fnAggregate" : "function(current, a, b)      { return current + (Math.log(a) * b); }",
  "fnCombine"   : "function(partialA, partialB) { return partialA + partialB; }",
  "fnReset"     : "function()                   { return 10; }"
}
```

:::info
JavaScript-based functionality is disabled by default. Refer to the Druid [JavaScript programming guide](../development/javascript.md) for guidelines about using Druid's JavaScript functionality, including instructions on how to enable it.
:::


## Miscellaneous aggregations

### Filtered aggregator

A filtered aggregator wraps any given aggregator, but only aggregates the values for which the given dimension filter matches.

This makes it possible to compute the results of a filtered and an unfiltered aggregation simultaneously, without having to issue multiple queries, and use both results as part of post-aggregations.

If only the filtered results are required, consider putting the filter on the query itself. This will be much faster since it does not require scanning all the data.

| Property | Description | Required |
| --- | --- | --- |
| `type` | Must be "filtered". | Yes |
| `name` | The aggregator output name. | No |
| `aggregator` | Inline aggregator specification. | Yes |
| `filter` | Inline [filter](./filters.md) specification. | Yes |

Example:
```json
{
  "type": "filtered",
  "name": "filteredSumLong",
  "filter": {
    "type" : "selector",
    "dimension" : "someColumn",
    "value" : "abcdef"
  },
  "aggregator": {
    "type": "longSum",
    "name": "sumLong",
    "fieldName": "aLong"
  }
}
```

### Grouping aggregator

A grouping aggregator can only be used as part of GroupBy queries which have a subtotal spec. It returns a number for
each output row that lets you infer whether a particular dimension is included in the sub-grouping used for that row. You can pass
a *non-empty* list of dimensions to this aggregator which *must* be a subset of dimensions that you are grouping on. 

| Property | Description | Required |
| --- | --- | --- |
| `type` | Must be "grouping". | Yes |
| `name` | The aggregator output name. | Yes |
| `groupings` | The list of columns to use in the grouping set. | Yes |


For example, the following aggregator has `["dim1", "dim2"]` as input dimensions:

```json
{ "type" : "grouping", "name" : "someGrouping", "groupings" : ["dim1", "dim2"] }
```

and used in a grouping query with `[["dim1", "dim2"], ["dim1"], ["dim2"], []]` as subtotals, the
possible output of the aggregator is:

| subtotal used in query | Output | (bits representation) |
|------------------------|--------|-----------------------|
| `["dim1", "dim2"]`       | 0      | (00)                  |
| `["dim1"]`               | 1      | (01)                  |
| `["dim2"]`               | 2      | (10)                  |
| `[]`                     | 3      | (11)                  |  

As the example illustrates, you can think of the output number as an unsigned _n_ bit number where _n_ is the number of dimensions passed to the aggregator. 
Druid sets the bit at position X for the number to 0 if the sub-grouping includes a dimension at position X in the aggregator input. Otherwise, Druid sets this bit to 1.
