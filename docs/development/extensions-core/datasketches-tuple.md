---
id: datasketches-tuple
title: "DataSketches Tuple Sketch module"
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


This module provides Apache&circledR; Druid aggregators based on Tuple sketch from [Apache DataSketches](https://datasketches.apache.org/) library.

A Tuple sketch is a [Theta sketch](datasketches-theta.md) plus a vector of numeric values associated with each distinct key. Distinct keys are counted and can be combined with union, intersection, and set difference just like a Theta sketch. Beyond that, you can compute the mean, variance, and estimated sum of the numeric values attached to those keys.

This is an extension of Theta sketches, not a quantile sketch. Quantile sketches estimate the distribution of a stream of values. A Tuple sketch retains distinct keys and attaches a numeric summary to each key.

For example, you can add user IDs as keys and associate each user with a lifetime value (a one-element vector). After restricting the sketch to users that match a set of parameters, you can estimate how many of those users there are and compute their average lifetime value.

Druid implements this as the DataSketches `ArrayOfDoublesSketch`: each distinct key is associated with an array of double values. See the [Tuple sketch overview](https://datasketches.apache.org/docs/Tuple/TupleOverview.html) in the DataSketches documentation.

To use this aggregator, make sure you [include](../../configuration/extensions.md#loading-extensions) the extension in your config file:

```
druid.extensions.loadList=["druid-datasketches"]
```

For additional sketch types supported in Druid, see [DataSketches extension](datasketches-extension.md).

You can also build and query Tuple sketches in SQL. See [Tuple sketch aggregation functions](../../querying/sql-aggregations.md#tuple-sketch-functions) and [Tuple sketch scalar functions](../../querying/sql-scalar.md#tuple-sketch-functions).

## Aggregator

```json
{
  "type" : "arrayOfDoublesSketch",
  "name" : <output_name>,
  "fieldName" : <metric_name>,
  "nominalEntries": <number>,
  "metricColumns" : <array of strings>,
  "numberOfValues" : <number>
}
```

|Property|Description|Required?|
|--------|-----------|---------|
|`type`|This string should always be "arrayOfDoublesSketch"|yes|
|`name`|String representing the output column to store sketch values.|yes|
|`fieldName`|A string for the name of the input field (the key column when building from raw data, or a precomputed `arrayOfDoublesSketch` column when merging sketches).|yes|
|`nominalEntries`|Parameter that determines the accuracy and size of the sketch. Higher `nominalEntries` means higher accuracy but more space to store sketches. Must be a power of 2. See the [Theta sketch accuracy](https://datasketches.apache.org/docs/Theta/ThetaErrorTable) for details.|no, defaults to 4096|
|`metricColumns`|When building sketches from raw data, an array of input columns that contain numeric values to associate with each distinct key. If not provided, `fieldName` is assumed to be an `arrayOfDoublesSketch`.|no, if not provided `fieldName` is assumed to be an arrayOfDoublesSketch|
|`numberOfValues`|Number of values associated with each distinct key. If you also set `metricColumns`, this value must equal the length of `metricColumns`.|no, defaults to the length of `metricColumns` if provided and 1 otherwise|

You can use the `arrayOfDoublesSketch` aggregator to:

- Build a sketch from raw data. In this case, set `metricColumns` to the numeric columns to associate with each key in `fieldName`. If any metric column is null in a row, that row is skipped. If the same key is added more than once, the associated double values are summed.
- Merge or ingest a precomputed `ArrayOfDoubles` sketch. In this case, leave `metricColumns` unset and set `fieldName` to an `ArrayOfDoubles` sketch with `numberOfValues` doubles. When you ingest a precomputed sketch from a text column, encode it as a Base64 string. Druid also accepts a raw byte array or an `ArrayOfDoublesSketch` object.

If you query this aggregator as a result column, Druid finalizes the value to a DOUBLE distinct-count estimate (`ArrayOfDoublesSketch.getEstimate()`). This aggregator has no `shouldFinalize` parameter. Use the post-aggregators below to compute means, variances, estimated sums, set operations, and other results from the sketch before it is finalized. In SQL, `DS_TUPLE_DOUBLES` does not read [`sqlFinalizeOuterSketches`](../../querying/sql-query-context.md); as a result column it is also finalized to a distinct-count estimate by default. To return the sketch itself from a native or SQL query, set the query context parameter [`finalize`](../../querying/query-context-reference.md#finalize) to `false`.

### Example on top of raw data

Build a Tuple sketch of unique users and associate each user with a lifetime value. The new sketch column is called `users_ltv`. After aggregation, use a post-aggregator such as `arrayOfDoublesSketchToMeans` to compute the average lifetime value of the retained users.

```json
{
  "type": "arrayOfDoublesSketch",
  "name": "users_ltv",
  "fieldName": "user_id",
  "nominalEntries": 4096,
  "metricColumns": ["lifetime_value"]
}
```

You can associate more than one metric with each key. For example, `metricColumns` can be `["added", "deleted"]`.

### Example ingesting a precomputed sketch column

Ingest a sketch column called `user_sketches` that has a Base64-encoded value of two doubles in its array and store it in a column called `users_sketch`.

```json
{
  "type": "arrayOfDoublesSketch",
  "name": "users_sketch",
  "fieldName": "user_sketches",
  "nominalEntries": 4096,
  "numberOfValues": 2
}
```

## Post aggregators

### Estimate of the number of distinct keys

Returns a distinct count estimate from a given ArrayOfDoublesSketch.

```json
{
  "type"  : "arrayOfDoublesSketchToEstimate",
  "name": <output name>,
  "field"  : <post aggregator that refers to an ArrayOfDoublesSketch (fieldAccess or another post aggregator)>
}
```

### Estimate of the number of distinct keys with error bounds

Returns a distinct count estimate and error bounds from a given ArrayOfDoublesSketch. The result will be three double values: estimate of the number of distinct keys, lower bound and upper bound. The bounds are provided at the given number of standard deviations (optional, defaults to 1). This must be an integer value of 1, 2 or 3 corresponding to approximately 68.3%, 95.4% and 99.7% confidence intervals.

```json
{
  "type"  : "arrayOfDoublesSketchToEstimateAndBounds",
  "name": <output name>,
  "field"  : <post aggregator that refers to an ArrayOfDoublesSketch (fieldAccess or another post aggregator)>,
  "numStdDevs": <number from 1 to 3>
}
```

### Number of retained entries

Returns the number of retained entries from a given ArrayOfDoublesSketch.

```json
{
  "type"  : "arrayOfDoublesSketchToNumEntries",
  "name": <output name>,
  "field"  : <post aggregator that refers to an ArrayOfDoublesSketch (fieldAccess or another post aggregator)>
}
```

### Mean values for each column

Returns a list of mean values from a given ArrayOfDoublesSketch, computed from the values on retained keys. The result will be N double values, where N is the number of double values kept in the sketch per key.

```json
{
  "type"  : "arrayOfDoublesSketchToMeans",
  "name": <output name>,
  "field"  : <post aggregator that refers to an ArrayOfDoublesSketch (fieldAccess or another post aggregator)>
}
```

For the lifetime-value example above, this post-aggregator returns a one-element array with the mean lifetime value of the retained users:

```json
{
  "type": "arrayOfDoublesSketchToMeans",
  "name": "avg_lifetime_value",
  "field": {
    "type": "fieldAccess",
    "fieldName": "users_ltv"
  }
}
```

### Variance values for each column

Returns a list of variance values from a given ArrayOfDoublesSketch, computed from the values on retained keys. The result will be N double values, where N is the number of double values kept in the sketch per key.

```json
{
  "type"  : "arrayOfDoublesSketchToVariances",
  "name": <output name>,
  "field"  : <post aggregator that refers to an ArrayOfDoublesSketch (fieldAccess or another post aggregator)>
}
```

### Quantiles sketch from a column

Returns a quantiles DoublesSketch constructed from a given column of values from a given ArrayOfDoublesSketch using optional parameter k that determines the accuracy and size of the quantiles sketch. See [Quantiles Sketch Module](datasketches-quantiles.md)

* The column number is 1-based and is optional (the default is 1).
* The parameter k is optional (the default is 128).
* The result is a quantiles sketch.

```json
{
  "type"  : "arrayOfDoublesSketchToQuantilesSketch",
  "name": <output name>,
  "field"  : <post aggregator that refers to an ArrayOfDoublesSketch (fieldAccess or another post aggregator)>,
  "column" : <number>,
  "k" : <parameter that determines the accuracy and size of the quantiles sketch>
}
```

### Set operations

Returns a result of a specified set operation on the given array of sketches. Supported operations are: union, intersection and set difference (UNION, INTERSECT, NOT).

You must provide at least two `fields`. `nominalEntries` is optional and defaults to 4096; it must be a power of 2. `numberOfValues` is optional and defaults to 1; set it to the number of double values per key in the input sketches.

UNION and INTERSECT sum the associated values when the same key appears in more than one sketch. NOT (A-not-B, applied left to right) keeps the values from the first sketch for the keys that remain.

```json
{
  "type"  : "arrayOfDoublesSketchSetOp",
  "name": <output name>,
  "operation": <"UNION"|"INTERSECT"|"NOT">,
  "fields"  : <array of post aggregators to access sketch aggregators or post aggregators to allow arbitrary combination of set operations>,
  "nominalEntries" : <parameter that determines the accuracy and size of the sketch>,
  "numberOfValues" : <number of values associated with each distinct key>
}
```

### Student's t-test

Performs Student's t-test and returns a list of p-values given two instances of ArrayOfDoublesSketch. You must provide exactly two sketches, and they must have the same number of double values per key. The result will be N double values, where N is the number of double values kept in the sketch per key. See [t-test documentation](https://commons.apache.org/proper/commons-math/javadocs/api-3.6.1/org/apache/commons/math3/stat/inference/TTest.html).

```json
{
  "type"  : "arrayOfDoublesSketchTTest",
  "name": <output name>,
  "fields"  : <array with two post aggregators to access sketch aggregators or post aggregators referring to an ArrayOfDoublesSketch>
}
```

### Sketch summary

Returns a human-readable summary of a given ArrayOfDoublesSketch. This is a string returned by toString() method of the sketch. This can be useful for debugging.

```json
{
  "type"  : "arrayOfDoublesSketchToString",
  "name": <output name>,
  "field"  : <post aggregator that refers to an ArrayOfDoublesSketch (fieldAccess or another post aggregator)>
}
```


### Constant ArrayOfDoublesSketch

This post aggregator adds a Base64-encoded constant ArrayOfDoublesSketch value that you can use in other post aggregators.
```json
{
  "type": "arrayOfDoublesSketchConstant",
  "name": DESTINATION_COLUMN_NAME,
  "value": CONSTANT_SKETCH_VALUE
}
```

### Base64 output of ArrayOfDoublesSketch

This post aggregator outputs an ArrayOfDoublesSketch as a Base64-encoded string. You can use that string in other post aggregators, such as `arrayOfDoublesSketchConstant`, or as a precomputed sketch to ingest later.

```json
{
  "type": "arrayOfDoublesSketchToBase64String",
  "name": DESTINATION_COLUMN_NAME,
  "field": <post aggregator that refers to an ArrayOfDoublesSketch (fieldAccess or another post aggregator)>
}
```

### Estimated metrics values for each column of ArrayOfDoublesSketch

For the key-value pairs in the given ArrayOfDoublesSketch, this post aggregator estimates the sum of each metric across the keys. The estimate is the sum of the values on retained keys divided by the sketch's theta. When the sketch is not in estimation mode, theta is 1 and the result equals the exact sum of the retained values. For example, when theta is 1, the post aggregator returns `{3.0, 8.0}` for the following key-value pairs:

```
Key_1, {1.0, 3.0}
Key_2, {2.0, 5.0}
```

The post aggregator returns _N_ double values, where _N_ is the number of values associated with each key.

```json
{
  "type": "arrayOfDoublesSketchToMetricsSumEstimate",
  "name": DESTINATION_COLUMN_NAME,
  "field": <post aggregator that refers to an ArrayOfDoublesSketch (fieldAccess or another post aggregator)>
}
```
