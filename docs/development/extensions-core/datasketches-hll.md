---
id: datasketches-hll
title: "DataSketches HLL Sketch module"
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


This module provides Apache Druid aggregators for distinct counting based on HLL sketch from [Apache DataSketches](https://datasketches.apache.org/) library. At ingestion time, this aggregator creates the HLL sketch objects to be stored in Druid segments. At query time, sketches are read and merged together. In the end, by default, you receive the estimate of the number of distinct values presented to the sketch. Also, you can use post aggregator to produce a union of sketch columns in the same row.
You can use the HLL sketch aggregator on columns of any identifiers. It will return estimated cardinality of the column.

To use this aggregator, make sure you [include](../../development/extensions.md#loading-extensions) the extension in your config file:

```
druid.extensions.loadList=["druid-datasketches"]
```

### Aggregators

|property|description|required?|
|--------|-----------|---------|
|`type`|This String should be [`HLLSketchBuild`](#hllsketchbuild-aggregator) or [`HLLSketchMerge`](#hllsketchmerge-aggregator)|yes|
|`name`|A String for the output (result) name of the calculation.|yes|
|`fieldName`|A String for the name of the input field.|yes|
|`lgK`|log2 of K that is the number of buckets in the sketch, parameter that controls the size and the accuracy. Must be between 4 and 21 inclusively.|no, defaults to `12`|
|`tgtHllType`|The type of the target HLL sketch. Must be `HLL_4`, `HLL_6` or `HLL_8` |no, defaults to `HLL_4`|
|`round`|Round off values to whole numbers. Only affects query-time behavior and is ignored at ingestion-time.|no, defaults to `false`|


> The default `lgK` value has proven to be sufficient for most use cases; expect only very negligible improvements in accuracy with `lgK` values over `16` in normal circumstances.

#### HLLSketchBuild Aggregator

```
{
  "type" : "HLLSketchBuild",
  "name" : <output name>,
  "fieldName" : <metric name>,
  "lgK" : <size and accuracy parameter>,
  "tgtHllType" : <target HLL type>,
  "round": <false | true>
 }
```

The `HLLSketchBuild` aggregator builds an HLL sketch object from the specified input column. When used during ingestion, Druid stores pre-generated HLL sketch objects in the datasource instead of the raw data from the input column.
When applied at query time on an existing dimension, you can use the resulting column as an intermediate dimension by the [post-aggregators](#post-aggregators).

> It is very common to use `HLLSketchBuild` in combination with [rollup](../../ingestion/rollup.md) to create a [metric](../../ingestion/ingestion-spec.html#metricsspec) on high-cardinality columns.  In this example, a metric called `userid_hll` is included in the `metricsSpec`.  This will perform a HLL sketch on the `userid` field at ingestion time, allowing for highly-performant approximate `COUNT DISTINCT` query operations and improving roll-up ratios when `userid` is then left out of the `dimensionsSpec`.
>
> ```
> :
> "metricsSpec": [
>  {
>    "type" : "HLLSketchBuild",
>    "name" : "userid_hll",
>    "fieldName" : "userid",
>    "lgK" : 12,
>    "tgtHllType" : "HLL_4"
>  }
> ]
> :
> ```
>

#### HLLSketchMerge Aggregator

```
{
  "type" : "HLLSketchMerge",
  "name" : <output name>,
  "fieldName" : <metric name>,
  "lgK" : <size and accuracy parameter>,
  "tgtHllType" : <target HLL type>,
  "round": <false | true>
 }
```

You can use the `HLLSketchMerge` aggregator to ingest pre-generated sketches from an input dataset. For example, you can set up a batch processing job to generate the sketches before sending the data to Druid. You must serialize the sketches in the input dataset to Base64-encoded bytes. Then, specify `HLLSketchMerge` for the input column in the native ingestion `metricsSpec`.

### Post Aggregators

#### Estimate

Returns the distinct count estimate as a double.

```
{
  "type"  : "HLLSketchEstimate",
  "name": <output name>,
  "field"  : <post aggregator that returns an HLL Sketch>,
  "round" : <if true, round the estimate. Default is false>
}
```

#### Estimate with bounds

Returns a distinct count estimate and error bounds from an HLL sketch.
The result will be an array containing three double values: estimate, lower bound and upper bound.
The bounds are provided at a given number of standard deviations (optional, defaults to 1).
This must be an integer value of 1, 2 or 3 corresponding to approximately 68.3%, 95.4% and 99.7% confidence intervals.

```
{
  "type"  : "HLLSketchEstimateWithBounds",
  "name": <output name>,
  "field"  : <post aggregator that returns an HLL Sketch>,
  "numStdDev" : <number of standard deviations: 1 (default), 2 or 3>
}
```

#### Union

```
{
  "type"  : "HLLSketchUnion",
  "name": <output name>,
  "fields"  : <array of post aggregators that return HLL sketches>,
  "lgK": <log2 of K for the target sketch>,
  "tgtHllType" : <target HLL type>
}
```

#### Sketch to string

Human-readable sketch summary for debugging.

```
{
  "type"  : "HLLSketchToString",
  "name": <output name>,
  "field"  : <post aggregator that returns an HLL Sketch>
}
```
