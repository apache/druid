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


This module provides Apache Druid (incubating) aggregators for distinct counting based on HLL sketch from [datasketches](https://datasketches.github.io/) library. At ingestion time, this aggregator creates the HLL sketch objects to be stored in Druid segments. At query time, sketches are read and merged together. In the end, by default, you receive the estimate of the number of distinct values presented to the sketch. Also, you can use post aggregator to produce a union of sketch columns in the same row.
You can use the HLL sketch aggregator on columns of any identifiers. It will return estimated cardinality of the column.

To use this aggregator, make sure you [include](../../development/extensions.md#loading-extensions) the extension in your config file:

```
druid.extensions.loadList=["druid-datasketches"]
```

### Aggregators

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

|property|description|required?|
|--------|-----------|---------|
|type|This String should be "HLLSketchBuild" or "HLLSketchMerge"|yes|
|name|A String for the output (result) name of the calculation.|yes|
|fieldName|A String for the name of the input field.|yes|
|lgK|log2 of K that is the number of buckets in the sketch, parameter that controls the size and the accuracy. Must be a power of 2 from 4 to 21 inclusively.|no, defaults to 12|
|tgtHllType|The type of the target HLL sketch. Must be "HLL&lowbar;4", "HLL&lowbar;6" or "HLL&lowbar;8" |no, defaults to "HLL&lowbar;4"|
|round|Round off values to whole numbers. Only affects query-time behavior and is ignored at ingestion-time.|no, defaults to false|

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
