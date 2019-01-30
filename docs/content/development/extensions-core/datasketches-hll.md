---
layout: doc_page
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

# DataSketches HLL Sketch module

This module provides Druid aggregators for distinct counting based on HLL sketch from [datasketches](http://datasketches.github.io/) library. At ingestion time, this aggregator creates the HLL sketch objects to be stored in Druid segments. At query time, sketches are read and merged together. In the end, by default, you receive the estimate of the number of distinct values presented to the sketch. Also, you can use post aggregator to produce a union of sketch columns in the same row. 
You can use the HLL sketch aggregator on columns of any identifiers. It will return estimated cardinality of the column.

To use this aggregator, make sure you [include](../../operations/including-extensions.html) the extension in your config file:

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
  "tgtHllType" : <target HLL type>
 }
```

```
{
  "type" : "HLLSketchMerge",
  "name" : <output name>,
  "fieldName" : <metric name>,
  "lgK" : <size and accuracy parameter>,
  "tgtHllType" : <target HLL type>
 }
```

|property|description|required?|
|--------|-----------|---------|
|type|This String should be "HLLSketchBuild" or "HLLSketchMerge"|yes|
|name|A String for the output (result) name of the calculation.|yes|
|fieldName|A String for the name of the input field.|yes|
|lgK|log2 of K that is the number of buckets in the sketch, parameter that controls the size and the accuracy. Must be a power of 2 from 4 to 21 inclusively.|no, defaults to 12|
|tgtHllType|The type of the target HLL sketch. Must be "HLL&lowbar;4", "HLL&lowbar;6" or "HLL&lowbar;8" |no, defaults to "HLL&lowbar;4"|

### Post Aggregators

#### Estimate with bounds

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

Human-readable sketch summary for debugging

```
{
  "type"  : "HLLSketchToString",
  "name": <output name>,
  "field"  : <post aggregator that returns an HLL Sketch>
}
 
```
