---
layout: doc_page
title: "Moment Sketches for Approximate Quantiles module"
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

# MomentSketch Quantiles Sketch module

This module provides Druid aggregators for approximate quantile queries using the [momentsketch](https://github.com/stanford-futuredata/momentsketch) library. 
The momentsketch provides coarse quantile estimates with less space and aggregation time overheads than traditional sketches, approaching the performance of counts and sums by reconstructing distributions from computed statistics.

To use this aggregator, make sure you [include](../../operations/including-extensions.html) the extension in your config file:

```
druid.extensions.loadList=["druid-momentsketch"]
```

### Aggregator

The result of the aggregation is a momentsketch that is the union of all sketches either built from raw data or read from the segments.

The `momentSketch` aggregator operates over raw data while the `momentSketchMerge` aggregator should be used when aggregating pre-computed sketches.
```json
{
  "type" : <aggregator_type>,
  "name" : <output_name>,
  "fieldName" : <input_name>,
  "k" : <int>,
  "compress" : <boolean>
 }
```

|property|description|required?|
|--------|-----------|---------|
|type|Type of aggregator desired. Either "momentSketch" or "momentSketchMerge" |yes|
|name|A String for the output (result) name of the calculation.|yes|
|fieldName|A String for the name of the input field (can contain sketches or raw numeric values).|yes|
|k|Parameter that determines the accuracy and size of the sketch. Higher k means higher accuracy but more space to store sketches. Usable range is generally [3,15] |no, defaults to 13.|
|compress|Flag for whether the aggregator compresses numeric values using arcsinh. Can improve robustness to skewed and long-tailed distributions, but reduces accuracy slightly on more uniform distributions.| no, defaults to true

### Post Aggregators

Users can query for a set of quantiles using the `momentSketchSolveQuantiles` post-aggregator on the sketches created by the `momentSketch` or `momentSketchMerge` aggregators.
```json
{
  "type"  : "momentSketchSolveQuantiles",
  "name" : <output_name>,
  "field" : <reference to moment sketch>,
  "fractions" : <array of doubles in [0,1]>
}
```

Users can also query for the min/max of a distribution:
```json
{
  "type" : "momentSketchMin" | "momentSketchMax",
  "name" : <output_name>,
  "field" : <reference to moment sketch>,
}
```

### Example
As an example of a query with sketches pre-aggregated at ingestion time, one could set up the following aggregator at ingest:
```json
{
  "type": "momentSketch", 
  "name": "sketch", 
  "fieldName": "value", 
  "k": 10, 
  "compress": true,
}
```
and make queries using the following aggregator + post-aggregator:
```json
{
  "aggregations": [{
    "type": "momentSketchMerge",
    "name": "sketch",
    "fieldName": "sketch",
    "k": 10,
    "compress": true
  }],
  "postAggregations": [
  {
    "type": "momentSketchSolveQuantiles",
    "name": "quantiles",
    "fractions": [0.1, 0.5, 0.9],
    "field": {
      "type": "fieldAccess",
      "fieldName": "sketch"
    }
  },
  {
    "type": "momentSketchMin",
    "name": "min",
    "field": {
      "type": "fieldAccess",
      "fieldName": "sketch"
    }
  }]
}
```