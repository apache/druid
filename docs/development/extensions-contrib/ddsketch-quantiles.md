---
id: ddsketch-quantiles
title: "DDSketches for Approximate Quantiles module"
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


This module provides aggregators for approximate quantile queries using the [DDSketch](https://github.com/datadog/sketches-java) library. The DDSketch library provides a fast, and fully-mergeable quantile sketch with relative error. If the true quantile is 100, a sketch with relative error of 1% guarantees a quantile value between 101 and 99. This is important and highly valuable behavior for long tail distributions. The best use case for these sketches is for accurately describing the upper quantiles of long tailed distributions such as network latencies.

To use this Apache Druid extension, [include](../../configuration/extensions.md#loading-extensions) in the extensions load list.

```
druid.extensions.loadList=["druid-ddsketch", ...]
```

### Aggregator

The result of the aggregation is a DDSketch that is the union of all sketches either built from raw data or read from the segments. The single number that is returned represents the total number of included data points. The default aggregator type of `ddSketch` uses the collapsingLowestDense strategy for storing and merging sketch. This means that in favor of keeping the highest values represented at the highest accuracy, the sketch will collapse and merge lower, smaller values in the sketch. Collapsed bins will lose accuracy guarantees. The default number of bins is 1000. Sketches can only be merged when using the same relativeError values.

The `ddSketch` aggregator operates over raw data and precomputed sketches.

```json
{
  "type" : "ddSketch",
  "name" : <output_name>,
  "fieldName" : <input_name>,
  "relativeError" : <double(0, 1)>,
  "numBins": <int>
 }
```

|property|description|required?|
|--------|-----------|---------|
|type|Must be "ddSketch" |yes|
|name|A String for the output (result) name of the calculation.|yes|
|fieldName|A String for the name of the input field (can contain sketches or raw numeric values).|yes|
|relativeError|Describes the precision in which to store the sketch. Must be a number between 0 and 1.|no, defaults to 0.01 (1% error)|
|numBins|Total number of bins the sketch is allowed to use to describe the distribution. This has a direct impact on max memory used. The more total bins available, the larger the range of accurate quantiles. With relative accuracy of 2%, only 275 bins are required to cover values between 1 millisecond and 1 minute. 800 bins are required to cover values between 1 nanosecond and 1 day.|no, defaults to 1000|


### Post Aggregators

To compute approximate quantiles, use `quantilesFromDDSketch` to query for a set of quantiles or `quantileFromDDSketch` to query for a single quantile. Call these post-aggregators on the sketches created by the `ddSketch` aggregators.


#### quantilesFromDDSketch

Use `quantilesFromDDSketch` to fetch multiple quantiles.

```json
{
  "type"  : "quantilesFromDDSketch",
  "name" : <output_name>,
  "field" : <reference to DDSketch>,
  "fractions" : <array of doubles in [0,1]>
}
```

|property|description|required?|
|--------|-----------|---------|
|type|Must be "quantilesFromDDSketch" |yes|
|name|A String for the output (result) name of the calculation.|yes|
|field|A computed ddSketch.|yes|
|fractions|Array of doubles from 0 to 1 of the quantiles to compute|yes|

#### quantileFromDDSketch

Use `quantileFromDDSketch` to fetch a single quantile.

```json
{
  "type"  : "quantileFromDDSketch",
  "name" : <output_name>,
  "field" : <reference to DDsketch>,
  "fraction" : <double [0,1]>
}
```

|property|description|required?|
|--------|-----------|---------|
|type|Must be "quantileFromDDSketch" |yes|
|name|A String for the output (result) name of the calculation.|yes|
|field|A computed ddSketch.|yes|
|fraction|A double from 0 to 1 of the quantile to compute|yes|


### Example

As an example of a query with sketches pre-aggregated at ingestion time, one could set up the following aggregator at ingest:

```json
{
  "type": "ddSketch",
  "name": "sketch",
  "fieldName": "value",
  "relativeError": 0.01,
  "numBins": 1000,
}
```

Compute quantiles from the pre-aggregated sketches using the following aggregator and post-aggregator.

```json
{
  "aggregations": [{
    "type": "ddSketch",
    "name": "sketch",
    "fieldName": "sketch",
  }],
  "postAggregations": [
  {
    "type": "quantilesFromDDSketch",
    "name": "quantiles",
    "fractions": [0.5, 0.75, 0.9, 0.99],
    "field": {
      "type": "fieldAccess",
      "fieldName": "sketch"
    }
  }]
}
```
