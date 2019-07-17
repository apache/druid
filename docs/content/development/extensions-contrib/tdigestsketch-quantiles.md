---
layout: doc_page
title: "T-Digest Quantiles Sketch module"
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

# T-Digest Quantiles Sketch module

This module provides Apache Druid (incubating) approximate sketch aggregators based on T-Digest.
T-Digest (https://github.com/tdunning/t-digest) is a popular datastructure for accurate on-line accumulation of
rank-based statistics such as quantiles and trimmed means.
The datastructure is also designed for parallel programming use cases like distributed aggregations or map reduce jobs by making combining two intermediate t-digests easy and efficient.

There are two flavors of T-Digest sketch aggregator available in Apache Druid (incubating):

1. buildTDigestSketch - It generally makes sense to use this aggregator when ingesting raw data into Druid to
generate pre-agrgegated sketches. One can also use this aggregator during query time too to combine ingested T-Digest
sketches.
2. quantilesFromTDigestSketch - used for generating quantiles from T-Digest sketches. This aggregator is generally used
during query time to generate quantiles from sketches built using buildTDigestSketch.

To use this aggregator, make sure you [include](../../operations/including-extensions.html) the extension in your config file:

```
druid.extensions.loadList=["druid-tdigestsketch"]
```

### Aggregator

The result of the aggregation is a T-Digest sketch that is built ingesting numeric values from the raw data or from
combining pre-generated T-Digest sketches.

```json
{
  "type" : "buildTDigestSketch",
  "name" : <output_name>,
  "fieldName" : <metric_name>,
  "compression": <parameter that controls size and accuracy>
 }
```

Example:

```json
{
	"type": "buildTDigestSketch",
	"name": "sketch",
	"fieldName": "session_duration",
	"compression": 200
}
```

```json
{
	"type": "buildTDigestSketch",
	"name": "combined_sketch",
	"fieldName": "ingested_sketch",
	"compression": 200
}
```

|property|description|required?|
|--------|-----------|---------|
|type|This String should always be "buildTDigestSketch"|yes|
|name|A String for the output (result) name of the calculation.|yes|
|fieldName|A String for the name of the input field containing raw numeric values or pre-generated T-Digest sketches.|yes|
|compression|Parameter that determines the accuracy and size of the sketch. Higher compression means higher accuracy but more space to store sketches.|no, defaults to 100|


### Post Aggregators

#### Quantiles

This returns an array of quantiles corresponding to a given array of fractions.

```json
{
  "type"  : "quantilesFromTDigestSketch",
  "name": <output name>,
  "field"  : <post aggregator that refers to a TDigestSketch (fieldAccess or another post aggregator)>,
  "fractions" : <array of fractions>
}
```

|property|description|required?|
|--------|-----------|---------|
|type|This String should always be "quantilesFromTDigestSketch"|yes|
|name|A String for the output (result) name of the calculation.|yes|
|field|A field reference pointing to the field aggregated/combined T-Digest sketch.|yes|
|fractions|Non-empty array of fractions between 0 and 1|yes|

Example:

```json
{
	"queryType": "groupBy",
	"dataSource": "test_datasource",
	"granularity": "ALL",
	"dimensions": [],
	"aggregations": [{
		"type": "buildTDigestSketch",
		"name": "merged_sketch",
		"fieldName": "ingested_sketch",
		"compression": 200
	}],
	"postAggregations": [{
		"type": "quantilesFromTDigestSketch",
		"name": "quantiles",
		"fractions": [0, 0.5, 1],
		"field": {
			"type": "fieldAccess",
			"fieldName": "merged_sketch"
		}
	}],
	"intervals": ["2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z"]
}
```

Similar to quantilesFromTDigestSketch except it takes in a single fraction for computing quantile.

```json
{
  "type"  : "quantileFromTDigestSketch",
  "name": <output name>,
  "field"  : <post aggregator that refers to a TDigestSketch (fieldAccess or another post aggregator)>,
  "fraction" : <value>
}
```

|property|description|required?|
|--------|-----------|---------|
|type|This String should always be "quantilesFromTDigestSketch"|yes|
|name|A String for the output (result) name of the calculation.|yes|
|field|A field reference pointing to the field aggregated/combined T-Digest sketch.|yes|
|fraction|Decimal value between 0 and 1|yes|
