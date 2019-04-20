---
layout: doc_page
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

# Aggregations

Aggregations can be provided at ingestion time as part of the ingestion spec as a way of summarizing data before it enters Apache Druid (incubating). 
Aggregations can also be specified as part of many queries at query time.

Available aggregations are:

### Count aggregator

`count` computes the count of Druid rows that match the filters.

```json
{ "type" : "count", "name" : <output_name> }
```

Please note the count aggregator counts the number of Druid rows, which does not always reflect the number of raw events ingested. 
This is because Druid can be configured to roll up data at ingestion time. To 
count the number of ingested rows of data, include a count aggregator at ingestion time, and a longSum aggregator at 
query time.

### Sum aggregators

#### `longSum` aggregator

computes the sum of values as a 64-bit, signed integer

```json
{ "type" : "longSum", "name" : <output_name>, "fieldName" : <metric_name> }
```

`name` – output name for the summed value
`fieldName` – name of the metric column to sum over

#### `doubleSum` aggregator

Computes and stores the sum of values as 64-bit floating point value. Similar to `longSum`

```json
{ "type" : "doubleSum", "name" : <output_name>, "fieldName" : <metric_name> }
```

#### `floatSum` aggregator

Computes and stores the sum of values as 32-bit floating point value. Similar to `longSum` and `doubleSum`

```json
{ "type" : "floatSum", "name" : <output_name>, "fieldName" : <metric_name> }
```

### Min / Max aggregators

#### `doubleMin` aggregator

`doubleMin` computes the minimum of all metric values and Double.POSITIVE_INFINITY

```json
{ "type" : "doubleMin", "name" : <output_name>, "fieldName" : <metric_name> }
```

#### `doubleMax` aggregator

`doubleMax` computes the maximum of all metric values and Double.NEGATIVE_INFINITY

```json
{ "type" : "doubleMax", "name" : <output_name>, "fieldName" : <metric_name> }
```

#### `floatMin` aggregator

`floatMin` computes the minimum of all metric values and Float.POSITIVE_INFINITY

```json
{ "type" : "floatMin", "name" : <output_name>, "fieldName" : <metric_name> }
```

#### `floatMax` aggregator

`floatMax` computes the maximum of all metric values and Float.NEGATIVE_INFINITY

```json
{ "type" : "floatMax", "name" : <output_name>, "fieldName" : <metric_name> }
```

#### `longMin` aggregator

`longMin` computes the minimum of all metric values and Long.MAX_VALUE

```json
{ "type" : "longMin", "name" : <output_name>, "fieldName" : <metric_name> }
```

#### `longMax` aggregator

`longMax` computes the maximum of all metric values and Long.MIN_VALUE

```json
{ "type" : "longMax", "name" : <output_name>, "fieldName" : <metric_name> }
```

### First / Last aggregator

(Double/Float/Long) First and Last aggregator cannot be used in ingestion spec, and should only be specified as part of queries.

Note that queries with first/last aggregators on a segment created with rollup enabled will return the rolled up value, and not the last value within the raw ingested data.

#### `doubleFirst` aggregator

`doubleFirst` computes the metric value with the minimum timestamp or 0 if no row exist

```json
{
  "type" : "doubleFirst",
  "name" : <output_name>,
  "fieldName" : <metric_name>
}
```

#### `doubleLast` aggregator

`doubleLast` computes the metric value with the maximum timestamp or 0 if no row exist

```json
{
  "type" : "doubleLast",
  "name" : <output_name>,
  "fieldName" : <metric_name>
}
```

#### `floatFirst` aggregator

`floatFirst` computes the metric value with the minimum timestamp or 0 if no row exist

```json
{
  "type" : "floatFirst",
  "name" : <output_name>,
  "fieldName" : <metric_name>
}
```

#### `floatLast` aggregator

`floatLast` computes the metric value with the maximum timestamp or 0 if no row exist

```json
{
  "type" : "floatLast",
  "name" : <output_name>,
  "fieldName" : <metric_name>
}
```

#### `longFirst` aggregator

`longFirst` computes the metric value with the minimum timestamp or 0 if no row exist

```json
{
  "type" : "longFirst",
  "name" : <output_name>,
  "fieldName" : <metric_name>
}
```

#### `longLast` aggregator

`longLast` computes the metric value with the maximum timestamp or 0 if no row exist

```json
{ 
  "type" : "longLast",
  "name" : <output_name>, 
  "fieldName" : <metric_name>,
}
```

#### `stringFirst` aggregator

`stringFirst` computes the metric value with the minimum timestamp or `null` if no row exist

```json
{
  "type" : "stringFirst",
  "name" : <output_name>,
  "fieldName" : <metric_name>,
  "maxStringBytes" : <integer> # (optional, defaults to 1024),
  "filterNullValues" : <boolean> # (optional, defaults to false)
}
```



#### `stringLast` aggregator

`stringLast` computes the metric value with the maximum timestamp or `null` if no row exist

```json
{
  "type" : "stringLast",
  "name" : <output_name>,
  "fieldName" : <metric_name>,
  "maxStringBytes" : <integer> # (optional, defaults to 1024),
  "filterNullValues" : <boolean> # (optional, defaults to false)
}
```

### JavaScript aggregator

Computes an arbitrary JavaScript function over a set of columns (both metrics and dimensions are allowed). Your
JavaScript functions are expected to return floating-point values.

```json
{ "type": "javascript",
  "name": "<output_name>",
  "fieldNames"  : [ <column1>, <column2>, ... ],
  "fnAggregate" : "function(current, column1, column2, ...) {
                     <updates partial aggregate (current) based on the current row values>
                     return <updated partial aggregate>
                   }",
  "fnCombine"   : "function(partialA, partialB) { return <combined partial results>; }",
  "fnReset"     : "function()                   { return <initial value>; }"
}
```

**Example**

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

<div class="note info">
JavaScript-based functionality is disabled by default. Please refer to the Druid <a href="../development/javascript.html">JavaScript programming guide</a> for guidelines about using Druid's JavaScript functionality, including instructions on how to enable it.
</div>

<a name="approx" />
## Approximate Aggregations

### Count distinct

#### DataSketches Theta Sketch

The [DataSketches Theta Sketch](../development/extensions-core/datasketches-theta.html) extension-provided aggregator gives distinct count estimates with support for set union, intersection, and difference post-aggregators, using Theta sketches from the [datasketches](http://datasketches.github.io/) library.

#### DataSketches HLL Sketch

The [DataSketches HLL Sketch](../development/extensions-core/datasketches-hll.html) extension-provided aggregator gives distinct count estimates using the HyperLogLog algorithm.

Compared to the Theta sketch, the HLL sketch does not support set operations and has slightly slower update and merge speed, but requires significantly less space.

#### Cardinality/HyperUnique (Deprecated)

<div class="note caution">
The Cardinality and HyperUnique aggregators are deprecated. Please use <a href="../development/extensions-core/datasketches-theta.html">DataSketches Theta Sketch</a> or <a href="../development/extensions-core/datasketches-hll.html">DataSketches HLL Sketch</a> instead.
</div>

The [Cardinality and HyperUnique](../querying/hll-old.html) aggregators are older aggregator implementations available by default in Druid that also provide distinct count estimates using the HyperLogLog algorithm. The newer DataSketches Theta and HLL extension-provided aggregators described above have superior accuracy and performance and are recommended instead. 

The DataSketches team has published a [comparison study](https://datasketches.github.io/docs/HLL/HllSketchVsDruidHyperLogLogCollector.html) between Druid's original HLL algorithm and the DataSketches HLL algorithm. Based on the demonstrated advantages of the DataSketches implementation, we have deprecated Druid's original HLL aggregator.

Please note that `hyperUnique` aggregators are not mutually compatible with Datasketches HLL or Theta sketches.

##### Multi-column handling

Note the DataSketches Theta and HLL aggregators currently only support single-column inputs. If you were previously using the Cardinality aggregator with multiple-column inputs, equivalent operations using Theta or HLL sketches are described below:

* Multi-column `byValue` Cardinality can be replaced with a union of Theta sketches on the individual input columns
* Multi-column `byRow` Cardinality can be replaced with a Theta or HLL sketch on a single [virtual column]((../querying/virtual-columns.html) that combines the individual input columns.

### Histograms and quantiles

#### DataSketches Quantiles Sketch

The [DataSketches Quantiles Sketch](../development/extensions-core/datasketches-quantiles.html) extension-provided aggregator provides quantile estimates and histogram approximations using the numeric quantiles DoublesSketch from the [datasketches](http://datasketches.github.io/) library.

We recommend this aggregator in general for quantiles/histogram use cases, as it provides formal error bounds and has distribution-independent accuracy.

#### Fixed Buckets Histogram

Druid also provides a [simple histogram implementation]((../development/extensions-core/approxiate-histograms.html#fixed-buckets-histogram) that uses a fixed range and fixed number of buckets with support for quantile estimation, backed by an array of bucket count values.

The fixed buckets histogram can perform well when the distribution of the input data allows a small number of buckets to be used.

We do not recommend the fixed buckets histogram for general use, as its usefulness is extremely data dependent. However, it is made available for users that have already identified use cases where a fixed buckets histogram is suitable.

#### Approximate Histogram (Deprecated)

The [Approximate Histogram](../development/extensions-core/approximate-histograms.html) extension-provided aggregator also provides quantile estimates and histogram approximations, based on [http://jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf](http://jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf).

The algorithm used by this deprecated aggregator is highly distribution-dependent and its output is subject to serious distortions when the input does not fit within the algorithm's limitations.

A [study published by the DataSketches team](https://datasketches.github.io/docs/Quantiles/DruidApproxHistogramStudy.html) demonstrates some of the known failure modes of this algorithm:
- The algorithm's quantile calculations can fail to provide results for a large range of rank values (all ranks less than 0.89 in the example used in the study), returning all zeroes instead.
- The algorithm can completely fail to record spikes in the tail ends of the distribution
- In general, the histogram produced by the algorithm can deviate significantly from the true histogram, with no bounds on the errors.

It is not possible to determine a priori how well this aggregator will behave for a given input stream, nor does the aggregator provide any indication that serious distortions are present in the output.

For these reasons, we have deprecated this aggregator and do not recommend its use.

## Miscellaneous Aggregations

### Filtered Aggregator

A filtered aggregator wraps any given aggregator, but only aggregates the values for which the given dimension filter matches.

This makes it possible to compute the results of a filtered and an unfiltered aggregation simultaneously, without having to issue multiple queries, and use both results as part of post-aggregations.

*Note:* If only the filtered results are required, consider putting the filter on the query itself, which will be much faster since it does not require scanning all the data.

```json
{
  "type" : "filtered",
  "filter" : {
    "type" : "selector",
    "dimension" : <dimension>,
    "value" : <dimension value>
  }
  "aggregator" : <aggregation>
}
```
