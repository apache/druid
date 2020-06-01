---
id: approximate-histograms
title: "Approximate Histogram aggregators"
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


To use this Apache Druid extension, make sure to [include](../../development/extensions.md#loading-extensions) `druid-histogram` as an extension.

The `druid-histogram` extension provides an approximate histogram aggregator and a fixed buckets histogram aggregator.

<a name="approximate-histogram-aggregator"></a>

## Approximate Histogram aggregator (Deprecated)

> The Approximate Histogram aggregator is deprecated. Please use [DataSketches Quantiles](../extensions-core/datasketches-quantiles.md) instead which provides a superior distribution-independent algorithm with formal error guarantees.

This aggregator is based on
[http://jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf](http://jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf)
to compute approximate histograms, with the following modifications:

- some tradeoffs in accuracy were made in the interest of speed (see below)
- the sketch maintains the exact original data as long as the number of
  distinct data points is fewer than the resolutions (number of centroids),
  increasing accuracy when there are few data points, or when dealing with
  discrete data points. You can find some of the details in [this post](https://metamarkets.com/2013/histograms/).

Approximate histogram sketches are still experimental for a reason, and you
should understand the limitations of the current implementation before using
them. The approximation is heavily data-dependent, which makes it difficult to
give good general guidelines, so you should experiment and see what parameters
work well for your data.

Here are a few things to note before using them:

- As indicated in the original paper, there are no formal error bounds on the
  approximation. In practice, the approximation gets worse if the distribution
  is skewed.
- The algorithm is order-dependent, so results can vary for the same query, due
  to variations in the order in which results are merged.
- In general, the algorithm only works well if the data that comes is randomly
  distributed (i.e. if data points end up sorted in a column, approximation
  will be horrible)
- We traded accuracy for aggregation speed, taking some shortcuts when adding
  histograms together, which can lead to pathological cases if your data is
  ordered in some way, or if your distribution has long tails. It should be
  cheaper to increase the resolution of the sketch to get the accuracy you need.

That being said, those sketches can be useful to get a first order approximation
when averages are not good enough. Assuming most rows in your segment store
fewer data points than the resolution of histogram, you should be able to use
them for monitoring purposes and detect meaningful variations with a few
hundred centroids. To get good accuracy readings on 95th percentiles with
millions of rows of data, you may want to use several thousand centroids,
especially with long tails, since that's where the approximation will be worse.

### Creating approximate histogram sketches at ingestion time

To use this feature, an "approxHistogram" or "approxHistogramFold" aggregator must be included at
indexing time. The ingestion aggregator can only apply to numeric values. If you use "approxHistogram"
then any input rows missing the value will be considered to have a value of 0, while with "approxHistogramFold"
such rows will be ignored.

To query for results, an "approxHistogramFold" aggregator must be included in the
query.

```json
{
  "type" : "approxHistogram or approxHistogramFold (at ingestion time), approxHistogramFold (at query time)",
  "name" : <output_name>,
  "fieldName" : <metric_name>,
  "resolution" : <integer>,
  "numBuckets" : <integer>,
  "lowerLimit" : <float>,
  "upperLimit" : <float>
}
```

|Property                 |Description                   |Default                           |
|-------------------------|------------------------------|----------------------------------|
|`resolution`             |Number of centroids (data points) to store. The higher the resolution, the more accurate results are, but the slower the computation will be.|50|
|`numBuckets`             |Number of output buckets for the resulting histogram. Bucket intervals are dynamic, based on the range of the underlying data. Use a post-aggregator to have finer control over the bucketing scheme|7|
|`lowerLimit`/`upperLimit`|Restrict the approximation to the given range. The values outside this range will be aggregated into two centroids. Counts of values outside this range are still maintained. |-INF/+INF|
|`finalizeAsBase64Binary` |If true, the finalized aggregator value will be a Base64-encoded byte array containing the serialized form of the histogram. If false, the finalized aggregator value will be a JSON representation of the histogram.|false|

## Fixed Buckets Histogram

The fixed buckets histogram aggregator builds a histogram on a numeric column, with evenly-sized buckets across a specified value range. Values outside of the range are handled based on a user-specified outlier handling mode.

This histogram supports the min/max/quantiles post-aggregators but does not support the bucketing post-aggregators.

### When to use

The accuracy/usefulness of the fixed buckets histogram is extremely data-dependent; it is provided to support special use cases where the user has a great deal of prior information about the data being aggregated and knows that a fixed buckets implementation is suitable.

For general histogram and quantile use cases, the [DataSketches Quantiles Sketch](../extensions-core/datasketches-quantiles.md) extension is recommended.

### Properties


|Property                 |Description                   |Default                           |
|-------------------------|------------------------------|----------------------------------|
|`type`|Type of the aggregator. Must `fixedBucketsHistogram`.|No default, must be specified|
|`name`|Column name for the aggregator.|No default, must be specified|
|`fieldName`|Column name of the input to the aggregator.|No default, must be specified|
|`lowerLimit`|Lower limit of the histogram. |No default, must be specified|
|`upperLimit`|Upper limit of the histogram. |No default, must be specified|
|`numBuckets`|Number of buckets for the histogram. The range [lowerLimit, upperLimit] will be divided into `numBuckets` intervals of equal size.|10|
|`outlierHandlingMode`|Specifies how values outside of [lowerLimit, upperLimit] will be handled. Supported modes are "ignore", "overflow", and "clip". See [outlier handling modes](#outlier-handling-modes) for more details.|No default, must be specified|
|`finalizeAsBase64Binary`|If true, the finalized aggregator value will be a Base64-encoded byte array containing the [serialized form](#serialization-formats) of the histogram. If false, the finalized aggregator value will be a JSON representation of the histogram.|false|

An example aggregator spec is shown below:

```json
{
  "type" : "fixedBucketsHistogram",
  "name" : <output_name>,
  "fieldName" : <metric_name>,
  "numBuckets" : <integer>,
  "lowerLimit" : <double>,
  "upperLimit" : <double>,
  "outlierHandlingMode": <mode>
}
```

### Outlier handling modes

The outlier handling mode specifies what should be done with values outside of the histogram's range. There are three supported modes:

- `ignore`: Throw away outlier values.
- `overflow`: A count of outlier values will be tracked by the histogram, available in the `lowerOutlierCount` and `upperOutlierCount` fields.
- `clip`: Outlier values will be clipped to the `lowerLimit` or the `upperLimit` and included in the histogram.

If you don't care about outliers, `ignore` is the cheapest option performance-wise. There is currently no difference in storage size among the modes.

### Output fields

The histogram aggregator's output object has the following fields:

- `lowerLimit`: Lower limit of the histogram
- `upperLimit`: Upper limit of the histogram
- `numBuckets`: Number of histogram buckets
- `outlierHandlingMode`: Outlier handling mode
- `count`: Total number of values contained in the histogram, excluding outliers
- `lowerOutlierCount`: Count of outlier values below `lowerLimit`. Only used if the outlier mode is `overflow`.
- `upperOutlierCount`: Count of outlier values above `upperLimit`. Only used if the outlier mode is `overflow`.
- `missingValueCount`: Count of null values seen by the histogram.
- `max`: Max value seen by the histogram. This does not include outlier values.
- `min`: Min value seen by the histogram. This does not include outlier values.
- `histogram`: An array of longs with size `numBuckets`, containing the bucket counts

### Ingesting existing histograms

It is also possible to ingest existing fixed buckets histograms. The input must be a Base64 string encoding a byte array that contains a serialized histogram object. Both "full" and "sparse" formats can be used. Please see [Serialization formats](#serialization-formats) below for details.

### Serialization formats

#### Full serialization format

This format includes the full histogram bucket count array in the serialization format.

```
byte: serialization version, must be 0x01
byte: encoding mode, 0x01 for full
double: lowerLimit
double: upperLimit
int: numBuckets
byte: outlier handling mode (0x00 for `ignore`, 0x01 for `overflow`, and 0x02 for `clip`)
long: count, total number of values contained in the histogram, excluding outliers
long: lowerOutlierCount
long: upperOutlierCount
long: missingValueCount
double: max
double: min
array of longs: bucket counts for the histogram
```

#### Sparse serialization format

This format represents the histogram bucket counts as (bucketNum, count) pairs. This serialization format is used when less than half of the histogram's buckets have values.

```
byte: serialization version, must be 0x01
byte: encoding mode, 0x02 for sparse
double: lowerLimit
double: upperLimit
int: numBuckets
byte: outlier handling mode (0x00 for `ignore`, 0x01 for `overflow`, and 0x02 for `clip`)
long: count, total number of values contained in the histogram, excluding outliers
long: lowerOutlierCount
long: upperOutlierCount
long: missingValueCount
double: max
double: min
int: number of following (bucketNum, count) pairs
sequence of (int, long) pairs:
  int: bucket number
  count: bucket count
```

### Combining histograms with different bucketing schemes

It is possible to combine two histograms with different bucketing schemes (lowerLimit, upperLimit, numBuckets) together.

The bucketing scheme of the "left hand" histogram will be preserved (i.e., when running a query, the bucketing schemes specified in the query's histogram aggregators will be preserved).

When merging, we assume that values are evenly distributed within the buckets of the "right hand" histogram.

When the right-hand histogram contains outliers (when using `overflow` mode), we assume that all of the outliers counted in the right-hand histogram will be outliers in the left-hand histogram as well.

For performance and accuracy reasons, we recommend avoiding aggregation of histograms with different bucketing schemes if possible.

### Null handling

If `druid.generic.useDefaultValueForNull` is false, null values will be tracked in the `missingValueCount` field of the histogram.

If `druid.generic.useDefaultValueForNull` is true, null values will be added to the histogram as the default 0.0 value.

## Histogram post-aggregators

Post-aggregators are used to transform opaque approximate histogram sketches
into bucketed histogram representations, as well as to compute various
distribution metrics such as quantiles, min, and max.

### Equal buckets post-aggregator

Computes a visual representation of the approximate histogram with a given number of equal-sized bins.
Bucket intervals are based on the range of the underlying data. This aggregator is not supported for the fixed buckets histogram.

```json
{
  "type": "equalBuckets",
  "name": "<output_name>",
  "fieldName": "<aggregator_name>",
  "numBuckets": <count>
}
```

### Buckets post-aggregator

Computes a visual representation given an initial breakpoint, offset, and a bucket size.

Bucket size determines the width of the binning interval.

Offset determines the value on which those interval bins align.

This aggregator is not supported for the fixed buckets histogram.

```json
{
  "type": "buckets",
  "name": "<output_name>",
  "fieldName": "<aggregator_name>",
  "bucketSize": <bucket_size>,
  "offset": <offset>
}
```

### Custom buckets post-aggregator

Computes a visual representation of the approximate histogram with bins laid out according to the given breaks.

This aggregator is not supported for the fixed buckets histogram.

```json
{ "type" : "customBuckets", "name" : <output_name>, "fieldName" : <aggregator_name>,
  "breaks" : [ <value>, <value>, ... ] }
```

### min post-aggregator

Returns the minimum value of the underlying approximate or fixed buckets histogram aggregator

```json
{ "type" : "min", "name" : <output_name>, "fieldName" : <aggregator_name> }
```

### max post-aggregator

Returns the maximum value of the underlying approximate or fixed buckets histogram aggregator

```json
{ "type" : "max", "name" : <output_name>, "fieldName" : <aggregator_name> }
```

#### quantile post-aggregator

Computes a single quantile based on the underlying approximate or fixed buckets histogram aggregator

```json
{ "type" : "quantile", "name" : <output_name>, "fieldName" : <aggregator_name>,
  "probability" : <quantile> }
```

#### quantiles post-aggregator

Computes an array of quantiles based on the underlying approximate or fixed buckets histogram aggregator

```json
{ "type" : "quantiles", "name" : <output_name>, "fieldName" : <aggregator_name>,
  "probabilities" : [ <quantile>, <quantile>, ... ] }
```
