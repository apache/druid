---
layout: doc_page
---

### Approximate Histogram aggregator

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

### Creating approxiate histogram sketches at ingestion time

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


### Approximate Histogram post-aggregators

Post-aggregators are used to transform opaque approximate histogram sketches
into bucketed histogram representations, as well as to compute various
distribution metrics such as quantiles, min, and max.

#### Equal buckets post-aggregator

Computes a visual representation of the approximate histogram with a given number of equal-sized bins.
Bucket intervals are based on the range of the underlying data.

```json
{
  "type": "equalBuckets",
  "name": "<output_name>",
  "fieldName": "<aggregator_name>",
  "numBuckets": <count>
}
```

#### Buckets post-aggregator

Computes a visual representation given an initial breakpoint, offset, and a bucket size.

Bucket size determines the width of the binning interval.

Offset determines the value on which those interval bins align.

```json
{
  "type": "buckets",
  "name": "<output_name>",
  "fieldName": "<aggregator_name>",
  "bucketSize": <bucket_size>,
  "offset": <offset>
}
```

#### Custom buckets post-aggregator

Computes a visual representation of the approximate histogram with bins laid out according to the given breaks.

```json
{ "type" : "customBuckets", "name" : <output_name>, "fieldName" : <aggregator_name>,
  "breaks" : [ <value>, <value>, ... ] }
```

#### min post-aggregator

Returns the minimum value of the underlying approximate histogram aggregator

```json
{ "type" : "min", "name" : <output_name>, "fieldName" : <aggregator_name> }
```

#### max post-aggregator

Returns the maximum value of the underlying approximate histogram aggregator

```json
{ "type" : "max", "name" : <output_name>, "fieldName" : <aggregator_name> }
```

#### quantile post-aggregator

Computes a single quantile based on the underlying approximate histogram aggregator

```json
{ "type" : "quantile", "name" : <output_name>, "fieldName" : <aggregator_name>,
  "probability" : <quantile> }
```

#### quantiles post-aggregator

Computes an array of quantiles based on the underlying approximate histogram aggregator

```json
{ "type" : "quantiles", "name" : <output_name>, "fieldName" : <aggregator_name>,
  "probabilities" : [ <quantile>, <quantile>, ... ] }
```
