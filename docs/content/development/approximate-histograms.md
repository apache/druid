---
layout: doc_page
---
### ApproxHistogram aggregator

This aggregator is based on [http://jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf](http://jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf) to compute approximate histograms.

To use this feature, an "approxHistogram" aggregator must be included at indexing time. The ingestion aggregator can only apply to numeric values. To query for results, an "approxHistogramFold" aggregator must be included in the query.

```json
{
  "type" : "approxHistogram(ingestion), approxHistogramFold(query)",
  "name" : <output_name>,
  "fieldName" : <metric_name>,
  "resolution" : <integer>,
  "numBuckets" : <integer>,
  "lowerLimit" : <float>,
  "upperLimit" : <float>
}
```

|Property|Description|Default|
|--------|-----------|-------|
|`resolution`|Number of centroids (data points) to store. The higher the resolution, the more accurate results are, but the slower computation will be.|50|
|`numBuckets`|Number of output buckets for the resulting histogram.|7|
|`lowerLimit`/`upperLimit`|Restrict the approximation to the given range. The values outside this range will be aggregated into two centroids. Counts of values outside this range are still maintained. |-INF/+INF|


### Approximate Histogram post-aggregators

Post-aggregators used to transform opaque approximate histogram objects
into actual histogram representations, and to compute various distribution metrics.

#### equal buckets post-aggregator

Computes a visual representation of the approximate histogram with a given number of equal-sized bins

```json
{ "type" : "equalBuckets", "name" : <output_name>, "fieldName" : <aggregator_name>,
  "numBuckets" : <count> }
```

#### buckets post-aggregator

Computes a visual representation given an initial breakpoint, offset, and a bucket size.

```json
{ "type" : "buckets", "name" : <output_name>, "fieldName" : <aggregator_name>,
  "bucketSize" : <bucket_size>, "offset" : <offset> }
```

#### custom buckets post-aggregator

Computes a visual representation of the approximate histogram with bins laid out according to the given breaks

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