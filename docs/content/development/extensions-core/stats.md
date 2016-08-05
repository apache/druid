---
layout: doc_page
---

# Stats aggregator

Includes stat-related aggregators, including variance and standard deviations, etc. Make sure to [include](../../operations/including-extensions.html) `druid-stats` as an extension.

## Variance aggregator

Algorithm of the aggregator is the same with that of apache hive. This is the description in GenericUDAFVariance in hive.

Evaluate the variance using the algorithm described by Chan, Golub, and LeVeque in
"Algorithms for computing the sample variance: analysis and recommendations"
The American Statistician, 37 (1983) pp. 242--247.

variance = variance1 + variance2 + n/(m*(m+n)) * pow(((m/n)*t1 - t2),2)

where: - variance is sum[x-avg^2] (this is actually n times the variance)
and is updated at every step. - n is the count of elements in chunk1 - m is
the count of elements in chunk2 - t1 = sum of elements in chunk1, t2 =
sum of elements in chunk2.

This algorithm was proven to be numerically stable by J.L. Barlow in
"Error analysis of a pairwise summation algorithm to compute sample variance"
Numer. Math, 58 (1991) pp. 583--590

### Pre-aggregating variance at ingestion time

To use this feature, an "variance" aggregator must be included at indexing time.
The ingestion aggregator can only apply to numeric values. If you use "variance"
then any input rows missing the value will be considered to have a value of 0.

User can specify expected input type as one of "float", "long", "variance" for ingestion, which is by default "float".

```json
{
  "type" : "variance",
  "name" : <output_name>,
  "fieldName" : <metric_name>,
  "inputType" : <input_type>,
  "estimator" : <string>
}
```

To query for results, "variance" aggregator with "variance" input type or simply a "varianceFold" aggregator must be included in the query.

```json
{
  "type" : "varianceFold",
  "name" : <output_name>,
  "fieldName" : <metric_name>,
  "estimator" : <string>
}
```

|Property                 |Description                   |Default                           |
|-------------------------|------------------------------|----------------------------------|
|`estimator`|Set "population" to get variance_pop rather than variance_sample, which is default.|null|


### Standard Deviation post-aggregator

To acquire standard deviation from variance, user can use "stddev" post aggregator.

```json
{
  "type": "stddev",
  "name": "<output_name>",
  "fieldName": "<aggregator_name>",
  "estimator": <string>
}
```

## Query Examples:

### Timeseries Query

```json
{
  "queryType": "timeseries",
  "dataSource": "testing",
  "granularity": "day",
  "aggregations": [
    {
      "type": "variance",
      "name": "index_var",
      "fieldName": "index_var"
    }
  ],
  "intervals": [
    "2016-03-01T00:00:00.000/2013-03-20T00:00:00.000"
  ]
}
```

### TopN Query

```json
{
  "queryType": "topN",
  "dataSource": "testing",
  "dimensions": ["alias"],
  "threshold": 5,
  "granularity": "all",
  "aggregations": [
    {
      "type": "variance",
      "name": "index_var",
      "fieldName": "index"
    }
  ],
  "postAggregations": [
    {
      "type": "stddev",
      "name": "index_stddev",
      "fieldName": "index_var"
    }
  ],
  "intervals": [
    "2016-03-06T00:00:00/2016-03-06T23:59:59"
  ]
}
```

### GroupBy Query

```json
{
  "queryType": "groupBy",
  "dataSource": "testing",
  "dimensions": ["alias"],
  "granularity": "all",
  "aggregations": [
    {
      "type": "variance",
      "name": "index_var",
      "fieldName": "index"
    }
  ],
  "postAggregations": [
    {
      "type": "stddev",
      "name": "index_stddev",
      "fieldName": "index_var"
    }
  ],
  "intervals": [
    "2016-03-06T00:00:00/2016-03-06T23:59:59"
  ]
}
```
