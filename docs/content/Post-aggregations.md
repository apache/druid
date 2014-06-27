---
layout: doc_page
---
# Post-Aggregations
Post-aggregations are specifications of processing that should happen on aggregated values as they come out of Druid. If you include a post aggregation as part of a query, make sure to include all aggregators the post-aggregator requires.

There are several post-aggregators available.

### Arithmetic post-aggregator

The arithmetic post-aggregator applies the provided function to the given fields from left to right. The fields can be aggregators or other post aggregators.

Supported functions are `+`, `-`, `*`, and `/`

The grammar for an arithmetic post aggregation is:

```json
postAggregation : {
  "type"  : "arithmetic",
  "name"  : <output_name>,
  "fn"    : <arithmetic_function>,
  "fields": [<post_aggregator>, <post_aggregator>, ...]
}
```

### Field accessor post-aggregator

This returns the value produced by the specified [aggregator](Aggregations.html).

`fieldName` refers to the output name of the aggregator given in the [aggregations](Aggregations.html) portion of the query.

```json
{ "type" : "fieldAccess", "fieldName" : <aggregator_name> }
```

### Constant post-aggregator

The constant post-aggregator always returns the specified value.

```json
{ "type"  : "constant", "name"  : <output_name>, "value" : <numerical_value> }
```

### JavaScript post-aggregator

Applies the provided JavaScript function to the given fields. Fields are passed as arguments to the JavaScript function in the given order.

```json
postAggregation : {
  "type": "javascript",
  "name": <output_name>,
  "fieldNames" : [<aggregator_name>, <aggregator_name>, ...],
  "function": <javascript function>
}
```

Example JavaScript aggregator:

```json
{
  "type": "javascript",
  "name": "absPercent",
  "fieldNames": ["delta", "total"],
  "function": "function(delta, total) { return 100 * Math.abs(delta) / total; }"
}
```
### HyperUnique Cardinality post-aggregator

The hyperUniqueCardinality post aggregator is used to wrap a hyperUnique object such that it can be used in post aggregations.

```json
{ "type"  : "hyperUniqueCardinality", "fieldName"  : <the name field value of the hyperUnique aggregator>}
```

It can be used in a sample calculation as so:

```json
  "aggregations" : [{
    {"type" : "count", "name" : "rows"},
    {"type" : "hyperUnique", "name" : "unique_users", "fieldName" : "uniques"}
  }],
  "postAggregations" : {
    "type"   : "arithmetic",
    "name"   : "average_users_per_row",
    "fn"     : "/",
    "fields" : [
      { "type" : "hyperUniqueCardinality", "fieldName" : "unique_users" },
      { "type" : "fieldAccess", "name" : "rows", "fieldName" : "rows" }
    ]
  }
```

#### Example Usage

In this example, let’s calculate a simple percentage using post aggregators. Let’s imagine our data set has a metric called "total".

The format of the query JSON is as follows:

```json
{
  ...
  "aggregations" : [
    { "type" : "count", "name" : "rows" },
    { "type" : "doubleSum", "name" : "tot", "fieldName" : "total" }
  ],
  "postAggregations" : {
    "type"   : "arithmetic",
    "name"   : "average",
    "fn"     : "*",
    "fields" : [
       { "type"   : "arithmetic",
         "name"   : "div",
         "fn"     : "/",
         "fields" : [
           { "type" : "fieldAccess", "name" : "tot", "fieldName" : "tot" },
           { "type" : "fieldAccess", "name" : "rows", "fieldName" : "rows" }
         ]
       },
       { "type" : "constant", "name": "const", "value" : 100 }
    ]
  }
  ...
}
```

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
