---
layout: doc_page
---
# Post-Aggregations
Post-aggregations are specifications of processing that should happen on aggregated values as they come out of Druid. If you include a post aggregation as part of a query, make sure to include all aggregators the post-aggregator requires.

There are several post-aggregators available.

### Arithmetic post-aggregator

The arithmetic post-aggregator applies the provided function to the given
fields from left to right. The fields can be aggregators or other post aggregators.

Supported functions are `+`, `-`, `*`, `/`, and `quotient`.

**Note**:

* `/` division always returns `0` if dividing by`0`, regardless of the numerator.
* `quotient` division behaves like regular floating point division

Arithmetic post-aggregators may also specify an `ordering`, which defines the order
of resulting values when sorting results (this can be useful for topN queries for instance):

- If no ordering (or `null`) is specified, the default floating point ordering is used.
- `numericFirst` ordering always returns finite values first, followed by `NaN`, and infinite values last.

The grammar for an arithmetic post aggregation is:

```json
postAggregation : {
  "type"  : "arithmetic",
  "name"  : <output_name>,
  "fn"    : <arithmetic_function>,
  "fields": [<post_aggregator>, <post_aggregator>, ...],
  "ordering" : <null (default), or "numericFirst">
}
```

### Field accessor post-aggregator

This returns the value produced by the specified [aggregator](../querying/aggregations.html).

`fieldName` refers to the output name of the aggregator given in the [aggregations](../querying/aggregations.html) portion of the query.

```json
{ "type" : "fieldAccess", "name": <output_name>, "fieldName" : <aggregator_name> }
```

### Constant post-aggregator

The constant post-aggregator always returns the specified value.

```json
{ "type"  : "constant", "name"  : <output_name>, "value" : <numerical_value> }
```

### Greatest / Least post-aggregators

`doubleGreatest` and `longGreatest` computes the maximum of all fields and Double.NEGATIVE_INFINITY.
`doubleLeast` and `longLeast` computes the minimum of all fields and Double.POSITIVE_INFINITY.

The difference between the `doubleMax` aggregator and the `doubleGreatest` post-aggregator is that `doubleMax` returns the highest value of
all rows for one specific column while `doubleGreatest` returns the highest value of multiple columns in one row. These are similar to the
SQL [MAX](https://dev.mysql.com/doc/refman/5.7/en/group-by-functions.html#function_max) and
[GREATEST](shttp://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_greatest) functions.

Example:

```json
{
  "type"  : "doubleGreatest",
  "name"  : <output_name>,
  "fields": [<post_aggregator>, <post_aggregator>, ...]
}
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

<div class="note info">
Please refer to the Druid <a href="../development/javascript.html">JavaScript programming guide</a> for guidelines
about using Druid's JavaScript functionality.
</div>

### HyperUnique Cardinality post-aggregator

The hyperUniqueCardinality post aggregator is used to wrap a hyperUnique object such that it can be used in post aggregations.

```json
{ "type"  : "hyperUniqueCardinality", "name": <output name>, "fieldName"  : <the name field value of the hyperUnique aggregator>}
```

It can be used in a sample calculation as so:

```json
  "aggregations" : [{
    {"type" : "count", "name" : "rows"},
    {"type" : "hyperUnique", "name" : "unique_users", "fieldName" : "uniques"}
  }],
  "postAggregations" : [{
    "type"   : "arithmetic",
    "name"   : "average_users_per_row",
    "fn"     : "/",
    "fields" : [
      { "type" : "hyperUniqueCardinality", "fieldName" : "unique_users" },
      { "type" : "fieldAccess", "name" : "rows", "fieldName" : "rows" }
    ]
  }]
```

## Example Usage

In this example, let’s calculate a simple percentage using post aggregators. Let’s imagine our data set has a metric called "total".

The format of the query JSON is as follows:

```json
{
  ...
  "aggregations" : [
    { "type" : "count", "name" : "rows" },
    { "type" : "doubleSum", "name" : "tot", "fieldName" : "total" }
  ],
  "postAggregations" : [{
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
  }]
  ...
}
```
