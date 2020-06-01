---
id: post-aggregations
title: "Post-aggregations"
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

> Apache Druid supports two query languages: [Druid SQL](sql.md) and [native queries](querying.md).
> This document describes the native
> language. For information about functions available in SQL, refer to the
> [SQL documentation](sql.md#aggregation-functions).

Post-aggregations are specifications of processing that should happen on aggregated values as they come out of Apache Druid. If you include a post aggregation as part of a query, make sure to include all aggregators the post-aggregator requires.

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

### Field accessor post-aggregators

These post-aggregators return the value produced by the specified [aggregator](../querying/aggregations.md).

`fieldName` refers to the output name of the aggregator given in the [aggregations](../querying/aggregations.md) portion of the query.
For complex aggregators, like "cardinality" and "hyperUnique", the `type` of the post-aggregator determines what
the post-aggregator will return. Use type "fieldAccess" to return the raw aggregation object, or use type
"finalizingFieldAccess" to return a finalized value, such as an estimated cardinality.

```json
{ "type" : "fieldAccess", "name": <output_name>, "fieldName" : <aggregator_name> }
```

or

```json
{ "type" : "finalizingFieldAccess", "name": <output_name>, "fieldName" : <aggregator_name> }
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
[GREATEST](https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_greatest) functions.

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

> JavaScript-based functionality is disabled by default. Please refer to the Druid [JavaScript programming guide](../development/javascript.md) for guidelines about using Druid's JavaScript functionality, including instructions on how to enable it.

### HyperUnique Cardinality post-aggregator

The hyperUniqueCardinality post aggregator is used to wrap a hyperUnique object such that it can be used in post aggregations.

```json
{
  "type"  : "hyperUniqueCardinality",
  "name": <output name>,
  "fieldName"  : <the name field value of the hyperUnique aggregator>
}
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

This post-aggregator will inherit the rounding behavior of the aggregator it references. Note that this inheritance
is only effective if you directly reference an aggregator. Going through another post-aggregator, for example, will
cause the user-specified rounding behavior to get lost and default to "no rounding".

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
    "fn"     : "/",
    "fields" : [
           { "type" : "fieldAccess", "name" : "tot", "fieldName" : "tot" },
           { "type" : "fieldAccess", "name" : "rows", "fieldName" : "rows" }
         ]
  }]
  ...
}
```


```json
{
  ...
  "aggregations" : [
    { "type" : "doubleSum", "name" : "tot", "fieldName" : "total" },
    { "type" : "doubleSum", "name" : "part", "fieldName" : "part" }
  ],
  "postAggregations" : [{
    "type"   : "arithmetic",
    "name"   : "part_percentage",
    "fn"     : "*",
    "fields" : [
       { "type"   : "arithmetic",
         "name"   : "ratio",
         "fn"     : "/",
         "fields" : [
           { "type" : "fieldAccess", "name" : "part", "fieldName" : "part" },
           { "type" : "fieldAccess", "name" : "tot", "fieldName" : "tot" }
         ]
       },
       { "type" : "constant", "name": "const", "value" : 100 }
    ]
  }]
  ...
}
```
