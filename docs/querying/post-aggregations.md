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

:::info
 Apache Druid supports two query languages: [Druid SQL](sql.md) and [native queries](querying.md).
 This document describes the native
 language. For information about functions available in SQL, refer to the
 [SQL documentation](sql-aggregations.md).
:::

Post-aggregations are specifications of processing that should happen on aggregated values as they come out of Apache Druid. If you include a post aggregation as part of a query, make sure to include all aggregators the post-aggregator requires.

There are several post-aggregators available.

### Arithmetic post-aggregator

The arithmetic post-aggregator applies the provided function to the given
fields from left to right. The fields can be aggregators or other post aggregators.

| Property | Description | Required |
| --- | --- | --- |
| `type` | Must be `"arithmetic"`. | Yes |
| `name` | Output name of the post-aggregation | Yes |
| `fn`| Supported functions are `+`, `-`, `*`, `/`, `pow` and `quotient` | Yes |
| `fields` | List of post-aggregator specs which define inputs to the `fn` | Yes |
| `ordering` | If no ordering (or `null`) is specified, the default floating point ordering is used. `numericFirst` ordering always returns finite values first, followed by `NaN`, and infinite values last. | No |


**Note**:
* `/` division always returns `0` if dividing by`0`, regardless of the numerator.
* `quotient` division behaves like regular floating point division
* Arithmetic post-aggregators always use floating point arithmetic.

Example:

```json
{
  "type"  : "arithmetic",
  "name"  : "mult",
  "fn"    : "*",
  "fields": [
    {"type": "fieldAccess", "fieldName":  "someAgg"},
    {"type": "fieldAccess", "fieldName":  "someOtherAgg"}
  ]
}
```

### Field accessor post-aggregators

These post-aggregators return the value produced by the specified [dimension](../querying/dimensionspecs.md) or [aggregator](../querying/aggregations.md).

| Property | Description | Required |
| --- | --- | --- |
| `type` | Must be `"fieldAccess"` or `"finalizingFieldAccess"`. Use type `"fieldAccess"` to return the raw aggregation object, or use type `"finalizingFieldAccess"` to return a finalized value, such as an estimated cardinality. | Yes |
| `name` | Output name of the post-aggregation | Yes if defined as a standalone post-aggregation, but may be omitted if used inline to some other post-aggregator in a `fields` list |
| `fieldName` | The output name of the dimension or aggregator to reference | Yes |

Example:

```json
{ "type" : "fieldAccess", "name": "someField", "fieldName" : "someAggregator" }
```

or

```json
{ "type" : "finalizingFieldAccess", "name": "someFinalizedField", "fieldName" : "someAggregator" }
```


### Constant post-aggregator

The constant post-aggregator always returns the specified value.

| Property | Description | Required |
| --- | --- | --- |
| `type` | Must be `"constant"` | Yes |
| `name` | Output name of the post-aggregation | Yes |
| `value` | The constant value | Yes |

Example:

```json
{ "type"  : "constant", "name"  : "someConstant", "value" : 1234 }
```


### Expression post-aggregator
The expression post-aggregator is defined using a Druid [expression](math-expr.md).

| Property | Description | Required |
| --- | --- | --- |
| `type` | Must be `"expression"` | Yes |
| `name` | Output name of the post-aggregation | Yes |
| `expression` | Native Druid [expression](math-expr.md) to compute, may refer to any dimension or aggregator output names | Yes |
| `ordering` | If no ordering (or `null`) is specified, the "natural" ordering is used. `numericFirst` ordering always returns finite values first, followed by `NaN`, and infinite values last. If the expression produces array or complex types, specify `ordering` as null and use `outputType` instead to use the correct type native ordering. | No |
| `outputType` | Output type is optional, and can be any native Druid type: `LONG`, `FLOAT`, `DOUBLE`, `STRING`, `ARRAY` types (e.g. `ARRAY<LONG>`), or `COMPLEX` types (e.g. `COMPLEX<json>`). If not specified, the output type will be inferred from the `expression`. If specified and `ordering` is null, the type native ordering will be used for sorting values. If the expression produces array or complex types, this value must be non-null to ensure the correct ordering is used. If `outputType` does not match the actual output type of the `expression`, the value will be attempted to coerced to the specified type, possibly failing if coercion is not possible. | No |

Example:
```json
{
  "type": "expression",
  "name": "someExpression",
  "expression": "someAgg + someOtherAgg",
  "ordering": null,
  "outputType": "LONG" 
}
```

### Greatest / Least post-aggregators

`doubleGreatest` and `longGreatest` computes the maximum of all fields and Double.NEGATIVE_INFINITY.
`doubleLeast` and `longLeast` computes the minimum of all fields and Double.POSITIVE_INFINITY.

| Property | Description | Required |
| --- | --- | --- |
| `type` | Must be `"doubleGreatest"`, `"doubleLeast"`, `"longGreatest"`, or `"longLeast"`. | Yes |
| `name` | Output name of the post-aggregation | Yes |
| `fields` | List of post-aggregator specs which define inputs to the greatest or least function | Yes |

The difference between the `doubleMax` aggregator and the `doubleGreatest` post-aggregator is that `doubleMax` returns the highest value of
all rows for one specific column while `doubleGreatest` returns the highest value of multiple columns in one row. These are similar to the
SQL `MAX` and `GREATEST` functions.

Example:

```json
{
  "type"  : "doubleGreatest",
  "name"  : "theGreatest",
  "fields": [
   { "type": "fieldAccess", "fieldName": "someAgg" },
   { "type": "fieldAccess", "fieldName": "someOtherAgg" }
  ]
}
```

### JavaScript post-aggregator

Applies the provided JavaScript function to the given fields. Fields are passed as arguments to the JavaScript function in the given order.

| Property | Description | Required |
| --- | --- | --- |
| `type` | Must be `"javascript"` | Yes |
| `name` | Output name of the post-aggregation | Yes |
| `fieldNames` | List of input dimension or aggregator output names | Yes |
| `function` | String javascript function which accepts `fieldNames` as arguments | Yes |

Example:
```json
{
  "type": "javascript",
  "name": "someJavascript",
  "fieldNames" : ["someAgg", "someOtherAgg"],
  "function": "function(someAgg, someOtherAgg) { return 100 * Math.abs(someAgg) / someOtherAgg;"
}
```

:::info
 JavaScript-based functionality is disabled by default. Please refer to the Druid [JavaScript programming guide](../development/javascript.md) for guidelines about using Druid's JavaScript functionality, including instructions on how to enable it.
:::

### HyperUnique Cardinality post-aggregator

The hyperUniqueCardinality post aggregator is used to wrap a hyperUnique object such that it can be used in post aggregations.

| Property | Description | Required |
| --- | --- | --- |
| `type` | Must be `"hyperUniqueCardinality"` | Yes |
| `name` | Output name of the post-aggregation | Yes |
| `fieldName` | The output name of a [`hyperUnique` aggregator](aggregations.md#cardinality-hyperunique) | Yes |

```json
{
  "type"  : "hyperUniqueCardinality",
  "name": "someCardinality",
  "fieldName"  : "someHyperunique"
}
```

It can be used in a sample calculation as so:

```json
{
  ...
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
  ...
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

The same could be computed using an expression post-aggregator: 
```json
{
  ...
  "aggregations" : [
    { "type" : "doubleSum", "name" : "tot", "fieldName" : "total" },
    { "type" : "doubleSum", "name" : "part", "fieldName" : "part" }
  ],
  "postAggregations" : [{
    "type"       : "expression",
    "name"       : "part_percentage",
    "expression" : "100 * (part / tot)"
  }]
  ...
}
```

