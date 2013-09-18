---
layout: default
---
Post-aggregations are specifications of processing that should happen on aggregated values as they come out of Druid. If you include a post aggregation as part of a query, make sure to include all aggregators the post-aggregator requires.

There are several post-aggregators available.

### Arithmetic post-aggregator

The arithmetic post-aggregator applies the provided function to the given fields from left to right. The fields can be aggregators or other post aggregators.

Supported functions are `+`, `-`, `*`, and `/`

The grammar for an arithmetic post aggregation is:

    <code>postAggregation : {
        "type"  : "arithmetic",
        "name"  : <output_name>,
        "fn"    : <arithmetic_function>,
        "fields": [<post_aggregator>, <post_aggregator>, ...]
    }</code>

### Field accessor post-aggregator

This returns the value produced by the specified [aggregator|Aggregations](aggregator|Aggregations.html).

`fieldName` refers to the output name of the aggregator given in the [aggregations|Aggregations](aggregations|Aggregations.html) portion of the query.

    <code>field_accessor : {
        "type"      : "fieldAccess",
        "fieldName" : <aggregator_name>
    }</code>

### Constant post-aggregator

The constant post-aggregator always returns the specified value.

    <code>constant : {
        "type"  : "constant",
        "name"  : <output_name>,
        "value" : <numerical_value>,
    }</code>

### Example Usage

In this example, let’s calculate a simple percentage using post aggregators. Let’s imagine our data set has a metric called “total”.

The format of the query JSON is as follows:

    <code>
    {
        ...
        "aggregations" : [
            {
                "type" : "count",
                "name" : "rows"
            },
            {
                "type"      : "doubleSum",
                "name"      : "tot",
                "fieldName" : "total"
            }
        ],
        "postAggregations" : {
            "type"   : "arithmetic",
            "name"   : "average",
            "fn"     : "*",
            "fields" : [
                {
                    "type"   : "arithmetic",
                    "name"   : "div",
                    "fn"     : "/",
                    "fields" : [
                        {
                            "type"      : "fieldAccess",
                            "name"      : "tot",
                            "fieldName" : "tot"
                         },
                         {
                            "type"      : "fieldAccess",
                            "name"      : "rows",
                            "fieldName" : "rows"
                         }
                    ]
                },
                {
                    "type" : "constant",
                    "name": "const",
                    "value" : 100
                }
            ]
        }
        ...
    }
    </code>
