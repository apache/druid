---
layout: doc_page
---
TopNMetricSpec
==================

The topN metric spec specifies how topN values should be sorted.

## Numeric TopNMetricSpec

The simplest metric specification is a String value indicating the metric to sort topN results by. They are included in a topN query with:

```json
"metric": "<metric_name>"
```

The metric field can also be given as a JSON object. The grammar for dimension values sorted by numeric value is shown below:

```json
"metric": {
    "type": "numeric",
    "metric": "<metric_name>"
}
```

|property|description|required?|
|--------|-----------|---------|
|type|this indicates a numeric sort|yes|
|metric|the actual metric field in which results will be sorted by|yes|

## Dimension TopNMetricSpec

This metric specification sorts TopN results by dimension value, using one of the sorting orders described here: [Sorting Orders](./sorting-orders.html)

|property|type|description|required?|
|--------|----|-----------|---------|
|type|String|this indicates a sort a dimension's values|yes, must be 'dimension'|
|ordering|String|Specifies the sorting order. Can be one of the following values: "lexicographic", "alphanumeric", "numeric", "strlen". See [Sorting Orders](./sorting-orders.html) for more details.|no, default: "lexicographic"|
|previousStop|String|the starting point of the sort. For example, if a previousStop value is 'b', all values before 'b' are discarded. This field can be used to paginate through all the dimension values.|no|

The following metricSpec uses lexicographic sorting.

```json
"metric": {
    "type": "dimension",
    "ordering": "lexicographic",
    "previousStop": "<previousStop_value>"
}
```

Note that in earlier versions of Druid, the functionality provided by the DimensionTopNMetricSpec was handled by two separate spec types, Lexicographic and Alphanumeric (when only two sorting orders were supported). These spec types have been deprecated but are still usable.

## Inverted TopNMetricSpec

Sort dimension values in inverted order, i.e inverts the order of the delegate metric spec. It can be used to sort the values in ascending order.

```json
"metric": {
    "type": "inverted",
    "metric": <delegate_top_n_metric_spec>
}
```

|property|description|required?|
|--------|-----------|---------|
|type|this indicates an inverted sort|yes|
|metric|the delegate metric spec. |yes|