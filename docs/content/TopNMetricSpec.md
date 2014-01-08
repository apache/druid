---
layout: doc_page
---
TopNMetricSpec
==================

The topN metric spec specifies how topN values should be sorted.

## Numeric TopNMetricSpec

The simplest metric specification is a String value indicating the metric to sort topN results by. They are included in a topN query with:

```json
"metric": <metric_value_string>
```

The metric field can also be given as a JSON object. The grammar for dimension values sorted by numeric value is shown below:

```json
"metric": {
    "type": "numeric",
    "metric": "<metric_value>"
}
```

|property|description|required?|
|--------|-----------|---------|
|type|this indicates a numeric sort|yes|
|metric|the actual metric field in which results will be sorted by|yes|

## Lexicographic TopNMetricSpec

The grammar for dimension values sorted lexicographically is as follows:

```json
"metric": {
    "type": "lexicographic",
    "previousStop": "<previousStop_value>"
}
```

|property|description|required?|
|--------|-----------|---------|
|type|this indicates a lexicographic sort|yes|
|previousStop|the starting point of the lexicographic sort. For example, if a previousStop value is 'b', all values before 'b' are discarded. This field can be used to paginate through all the dimension values.|no|
