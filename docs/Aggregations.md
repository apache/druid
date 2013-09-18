---
layout: default
---
Aggregations are specifications of processing over metrics available in Druid.
Available aggregations are:

### Sum aggregators

#### `longSum` aggregator

computes the sum of values as a 64-bit, signed integer

    <code>{
        "type" : "longSum",
        "name" : <output_name>,
        "fieldName" : <metric_name>
    }</code>

`name` – output name for the summed value
`fieldName` – name of the metric column to sum over

#### `doubleSum` aggregator

Computes the sum of values as 64-bit floating point value. Similar to `longSum`

    <code>{
        "type" : "doubleSum",
        "name" : <output_name>,
        "fieldName" : <metric_name>
    }</code>

### Count aggregator

`count` computes the row count that match the filters

    <code>{
        "type" : "count",
        "name" : <output_name>,
    }</code>

### Min / Max aggregators

#### `min` aggregator

`min` computes the minimum metric value

    <code>{
        "type" : "min",
        "name" : <output_name>,
        "fieldName" : <metric_name>
    }</code>

#### `max` aggregator

`max` computes the maximum metric value

    <code>{
        "type" : "max",
        "name" : <output_name>,
        "fieldName" : <metric_name>
    }</code>

### JavaScript aggregator

Computes an arbitrary JavaScript function over a set of columns (both metrics and dimensions).

All JavaScript functions must return numerical values.

    <code>{
      "type": "javascript",
      "name": "<output_name>",
      "fieldNames"  : [ <column1>, <column2>, ... ],
      "fnAggregate" : "function(current, column1, column2, ...) {
                         <updates partial aggregate (current) based on the current row values>
                         return <updated partial aggregate>
                       }"
      "fnCombine"   : "function(partialA, partialB) { return <combined partial results>; }"
      "fnReset"     : "function()                   { return <initial value>; }"
    }</code>

**Example**

    <code>{
      "type": "javascript",
      "name": "sum(log(x)/y) + 10",
      "fieldNames": ["x", "y"],
      "fnAggregate" : "function(current, a, b)      { return current + (Math.log(a) * b); }"
      "fnCombine"   : "function(partialA, partialB) { return partialA + partialB; }"
      "fnReset"     : "function()                   { return 10; }"
    }</code>