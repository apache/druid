---
layout: doc_page
---
Aggregations are specifications of processing over metrics available in Druid.
Available aggregations are:

### Count aggregator

`count` computes the row count that match the filters

```json
{ "type" : "count", "name" : <output_name> }
```

### Sum aggregators

#### `longSum` aggregator

computes the sum of values as a 64-bit, signed integer

```json
{ "type" : "longSum", "name" : <output_name>, "fieldName" : <metric_name> }
```

`name` – output name for the summed value
`fieldName` – name of the metric column to sum over

#### `doubleSum` aggregator

Computes the sum of values as 64-bit floating point value. Similar to `longSum`

```json
{ "type" : "doubleSum", "name" : <output_name>, "fieldName" : <metric_name> }
```

### Min / Max aggregators

#### `min` aggregator

`min` computes the minimum metric value

```json
{ "type" : "min", "name" : <output_name>, "fieldName" : <metric_name> }
```

#### `max` aggregator

`max` computes the maximum metric value

```json
{ "type" : "max", "name" : <output_name>, "fieldName" : <metric_name> }
```

### JavaScript aggregator

Computes an arbitrary JavaScript function over a set of columns (both metrics and dimensions).

All JavaScript functions must return numerical values.

```json
{ "type": "javascript", "name": "<output_name>",
  "fieldNames"  : [ <column1>, <column2>, ... ],
  "fnAggregate" : "function(current, column1, column2, ...) {
                     <updates partial aggregate (current) based on the current row values>
                     return <updated partial aggregate>
                   }",
  "fnCombine"   : "function(partialA, partialB) { return <combined partial results>; }",
  "fnReset"     : "function()                   { return <initial value>; }"
}
```

**Example**

```json
{
  "type": "javascript",
  "name": "sum(log(x)/y) + 10",
  "fieldNames": ["x", "y"],
  "fnAggregate" : "function(current, a, b)      { return current + (Math.log(a) * b); }",
  "fnCombine"   : "function(partialA, partialB) { return partialA + partialB; }",
  "fnReset"     : "function()                   { return 10; }"
}
```
