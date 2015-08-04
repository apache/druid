---
layout: doc_page
---
# Aggregations
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

#### `doubleMin` aggregator

`doubleMin` computes the minimum of all metric values and Double.POSITIVE_INFINITY

```json
{ "type" : "doubleMin", "name" : <output_name>, "fieldName" : <metric_name> }
```

#### `doubleMax` aggregator

`doubleMax` computes the maximum of all metric values and Double.NEGATIVE_INFINITY

```json
{ "type" : "doubleMax", "name" : <output_name>, "fieldName" : <metric_name> }
```

#### `longMin` aggregator

`longMin` computes the minimum of all metric values and Long.MAX_VALUE

```json
{ "type" : "longMin", "name" : <output_name>, "fieldName" : <metric_name> }
```

#### `longMax` aggregator

`longMax` computes the maximum of all metric values and Long.MIN_VALUE

```json
{ "type" : "longMax", "name" : <output_name>, "fieldName" : <metric_name> }
```

### JavaScript aggregator

Computes an arbitrary JavaScript function over a set of columns (both metrics and dimensions).

All JavaScript functions must return numerical values.

```json
{ "type": "javascript",
  "name": "<output_name>",
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
  "name": "sum(log(x)*y) + 10",
  "fieldNames": ["x", "y"],
  "fnAggregate" : "function(current, a, b)      { return current + (Math.log(a) * b); }",
  "fnCombine"   : "function(partialA, partialB) { return partialA + partialB; }",
  "fnReset"     : "function()                   { return 10; }"
}
```

### Cardinality aggregator

Computes the cardinality of a set of Druid dimensions, using HyperLogLog to estimate the cardinality.

```json
{
  "type": "cardinality",
  "name": "<output_name>",
  "fieldNames": [ <dimension1>, <dimension2>, ... ],
  "byRow": <false | true> # (optional, defaults to false)
}
```

#### Cardinality by value

When setting `byRow` to `false` (the default) it computes the cardinality of the set composed of the union of all dimension values for all the given dimensions.

* For a single dimension, this is equivalent to

```sql
SELECT COUNT(DISTINCT(dimension)) FROM <datasource>
```

* For multiple dimensions, this is equivalent to something akin to

```sql
SELECT COUNT(DISTINCT(value)) FROM (
  SELECT dim_1 as value FROM <datasource>
  UNION
  SELECT dim_2 as value FROM <datasource>
  UNION
  SELECT dim_3 as value FROM <datasource>
)
```

#### Cardinality by row

When setting `byRow` to `true` it computes the cardinality by row, i.e. the cardinality of distinct dimension combinations.
This is equivalent to something akin to

```sql
SELECT COUNT(*) FROM ( SELECT DIM1, DIM2, DIM3 FROM <datasource> GROUP BY DIM1, DIM2, DIM3 )
```

**Example**

Determine the number of distinct countries people are living in or have come from.

```json
{
  "type": "cardinality",
  "name": "distinct_countries",
  "fieldNames": [ "coutry_of_origin", "country_of_residence" ]
}
```

Determine the number of distinct people (i.e. combinations of first and last name).

```json
{
  "type": "cardinality",
  "name": "distinct_people",
  "fieldNames": [ "first_name", "last_name" ],
  "byRow" : true
}
```

## Complex Aggregations

### HyperUnique aggregator

Uses [HyperLogLog](http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf) to compute the estimated cardinality of a dimension that has been aggregated as a "hyperUnique" metric at indexing time.

```json
{ "type" : "hyperUnique", "name" : <output_name>, "fieldName" : <metric_name> }
```

## Miscellaneous Aggregations

### Filtered Aggregator

A filtered aggregator wraps any given aggregator, but only aggregates the values for which the given dimension filter matches.

This makes it possible to compute the results of a filtered and an unfiltered aggregation simultaneously, without having to issue multiple queries, and use both results as part of post-aggregations.

*Limitations:* The filtered aggregator currently only supports 'or', 'and', 'selector' and 'not' filters, i.e. matching one or multiple dimensions against a single value.

*Note:* If only the filtered results are required, consider putting the filter on the query itself, which will be much faster since it does not require scanning all the data.

```json
{
  "type" : "filtered",
  "filter" : {
    "type" : "selector",
    "dimension" : <dimension>,
    "value" : <dimension value>
  }
  "aggregator" : <aggregation>
}
```
