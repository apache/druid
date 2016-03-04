---
layout: doc_page
---
# Aggregations

Aggregations can be provided at ingestion time as part of the ingestion spec as a way of summarizing data before it enters Druid. 
Aggregations can also be specified as part of many queries at query time.

Available aggregations are:

### Count aggregator

`count` computes the count of Druid rows that match the filters.

```json
{ "type" : "count", "name" : <output_name> }
```

Please note the count aggregator counts the number of Druid rows, which does not always reflect the number of raw events ingested. 
This is because Druid rolls up data at ingestion time. To 
count the number of ingested rows of data, include a count aggregator at ingestion time, and a longSum aggregator at 
query time.

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

JavaScript aggregators are much slower than native Java aggregators and if performance is critical, you should implement 
your functionality as a native Java aggregator.

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

The javascript aggregator is recommended for rapidly prototyping features. This aggregator will be much slower in production 
use than a native Java aggregator.

## Approximate Aggregations

### Cardinality aggregator

Computes the cardinality of a set of Druid dimensions, using HyperLogLog to estimate the cardinality. Please note that this 
aggregator will be much slower than indexing a column with the hyperUnique aggregator. This aggregator also runs over a dimension column, which 
means the string dimension cannot be removed from the dataset to improve rollup. In general, we strongly recommend using the hyperUnique aggregator 
instead of the cardinality aggregator if you do not care about the individual values of a dimension.

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

### HyperUnique aggregator

Uses [HyperLogLog](http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf) to compute the estimated cardinality of a dimension that has been aggregated as a "hyperUnique" metric at indexing time.

```json
{ "type" : "hyperUnique", "name" : <output_name>, "fieldName" : <metric_name> }
```

For more approximate aggregators, please see [theta sketches](../development/datasketches-aggregators.html).

## Miscellaneous Aggregations

### Filtered Aggregator

A filtered aggregator wraps any given aggregator, but only aggregates the values for which the given dimension filter matches.

This makes it possible to compute the results of a filtered and an unfiltered aggregation simultaneously, without having to issue multiple queries, and use both results as part of post-aggregations.

*Limitations:* The filtered aggregator currently only supports 'or', 'and', 'selector', 'not' and 'Extraction' filters, i.e. matching one or multiple dimensions against a single value.

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
