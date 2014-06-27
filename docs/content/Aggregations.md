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
  "name": "sum(log(x)/y) + 10",
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
SELECT COUNT(DISCTINCT(dimension)) FROM <datasource>
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

When setting `byRow` to `true` it computes the cardinality by row, i.e. the cardinality of distinct dimension combinations
This is equivalent to something akin to

```sql
SELECT COUNT(*) FROM ( SELECT DIM1, DIM2, DIM3 FROM <datasource> GROUP BY DIM1, DIM2, DIM3
```

**Example**

Determine the number of distinct categories items are assigned to.

```json
{
  "type": "cardinality",
  "name": "distinct_values",
  "fieldNames": [ "main_category", "secondary_category" ]
}
```

Determine the number of distinct   are assigned to.

```json
{
  "type": "cardinality",
  "name": "distinct_values",
  "fieldNames": [ "", "secondary_category" ],
  "byRow" : true
}
```

## Complex Aggregations

### HyperUnique aggregator

Uses [HyperLogLog](http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf) to compute the estimated cardinality of a dimension that has been aggregated as a "hyperUnique" metric at indexing time.

```json
{ "type" : "hyperUnique", "name" : <output_name>, "fieldName" : <metric_name> }
```

### ApproxHistogram aggregator

This aggregator is based on [http://jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf](http://jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf) to compute approximate histograms.

To use this feature, an "approxHistogram" aggregator must be included at indexing time. The ingestion aggregator can only apply to numeric values. To query for results, an "approxHistogramFold" aggregator must be included in the query.

```json
{
  "type" : "approxHistogram(ingestion), approxHistogramFold(query)",
  "name" : <output_name>,
  "fieldName" : <metric_name>,
  "resolution" : <integer>,
  "numBuckets" : <integer>,
  "lowerLimit" : <float>,
  "upperLimit" : <float>
}
```

|Property|Description|Default|
|--------|-----------|-------|
|`resolution`|Number of centroids (data points) to store. The higher the resolution, the more accurate results are, but the slower computation will be.|50|
|`numBuckets`|Number of output buckets for the resulting histogram.|7|
|`lowerLimit`/`upperLimit`|Restrict the approximation to the given range. The values outside this range will be aggregated into two centroids. Counts of values outside this range are still maintained. |-INF/+INF|

