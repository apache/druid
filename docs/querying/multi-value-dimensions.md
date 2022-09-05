---
id: multi-value-dimensions
title: "Multi-value dimensions"
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


Apache Druid supports "multi-value" string dimensions. Multi-value string dimensions result from input fields that contain an
array of values instead of a single value, such as the `tags` values in the following JSON array example: 

```
{"timestamp": "2011-01-12T00:00:00.000Z", "tags": ["t1","t2","t3"]} 
```

This document describes filtering and grouping behavior for multi-value dimensions. For information about the internal representation of multi-value dimensions, see
[segments documentation](../design/segments.md#multi-value-columns). Examples in this document
are in the form of [native Druid queries](querying.md). Refer to the [Druid SQL documentation](sql-multivalue-string-functions.md) for details
about using multi-value string dimensions in SQL.

## Overview

At ingestion time, Druid can detect multi-value dimensions and configure the `dimensionsSpec` accordingly. It detects JSON arrays or CSV/TSV fields as multi-value dimensions.

For TSV or CSV data, you can specify the multi-value delimiters using the `listDelimiter` field in the `parseSpec`. JSON data must be formatted as a JSON array to be ingested as a multi-value dimension. JSON data does not require `parseSpec` configuration.

The following shows an example multi-value dimension named `tags` in a `dimensionsSpec`:

```
"dimensions": [
  {
    "type": "string",
    "name": "tags",
    "multiValueHandling": "SORTED_ARRAY",
    "createBitmapIndex": true
  }
],
```

By default, Druid sorts values in multi-value dimensions. This behavior is controlled by the `SORTED_ARRAY` value of the `multiValueHandling` field. Alternatively, you can specify multi-value handling as:

* `SORTED_SET`: results in the removal of duplicate values
* `ARRAY`: retains the original order of the values

See [Dimension Objects](../ingestion/ingestion-spec.md#dimension-objects) for information on configuring multi-value handling.


## Querying multi-value dimensions

The following sections describe filtering and grouping behavior based on the following example data, which includes a multi-value dimension, `tags`.

```
{"timestamp": "2011-01-12T00:00:00.000Z", "tags": ["t1","t2","t3"]}  #row1
{"timestamp": "2011-01-13T00:00:00.000Z", "tags": ["t3","t4","t5"]}  #row2
{"timestamp": "2011-01-14T00:00:00.000Z", "tags": ["t5","t6","t7"]}  #row3
{"timestamp": "2011-01-14T00:00:00.000Z", "tags": []}                #row4
```
> Be sure to remove the comments before trying out the sample data. 

### Filtering

All query types, as well as [filtered aggregators](aggregations.md#filtered-aggregator), can filter on multi-value
dimensions. Filters follow these rules on multi-value dimensions:

- Value filters (like "selector", "bound", and "in") match a row if any of the values of a multi-value dimension match
  the filter.
- The Column Comparison filter will match a row if the dimensions have any overlap.
- Value filters that match `null` or `""` (empty string) will match empty cells in a multi-value dimension.
- Logical expression filters behave the same way they do on single-value dimensions: "and" matches a row if all
  underlying filters match that row; "or" matches a row if any underlying filters match that row; "not" matches a row
  if the underlying filter does not match the row.

The following example illustrates these rules. This query applies an "or" filter to match row1 and row2 of the dataset above, but not row3:

```
{
  "type": "or",
  "fields": [
    {
      "type": "selector",
      "dimension": "tags",
      "value": "t1"
    },
    {
      "type": "selector",
      "dimension": "tags",
      "value": "t3"
    }
  ]
}
```

This "and" filter would match only row1 of the dataset above:

```
{
  "type": "and",
  "fields": [
    {
      "type": "selector",
      "dimension": "tags",
      "value": "t1"
    },
    {
      "type": "selector",
      "dimension": "tags",
      "value": "t3"
    }
  ]
}
```

This "selector" filter would match row4 of the dataset above:

```
{
  "type": "selector",
  "dimension": "tags",
  "value": null
}
```

### Grouping

topN and groupBy queries can group on multi-value dimensions. When grouping on a multi-value dimension, _all_ values
from matching rows will be used to generate one group per value. This can be thought of as the equivalent to the
`UNNEST` operator used on an `ARRAY` type that many SQL dialects support. This means it's possible for a query to return
more groups than there are rows. For example, a topN on the dimension `tags` with filter `"t1" AND "t3"` would match
only row1, and generate a result with three groups: `t1`, `t2`, and `t3`. If you only need to include values that match
your filter, you can use a [filtered dimensionSpec](dimensionspecs.md#filtered-dimensionspecs). This can also
improve performance.

## Example: GroupBy query with no filtering

See [GroupBy querying](groupbyquery.md) for details.

```json
{
  "queryType": "groupBy",
  "dataSource": "test",
  "intervals": [
    "1970-01-01T00:00:00.000Z/3000-01-01T00:00:00.000Z"
  ],
  "granularity": {
    "type": "all"
  },
  "dimensions": [
    {
      "type": "default",
      "dimension": "tags",
      "outputName": "tags"
    }
  ],
  "aggregations": [
    {
      "type": "count",
      "name": "count"
    }
  ]
}
```

This query returns the following result:

```json
[
  {
    "timestamp": "1970-01-01T00:00:00.000Z",
    "event": {
      "count": 1,
      "tags": "t1"
    }
  },
  {
    "timestamp": "1970-01-01T00:00:00.000Z",
    "event": {
      "count": 1,
      "tags": "t2"
    }
  },
  {
    "timestamp": "1970-01-01T00:00:00.000Z",
    "event": {
      "count": 2,
      "tags": "t3"
    }
  },
  {
    "timestamp": "1970-01-01T00:00:00.000Z",
    "event": {
      "count": 1,
      "tags": "t4"
    }
  },
  {
    "timestamp": "1970-01-01T00:00:00.000Z",
    "event": {
      "count": 2,
      "tags": "t5"
    }
  },
  {
    "timestamp": "1970-01-01T00:00:00.000Z",
    "event": {
      "count": 1,
      "tags": "t6"
    }
  },
  {
    "timestamp": "1970-01-01T00:00:00.000Z",
    "event": {
      "count": 1,
      "tags": "t7"
    }
  }
]
```

Notice that original rows are "exploded" into multiple rows and merged.

## Example: GroupBy query with a selector query filter

See [query filters](filters.md) for details of selector query filter.

```json
{
  "queryType": "groupBy",
  "dataSource": "test",
  "intervals": [
    "1970-01-01T00:00:00.000Z/3000-01-01T00:00:00.000Z"
  ],
  "filter": {
    "type": "selector",
    "dimension": "tags",
    "value": "t3"
  },
  "granularity": {
    "type": "all"
  },
  "dimensions": [
    {
      "type": "default",
      "dimension": "tags",
      "outputName": "tags"
    }
  ],
  "aggregations": [
    {
      "type": "count",
      "name": "count"
    }
  ]
}
```

This query returns the following result:

```json
[
  {
    "timestamp": "1970-01-01T00:00:00.000Z",
    "event": {
      "count": 1,
      "tags": "t1"
    }
  },
  {
    "timestamp": "1970-01-01T00:00:00.000Z",
    "event": {
      "count": 1,
      "tags": "t2"
    }
  },
  {
    "timestamp": "1970-01-01T00:00:00.000Z",
    "event": {
      "count": 2,
      "tags": "t3"
    }
  },
  {
    "timestamp": "1970-01-01T00:00:00.000Z",
    "event": {
      "count": 1,
      "tags": "t4"
    }
  },
  {
    "timestamp": "1970-01-01T00:00:00.000Z",
    "event": {
      "count": 1,
      "tags": "t5"
    }
  }
]
```

You might be surprised to see "t1", "t2", "t4" and "t5" included in the results. This is because the query filter is
applied on the row before explosion. For multi-value dimensions, a selector filter for "t3" would match row1 and row2,
after which exploding is done. For multi-value dimensions, a query filter matches a row if any individual value inside
the multiple values matches the query filter.

## Example: GroupBy query with selector query and dimension filters

To solve the problem above and to get only rows for "t3", use a "filtered dimension spec", as in the query below.

See filtered `dimensionSpecs` in [dimensionSpecs](dimensionspecs.md#filtered-dimensionspecs) for details.

```json
{
  "queryType": "groupBy",
  "dataSource": "test",
  "intervals": [
    "1970-01-01T00:00:00.000Z/3000-01-01T00:00:00.000Z"
  ],
  "filter": {
    "type": "selector",
    "dimension": "tags",
    "value": "t3"
  },
  "granularity": {
    "type": "all"
  },
  "dimensions": [
    {
      "type": "listFiltered",
      "delegate": {
        "type": "default",
        "dimension": "tags",
        "outputName": "tags"
      },
      "values": ["t3"]
    }
  ],
  "aggregations": [
    {
      "type": "count",
      "name": "count"
    }
  ]
}
```

This query returns the following result:

```json
[
  {
    "timestamp": "1970-01-01T00:00:00.000Z",
    "event": {
      "count": 2,
      "tags": "t3"
    }
  }
]
```

Note that, for groupBy queries, you could get similar result with a [having spec](having.md) but using a filtered
`dimensionSpec` is much more efficient because that gets applied at the lowest level in the query processing pipeline.
Having specs are applied at the outermost level of groupBy query processing.

## Disable GroupBy on multi-value columns

You can disable the implicit unnesting behavior for groupBy by setting groupByEnableMultiValueUnnesting: false in your 
query context. In this mode, the groupBy engine will return an error instead of completing the query. This is a safety 
feature for situations where you believe that all dimensions are singly-valued and want the engine to reject any 
multi-valued dimensions that were inadvertently included.