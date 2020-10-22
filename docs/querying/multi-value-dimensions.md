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


Apache Druid supports "multi-value" string dimensions. These are generated when an input field contains an
array of values instead of a single value (e.g. JSON arrays, or a TSV field containing one or more `listDelimiter`
characters).

This document describes the behavior of groupBy (topN has similar behavior) queries on multi-value dimensions when they
are used as a dimension being grouped by. See the section on multi-value columns in
[segments](../design/segments.md#multi-value-columns) for internal representation details. Examples in this document
are in the form of [native Druid queries](querying.md). Refer to the [Druid SQL documentation](sql.md) for details
about using multi-value string dimensions in SQL.

## Querying multi-value dimensions

Suppose, you have a dataSource with a segment that contains the following rows, with a multi-value dimension
called `tags`.

```
{"timestamp": "2011-01-12T00:00:00.000Z", "tags": ["t1","t2","t3"]}  #row1
{"timestamp": "2011-01-13T00:00:00.000Z", "tags": ["t3","t4","t5"]}  #row2
{"timestamp": "2011-01-14T00:00:00.000Z", "tags": ["t5","t6","t7"]}  #row3
{"timestamp": "2011-01-14T00:00:00.000Z", "tags": []}                #row4
```

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

For example, this "or" filter would match row1 and row2 of the dataset above, but not row3:

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

### Example: GroupBy query with no filtering

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

returns following result.

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

notice how original rows are "exploded" into multiple rows and merged.

### Example: GroupBy query with a selector query filter

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

returns following result.

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

You might be surprised to see inclusion of "t1", "t2", "t4" and "t5" in the results. It happens because query filter is
applied on the row before explosion. For multi-value dimensions, selector filter for "t3" would match row1 and row2,
after which exploding is done. For multi-value dimensions, query filter matches a row if any individual value inside
the multiple values matches the query filter.

### Example: GroupBy query with a selector query filter and additional filter in "dimensions" attributes

To solve the problem above and to get only rows for "t3" returned, you would have to use a "filtered dimension spec" as
in the query below.

See section on filtered dimensionSpecs in [dimensionSpecs](dimensionspecs.md#filtered-dimensionspecs) for details.

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

returns the following result.

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
dimensionSpec is much more efficient because that gets applied at the lowest level in the query processing pipeline.
Having specs are applied at the outermost level of groupBy query processing.
