---
layout: doc_page
---

Druid supports "multi-valued" dimensions. See the section on multi-valued columns in [segments](../design/segments.html) for internal representation details. This document describes the behavior of groupBy(topN has similar behavior) queries on multi-valued dimensions when they are used as a dimension being grouped by.

Suppose, you have a dataSource with a segment that contains following rows with a multi-valued dimension called tags.

```
2772011-01-12T00:00:00.000Z,["t1","t2","t3"],  #row1
2782011-01-13T00:00:00.000Z,["t3","t4","t5"],  #row2
2792011-01-14T00:00:00.000Z,["t5","t6","t7"]   #row3
```

### Group-By query with no filtering

See [GroupBy querying](groupbyquery.html) for details.

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

### Group-By query with a selector query filter

See [query filters](filters.html) for details of selector query filter.

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

You might be surprised to see inclusion of "t1", "t2", "t4" and "t5" in the results. It happens because query filter is applied on the row before explosion. For multi-valued dimensions, selector filter for "t3" would match row1 and row2, after which exploding is done. For multi-valued dimensions, query filter matches a row if any individual value inside the multiple values matches the query filter.

### Group-By query with a selector query filter and additional filter in "dimensions" attributes

To solve the problem above and to get only rows for "t3" returned, you would have to use a "filtered dimension spec" as in the query below.

See section on filtered dimensionSpecs in [dimensionSpecs](dimensionspecs.html) for details.

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

returns following result.

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

Note that, for groupBy queries, you could get similar result with a [having spec](having.html) but using a filtered dimensionSpec would be much more efficient because that gets applied at the lowest level in the query processing pipeline while having spec is applied at the highest level of groupBy query processing.

