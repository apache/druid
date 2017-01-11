---
layout: doc_page
---
# Search Queries
A search query returns dimension values that match the search specification.

```json
{
  "queryType": "search",
  "dataSource": "sample_datasource",
  "granularity": "day",
  "searchDimensions": [
    "dim1",
    "dim2"
  ],
  "query": {
    "type": "insensitive_contains",
    "value": "Ke"
  },
  "sort" : {
    "type": "lexicographic"
  },
  "intervals": [
    "2013-01-01T00:00:00.000/2013-01-03T00:00:00.000"
  ]
}
```

There are several main parts to a search query:

|property|description|required?|
|--------|-----------|---------|
|queryType|This String should always be "search"; this is the first thing Druid looks at to figure out how to interpret the query.|yes|
|dataSource|A String or Object defining the data source to query, very similar to a table in a relational database. See [DataSource](../querying/datasource.html) for more information.|yes|
|granularity|Defines the granularity of the query. See [Granularities](../querying/granularities.html).|yes|
|filter|See [Filters](../querying/filters.html).|no|
|limit| Defines the maximum number per historical node (parsed as int) of search results to return. |no (default to 1000)|
|intervals|A JSON Object representing ISO-8601 Intervals. This defines the time ranges to run the query over.|yes|
|searchDimensions|The dimensions to run the search over. Excluding this means the search is run over all dimensions.|no|
|query|See [SearchQuerySpec](../querying/searchqueryspec.html).|yes|
|sort|An object specifying how the results of the search should be sorted.<br/>Possible types are "lexicographic" (the default sort), "alphanumeric", "strlen", and "numeric".<br/>See [Sorting Orders](./sorting-orders.html) for more details.|no|
|context|See [Context](../querying/query-context.html)|no|

The format of the result is:

```json
[
  {
    "timestamp": "2012-01-01T00:00:00.000Z",
    "result": [
      {
        "dimension": "dim1",
        "value": "Ke$ha",
        "count": 3
      },
      {
        "dimension": "dim2",
        "value": "Ke$haForPresident",
        "count": 1
      }
    ]
  },
  {
    "timestamp": "2012-01-02T00:00:00.000Z",
    "result": [
      {
        "dimension": "dim1",
        "value": "SomethingThatContainsKe",
        "count": 1
      },
      {
        "dimension": "dim2",
        "value": "SomethingElseThatContainsKe",
        "count": 2
      }
    ]
  }
]
```

### Implementation details

#### Strategies

Search queries can be executed using two different strategies. The default strategy is determined by the
"druid.query.search.searchStrategy" runtime property on the broker. This can be overridden using "searchStrategy" in the
query context. If neither the context field nor the property is set, the "useIndexes" strategy will be used.

- "useIndexes" strategy, the default, first categorizes search dimensions into two groups according to their support for
bitmap indexes. And then, it applies index-only and cursor-based execution plans to the group of dimensions supporting
bitmaps and others, respectively. The index-only plan uses only indexes for search query processing. For each dimension,
it reads the bitmap index for each dimension value, evaluates the search predicate, and finally checks the time interval
and filter predicates. For the cursor-based execution plan, please refer to the "cursorOnly" strategy. The index-only
plan shows low performance for the search dimensions of large cardinality which means most values of search dimensions
are unique.

- "cursorOnly" strategy generates a cursor-based execution plan. This plan creates a cursor which reads a row from a
queryableIndexSegment, and then evaluates search predicates. If some filters support bitmap indexes, the cursor can read
only the rows which satisfy those filters, thereby saving I/O cost. However, it might be slow with filters of low selectivity.

- "auto" strategy uses a cost-based planner for choosing an optimal search strategy. It estimates the cost of index-only
and cursor-based execution plans, and chooses the optimal one. Currently, it is not enabled by default due to the overhead
of cost estimation.

#### Server configuration

The following runtime properties apply:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.query.search.searchStrategy`|Default search query strategy.|useIndexes|

#### Query context

The following query context parameters apply:

|Property|Description|
|--------|-----------|
|`searchStrategy`|Overrides the value of `druid.query.search.searchStrategy` for this query.|
