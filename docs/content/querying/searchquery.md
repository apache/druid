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
|sort|An object specifying how the results of the search should be sorted. Two possible types here are "lexicographic" (the default sort) and "strlen".|no|
|context|See [Context](../querying/query-context.html)|no|

The format of the result is:

```json
[
  {
    "timestamp": "2012-01-01T00:00:00.000Z",
    "result": [
      {
        "dimension": "dim1",
        "value": "Ke$ha"
      },
      {
        "dimension": "dim2",
        "value": "Ke$haForPresident"
      }
    ]
  },
  {
    "timestamp": "2012-01-02T00:00:00.000Z",
    "result": [
      {
        "dimension": "dim1",
        "value": "SomethingThatContainsKe"
      },
      {
        "dimension": "dim2",
        "value": "SomethingElseThatContainsKe"
      }
    ]
  }
]
```
