---
layout: doc_page
---
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
|queryType|This String should always be "search"; this is the first thing Druid looks at to figure out how to interpret the query|yes|
|dataSource|A String defining the data source to query, very similar to a table in a relational database|yes|
|granularity|Defines the granularity of the query. See [Granularities](Granularities.html)|yes|
|filter|See [Filters](Filters.html)|no|
|intervals|A JSON Object representing ISO-8601 Intervals. This defines the time ranges to run the query over.|yes|
|searchDimensions|The dimensions to run the search over. Excluding this means the search is run over all dimensions.|no|
|query|See [SearchQuerySpec](SearchQuerySpec.html).|yes|
|sort|How the results of the search should sorted. Two possible types here are "lexicographic" and "strlen".|yes|
|context|An additional JSON Object which can be used to specify certain flags.|no|

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
