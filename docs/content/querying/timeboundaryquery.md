---
layout: doc_page
---
# Time Boundary Queries
Time boundary queries return the earliest and latest data points of a data set. The grammar is:

```json
{
    "queryType" : "timeBoundary",
    "dataSource": "sample_datasource",
    "bound"     : < "maxTime" | "minTime" > # optional, defaults to returning both timestamps if not set 
    "filter"    : { "type": "and", "fields": [<filter>, <filter>, ...] } # optional
}
```

There are 3 main parts to a time boundary query:

|property|description|required?|
|--------|-----------|---------|
|queryType|This String should always be "timeBoundary"; this is the first thing Druid looks at to figure out how to interpret the query|yes|
|dataSource|A String or Object defining the data source to query, very similar to a table in a relational database. See [DataSource](../querying/datasource.html) for more information.|yes|
|bound   | Optional, set to `maxTime` or `minTime` to return only the latest or earliest timestamp. Default to returning both if not set| no |
|filter|See [Filters](../querying/filters.html)|no|
|context|See [Context](../querying/query-context.html)|no|

The format of the result is:

```json
[ {
  "timestamp" : "2013-05-09T18:24:00.000Z",
  "result" : {
    "minTime" : "2013-05-09T18:24:00.000Z",
    "maxTime" : "2013-05-09T18:37:00.000Z"
  }
} ]
```
