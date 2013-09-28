---
layout: doc_page
---
Time boundary queries return the earliest and latest data points of a data set. The grammar is:

```json
{
    "queryType" : "timeBoundary",
    "dataSource": "sample_datasource"
}
```

There are 3 main parts to a time boundary query:

|property|description|required?|
|--------|-----------|---------|
|queryType|This String should always be "timeBoundary"; this is the first thing Druid looks at to figure out how to interpret the query|yes|
|dataSource|A String defining the data source to query, very similar to a table in a relational database|yes|
|context|An additional JSON Object which can be used to specify certain flags.|no|

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
