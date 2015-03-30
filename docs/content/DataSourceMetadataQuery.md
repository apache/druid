---
layout: doc_page
---
# Data Source Metadata Queries
Data Source Metadata queries return metadata information for a dataSource. It returns the timestamp of latest ingested event for the datasource. The grammar is:

```json
{
    "queryType" : "dataSourceMetadata",
    "dataSource": "sample_datasource"
}
```

There are 2 main parts to a Data Source Metadata query:

|property|description|required?|
|--------|-----------|---------|
|queryType|This String should always be "dataSourceMetadata"; this is the first thing Druid looks at to figure out how to interpret the query|yes|
|dataSource|A String defining the data source to query, very similar to a table in a relational database|yes|
|context|An additional JSON Object which can be used to specify certain flags.|no|

The format of the result is:

```json
[ {
  "timestamp" : "2013-05-09T18:24:00.000Z",
  "result" : {
    "maxIngestedEventTime" : "2013-05-09T18:24:09.007Z",
  }
} ]
```
