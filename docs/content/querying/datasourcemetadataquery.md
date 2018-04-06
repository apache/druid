---
layout: doc_page
---
# Data Source Metadata Queries
Data Source Metadata queries return metadata information for a dataSource.  These queries return information about:

* The timestamp of latest ingested event for the dataSource. This is the ingested event without any consideration of rollup.

The grammar for these queries is:

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
|dataSource|A String or Object defining the data source to query, very similar to a table in a relational database. See [DataSource](../querying/datasource.html) for more information.|yes|
|context|See [Context](../querying/query-context.html)|no|

The format of the result is:

```json
[ {
  "timestamp" : "2013-05-09T18:24:00.000Z",
  "result" : {
    "maxIngestedEventTime" : "2013-05-09T18:24:09.007Z"
  }
} ]
```
