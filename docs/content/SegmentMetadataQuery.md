---
layout: doc_page
---
# Segment Metadata Queries
Segment metadata queries return per segment information about:

* Cardinality of all columns in the segment
* Estimated byte size for the segment columns in TSV format
* Interval the segment covers
* Column type of all the columns in the segment
* Estimated total segment byte size in TSV format
* Segment id

```json
{
  "queryType":"segmentMetadata",
  "dataSource":"sample_datasource",
  "intervals":["2013-01-01/2014-01-01"]
}
```

There are several main parts to a segment metadata query:

|property|description|required?|
|--------|-----------|---------|
|queryType|This String should always be "segmentMetadata"; this is the first thing Druid looks at to figure out how to interpret the query|yes|
|dataSource|A String defining the data source to query, very similar to a table in a relational database|yes|
|intervals|A JSON Object representing ISO-8601 Intervals. This defines the time ranges to run the query over.|yes|
|merge|Merge all individual segment metadata results into a single result|no|
|context|An additional JSON Object which can be used to specify certain flags.|no|

The format of the result is:

```json
[ {
  "id" : "some_id",
  "intervals" : [ "2013-05-13T00:00:00.000Z/2013-05-14T00:00:00.000Z" ],
  "columns" : {
    "__time" : { "type" : "LONG", "size" : 407240380, "cardinality" : null },
    "dim1" : { "type" : "STRING", "size" : 100000, "cardinality" : 1944 },
    "dim2" : { "type" : "STRING", "size" : 100000, "cardinality" : 1504 },
    "metric1" : { "type" : "FLOAT", "size" : 100000, "cardinality" : null }
  },
  "size" : 300000
} ]
```
