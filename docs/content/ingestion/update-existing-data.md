---
layout: doc_page
---
# Updating Existing Data

Once you ingest some data in a dataSource for an interval and create Druid segments, you might want to make changes to 
the ingested data. There are several ways this can be done.

##### Updating Dimension Values

If you have a dimension where values need to be updated frequently, try first using [lookups](../querying/lookups.html). A 
classic use case of lookups is when you have an ID dimension stored in a Druid segment, and want to map the ID dimension to a 
human-readable String value that may need to be updated periodically.

##### Rebuilding Segments (Reindexing)

If lookups are not sufficient, you can entirely rebuild Druid segments for specific intervals of time. Rebuilding a segment 
is known as reindexing the data. For example, if you want to add or remove columns from your existing segments, or you want to 
change the rollup granularity of your segments, you will have to reindex your data.

We recommend keeping a copy of your raw data around in case you ever need to reindex your data.

##### Dealing with Delayed Events (Delta Ingestion)

If you have a batch ingestion pipeline and have delayed events come in and want to append these events to existing 
segments and avoid the overhead of rebuilding new segments with reindexing, you can use delta ingestion.

### Reindexing and Delta Ingestion with Hadoop Batch Ingestion

This section assumes the reader understands how to do batch ingestion using Hadoop. See 
[batch-ingestion](batch-ingestion.html) for more information. Hadoop batch-ingestion can be used for reindexing and delta ingestion.

Druid uses an `inputSpec` in the `ioConfig` to know where the data to be ingested is located and how to read it. 
For simple Hadoop batch ingestion, `static` or `granularity` spec types allow you to read data stored in deep storage.

There are other types of `inputSpec` to enable reindexing and delta ingestion.

#### `dataSource`

This is a type of `inputSpec` that reads data already stored inside Druid.

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|type|String.|This should always be 'dataSource'.|yes|
|ingestionSpec|JSON object.|Specification of Druid segments to be loaded. See below.|yes|
|maxSplitSize|Number|Enables combining multiple segments into single Hadoop InputSplit according to size of segments. Default is none. |no|

Here is what goes inside `ingestionSpec`:

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|dataSource|String|Druid dataSource name from which you are loading the data.|yes|
|intervals|List|A list of strings representing ISO-8601 Intervals.|yes|
|granularity|String|Defines the granularity of the query while loading data. Default value is "none". See [Granularities](../querying/granularities.html).|no|
|filter|JSON|See [Filters](../querying/filters.html)|no|
|dimensions|Array of String|Name of dimension columns to load. By default, the list will be constructed from parseSpec. If parseSpec does not have an explicit list of dimensions then all the dimension columns present in stored data will be read.|no|
|metrics|Array of String|Name of metric columns to load. By default, the list will be constructed from the "name" of all the configured aggregators.|no|
|ignoreWhenNoSegments|boolean|Whether to ignore this ingestionSpec if no segments were found. Default behavior is to throw error when no segments were found.|no|

For example

```json
"ioConfig" : {
  "type" : "hadoop",
  "inputSpec" : {
    "type" : "dataSource",
    "ingestionSpec" : {
      "dataSource": "wikipedia",
      "intervals": ["2014-10-20T00:00:00Z/P2W"]
    }
  },
  ...
}
```

#### `multi`

This is a composing inputSpec to combine other inputSpecs. This inputSpec is used for delta ingestion. 
Please note that delta ingestion is not an idempotent operation. We may add change things in future to make it idempotent.

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|children|Array of JSON objects|List of JSON objects containing other inputSpecs.|yes|

For example:

```json
"ioConfig" : {
  "type" : "hadoop",
  "inputSpec" : {
    "type" : "multi",
    "children": [
      {
        "type" : "dataSource",
        "ingestionSpec" : {
          "dataSource": "wikipedia",
          "intervals": ["2014-10-20T00:00:00Z/P2W"]
        }
      },
      {
        "type" : "static",
        "paths": "/path/to/more/wikipedia/data/"
      }
    ]  
  },
  ...
}
```

### Reindexing without Hadoop Batch Ingestion

This section assumes the reader understands how to do batch ingestion without Hadoop using the [IndexTask](../ingestion/tasks.html#index-task),  
which uses a "firehose" to know where and how to read the input data. [IngestSegmentFirehose](firehose.html#ingestsegmentfirehose) 
can be used to read data from segments inside Druid. Note that IndexTask is to be used for prototyping purposes only as 
it has to do all processing inside a single process and can't scale. Please use Hadoop batch ingestion for production 
scenarios dealing with more than 1GB of data.
