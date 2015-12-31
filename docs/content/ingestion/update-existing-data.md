---
layout: doc_page
---

Once you ingest some data in a dataSource for an interval, You might want to make following kind of changes to existing data.

##### Reindexing
You ingested some raw data to a dataSource A and later you want to re-index and create another dataSource B which has a subset of columns or different granularity. Or, you may want to change granularity of data in A itself for some interval.

##### Delta-ingestion
You ingested some raw data to a dataSource A in an interval, later you want to "append" more data to same interval. This might happen because you used realtime ingestion originally and then received some late events. 

Here are the Druid Features you could use to achieve above updates.

- You can use batch ingestion to override an interval completely by doing the ingestion again with the raw data.
- You can use re-indexing and delta-ingestion features provided by batch ingestion.


### Re-indexing and Delta Ingestion with Hadoop Batch Ingestion

This section assumes the reader has understanding of batch ingestion using Hadoop. See [HadoopIndexTask](../misc/tasks.html#index-hadoop-task) and further explained in [batch-ingestion](batch-ingestion.md). You can use hadoop batch-ingestion to do re-indexing and delta-ingestion as well.

It is enabled by how Druid reads input data for doing hadoop batch ingestion. Druid uses specified `inputSpec` to know where the data to be ingested is located and how to read it. For simple hadoop batch ingestion you would use `static` or `granularity` spec types  which allow you to read data stored on HDFS.

There are two other `inputSpec` types to enable reindexing and delta-ingestion.

#### `dataSource`

It is a type of inputSpec that reads data already stored inside druid. It is useful for doing "re-indexing".

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|ingestionSpec|Json Object|Specification of druid segments to be loaded. See below.|yes|
|maxSplitSize|Number|Enables combining multiple segments into single Hadoop InputSplit according to size of segments. Default is none. |no|

Here is what goes inside "ingestionSpec"

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|dataSource|String|Druid dataSource name from which you are loading the data.|yes|
|intervals|List|A list of strings representing ISO-8601 Intervals.|yes|
|granularity|String|Defines the granularity of the query while loading data. Default value is "none".See [Granularities](../querying/granularities.html).|no|
|filter|Json|See [Filters](../querying/filters.html)|no|
|dimensions|Array of String|Name of dimension columns to load. By default, the list will be constructed from parseSpec. If parseSpec does not have explicit list of dimensions then all the dimension columns present in stored data will be read.|no|
|metrics|Array of String|Name of metric columns to load. By default, the list will be constructed from the "name" of all the configured aggregators.|no|

For example

```
"ingestionSpec" :
    {
        "dataSource": "wikipedia",
        "intervals": ["2014-10-20T00:00:00Z/P2W"]
    }
```

#### `multi`

It is a composing inputSpec to combine two other input specs. It is useful for doing delta ingestion. Note that this is not idempotent operation, we might add some features in future to make it idempotent.

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|children|Array of Json Objects|List of json objects containing other inputSpecs |yes|

For example

```
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
```

### Re-indexing with non-hadoop Batch Ingestion
This section assumes the reader has understanding of batch ingestion without hadoop using [IndexTask](../misc/tasks.html#index-task) which uses a "firehose" to know where and how to read the input data. [IngestSegmentFirehose](firehose.html#ingestsegmentfirehose) can be used to read data from segments inside Druid. Note that IndexTask is to be used for prototyping purposes only as it has to do all processing inside a single process and can't scale, please use hadoop batch ingestion for realistic scenarios such as dealing with data more than a GB.
