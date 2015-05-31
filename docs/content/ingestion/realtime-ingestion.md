---
layout: doc_page
---

Realtime Data Ingestion
=======================
For general Real-time Node information, see [here](../design/realtime.html).

For Real-time Node Configuration, see [Realtime Configuration](../configuration/realtime.html).

For writing your own plugins to the real-time node, see [Firehose](../ingestion/firehose.html).

There are two ways of ingesting real-time data. This can be achieved with a standalone real-time node, or using the [Tranquility](https://github.com/druid-io/tranquility) client library as part of the [Indexing Service](../design/indexing-service.html). For a full explanation of why there are two methods, please see [this link](https://groups.google.com/forum/#!searchin/druid-development/fangjin$20yang$20%22thoughts%22/druid-development/aRMmNHQGdhI/muBGl0Xi_wgJ). If you are comfortable with the limitations of standalone real-time nodes, you can use them as they are easier to set up. The indexing service is a more robust and highly available solution but will also require more effort to set up.

## Realtime Node Ingestion

Much of the configuration governing Realtime nodes and the ingestion of data is set in the Realtime spec file, discussed on this page.

<a id="realtime-specfile"></a>
## Realtime "specFile"

The property `druid.realtime.specFile` has the path of a file (absolute or relative path and file name) with realtime specifications in it. This "specFile" should be a JSON Array of JSON objects like the following:

```json
[
  {
    "dataSchema" : {
      "dataSource" : "wikipedia",
      "parser" : {
        "type" : "string",
        "parseSpec" : {
          "format" : "json",
          "timestampSpec" : {
            "column" : "timestamp",
            "format" : "auto"
          },
          "dimensionsSpec" : {
            "dimensions": ["page","language","user","unpatrolled","newPage","robot","anonymous","namespace","continent","country","region","city"],
            "dimensionExclusions" : [],
            "spatialDimensions" : []
          }
        }
      },
      "metricsSpec" : [{
        "type" : "count",
        "name" : "count"
      }, {
        "type" : "doubleSum",
        "name" : "added",
        "fieldName" : "added"
      }, {
        "type" : "doubleSum",
        "name" : "deleted",
        "fieldName" : "deleted"
      }, {
        "type" : "doubleSum",
        "name" : "delta",
        "fieldName" : "delta"
      }],
      "granularitySpec" : {
        "type" : "uniform",
        "segmentGranularity" : "DAY",
        "queryGranularity" : "NONE"
      }
    },
    "ioConfig" : {
      "type" : "realtime",
      "firehose": {
        "type": "kafka-0.8",
        "consumerProps": {
          "zookeeper.connect": "localhost:2181",
          "zookeeper.connection.timeout.ms" : "15000",
          "zookeeper.session.timeout.ms" : "15000",
          "zookeeper.sync.time.ms" : "5000",
          "group.id": "druid-example",
          "fetch.message.max.bytes" : "1048586",
          "auto.offset.reset": "largest",
          "auto.commit.enable": "false"
        },
        "feed": "wikipedia"
      },
      "plumber": {
        "type": "realtime"
      }
    },
    "tuningConfig": {
      "type" : "realtime",
      "maxRowsInMemory": 500000,
      "intermediatePersistPeriod": "PT10m",
      "windowPeriod": "PT10m",
      "basePersistDirectory": "\/tmp\/realtime\/basePersist",
      "rejectionPolicy": {
        "type": "serverTime"
      }
    }
  }
]
```

This is a JSON Array so you can give more than one realtime stream to a given node. The number you can put in the same process depends on the exact configuration. In general, it is best to think of each realtime stream handler as requiring 2-threads: 1 thread for data consumption and aggregation, 1 thread for incremental persists and other background tasks.

There are three parts to a realtime stream specification, `dataSchema`, `IOConfig`, and `tuningConfig` which we will go into here.

### DataSchema

This field is required.

See [Ingestion](../ingestion/index.html)

### IOConfig

This field is required.

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|type|String|This should always be 'realtime'.|yes|
|firehose|JSON Object|Where the data is coming from. Described in detail below.|yes|
|plumber|JSON Object|Where the data is going. Described in detail below.|yes|

#### Firehose

See [Firehose](../ingestion/firehose.html) for more information on firehose configuration.

#### Plumber

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|type|String|This should always be 'realtime'.|no|

### TuningConfig

The tuningConfig is optional and default parameters will be used if no tuningConfig is specified.

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|type|String|This should always be 'realtime'.|no|
|maxRowsInMemory|Integer|The number of rows to aggregate before persisting. This number is the post-aggregation rows, so it is not equivalent to the number of input events, but the number of aggregated rows that those events result in. This is used to manage the required JVM heap size. Maximum heap memory usage for indexing scales with maxRowsInMemory * (2 + maxPendingPersists).|no (default == 5 million)|
|windowPeriod|ISO 8601 Period String|The amount of lag time to allow events. This is configured with a 10 minute window, meaning that any event more than 10 minutes ago will be thrown away and not included in the segment generated by the realtime server.|no (default == PT10m)|
|intermediatePersistPeriod|ISO8601 Period String|The period that determines the rate at which intermediate persists occur. These persists determine how often commits happen against the incoming realtime stream. If the realtime data loading process is interrupted at time T, it should be restarted to re-read data that arrived at T minus this period.|no (default == PT10m)|
|basePersistDirectory|String|The directory to put things that need persistence. The plumber is responsible for the actual intermediate persists and this tells it where to store those persists.|no (default == java tmp dir)|
|versioningPolicy|Object|How to version segments.|no (default == based on segment start time)|
|rejectionPolicy|Object|Controls how data sets the data acceptance policy for creating and handing off segments. More on this below.|no (default=='serverTime')|
|maxPendingPersists|Integer|Maximum number of persists that can be pending, but not started. If this limit would be exceeded by a new intermediate persist, ingestion will block until the currently-running persist finishes. Maximum heap memory usage for indexing scales with maxRowsInMemory * (2 + maxPendingPersists).|no (default == 0; meaning one persist can be running concurrently with ingestion, and none can be queued up)|
|shardSpec|Object|This describes the shard that is represented by this server. This must be specified properly in order to have multiple realtime nodes indexing the same data stream in a [sharded fashion](#sharding).|no (default == 'NoneShardSpec'|

#### Rejection Policy

The following policies are available:

* `serverTime` &ndash; The recommended policy for "current time" data, it is optimal for current data that is generated and ingested in real time. Uses `windowPeriod` to accept only those events that are inside the window looking forward and back.
* `messageTime` &ndash; Can be used for non-"current time" as long as that data is relatively in sequence. Events are rejected if they are less than `windowPeriod` from the event with the latest timestamp. Hand off only occurs if an event is seen after the segmentGranularity and `windowPeriod`.
* `none` &ndash; Never hands off data unless shutdown() is called on the configured firehose.


####<a id="sharding"></a> Sharding
Druid uses shards, or segments with partition numbers, to more efficiently handle large amounts of incoming data. In Druid, shards represent the segments that together cover a time interval based on the value of `segmentGranularity`. If, for example, `segmentGranularity` is set to "hour", then a number of shards may be used to store the data for that hour. Sharding along dimensions may also occur to optimize efficiency.

Segments are identified by datasource, time interval, and version. With sharding, a segment is also identified by a partition number. Typically, each shard will have the same version but a different partition number to uniquely identify it.

In small-data scenarios, sharding is unnecessary and can be set to none (the default):

```json
    "shardSpec": {"type": "none"}
```

However, in scenarios with multiple realtime nodes, `none` is less useful as it cannot help with scaling data volume (see below). Note that for the batch indexing service, no explicit configuration is required; sharding is provided automatically.

Druid uses sharding based on the `shardSpec` setting you configure. The recommended choices, `linear` and `numbered`, are discussed below; other types have been useful for internal Druid development but are not appropriate for production setups.

Keep in mind, that sharding configuration has nothing to do with configured firehose. For example, if you set partition number to 0, it doesn't mean that Kafka firehose will consume only from 0 topic partition.

##### Linear
This strategy provides following advantages:

* There is no need to update the fileSpec configurations of existing nodes when adding new nodes.
* All unique shards are queried, regardless of whether the partition numbering is sequential or not (it allows querying of partitions 0 and 2, even if partition 1 is missing).

Configure `linear` under `schema`:

```json
    "shardSpec": {
        "type": "linear",
        "partitionNum": 0
    }
```
            

##### Numbered
This strategy is similar to `linear` except that it does not tolerate non-sequential partition numbering (it will *not* allow querying of partitions 0 and 2 if partition 1 is missing). It also requires explicitly setting the total number of partitions.

Configure `numbered` under `schema`:

```json
    "shardSpec": {
        "type": "numbered",
        "partitionNum": 0,
        "partitions": 2
    }
```
     

##### Scale and Redundancy
The `shardSpec` configuration can be used to create redundancy by having the same `partitionNum` values on different nodes.

For example, if RealTimeNode1 has:

```json
    "shardSpec": {
        "type": "linear",
        "partitionNum": 0
    }
```
            
and RealTimeNode2 has:

```json
    "shardSpec": {
        "type": "linear",
        "partitionNum": 0
    }
```

then two realtime nodes can store segments with the same datasource, version, time interval, and partition number. Brokers that query for data in such segments will assume that they hold the same data, and the query will target only one of the segments.

`shardSpec` can also help achieve scale. For this, add nodes with a different `partionNum`. Continuing with the example, if RealTimeNode3 has:

```json
    "shardSpec": {
        "type": "linear",
        "partitionNum": 1
    }
```

then it can store segments with the same datasource, time interval, and version as in the first two nodes, but with a different partition number. Brokers that query for data in such segments will assume that a segment from RealTimeNode3 holds *different* data, and the query will target it along with a segment from the first two nodes.

You can use type `numbered` similarly. Note that type `none` is essentially type `linear` with all shards having a fixed `partitionNum` of 0.


## Realtime Ingestion using the Indexing Service

We strongly recommend using the client library [Tranquility](https://github.com/druid-io/tranquility) for this use case. Please read the documentation on the Tranquility web page.

## Constraints

The following table summarizes constraints between settings in the spec file for the Realtime subsystem.

|Name|Effect|Minimum|Recommended|
|----|------|-------|-----------|
|windowPeriod| When reading a row, events with timestamp older than now minus this window are discarded | time jitter tolerance | use this to reject outliers |
|segmentGranularity| Time granularity (minute, hour, day, week, month) for loading data at query time | equal to indexGranularity| more than queryGranularity|
|queryGranularity| Time granularity (minute, hour, day, week, month) for rollup | less than segmentGranularity| minute, hour, day, week, month |
|intermediatePersistPeriod| The max time (ISO8601 Period) between flushes of ingested rows from memory to disk | avoid excessive flushing | number of un-persisted rows in memory also constrained by maxRowsInMemory |
|maxRowsInMemory| The max number of ingested rows to hold in memory before a flush to disk | number of un-persisted post-aggregation rows in memory is also constrained by intermediatePersistPeriod | use this to avoid running out of heap if too many rows in an intermediatePersistPeriod |

The normal, expected use cases have the following overall constraints: `queryGranularity < intermediatePersistPeriod =< windowPeriod  < segmentGranularity`

