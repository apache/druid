---
layout: doc_page
---

Stream Pull Ingestion
=====================

If you have an external service that you want to pull data from, you have two options. The simplest
option is to set up a "copying" service that reads from the data source and writes to Druid using
the [stream push method](stream-push.html).

Another option is *stream pull*. With this approach, a Druid Realtime Node ingests data from a
[Firehose](../ingestion/firehose.html) connected to the data you want to
read. The Druid quickstart and tutorials do not include information about how to set up standalone realtime nodes, but 
they can be used in place for Tranquility server and the indexing service. Please note that Realtime nodes have different properties and roles than the indexing service.

## Realtime Node Ingestion

Much of the configuration governing Realtime nodes and the ingestion of data is set in the Realtime spec file, discussed on this page.

For general Real-time Node information, see [here](../design/realtime.html).

For Real-time Node Configuration, see [Realtime Configuration](../configuration/realtime.html).

For writing your own plugins to the real-time node, see [Firehose](../ingestion/firehose.html).

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
      "maxRowsInMemory": 1000000,
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

See [Firehose](../ingestion/firehose.html) for more information on various firehoses.

#### Plumber

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|type|String|This should always be 'realtime'.|no|

### TuningConfig

The tuningConfig is optional and default parameters will be used if no tuningConfig is specified.

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|type|String|This should always be 'realtime'.|no|
|maxRowsInMemory|Integer|The number of rows to aggregate before persisting. This number is the post-aggregation rows, so it is not equivalent to the number of input events, but the number of aggregated rows that those events result in. This is used to manage the required JVM heap size. Maximum heap memory usage for indexing scales with maxRowsInMemory * (2 + maxPendingPersists).|no (default == 1000000)|
|maxBytesInMemory|Long|The maximum number of bytes to keep in memory to aggregate before persisting. This is used to manage the required JVM heap size.|no (default == One-sixth of max JVM memory)|
|windowPeriod|ISO 8601 Period String|The amount of lag time to allow events. This is configured with a 10 minute window, meaning that any event more than 10 minutes ago will be thrown away and not included in the segment generated by the realtime server.|no (default == PT10m)|
|intermediatePersistPeriod|ISO8601 Period String|The period that determines the rate at which intermediate persists occur. These persists determine how often commits happen against the incoming realtime stream. If the realtime data loading process is interrupted at time T, it should be restarted to re-read data that arrived at T minus this period.|no (default == PT10m)|
|basePersistDirectory|String|The directory to put things that need persistence. The plumber is responsible for the actual intermediate persists and this tells it where to store those persists.|no (default == java tmp dir)|
|versioningPolicy|Object|How to version segments.|no (default == based on segment start time)|
|rejectionPolicy|Object|Controls how data sets the data acceptance policy for creating and handing off segments. More on this below.|no (default == 'serverTime')|
|maxPendingPersists|Integer|Maximum number of persists that can be pending, but not started. If this limit would be exceeded by a new intermediate persist, ingestion will block until the currently-running persist finishes. Maximum heap memory usage for indexing scales with maxRowsInMemory * (2 + maxPendingPersists).|no (default == 0; meaning one persist can be running concurrently with ingestion, and none can be queued up)|
|shardSpec|Object|This describes the shard that is represented by this server. This must be specified properly in order to have multiple realtime nodes indexing the same data stream in a [sharded fashion](#sharding).|no (default == 'NoneShardSpec')|
|persistThreadPriority|int|If `-XX:+UseThreadPriorities` is properly enabled, this will set the thread priority of the persisting thread to `Thread.NORM_PRIORITY` plus this value within the bounds of `Thread.MIN_PRIORITY` and `Thread.MAX_PRIORITY`. A value of 0 indicates to not change the thread priority.|no (default == 0; inherit and do not override)|
|mergeThreadPriority|int|If `-XX:+UseThreadPriorities` is properly enabled, this will set the thread priority of the merging thread to `Thread.NORM_PRIORITY` plus this value within the bounds of `Thread.MIN_PRIORITY` and `Thread.MAX_PRIORITY`. A value of 0 indicates to not change the thread priority.|no (default == 0; inherit and do not override)|
|reportParseExceptions|Boolean|If true, exceptions encountered during parsing will be thrown and will halt ingestion. If false, unparseable rows and fields will be skipped. If an entire row is skipped, the "unparseable" counter will be incremented. If some fields in a row were parseable and some were not, the parseable fields will be indexed and the "unparseable" counter will not be incremented.|no (default == false)|
|handoffConditionTimeout|long|Milliseconds to wait for segment handoff. It must be >= 0, where 0 means to wait forever.|no (default == 0)|
|alertTimeout|long|Milliseconds timeout after which an alert is created if the task isn't finished by then. This allows users to monitor tasks that are failing to finish and give up the worker slot for any unexpected errors.|no (default == 0)|
|segmentWriteOutMediumFactory|String|Segment write-out medium to use when creating segments. See [Indexing Service Configuration](../configuration/indexing-service.html) page, "SegmentWriteOutMediumFactory" section for explanation and available options.|no (not specified by default, the value from `druid.peon.defaultSegmentWriteOutMediumFactory` is used)|
|dedupColumn|String|the column to judge whether this row is already in this segment, if so, throw away this row. If it is String type column, to reduce heap cost, use long type hashcode of this column's value to judge whether this row is already ingested, so there maybe very small chance to throw away a row that is not ingested before.|no (default == null)|
|indexSpec|Object|Tune how data is indexed. See below for more information.|no|

Before enabling thread priority settings, users are highly encouraged to read the [original pull request](https://github.com/druid-io/druid/pull/984) and other documentation about proper use of `-XX:+UseThreadPriorities`. 

#### Rejection Policy

The following policies are available:

* `serverTime` &ndash; The recommended policy for "current time" data, it is optimal for current data that is generated and ingested in real time. Uses `windowPeriod` to accept only those events that are inside the window looking forward and back.
* `messageTime` &ndash; Can be used for non-"current time" as long as that data is relatively in sequence. Events are rejected if they are less than `windowPeriod` from the event with the latest timestamp. Hand off only occurs if an event is seen after the segmentGranularity and `windowPeriod` (hand off will not periodically occur unless you have a constant stream of data).
* `none` &ndash; All events are accepted. Never hands off data unless shutdown() is called on the configured firehose.

#### IndexSpec

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|bitmap|Object|Compression format for bitmap indexes. Should be a JSON object; see below for options.|no (defaults to Concise)|
|dimensionCompression|String|Compression format for dimension columns. Choose from `LZ4`, `LZF`, or `uncompressed`.|no (default == `LZ4`)|
|metricCompression|String|Compression format for metric columns. Choose from `LZ4`, `LZF`, or `uncompressed`.|no (default == `LZ4`)|

##### Bitmap types

For Concise bitmaps:

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|type|String|Must be `concise`.|yes|

For Roaring bitmaps:

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|type|String|Must be `roaring`.|yes|
|compressRunOnSerialization|Boolean|Use a run-length encoding where it is estimated as more space efficient.|no (default == `true`)|

#### Sharding

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

## Constraints

The following table summarizes constraints between settings in the spec file for the Realtime subsystem.

|Name|Effect|Minimum|Recommended|
|----|------|-------|-----------|
|windowPeriod| When reading a row, events with timestamp older than now minus this window are discarded | time jitter tolerance | use this to reject outliers |
|segmentGranularity| Time granularity (minute, hour, day, week, month) for loading data at query time | equal to indexGranularity| more than queryGranularity|
|queryGranularity| Time granularity (minute, hour, day, week, month) for rollup | less than segmentGranularity| minute, hour, day, week, month |
|intermediatePersistPeriod| The max time (ISO8601 Period) between flushes of ingested rows from memory to disk | avoid excessive flushing | number of un-persisted rows in memory also constrained by maxRowsInMemory |
|maxRowsInMemory| The max number of ingested rows to hold in memory before a flush to disk. Normally user does not need to set this, but depending on the nature of data, if rows are short in terms of bytes, user may not want to store a million rows in memory and this value should be set| number of un-persisted post-aggregation rows in memory is also constrained by intermediatePersistPeriod | use this to avoid running out of heap if too many rows in an intermediatePersistPeriod |
|maxBytesInMemory| The number of bytes to keep in memory before a flush to disk. Normally this is computed internally and user does not need to set it. This is based on a rough estimate of memory usage and not actual usage. The maximum heap memory usage for indexing is maxBytesInMemory * (2 + maxPendingPersists)| number of un-persisted post-aggregation bytes in memory is also constrained by intermediatePersistPeriod | use this to avoid running out of heap if too many rows in an intermediatePersistPeriod |

The normal, expected use cases have the following overall constraints: `intermediatePersistPeriod ≤ windowPeriod < segmentGranularity` and `queryGranularity ≤ segmentGranularity`

## Limitations

### Kafka

Standalone realtime nodes use the Kafka high level consumer, which imposes a few restrictions.

Druid replicates segment such that logically equivalent data segments are concurrently hosted on N nodes. If N–1 nodes go down, 
the data will still be available for querying. On real-time nodes, this process depends on maintaining logically equivalent 
data segments on each of the N nodes, which is not possible with standard Kafka consumer groups if your Kafka topic requires more than one consumer 
(because consumers in different consumer groups will split up the data differently).

For example, let's say your topic is split across Kafka partitions 1, 2, & 3 and you have 2 real-time nodes with linear shard specs 1 & 2. 
Both of the real-time nodes are in the same consumer group. Real-time node 1 may consume data from partitions 1 & 3, and real-time node 2 may consume data from partition 2. 
Querying for your data through the broker will yield correct results.

The problem arises if you want to replicate your data by creating real-time nodes 3 & 4. These new real-time nodes also 
have linear shard specs 1 & 2, and they will consume data from Kafka using a different consumer group. In this case, 
real-time node 3 may consume data from partitions 1 & 2, and real-time node 4 may consume data from partition 2. 
From Druid's perspective, the segments hosted by real-time nodes 1 and 3 are the same, and the data hosted by real-time nodes 
2 and 4 are the same, although they are reading from different Kafka partitions. Querying for the data will yield inconsistent 
results.

Is this always a problem? No. If your data is small enough to fit on a single Kafka partition, you can replicate without issues. 
Otherwise, you can run real-time nodes without replication.

Please note that druid will skip over event that failed its checksum and it is corrupt.

### Locking

Using stream pull ingestion with Realtime nodes together batch ingestion may introduce data override issues. For example, if you 
are generating hourly segments for the current day, and run a daily batch job for the current day's data, the segments created by 
the batch job will have a more recent version than most of the segments generated by realtime ingestion. If your batch job is indexing 
data that isn't yet complete for the day, the daily segment created by the batch job can override recent segments created by 
realtime nodes. A portion of data will appear to be lost in this case.

### Schema changes

Standalone realtime nodes require stopping a node to update a schema, and starting it up again for the schema to take effect. 
This can be difficult to manage at scale, especially with multiple partitions.

### Log management

Each standalone realtime node has its own set of logs. Diagnosing errors across many partitions across many servers may be 
difficult to manage and track at scale.

## Deployment Notes

Stream ingestion may generate a large number of small segments because it's difficult to optimize the segment size at
ingestion time. The number of segments will increase over time, and this might cause the query performance issue. 

Details on how to optimize the segment size can be found on [Segment size optimization](../../operations/segment-optimization.html).