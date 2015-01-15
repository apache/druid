---
layout: doc_page
---


Realtime Data Ingestion
=======================
For general Real-time Node information, see [here](Realtime.html).

For Real-time Node Configuration, see [Realtime Configuration](Realtime-Config.html).

For writing your own plugins to the real-time node, see [Firehose](Firehose.html).

Much of the configuration governing Realtime nodes and the ingestion of data is set in the Realtime spec file, discussed on this page.


<a id="realtime-specfile"></a>
## Realtime "specFile"

The property `druid.realtime.specFile` has the path of a file (absolute or relative path and file name) with realtime specifications in it. This "specFile" should be a JSON Array of JSON objects like the following:

```json
[
  {
    "schema": {
      "dataSource": "dataSourceName",
      "aggregators": [
        {
          "type": "count",
          "name": "events"
        },
        {
          "type": "doubleSum",
          "name": "outColumn",
          "fieldName": "inColumn"
        }
      ],
      "indexGranularity": "minute",
      "shardSpec": {
        "type": "none"
      }
    },
    "config": {
      "maxRowsInMemory": 500000,
      "intermediatePersistPeriod": "PT10m"
    },
    "firehose": {
      "type": "kafka-0.7.2",
      "consumerProps": {
        "zk.connect": "zk_connect_string",
        "zk.connectiontimeout.ms": "15000",
        "zk.sessiontimeout.ms": "15000",
        "zk.synctime.ms": "5000",
        "groupid": "consumer-group",
        "fetch.size": "1048586",
        "autooffset.reset": "largest",
        "autocommit.enable": "false"
      },
      "feed": "your_kafka_topic",
      "parser": {
        "timestampSpec": {
          "column": "timestamp",
          "format": "iso"
        },
        "data": {
          "format": "json"
        },
        "dimensionExclusions": [
          "value"
        ]
      }
    },
    "plumber": {
      "type": "realtime",
      "windowPeriod": "PT10m",
      "segmentGranularity": "hour",
      "basePersistDirectory": "\/tmp\/realtime\/basePersist"
    }
  }
]
```

This is a JSON Array so you can give more than one realtime stream to a given node. The number you can put in the same process depends on the exact configuration. In general, it is best to think of each realtime stream handler as requiring 2-threads: 1 thread for data consumption and aggregation, 1 thread for incremental persists and other background tasks.

There are four parts to a realtime stream specification, `schema`, `config`, `firehose` and `plumber` which we will go into here.


### Schema

This describes the data schema for the output Druid segment. More information about concepts in Druid and querying can be found at [Concepts-and-Terminology](Concepts-and-Terminology.html) and [Querying](Querying.html).

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|aggregators|Array of Objects|The list of aggregators to use to aggregate colliding rows together.|yes|
|dataSource|String|The name of the dataSource that the segment belongs to.|yes|
|indexGranularity|String|The granularity of the data inside the segment. E.g. a value of "minute" will mean that data is aggregated at minutely granularity. That is, if there are collisions in the tuple (minute(timestamp), dimensions), then it will aggregate values together using the aggregators instead of storing individual rows.|yes|
|shardSpec|Object|This describes the shard that is represented by this server. This must be specified properly in order to have multiple realtime nodes indexing the same data stream in a [sharded fashion](#sharding).|no|


####<a id="sharding"></a> Sharding 
Druid uses shards, or partitioned segments, to more efficiently handle large amounts of incoming data. In Druid, shards represent the segments that together cover a time interval based on the value of `segmentGranularity`. If, for example, `segmentGranularity` is set to "hour", then a number of shards (segments) may be used to store the data for that hour. Sharding along dimensions may also occur to optimize efficiency.

Segments are identified by datasource, time interval, and version. When sharded, a segment is also identified by a partition number. Typically, each shard will have the same version but a different partition number to uniquely identify it.

In small-data scenarios, sharding is unnecessary and can be set to none (the default):

    "shardSpec": {"type": "none"}
    
However, in scenarios with multiple realtime nodes, `none` is less useful as it cannot provide scale (see below). Note that for the batch indexing service, no explicit configuration is required; sharding is provided automatically.

Druid uses sharding based on the `shardSpec` setting you configure. The recommended choices, `linear` and `numbered`, are discussed below; other types have been useful for internal Druid development but are not appropriate for production setups.
 
##### Linear
This strategy is highly recommended for the following reasons:

* There is no need to update the fileSpec configurations of existing nodes when adding new nodes.
* All unique shards are queried, regardless of whether the partition numbering is sequential or not (it allows querying of partitions 0 and 2, even if partition 1 is missing).

Configure `linear` under `schema`:

    "schema": {
    ...
        "shardSpec": {
            "type": "linear",
            "partitionNum": 0
        }
    }
            

##### Numbered
This strategy is similar to `linear` except that it does not tolerate non-sequential partition numbering (it will *not* allow querying of partitions 0 and 2 if partition 1 is missing). It also requires explicitly setting the total number of partitions.

Configure `numbered` under `schema`:

    "schema": {
    ...
        "shardSpec": {
            "type": "numbered",
            "partitionNum": 0,
            "partitions": 2
        }
    }
            

##### Scale and Redundancy
The `shardSpec` configuration can be used to create redundancy by having the same `partitionNum` values on different nodes.

For example, if RealTimeNode1 has:

    "schema": {
    ...
        "shardSpec": {
            "type": "linear",
            "partitionNum": 0
        }
    }
            
and RealTimeNode2 has:

    "schema": {
    ...
        "shardSpec": {
            "type": "linear",
            "partitionNum": 0
        }
    }

then two realtime nodes can store segments with the same datasource, version, time interval, and partition number. Brokers that query for data in such segments will assume that they hold the same data, and the query will target only one of the segments.

`shardSpec` can also help achieve scale. For this, add nodes with a different `partionNum`. Continuing with the example, if RealTimeNode3 has:

    "schema": {
    ...
        "shardSpec": {
            "type": "linear",
            "partitionNum": 1
        }
    }

then it can store segments with the same datasource, time interval, and version as in the first two nodes, but with a different partition number. Brokers that query for data in such segments will assume that a segment from RealTimeNode3 holds *different* data, and the query will target it along with a segment from the first two nodes.

You can use type `numbered` similarly. Note that type `none` is essentially type `linear` with all shards having a fixed `partitionNum` of 0.


### Config

This provides configuration for the data processing portion of the realtime stream processor.

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|intermediatePersistPeriod|ISO8601 Period String|The period that determines the rate at which intermediate persists occur. These persists determine how often commits happen against the incoming realtime stream. If the realtime data loading process is interrupted at time T, it should be restarted to re-read data that arrived at T minus this period.|yes|
|maxRowsInMemory|Number|The number of rows to aggregate before persisting. This number is the post-aggregation rows, so it is not equivalent to the number of input events, but the number of aggregated rows that those events result in. This is used to manage the required JVM heap size.|yes|


### Firehose
Firehoses describe the data stream source. See [Firehose](Firehose.html) for more information on firehose configuration.


### Plumber
The Plumber handles generated segments both while they are being generated and when they are "done". The configuration parameters in the example are:

* `type` specifies the type of plumber in terms of configuration schema. The Plumber configuration in the example is for the often-used RealtimePlumber.
* `windowPeriod` is the amount of lag time to allow events. The example configures a 10 minute window, meaning that any event more than 10 minutes ago will be thrown away and not included in the segment generated by the realtime server.
* `segmentGranularity` specifies the granularity of the segment, or the amount of time a segment will represent.
* `basePersistDirectory` is the directory to put things that need persistence. The plumber is responsible for the actual intermediate persists and this tells it where to store those persists.
* `rejectionPolicy` determines what events are rejected upon ingestion.

See [Plumber](Plumber.html) for a fuller discussion of Plumber configuration.

Constraints
-----------

The following table summarizes constraints between settings in the spec file for the Realtime subsystem.

|Name|Effect|Minimum|Recommended|
|----|------|-------|-----------|
| windowPeriod| when reading an InputRow, events with timestamp older than now minus this window are discarded | time jitter tolerance | use this to reject outliers |
| segmentGranularity| time granularity (minute, hour, day, week, month) for loading data at query time | equal to indexGranularity| more than indexGranularity|
| indexGranularity| time granularity (minute, hour, day, week, month) of indexes | less than segmentGranularity| minute, hour, day, week, month |
| intermediatePersistPeriod| the max real time (ISO8601 Period) between flushes of InputRows from memory to disk | avoid excessive flushing | number of un-persisted rows in memory also constrained by maxRowsInMemory |
| maxRowsInMemory| the max number of InputRows to hold in memory before a flush to disk | number of un-persisted post-aggregation rows in memory is also constrained by intermediatePersistPeriod | use this to avoid running out of heap if too many rows in an intermediatePersistPeriod |

The normal, expected use cases have the following overall constraints: `indexGranularity < intermediatePersistPeriod =< windowPeriod  < segmentGranularity`

If the Realtime Node process runs out of heap, try adjusting druid.computation.buffer.size property which specifies a size in bytes that must fit into the heap.
