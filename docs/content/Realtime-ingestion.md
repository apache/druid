---
layout: doc_page
---

Realtime Data Ingestion
=======================
For general Real-time Node information, see [here](Realtime.html).

For Real-time Node Configuration, see [Realtime Configuration](Realtime-Config.html).

For writing your own plugins to the real-time node, see [Firehose](Firehose.html).

There are two ways of ingesting real-time data. This can be achieved with a standalone real-time node, or using the [Tranquility](https://github.com/metamx/tranquility) client library as part of the [Indexing Service](Indexing-Service.html). For a full explanation of why there are two methods, please see [this link](https://groups.google.com/forum/#!searchin/druid-development/fangjin$20yang$20%22thoughts%22/druid-development/aRMmNHQGdhI/muBGl0Xi_wgJ). If you are comfortable with the limitations of standalone real-time nodes, you can use them as they are easier to set up. The indexing service is a more robust and highly available solution but will also require more effort to set up.

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
        "type": "kafka-0.7.2",
        "consumerProps": {
          "zk.connect": "localhost:2181",
          "zk.connectiontimeout.ms": "15000",
          "zk.sessiontimeout.ms": "15000",
          "zk.synctime.ms": "5000",
          "groupid": "druid-example",
          "fetch.size": "1048586",
          "autooffset.reset": "largest",
          "autocommit.enable": "false"
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

See [Ingestion](Ingestion.html)

### IOConfig

This field is required.

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|type|String|This should always be 'realtime'.|yes|
|firehose|JSON Object|Where the data is coming from. Described in detail below.|yes|
|plumber|JSON Object|Where the data is going. Described in detail below.|yes|

#### Firehose

See [Firehose](Firehose.html) for more information on firehose configuration.

#### Plumber

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|type|String|This should always be 'realtime'.|no|

### TuningConfig

The tuningConfig is optional and default parameters will be used if no tuningConfig is specified.

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|type|String|This should always be 'realtime'.|no|
|maxRowsInMemory|Integer|The number of rows to aggregate before persisting. This number is the post-aggregation rows, so it is not equivalent to the number of input events, but the number of aggregated rows that those events result in. This is used to manage the required JVM heap size.|no (default == 5 million)|
|windowPeriod|ISO 8601 Period String|The amount of lag time to allow events. This is configured with a 10 minute window, meaning that any event more than 10 minutes ago will be thrown away and not included in the segment generated by the realtime server.|no (default == PT10m)|
|intermediatePersistPeriod|ISO8601 Period String|The period that determines the rate at which intermediate persists occur. These persists determine how often commits happen against the incoming realtime stream. If the realtime data loading process is interrupted at time T, it should be restarted to re-read data that arrived at T minus this period.|no (default == PT10m)|
|basePersistDirectory|String|The directory to put things that need persistence. The plumber is responsible for the actual intermediate persists and this tells it where to store those persists.|no (default == java tmp dir)|
|versioningPolicy|Object|How to version segments.|no (default == based on segment start time)|
|rejectionPolicy|Object|Controls how data sets the data acceptance policy for creating and handing off segments. More on this below.|no (default=='serverTime')|
|maxPendingPersists|Integer|How many persists a plumber can do concurrently without starting to block.|no (default == 0)|
|shardSpec|Object|This describes the shard that is represented by this server. This must be specified properly in order to have multiple realtime nodes indexing the same data stream in a sharded fashion.|no (default == 'NoneShardSpec'|

#### Rejection Policy

The following policies are available:
    * `serverTime` &ndash; The recommended policy for "current time" data, it is optimal for current data that is generated and ingested in real time. Uses `windowPeriod` to accept only those events that are inside the window looking forward and back.
    * `messageTime` &ndash; Can be used for non-"current time" as long as that data is relatively in sequence. Events are rejected if they are less than `windowPeriod` from the event with the latest timestamp. Hand off only occurs if an event is seen after the segmentGranularity and `windowPeriod`.
    * `none` &ndash; Never hands off data unless shutdown() is called on the configured firehose.

#### Sharding Real-time Ingestion

The `shardSpec` can be used to shard real-time ingestion. Different shardSpecs are required for different real-time nodes for the same dataSource. For example:

```json
"shardSpec" : {
  "type" : "linear",
  "partitionNumber" : 0
}
```

Other real-time nodes for the same dataSource will have monotonically increasing shardSpec numbers.

## Realtime Ingestion using the Indexing Service

We strongly recommend using the client library [Tranquility](https://github.com/metamx/tranquility) for this use case. Please read the documentation on the Tranquility web page.

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

