---	
layout: doc_page	
---	

# Native batch ingestion

The "Index Task" is Druid's native batch ingestion mechanism. The task executes within the indexing service and does not require an external Hadoop setup to use. The grammar of the index task is as follows:

```json
{
  "type" : "index",
  "spec" : {
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
      "metricsSpec" : [
        {
          "type" : "count",
          "name" : "count"
        },
        {
          "type" : "doubleSum",
          "name" : "added",
          "fieldName" : "added"
        },
        {
          "type" : "doubleSum",
          "name" : "deleted",
          "fieldName" : "deleted"
        },
        {
          "type" : "doubleSum",
          "name" : "delta",
          "fieldName" : "delta"
        }
      ],
      "granularitySpec" : {
        "type" : "uniform",
        "segmentGranularity" : "DAY",
        "queryGranularity" : "NONE",
        "intervals" : [ "2013-08-31/2013-09-01" ]
      }
    },
    "ioConfig" : {
      "type" : "index",
      "firehose" : {
        "type" : "local",
        "baseDir" : "examples/indexing/",
        "filter" : "wikipedia_data.json"
       }
    },
    "tuningConfig" : {
      "type" : "index",
      "targetPartitionSize" : 5000000,
      "maxRowsInMemory" : 75000
    }
  }
}
```

## Task Properties

|property|description|required?|
|--------|-----------|---------|
|type|The task type, this should always be "index".|yes|
|id|The task ID. If this is not explicitly specified, Druid generates the task ID using task type, data source name, interval, and date-time stamp. |no|
|spec|The ingestion spec including the data schema, IOConfig, and TuningConfig. See below for more details. |yes|
|context|Context containing various task configuration parameters. See below for more details.|no|

## Task Priority

Druid's indexing tasks use locks for atomic data ingestion. Each lock is acquired for the combination of a dataSource and an interval. Once a task acquires a lock, it can write data for the dataSource and the interval of the acquired lock unless the lock is released or preempted. Please see [the below Locking section](#locking)

Each task has a priority which is used for lock acquisition. The locks of higher-priority tasks can preempt the locks of lower-priority tasks if they try to acquire for the same dataSource and interval. If some locks of a task are preempted, the behavior of the preempted task depends on the task implementation. Usually, most tasks finish as failed if they are preempted.

Tasks can have different default priorities depening on their types. Here are a list of default priorities. Higher the number, higher the priority.

|task type|default priority|
|---------|----------------|
|Realtime index task|75|
|Batch index task|50|
|Merge/Append/Compaction task|25|
|Other tasks|0|

You can override the task priority by setting your priority in the task context like below.

```json
"context" : {
  "priority" : 100
}
```

## DataSchema

This field is required.

See [Ingestion Spec DataSchema](../ingestion/ingestion-spec.html#dataschema)

## IOConfig

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|The task type, this should always be "index".|none|yes|
|firehose|Specify a [Firehose](../ingestion/firehose.html) here.|none|yes|
|appendToExisting|Creates segments as additional shards of the latest version, effectively appending to the segment set instead of replacing it. This will only work if the existing segment set has extendable-type shardSpecs (which can be forced by setting 'forceExtendableShardSpecs' in the tuning config).|false|no|

## TuningConfig

The tuningConfig is optional and default parameters will be used if no tuningConfig is specified. See below for more details.

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|The task type, this should always be "index".|none|yes|
|targetPartitionSize|Used in sharding. Determines how many rows are in each segment.|5000000|no|
|maxRowsInMemory|Used in determining when intermediate persists to disk should occur.|75000|no|
|maxTotalRows|Total number of rows in segments waiting for being published. Used in determining when intermediate publish should occur.|150000|no|
|numShards|Directly specify the number of shards to create. If this is specified and 'intervals' is specified in the granularitySpec, the index task can skip the determine intervals/partitions pass through the data. numShards cannot be specified if targetPartitionSize is set.|null|no|
|indexSpec|defines segment storage format options to be used at indexing time, see [IndexSpec](#indexspec)|null|no|
|maxPendingPersists|Maximum number of persists that can be pending but not started. If this limit would be exceeded by a new intermediate persist, ingestion will block until the currently-running persist finishes. Maximum heap memory usage for indexing scales with maxRowsInMemory * (2 + maxPendingPersists).|0 (meaning one persist can be running concurrently with ingestion, and none can be queued up)|no|
|forceExtendableShardSpecs|Forces use of extendable shardSpecs. Experimental feature intended for use with the [Kafka indexing service extension](../development/extensions-core/kafka-ingestion.html).|false|no|
|forceGuaranteedRollup|Forces guaranteeing the [perfect rollup](../ingestion/index.html#roll-up-modes). The perfect rollup optimizes the total size of generated segments and querying time while indexing time will be increased. This flag cannot be used with either `appendToExisting` of IOConfig or `forceExtendableShardSpecs`. For more details, see the below __Segment publishing modes__ section.|false|no|
|reportParseExceptions|If true, exceptions encountered during parsing will be thrown and will halt ingestion; if false, unparseable rows and fields will be skipped.|false|no|
|publishTimeout|Milliseconds to wait for publishing segments. It must be >= 0, where 0 means to wait forever.|0|no|
|segmentWriteOutMediumFactory|Segment write-out medium to use when creating segments. See [Additional Peon Configuration: SegmentWriteOutMediumFactory](../configuration/index.html#segmentwriteoutmediumfactory) for explanation and available options.|Not specified, the value from `druid.peon.defaultSegmentWriteOutMediumFactory` is used|no|

### IndexSpec

The indexSpec defines segment storage format options to be used at indexing time, such as bitmap type and column
compression formats. The indexSpec is optional and default parameters will be used if not specified.

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|bitmap|Object|Compression format for bitmap indexes. Should be a JSON object; see below for options.|no (defaults to Concise)|
|dimensionCompression|String|Compression format for dimension columns. Choose from `LZ4`, `LZF`, or `uncompressed`.|no (default == `LZ4`)|
|metricCompression|String|Compression format for metric columns. Choose from `LZ4`, `LZF`, `uncompressed`, or `none`.|no (default == `LZ4`)|
|longEncoding|String|Encoding format for metric and dimension columns with type long. Choose from `auto` or `longs`. `auto` encodes the values using offset or lookup table depending on column cardinality, and store them with variable size. `longs` stores the value as is with 8 bytes each.|no (default == `longs`)|

#### Bitmap types

For Concise bitmaps:

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|type|String|Must be `concise`.|yes|

For Roaring bitmaps:

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|type|String|Must be `roaring`.|yes|
|compressRunOnSerialization|Boolean|Use a run-length encoding where it is estimated as more space efficient.|no (default == `true`)|

## Segment publishing modes

While ingesting data using the Index task, it creates segments from the input data and publishes them. For segment publishing, the Index task supports two segment publishing modes, i.e., _bulk publishing mode_ and _incremental publishing mode_ for [perfect rollup and best-effort rollup](../ingestion/index.html#roll-up-modes), respectively.

In the bulk publishing mode, every segment is published at the very end of the index task. Until then, created segments are stored in the memory and local storage of the node running the index task. As a result, this mode might cause a problem due to limited storage capacity, and is not recommended to use in production.

On the contrary, in the incremental publishing mode, segments are incrementally published, that is they can be published in the middle of the index task. More precisely, the index task collects data and stores created segments in the memory and disks of the node running that task until the total number of collected rows exceeds `maxTotalRows`. Once it exceeds, the index task immediately publishes all segments created until that moment, cleans all published segments up, and continues to ingest remaining data.

To enable bulk publishing mode, `forceGuaranteedRollup` should be set in the TuningConfig. Note that this option cannot be used with either `forceExtendableShardSpecs` of TuningConfig or `appendToExisting` of IOConfig.
