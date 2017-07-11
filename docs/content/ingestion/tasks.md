---
layout: doc_page
---
# Tasks
Tasks are run on middle managers and always operate on a single data source. Tasks are submitted using [POST requests](../design/indexing-service.html).

There are several different types of tasks.

Segment Creation Tasks
----------------------

### Hadoop Index Task

See [batch ingestion](../ingestion/batch-ingestion.html).

### Index Task

The Index Task is a simpler variation of the Index Hadoop task that is designed to be used for smaller data sets. The task executes within the indexing service and does not require an external Hadoop setup to use. The grammar of the index task is as follows:

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

#### Task Properties

|property|description|required?|
|--------|-----------|---------|
|type|The task type, this should always be "index".|yes|
|id|The task ID. If this is not explicitly specified, Druid generates the task ID using the name of the task file and date-time stamp. |no|
|spec|The ingestion spec. See below for more details. |yes|

#### DataSchema

This field is required.

See [Ingestion](../ingestion/index.html)

#### IOConfig

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|The task type, this should always be "index".|none|yes|
|firehose|Specify a [Firehose](../ingestion/firehose.html) here.|none|yes|
|appendToExisting|Creates segments as additional shards of the latest version, effectively appending to the segment set instead of replacing it. This will only work if the existing segment set has extendable-type shardSpecs (which can be forced by setting 'forceExtendableShardSpecs' in the tuning config).|false|no|

#### TuningConfig

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
|forceGuaranteedRollup|Forces guaranteeing the [perfect rollup](./design/index.html). The perfect rollup optimizes the total size of generated segments and querying time while indexing time will be increased. This flag cannot be used with either `appendToExisting` of IOConfig or `forceExtendableShardSpecs`. For more details, see the below __Segment publishing modes__ section.|false|no|
|reportParseExceptions|If true, exceptions encountered during parsing will be thrown and will halt ingestion; if false, unparseable rows and fields will be skipped.|false|no|
|publishTimeout|Milliseconds to wait for publishing segments. It must be >= 0, where 0 means to wait forever.|0|no|

#### IndexSpec

The indexSpec defines segment storage format options to be used at indexing time, such as bitmap type and column
compression formats. The indexSpec is optional and default parameters will be used if not specified.

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|bitmap|Object|Compression format for bitmap indexes. Should be a JSON object; see below for options.|no (defaults to Concise)|
|dimensionCompression|String|Compression format for dimension columns. Choose from `LZ4`, `LZF`, or `uncompressed`.|no (default == `LZ4`)|
|metricCompression|String|Compression format for metric columns. Choose from `LZ4`, `LZF`, `uncompressed`, or `none`.|no (default == `LZ4`)|
|longEncoding|String|Encoding format for metric and dimension columns with type long. Choose from `auto` or `longs`. `auto` encodes the values using offset or lookup table depending on column cardinality, and store them with variable size. `longs` stores the value as is with 8 bytes each.|no (default == `longs`)|

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

#### Segment publishing modes

While ingesting data using the Index task, it creates segments from the input data and publishes them. For segment publishing, the Index task supports two segment publishing modes, i.e., _bulk publishing mode_ and _incremental publishing mode_ for [perfect rollup and best-effort rollup](./design/index.html), respectively.

In the bulk publishing mode, every segment is published at the very end of the index task. Until then, created segments are stored in the memory and local storage of the node running the index task. As a result, this mode might cause a problem due to limited storage capacity, and is not recommended to use in production.

On the contrary, in the incremental publishing mode, segments are incrementally published, that is they can be published in the middle of the index task. More precisely, the index task collects data and stores created segments in the memory and disks of the node running that task until the total number of collected rows exceeds `maxTotalRows`. Once it exceeds, the index task immediately publishes all segments created until that moment, cleans all published segments up, and continues to ingest remaining data.

To enable bulk publishing mode, `forceGuaranteedRollup` should be set in the TuningConfig. Note that this option cannot be used with either `forceExtendableShardSpecs` of TuningConfig or `appendToExisting` of IOConfig.

Segment Merging Tasks
---------------------

### Append Task

Append tasks append a list of segments together into a single segment (one after the other). The grammar is:

```json
{
    "type": "append",
    "id": <task_id>,
    "dataSource": <task_datasource>,
    "segments": <JSON list of DataSegment objects to append>,
    "aggregations": <optional list of aggregators>
}
```

### Merge Task

Merge tasks merge a list of segments together. Any common timestamps are merged.
If rollup is disabled as part of ingestion, common timestamps are not merged and rows are reordered by their timestamp.

The grammar is:

```json
{
    "type": "merge",
    "id": <task_id>,
    "dataSource": <task_datasource>,
    "aggregations": <list of aggregators>,
    "rollup": <whether or not to rollup data during a merge>,
    "segments": <JSON list of DataSegment objects to merge>
}
```

### Same Interval Merge Task

Same Interval Merge task is a shortcut of merge task, all segments in the interval are going to be merged.

The grammar is:

```json
{
    "type": "same_interval_merge",
    "id": <task_id>,
    "dataSource": <task_datasource>,
    "aggregations": <list of aggregators>,
    "rollup": <whether or not to rollup data during a merge>,
    "interval": <DataSegment objects in this interval are going to be merged>
}
```

Segment Destroying Tasks
------------------------

### Kill Task

Kill tasks delete all information about a segment and removes it from deep storage. Killable segments must be disabled (used==0) in the Druid segment table. The available grammar is:

```json
{
    "type": "kill",
    "id": <task_id>,
    "dataSource": <task_datasource>,
    "interval" : <all_segments_in_this_interval_will_die!>
}
```

Misc. Tasks
-----------

### Version Converter Task
The convert task suite takes active segments and will recompress them using a new IndexSpec. This is handy when doing activities like migrating from Concise to Roaring, or adding dimension compression to old segments.

Upon success the new segments will have the same version as the old segment with `_converted` appended. A convert task may be run against the same interval for the same datasource multiple times. Each execution will append another `_converted` to the version for the segments

There are two types of conversion tasks. One is the Hadoop convert task, and the other is the indexing service convert task. The Hadoop convert task runs on a hadoop cluster, and simply leaves a task monitor on the indexing service (similar to the hadoop batch task). The indexing service convert task runs the actual conversion on the indexing service.

#### Hadoop Convert Segment Task
```json
{
  "type": "hadoop_convert_segment",
  "dataSource":"some_datasource",
  "interval":"2013/2015",
  "indexSpec":{"bitmap":{"type":"concise"},"dimensionCompression":"lz4","metricCompression":"lz4"},
  "force": true,
  "validate": false,
  "distributedSuccessCache":"hdfs://some-hdfs-nn:9000/user/jobrunner/cache",
  "jobPriority":"VERY_LOW",
  "segmentOutputPath":"s3n://somebucket/somekeyprefix"
}
```

The values are described below.

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|`type`|String|Convert task identifier|Yes: `hadoop_convert_segment`|
|`dataSource`|String|The datasource to search for segments|Yes|
|`interval`|Interval string|The interval in the datasource to look for segments|Yes|
|`indexSpec`|json|The compression specification for the index|Yes|
|`force`|boolean|Forces the convert task to continue even if binary versions indicate it has been updated recently (you probably want to do this)|No|
|`validate`|boolean|Runs validation between the old and new segment before reporting task success|No|
|`distributedSuccessCache`|URI|A location where hadoop should put intermediary files.|Yes|
|`jobPriority`|`org.apache.hadoop.mapred.JobPriority` as String|The priority to set for the hadoop job|No|
|`segmentOutputPath`|URI|A base uri for the segment to be placed. Same format as other places a segment output path is needed|Yes|


#### Indexing Service Convert Segment Task
```json
{
  "type": "convert_segment",
  "dataSource":"some_datasource",
  "interval":"2013/2015",
  "indexSpec":{"bitmap":{"type":"concise"},"dimensionCompression":"lz4","metricCompression":"lz4"},
  "force": true,
  "validate": false
}
```

|Field|Type|Description|Required (default)|
|-----|----|-----------|--------|
|`type`|String|Convert task identifier|Yes: `convert_segment`|
|`dataSource`|String|The datasource to search for segments|Yes|
|`interval`|Interval string|The interval in the datasource to look for segments|Yes|
|`indexSpec`|json|The compression specification for the index|Yes|
|`force`|boolean|Forces the convert task to continue even if binary versions indicate it has been updated recently (you probably want to do this)|No (false)|
|`validate`|boolean|Runs validation between the old and new segment before reporting task success|No (true)|

Unlike the hadoop convert task, the indexing service task draws its output path from the indexing service's configuration.

### Noop Task

These tasks start, sleep for a time and are used only for testing. The available grammar is:

```json
{
    "type": "noop",
    "id": <optional_task_id>,
    "interval" : <optional_segment_interval>,
    "runTime" : <optional_millis_to_sleep>,
    "firehose": <optional_firehose_to_test_connect>
}
```

Locking
-------

Once an overlord node accepts a task, a lock is created for the data source and interval specified in the task. 
Tasks do not need to explicitly release locks, they are released upon task completion. Tasks may potentially release 
locks early if they desire. Tasks ids are unique by naming them using UUIDs or the timestamp in which the task was created. 
Tasks are also part of a "task group", which is a set of tasks that can share interval locks.
