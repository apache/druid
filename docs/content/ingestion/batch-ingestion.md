---
layout: doc_page
---

# Batch Data Ingestion
There are two choices for batch data ingestion to your Druid cluster, you can use the [Indexing service](../design/indexing-service.html) or you can use the `HadoopDruidIndexer`.

Which should I use?
-------------------

The [Indexing service](../design/indexing-service.html) is a set of nodes that can run as part of your Druid cluster and can accomplish a number of different types of indexing tasks. Even if all you care about is batch indexing, it provides for the encapsulation of things like the [metadata store](../dependencies/metadata-storage.html) that is used for segment metadata and other things, so that your indexing tasks do not need to include such information. The indexing service was created such that external systems could programmatically interact with it and run periodic indexing tasks. Long-term, the indexing service is going to be the preferred method of ingesting data.

The `HadoopDruidIndexer` runs hadoop jobs in order to separate and index data segments. It takes advantage of Hadoop as a job scheduling and distributed job execution platform. It is a simple method if you already have Hadoop running and don’t want to spend the time configuring and deploying the [Indexing service](../design/indexing-service.html) just yet.

## Batch Ingestion using the HadoopDruidIndexer

The HadoopDruidIndexer can be run like so:

```
java -Xmx256m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -classpath lib/*:<hadoop_config_path> io.druid.cli.Main index hadoop <spec_file>
```

## Hadoop "specFile"

The spec\_file is a path to a file that contains JSON and an example looks like:

```json
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
    "type" : "hadoop",
    "inputSpec" : {
      "type" : "static",
      "paths" : "/MyDirectory/examples/indexing/wikipedia_data.json"
    },
    "metadataUpdateSpec" : {
      "type":"mysql",
      "connectURI" : "jdbc:mysql://localhost:3306/druid",
      "password" : "diurd",
      "segmentTable" : "druid_segments",
      "user" : "druid"
    },
    "segmentOutputPath" : "/MyDirectory/data/index/output"
  },
  "tuningConfig" : {
    "type" : "hadoop",
    "workingPath": "/tmp",
    "partitionsSpec" : {
      "type" : "dimension",
      "partitionDimension" : null,
      "targetPartitionSize" : 5000000,
      "maxPartitionSize" : 7500000,
      "assumeGrouped" : false,
      "numShards" : -1
    },
    "shardSpecs" : { },
    "leaveIntermediate" : false,
    "cleanupOnFailure" : true,
    "overwriteFiles" : false,
    "ignoreInvalidRows" : false,
    "jobProperties" : { },
    "combineText" : false,
    "persistInHeap" : false,
    "ingestOffheap" : false,
    "bufferSize" : 134217728,
    "aggregationBufferRatio" : 0.5,
    "rowFlushBoundary" : 300000
  }
}
```

### DataSchema

This field is required.

See [Ingestion](../ingestion/index.html)

### IOConfig

This field is required.

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|type|String|This should always be 'hadoop'.|yes|
|inputSpec|Object|a specification of where to pull the data in from. See below.|yes|
|segmentOutputPath|String|the path to dump segments into.|yes|
|metadataUpdateSpec|Object|a specification of how to update the metadata for the druid cluster these segments belong to.|yes|

#### InputSpec specification

There are multiple types of inputSpecs:

##### `static`

Is a type of inputSpec where a static path to where the data files are located is passed.

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|paths|Array of String|A String of input paths indicating where the raw data is located.|yes|

For example, using the static input paths:

```
"paths" : "s3n://billy-bucket/the/data/is/here/data.gz, s3n://billy-bucket/the/data/is/here/moredata.gz, s3n://billy-bucket/the/data/is/here/evenmoredata.gz"
```

##### `granularity`

Is a type of inputSpec that expects data to be laid out in a specific path format. Specifically, it expects it to be segregated by day in this directory format `y=XXXX/m=XX/d=XX/H=XX/M=XX/S=XX` (dates are represented by lowercase, time is represented by uppercase).

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|dataGranularity|Object|specifies the granularity to expect the data at, e.g. hour means to expect directories `y=XXXX/m=XX/d=XX/H=XX`.|yes|
|inputPath|String|Base path to append the expected time path to.|yes|
|filePattern|String|Pattern that files should match to be included.|yes|

For example, if the sample config were run with the interval 2012-06-01/2012-06-02, it would expect data at the paths

```
s3n://billy-bucket/the/data/is/here/y=2012/m=06/d=01/H=00
s3n://billy-bucket/the/data/is/here/y=2012/m=06/d=01/H=01
...
s3n://billy-bucket/the/data/is/here/y=2012/m=06/d=01/H=23
```
##### `dataSource`

It is a type of inputSpec that reads data already stored inside druid. It is useful for doing "re-indexing". A usecase would be that you ingested some data in some interval and at a later time you wanted to change granularity of rows or remove some columns from the data stored in druid.

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|ingestionSpec|Json Object|Specification of druid segments to be loaded. See below.|yes|
|maxSplitSize|Number|Enables combining multiple segments into single Hadoop InputSplit according to size of segments. Default is none. |no|

Here is what goes inside "ingestionSpec"
|Field|Type|Description|Required|
|dataSource|String|Druid dataSource name from which you are loading the data.|yes|
|interval|String|A string representing ISO-8601 Intervals.|yes|
|granularity|String|Defines the granularity of the query while loading data. Default value is "none".See [Granularities](../querying/granularities.html).|no|
|filter|Json|See [Filters](../querying/filters.html)|no|
|dimensions|Array of String|Name of dimension columns to load. By default, the list will be constructed from parseSpec. If parseSpec does not have explicit list of dimensions then all the dimension columns present in stored data will be read.|no|
|metrics|Array of String|Name of metric columns to load. By default, the list will be constructed from the "name" of all the configured aggregators.|no|


For example

```
"ingestionSpec" :
    {
        "dataSource": "wikipedia",
        "interval": "2014-10-20T00:00:00Z/P2W"
    }
```

##### `multi`

It is a composing inputSpec to combine two other input specs. It is useful for doing "delta ingestion". A usecase would be that you ingested some data in some interval and at a later time you wanted to "append" more data to that interval. You can use this inputSpec to combine `dataSource` and `static` (or others) input specs to add more data to an already indexed interval.

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
            "interval": "2014-10-20T00:00:00Z/P2W"
        }
    },
    {
        "type" : "static",
        "paths": "/path/to/more/wikipedia/data/"
    }
]
```


#### Metadata Update Job Spec

This is a specification of the properties that tell the job how to update metadata such that the Druid cluster will see the output segments and load them.


|Field|Type|Description|Required|
|-----|----|-----------|--------|
|type|String|"metadata" is the only value available.|yes|
|connectURI|String|A valid JDBC url to metadata storage.|yes|
|user|String|Username for db.|yes|
|password|String|password for db.|yes|
|segmentTable|String|Table to use in DB.|yes|

These properties should parrot what you have configured for your [Coordinator](../design/coordinator.html).

### TuningConfig

The tuningConfig is optional and default parameters will be used if no tuningConfig is specified.

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|workingPath|String|the working path to use for intermediate results (results between Hadoop jobs).|no (default == '/tmp/druid-indexing')|
|version|String|The version of created segments.|no (default == datetime that indexing starts at)|
|leaveIntermediate|leave behind files in the workingPath when job completes or fails (debugging tool).|no (default == false)|
|partitionsSpec|Object|a specification of how to partition each time bucket into segments, absence of this property means no partitioning will occur.More details below.|no (default == 'hashed'|
|maxRowsInMemory|Integer|The number of rows to aggregate before persisting. This number is the post-aggregation rows, so it is not equivalent to the number of input events, but the number of aggregated rows that those events result in. This is used to manage the required JVM heap size.|no (default == 5 million)|
|cleanupOnFailure|Boolean|Cleans up intermediate files when the job fails as opposed to leaving them around for debugging.|no (default == true)|
|overwriteFiles|Boolean|Override existing files found during indexing.|no (default == false)|
|ignoreInvalidRows|Boolean|Ignore rows found to have problems.|no (default == false)|
|useCombiner|Boolean|Use hadoop combiner to merge rows at mapper if possible.|no (default == false)|
|jobProperties|Object|a map of properties to add to the Hadoop job configuration.|no (default == null)|

### Partitioning specification

Segments are always partitioned based on timestamp (according to the granularitySpec) and may be further partitioned in
some other way depending on partition type. Druid supports two types of partitioning strategies: "hashed" (based on the
hash of all dimensions in each row), and "dimension" (based on ranges of a single dimension).

Hashed partitioning is recommended in most cases, as it will improve indexing performance and create more uniformly
sized data segments relative to single-dimension partitioning.

#### Hash-based partitioning

```json
  "partitionsSpec": {
     "type": "hashed",
     "targetPartitionSize": 5000000
   }
```

Hashed partitioning works by first selecting a number of segments, and then partitioning rows across those segments
according to the hash of all dimensions in each row. The number of segments is determined automatically based on the
cardinality of the input set and a target partition size.

The configuration options are:

|property|description|required?|
|--------|-----------|---------|
|type|type of partitionSpec to be used |"hashed"|
|targetPartitionSize|target number of rows to include in a partition, should be a number that targets segments of 500MB\~1GB.|either this or numShards|
|numShards|specify the number of partitions directly, instead of a target partition size. Ingestion will run faster, since it can skip the step necessary to select a number of partitions automatically.|either this or targetPartitionSize|

#### Single-dimension partitioning

```json
  "partitionsSpec": {
     "type": "dimension",
     "targetPartitionSize": 5000000
   }
```

Single-dimension partitioning works by first selecting a dimension to partition on, and then separating that dimension
into contiguous ranges. Each segment will contain all rows with values of that dimension in that range. For example,
your segments may be partitioned on the dimension "host" using the ranges "a.example.com" to "f.example.com" and
"f.example.com" to "z.example.com". By default, the dimension to use is determined automatically, although you can
override it with a specific dimension.

The configuration options are:

|property|description|required?|
|--------|-----------|---------|
|type|type of partitionSpec to be used |"dimension"|
|targetPartitionSize|target number of rows to include in a partition, should be a number that targets segments of 500MB\~1GB.|yes|
|maxPartitionSize|maximum number of rows to include in a partition. Defaults to 50% larger than the targetPartitionSize.|no|
|partitionDimension|the dimension to partition on. Leave blank to select a dimension automatically.|no|
|assumeGrouped|assume input data has already been grouped on time and dimensions. Ingestion will run faster, but can choose suboptimal partitions if the assumption is violated.|no|

### Remote Hadoop Cluster

If you have a remote Hadoop cluster, make sure to include the folder holding your configuration `*.xml` files in the classpath of the indexer.

Batch Ingestion Using the Indexing Service
------------------------------------------

Batch ingestion for the indexing service is done by submitting an [Index Task](../misc/tasks.html) (for datasets < 1G) or a [Hadoop Index Task](../misc/tasks.html). The indexing service can be started by issuing:

```
java -Xmx2g -Duser.timezone=UTC -Dfile.encoding=UTF-8 -classpath lib/*:config/overlord io.druid.cli.Main server overlord
```

This will start up a very simple local indexing service. For more complex deployments of the indexing service, see [here](../design/indexing-service.html).

The schema of the Hadoop Index Task contains a task "type" and a Hadoop Index Config. A sample Hadoop index task is shown below:

```json
{
  "type" : "index_hadoop",
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
      "type" : "hadoop",
      "inputSpec" : {
        "type" : "static",
        "paths" : "/MyDirectory/examples/indexing/wikipedia_data.json"
      }
    },
    "tuningConfig" : {
      "type": "hadoop"
    }
  }
}
```

### DataSchema

This field is required.

See [Ingestion](../ingestion/index.html)

### IOConfig

This field is required.

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|type|String|This should always be 'hadoop'.|yes|
|pathSpec|Object|a specification of where to pull the data in from|yes|

### TuningConfig

The tuningConfig is optional and default parameters will be used if no tuningConfig is specified. This is the same as the tuningConfig for the standalone Hadoop indexer. See above for more details.

### Running the Task

The Hadoop Index Config submitted as part of an Hadoop Index Task is identical to the Hadoop Index Config used by the `HadoopDruidIndexer` except that three fields must be omitted: `segmentOutputPath`, `workingPath`, `updaterJobSpec`. The Indexing Service takes care of setting these fields internally.

To run the task:

```
curl -X 'POST' -H 'Content-Type:application/json' -d @example_index_hadoop_task.json localhost:8090/druid/indexer/v1/task
```

If the task succeeds, you should see in the logs of the indexing service:

```
2013-10-16 16:38:31,945 INFO [pool-6-thread-1] io.druid.indexing.overlord.exec.TaskConsumer - Task SUCCESS: HadoopIndexTask...
```

### Remote Hadoop Cluster

If you have a remote Hadoop cluster, make sure to include the folder holding your configuration `*.xml` files in the classpath of the middle manager.

Having Problems?
----------------
Getting data into Druid can definitely be difficult for first time users. Please don't hesitate to ask questions in our IRC channel or on our [google groups page](https://groups.google.com/forum/#!forum/druid-development).
