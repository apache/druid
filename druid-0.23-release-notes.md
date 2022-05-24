Apache Druid 0.23.0 contains over 450 new features, bug fixes, performance enhancements, documentation improvements, and additional test coverage from 81 contributors. [See the complete set of changes for additional details](https://github.com/apache/druid/milestone/45?closed=1).

# New Features

## Query engine

### Grouping on arrays without exploding the arrays
You can now group on a multi-value dimension as an array. For a datasource named "test":
```
{"timestamp": "2011-01-12T00:00:00.000Z", "tags": ["t1","t2","t3"]}  #row1
{"timestamp": "2011-01-13T00:00:00.000Z", "tags": ["t3","t4","t5"]}  #row2
{"timestamp": "2011-01-14T00:00:00.000Z", "tags": ["t5","t6","t7"]}  #row3
{"timestamp": "2011-01-14T00:00:00.000Z", "tags": []}                #row4
```
The following query:
```json
{
  "queryType": "groupBy",
  "dataSource": "test",
  "intervals": [
    "1970-01-01T00:00:00.000Z/3000-01-01T00:00:00.000Z"
  ],
  "granularity": {
    "type": "all"
  },
  "virtualColumns" : [ {
    "type" : "expression",
    "name" : "v0",
    "expression" : "mv_to_array(\"tags\")",
    "outputType" : "ARRAY<STRING>"
  } ],
  "dimensions": [
    {
      "type": "default",
      "dimension": "v0",
      "outputName": "tags"
      "outputType":"ARRAY<STRING>"
    }
  ],
  "aggregations": [
    {
      "type": "count",
      "name": "count"
    }
  ]
}
```
Returns the following:

```
[
 {
    "timestamp": "1970-01-01T00:00:00.000Z",
    "event": {
      "count": 1,
      "tags": "[]"
    }
  },
  {
    "timestamp": "1970-01-01T00:00:00.000Z",
    "event": {
      "count": 1,
      "tags": "["t1","t2","t3"]"
    }
  },
  {
    "timestamp": "1970-01-01T00:00:00.000Z",
    "event": {
      "count": 1,
      "tags": "[t3","t4","t5"]"
    }
  },
  {
    "timestamp": "1970-01-01T00:00:00.000Z",
    "event": {
      "count": 2,
      "tags": "["t5","t6","t7"]"
    }
  }
]
```
(#12078)
(#12253)

### Specify a column other than __time column for row comparison in first/last aggregators

You can pass time column in `*first`/`*last` aggregators and LATEST / EARLIEST SQL functions. This provides support for cases where the time is stored as a part of a column different than "__time". You can also specify another logical time column.
(#11949)
(#12145)

### Improvements to querying user experience

This release includes several improvements for querying:
- Added the SQL query ID to response header for failed SQL query to aid in locating the error messages (#11756)
- Added input type validation for DataSketches HLL (#12131)
- Improved JDBC logging (#11676)
- Added SQL functions MV_FILTER_ONLY and MV_FILTER_NONE to filter rows of multi-value string dimensions to include only the  supplied list of values or none of them respectively (#11650)
- Added ARRAY_CONCAT_AGG to aggregate array inputs together into a single array (#12226)
- Added the ability to authorize the usage of query context parameters (#12396)
- Improved query IDs to make it easier to link queries and sub-queries for end-to-end query visibility (#11809)
- Added a safe divide function to protect against division by 0 (#11904)
- You can now add a query context to internally generated `SegmentMetadata` query (#11429)
- Added support for Druid complex types to the native expression processing system to make all Druid data usable within expressions (#11853, #12016)

## Streaming Ingestion

### Kafka input format for parsing headers and key
We've introduced a Kafka input format so you can ingest header data in addition to the message contents. For example:
- the event key field
- event headers
- the Kafka event timestamp
- the Kafka event value that stores the payload.

(#11630)

## Native Batch Ingestion

### Multi-dimension range partitioning
Multi-dimension range partitioning allows users to partition their data on the ranges of any number of dimensions. It develops further on the concepts behind "single-dim" partitioning and is now arguably the most preferable secondary partitioning, both for query performance and storage efficiency.
(#11848)
(#11973)

### Improved replace data behavior
In previous versions of Druid, if ingested data with `dropExisting` flag to replace data, Druid would retain the existing data for a time chunk if there was no new data to replace it. Now, if you set `dropExisting` to `true` in your `ioSpec` and ingest data for a time range that includes a time chunk with no data, Druid uses a tombstone to overshadow the existing data in the empty time chunk.
(#12137)

This release includes several improvements for native batch ingestion:
- Druid now emits a new metric when a batch task finishes waiting for segment availability. (#11090)
- Added `segmentAvailabilityWaitTimeMs`, the duration in milliseconds that a task waited for its segments to be handed off to Historical nodes, to `IngestionStatsAndErrorsTaskReportData` (#11090)
- Added functionality to preserve existing metrics during ingestion (#12185)
- Parallel native batch task can now provide task reports for the sequential and single phase mode (e.g., used with dynamic partitioning) as well as single phase mode subtasks (#11688)
- Added support for `RowStats` in `druid/indexer/v1/task/{task_id}/reports` API for multi-phase parallel indexing task (#12280)
- Fixed the OOM failures in the dimension distribution phase of parallel indexing (#12331)
- Added support to handle null dimension values while creating partition boundaries (#11973)

## Improvements to ingestion in general

This release includes several improvements for ingestion in general:
- Removed the template modifier from `IncrementalIndex<AggregatorType>` because it is no longer required
- You can now use `JsonPath` functions in `JsonPath` expressions during ingestion (#11722)
- Druid no longer creates a materialized list of segment files and elimited looping over the files to reduce OOM issues (#11903)
- Added an intermediate-persist `IndexSpec` to the main "merge" method in `IndexMerger` (#11940) 
- `Granularity.granularitiesFinerThan` now returns ALL if you pass in ALL (#12003)
- Added a configuation parameter for appending tasks to allow them to use a SHARED lock (#12041)
- `SchemaRegistryBasedAvroBytesDecoder` now throws a `ParseException` instead of RE when it fails to retrieve a schema (#12080)
- Added `includeAllDimensions` to `dimensionsSpec` to put all explicit dimensions first in `InputRow` and subsequently any other dimensions found in input data (#12276)
- Added the ability to store null columns in segments (#12279)

## Compaction

This release includes several improvements for compaction:
-  Automatic compaction now supports complex dimensions (#11924)
-  Automatic compaction now supports overlapping segment intervals (#12062)
- You can now configure automatic compaction to calculate the ratio of slots available for compaction tasks from maximum slots, including autoscaler maximum worker nodes (#12263)
- You can now configure the Coordinator auto compaction duty period separately from other indexing duties (#12263)

## SQL

### Human-readable and actionable SQL error messages
Until version 0.22.1, if you issued an unsupported SQL query, Druid would throw very cryptic and unhelpful error messages. With this change, error messages include exactly the part of the SQL query that is not supported in Druid. For example, if you run a scan query that is ordered on a dimension other than the time column. 

(#11911)

### Cancel API for SQL queries
We've added a new API to cancel SQL queries, so you can now cancel SQL queries just like you can cancel native queries. You can use the API from the web console. In previous versions, cancellation from the console only closed the client connection while the SQL query kept running on Druid. 

(#11643)
(#11738)
(#11710)

### Improvements to SQL user experience

This release includes several additional improvements for SQL:

- You no longer need to include a trailing slash `/` for JDBC connections to Druid (#11737)
- You can now use scans as outer queries (#11831)
- Added a class to sanitize JDBC exceptions and to log them (#11843)
- Added type headers to response format to make it easier for clients to interpret the results of SQL queries (#11914)
- Improved the way the `DruidRexExecutor` handles numeric arrays (#11968)
- Druid now returns an empty result after optimizing a GROUP BY query to a time series query (#12065)
- As an administrator, you can now configure the implementation for APPROX_COUNT_DISTINCT and COUNT(DISTINCT expr) in approximate mode (#11181)
- 


## Coordinator/Overlord

## Web console

- Query view can now cancel all queries issued from it (#11738)
- The auto refresh functions will now run in foreground only (#11750) this prevents forgotten background console tabs from putting any load on the cluster.
- Add a `Segment size` (in bytes) column to the Datasources view (#11797)
  
<img width="264" alt="image" src="https://user-images.githubusercontent.com/177816/167921423-b6ca2a24-4af7-4b48-bff7-3c16e3e9c398.png">

- Format numbers with commas in the query view (#12031)
- Add a JSON Diff view for supervisor specs (#12085)

![image](https://user-images.githubusercontent.com/177816/167922122-32466a67-0364-4f7e-93d0-0675abc29158.png)

- Improve the formatting and info contents of code auto suggestion docs (#12085)

![image](https://user-images.githubusercontent.com/177816/167922311-99c79f49-0aa1-4ae4-903d-097fae7ee3f4.png)

- Add shard detail column to segments view (#12212)

![image](https://user-images.githubusercontent.com/177816/167922493-8e621a41-631c-4eae-9005-359c5aab7473.png)

- Avoid refreshing tables if a menu is open (#12435)
- Misc other bug fixes and usability improvements 


## Metrics

### Query metrics now also set the `vectorized` dimension by default. This can be helpful in understanding performance profile of queries.

[12464](https://github.com/apache/druid/pull/12464)

### Auto-compaction duty also report duty metrics now. A dimension to indicate the duty group has also been added.

[12352](https://github.com/apache/druid/pull/12352) 

This release includes several additional improvements for metrics:
- Druid includes the Prometheus emitter by defult (#11812)
- Fixed the missing `conversionFactor` in Prometheus emitter (12338)
- Fixed an issue with the `ingest/events/messageGap` metric (#12337)
- Added metrics for Shenandoah GC (#12369)
- Added metrics as follows: `Cpu` and `CpuSet` to `java.util.metrics.cgroups`, `ProcFsUtil` for `procfs` info, and `CgroupCpuMonitor` and `CgroupCpuSetMonitor` (#11763)
- Added support to route data through an HTTP proxy (#11891)
- Added more metrics for Jetty server thread pool usage (#11113)
- Added worker category as a dimension TaskSlot metric of the indexing service (#11554)

## Cloud integrations

### Allow authenticating via Shared access resource for azure storage

[12266](https://github.com/apache/druid/pull/12266) 

## Other changes

- Druid now processes lookup load failures more quickly (#12397)
- `BalanceSegments#balanceServers` now exits early when there is no balancing work to do (#11768)
- `DimensionHandler` now allows you to define a `DimensionSpec` appropriate for the type of dimension to handle (#11873)
- Added an interface for external schema providers to Druid SQL (#12043)
- Added support for a SQL INSERT planner (#11959)

# Security fixes

## Support for access control on setting query contexts

Today, any context params are allowed to users. This can cause 1) a bad UX if the context param is not matured yet or 2) even query failure or system fault in the worst case if a sensitive param is abused, ex) maxSubqueryRows. Druid now has an ability to limit context params per user role. That means, a query will fail if you have a context param set in the query that is not allowed to you. 

The context parameter authorization can be enabled using Druid.`auth.authorizeQueryContextParam`s. This is disabled by default to enable a smoother upgrade experience.

(#12396)

## Other security improvements

This release includes several additional improvements for security:
- You can now optionally enable auhorization on Druid system tables (#11720)

## Performance improvements

### General performance

### Ingestion

- More accurate memory estimations while building an on-heap incremental index. Rather than using the maximum possible aggregated row size, Druid can now use (based on a task context flag) a closer estimate of the actual heap footprint of an aggregated row. This enables the indexer to fit more rows in memory before performing an intermediate persist. (#12073)

### SQL

# Bug fixes
Druid 0.23.0 contains over 68 bug fixes. You can find the complete list [here](https://github.com/apache/druid/issues?q=milestone%3A0.23.0+label%3ABug) 

# Upgrading to 0.23.0

Consider the following changes and updates when upgrading from Druid 0.22.x to 0.23.0. If you're updating from an earlier version than 0.22.1, see the release notes of the relevant intermediate versions.

# Developer notices

## updated airline dependency to 2.x

https://github.com/airlift/airline is no longer maintained and so druid has upgraded to https://github.com/rvesse/airline (Airline 2) to use an actively
maintained version, while minimizing breaking changes.

This is a backwards incompatible change, and custom extensions relying on the CliCommandCreator extension point will also need to be updated.

[12270](https://github.com/apache/druid/pull/12270)

# Known issues

For a full list of open issues, please see [Bug](https://github.com/apache/druid/labels/Bug) .

# Credits
Thanks to everyone who contributed to this release!

@2bethere
@317brian
@a2l007
@abhishekagarwal87
@adarshsanjeev
@aggarwalakshay
@AlexanderSaydakov
@AmatyaAvadhanula
@andreacyc
@ApoorvGuptaAi
@arunramani
@asdf2014
@AshishKapoor
@benkrug
@capistrant
@Caroline1000
@cheddar
@chenhuiyeh
@churromorales
@clintropolis
@cryptoe
@davidferlay
@dbardbar
@dependabot[bot]
@didip
@dkoepke
@dungdm93
@ektravel
@emirot
@FrankChen021
@gianm
@hqx871
@iMichka
@imply-cheddar
@isandeep41
@IvanVan
@jacobtolar
@jasonk000
@jgoz
@jihoonson
@jon-wei
@josephglanville
@joyking7
@kfaraz
@klarose
@LakshSingla
@liran-funaro
@lokesh-lingarajan
@loquisgon
@mark-imply
@maytasm
@mchades
@nikhil-ddu
@paul-rogers
@petermarshallio
@pjain1
@pjfanning
@rohangarg
@samarthjain
@sergioferragut
@shallada
@somu-imply
@sthetland
@suneet-s
@syacobovitz
@Tassatux
@techdocsmith
@tejaswini-imply
@themarcelor
@TSFenwick
@uschindler
@v-vishwa
@Vespira
@vogievetsky
@vtlim
@wangxiaobaidu11
@williamhyun
@wjhypo
@xvrl
@yuanlihan
@zachjsh
