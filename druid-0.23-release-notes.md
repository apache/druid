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
- Added the SQL query id to response header for failed SQL query to aid in location the error messages (#11756)
- Added input type validation for DataSketches HLL (#12131)
- Improved JDBC logging (#11676)
- Added SQL functions MV_FILTER_ONLY and MV_FILTER_NONE to filter rows of multi-value string dimensions to include only the  supplied list of values or none of them respectively (#11650)
- Added ARRAY_CONCAT_AGG to aggregate array inputs together into a single array(#12226)
- Added the ability to authorize the usage of query context parameters (#12396)
- Improved query IDs to make it easier to link queryies and sub-queries for end-to-end visibility of a query (#11809)
- Added a safe divide function to protect against division by 0 (#11904)
- You can now add a query context to internally generated SegmentMetadata Querie (#11429)
- Added support for Druid complex types to the native expression processing system to make all Druid data to be usable within expressions (#11853)

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
Multi-dimension range partitioning allows users to partition their data on the ranges of any number of dimensions. It develops further on the concepts behind "single-dim" partitioning and is now arguably the most preferable secondary partitioning both for query performance and storage efficiency.
(#11848)
(#11973)

### Improved replace data behavior
In previous versions of Druid, if ingested data with `dropExisting` flag to replace data, Druid would retain the existing data for a time chunk if there was no new data to replace it. Now, if you set `dropExisting` to `true` in your `ioSpec` and ingest data for a time range that includes a time chunk with no data, Druid uses a tombstone to overshadow the existing data in the empty time chunk.
(#12137)

## SQL

### Human-readable and actionable SQL error messages
Till 0.22.1, if a SQL query is not supported, users get a very cryptic and unhelpful error message. With this change, error message will include exactly what part of their SQL query is not supported by druid. One such example is executing a scan query that is ordered on a dimension other than time column. 

(#11911)

### Cancel API for SQL queries
Users can now cancel SQL queries just like native queries can be cancelled. A new API has been added for cancelling SQL queries. Web-console now users this API to cancel SQL queries. Earlier, it only used to close the client connection while sql query keeps running in druid. 

(#11643)
(#11738)

### Improvements to SQL user experience

This release includes several additional improvements for SQL:

- You no longer need to include a trailing slash `/` for JDBC connections to druid(#11737)


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

## Cloud integrations

### Allow authenticating via Shared access resource for azure storage

[12266](https://github.com/apache/druid/pull/12266) 

## Other changes

# Security fixes

## Support for access control on setting query contexts

Today, any context params are allowed to users. This can cause 1) a bad UX if the context param is not matured yet or 2) even query failure or system fault in the worst case if a sensitive param is abused, ex) maxSubqueryRows. Druid now has an ability to limit context params per user role. That means, a query will fail if you have a context param set in the query that is not allowed to you. 

The context param authorization can be enabled using druid.auth.authorizeQueryContextParams. This is disabled by default to avoid any hassle when performing an upgrade.

[12396](https://github.com/apache/druid/pull/12396)

## 

# Performance improvements

### General performance

### Ingestion

- More accurate memory estimations while building an on-heap incremental index. Rather than using the max possible size of an aggregated row, Druid can now use (based on a task context flag) a closer estimate of the actual heap footprint of an aggregated row. This enables the indexer to fit more rows in memory before doing an intermediate persist. [12073](https://github.com/apache/druid/pull/12073)

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
