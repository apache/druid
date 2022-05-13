Apache Druid 0.23.0 contains over 450 new features, bug fixes, performance enhancements, documentation improvements, and additional test coverage from 81 contributors. [See the complete set of changes for additional details](https://github.com/apache/druid/milestone/45?closed=1).

# New Features

## Query engine

### Grouping on arrays without exploding the arrays
[12078](https://github.com/apache/druid/pull/12078)

### Specify a column other than __time column for row comparison in first/last aggregators
[11949](https://github.com/apache/druid/pull/11949)

## Streaming Ingestion

### Kafka Input format for parsing headers and key
[11630](https://github.com/apache/druid/pull/11630)

## Native Batch Ingestion

### Multi-dimensional range partitioning
Multi-dimensional range partitioning allows users to partition their data on the ranges of any number of dimensions. It develops further on the concepts behind "single-dim" partitioning and is now arguably the most preferable secondary partitioning both for query performance and storage efficiency.
[11848](https://github.com/apache/druid/pull/11848)
[11973](https://github.com/apache/druid/pull/11973)

## SQL

### Human-readable and actionable SQL error messages
Till 0.22.1, if a SQL query is not supported, users get a very cryptic and unhelpful error message. With this change, error message will include exactly what part of their SQL query is not supported by druid. One such example is executing a scan query that is ordered on a dimension other than time column. 
[11911](https://github.com/apache/druid/pull/11911)

### Cancel API for SQL queries
Users can now cancel SQL queries just like native queries can be cancelled. A new API has been added for cancelling SQL queries. Web-console now users this API to cancel SQL queries. Earlier, it only used to close the client connection while sql query keeps running in druid. 

[11643](https://github.com/apache/druid/pull/11643)
[11738](https://github.com/apache/druid/pull/11738)

## Coordinator/Overlord

## Web console

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

- More accurate memory estimations while building an on-heap incremental index. Rather than using the max possible size of an aggregated row, Druid can now use (based on a task context flag) a closer estimate of the actual heap footprint of an aggregated row. This enables the indexer to fit more rows in memory before doing an intermediate persist.

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
