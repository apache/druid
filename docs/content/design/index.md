---
layout: doc_page
---

# Druid Concepts

Druid is an open source data store designed for [OLAP](http://en.wikipedia.org/wiki/Online_analytical_processing) queries on event data.
This page is meant to provide readers with a high level overview of how Druid stores data, and the architecture of a Druid cluster.

## The Data

To frame our discussion, let's begin with an example data set (from online advertising):

    timestamp             publisher          advertiser  gender  country  click  price
    2011-01-01T01:01:35Z  bieberfever.com    google.com  Male    USA      0      0.65
    2011-01-01T01:03:63Z  bieberfever.com    google.com  Male    USA      0      0.62
    2011-01-01T01:04:51Z  bieberfever.com    google.com  Male    USA      1      0.45
    2011-01-01T01:00:00Z  ultratrimfast.com  google.com  Female  UK       0      0.87
    2011-01-01T02:00:00Z  ultratrimfast.com  google.com  Female  UK       0      0.99
    2011-01-01T02:00:00Z  ultratrimfast.com  google.com  Female  UK       1      1.53

This data set is composed of three distinct components. If you are acquainted with OLAP terminology, the following concepts should be familiar.

* **Timestamp column**: We treat timestamp separately because all of our queries
 center around the time axis.

* **Dimension columns**: Dimensions are string attributes of an event, and the columns most commonly used in filtering the data. 
We have four dimensions in our example data set: publisher, advertiser, gender, and country.
They each represent an axis of the data that we’ve chosen to slice across.

* **Metric columns**: Metrics are columns used in aggregations and computations. In our example, the metrics are clicks and price. 
Metrics are usually numeric values, and computations include operations such as count, sum, and mean. 
Also known as measures in standard OLAP terminology.

## Sharding the Data

Druid shards are called `segments` and Druid always first shards data by time. In our compacted data set, we can create two segments, one for each hour of data.

For example:

Segment `sampleData_2011-01-01T01:00:00:00Z_2011-01-01T02:00:00:00Z_v1_0` contains

     2011-01-01T01:00:00Z  ultratrimfast.com  google.com  Male   USA     1800        25     15.70
     2011-01-01T01:00:00Z  bieberfever.com    google.com  Male   USA     2912        42     29.18


Segment `sampleData_2011-01-01T02:00:00:00Z_2011-01-01T03:00:00:00Z_v1_0` contains

     2011-01-01T02:00:00Z  ultratrimfast.com  google.com  Male   UK      1953        17     17.31
     2011-01-01T02:00:00Z  bieberfever.com    google.com  Male   UK      3194        170    34.01

Segments are self-contained containers for the time interval of data they hold. Segments
contain data stored in compressed column orientations, along with the indexes for those columns. Druid queries only understand how to
scan segments.

Segments are uniquely identified by a datasource, interval, version, and an optional partition number.
Examining our example segments, the segments are named following this convention: `dataSource_interval_version_partitionNumber`

## Roll-up

The individual events in our example data set are not very interesting because there may be trillions of such events. 
However, summarizations of this type of data can yield many useful insights.
Druid summarizes this raw data at ingestion time using a process we refer to as "roll-up".
Roll-up is a first-level aggregation operation over a selected set of dimensions, equivalent to (in pseudocode):

    GROUP BY timestamp, publisher, advertiser, gender, country
      :: impressions = COUNT(1),  clicks = SUM(click),  revenue = SUM(price)

The compacted version of our original raw data looks something like this:

     timestamp             publisher          advertiser  gender country impressions clicks revenue
     2011-01-01T01:00:00Z  ultratrimfast.com  google.com  Male   USA     1800        25     15.70
     2011-01-01T01:00:00Z  bieberfever.com    google.com  Male   USA     2912        42     29.18
     2011-01-01T02:00:00Z  ultratrimfast.com  google.com  Male   UK      1953        17     17.31
     2011-01-01T02:00:00Z  bieberfever.com    google.com  Male   UK      3194        170    34.01

In practice, we see that rolling up data can dramatically reduce the size of data that needs to be stored (up to a factor of 100).
Druid will roll up data as it is ingested to minimize the amount of raw data that needs to be stored. 
This storage reduction does come at a cost; as we roll up data, we lose the ability to query individual events. Phrased another way,
the rollup granularity is the minimum granularity you will be able to explore data at and events are floored to this granularity. 
Hence, Druid ingestion specs define this granularity as the `queryGranularity` of the data. The lowest supported `queryGranularity` is millisecond.

### Roll-up modes

Druid supports two roll-up modes, i.e., _perfect roll-up_ and _best-effort roll-up_. In the perfect roll-up mode, Druid guarantees that input data are perfectly aggregated at ingestion time. Meanwhile, in the best-effort roll-up, input data might not be perfectly aggregated and thus there can be multiple segments holding the rows which should belong to the same segment with the perfect roll-up since they have the same dimension value and their timestamps fall into the same interval.

The perfect roll-up mode encompasses an additional preprocessing step to determine intervals and shardSpecs before actual data ingestion if they are not specified in the ingestionSpec. This preprocessing step usually scans the entire input data which might increase the ingestion time. The [Hadoop indexing task](./ingestion/batch-ingestion.html) always runs with this perfect roll-up mode.

On the contrary, the best-effort roll-up mode doesn't require any preprocessing step, but the size of ingested data might be larger than that of the perfect roll-up. All types of [streaming indexing (i.e., realtime index task, kafka indexing service, ...)](./ingestion/stream-ingestion.html) run with this mode.

Finally, the [native index task](./ingestion/tasks.html) supports both modes and you can choose either one which fits to your application.

## Indexing the Data

Druid gets its speed in part from how it stores data. Borrowing ideas from search infrastructure,
Druid creates immutable snapshots of data, stored in data structures highly optimized for analytic queries.

Druid is a column store, which means each individual column is stored separately. Only the columns that pertain to a query are used
in that query, and Druid is pretty good about only scanning exactly what it needs for a query.
Different columns can also employ different compression methods. Different columns can also have different indexes associated with them.

Druid indexes data on a per-shard (segment) level.

## Loading the Data

Druid has two means of ingestion, real-time and batch. Real-time ingestion in Druid is best effort. Exactly once semantics are not guaranteed with real-time ingestion in Druid, although we have it on our roadmap to support this.
Batch ingestion provides exactly once guarantees and segments created via batch processing will accurately reflect the ingested data.
One common approach to operating Druid is to have a real-time pipeline for recent insights, and a batch pipeline for the accurate copy of the data.

## Querying the Data

Druid's native query language is JSON over HTTP, although the community has contributed query libraries in [numerous languages](../development/libraries.html), including SQL.

Druid is designed to perform single table operations and does not currently support joins.
Many production setups do joins at ETL because data must be denormalized before loading into Druid.

## The Druid Cluster

A Druid Cluster is composed of several different types of nodes. Each node is designed to do a small set of things very well.

* **Historical Nodes** Historical nodes commonly form the backbone of a Druid cluster. Historical nodes download immutable segments locally and serve queries over those segments.
The nodes have a shared nothing architecture and know how to load segments, drop segments, and serve queries on segments.

* **Broker Nodes** Broker nodes are what clients and applications query to get data from Druid. Broker nodes are responsible for scattering queries and gathering and merging results.
Broker nodes know what segments live where.

* **Coordinator Nodes** Coordinator nodes manage segments on historical nodes in a cluster. Coordinator nodes tell historical nodes to load new segments, drop old segments, and move segments to load balance.

* **Real-time Processing** Real-time processing in Druid can currently be done using standalone realtime nodes or using the indexing service. The real-time logic is common between these two services.
Real-time processing involves ingesting data, indexing the data (creating segments), and handing segments off to historical nodes. Data is queryable as soon as it is
 ingested by the realtime processing logic. The hand-off process is also lossless; data remains queryable throughout the entire process.

### External Dependencies

Druid has a couple of external dependencies for cluster operations.

* **Zookeeper** Druid relies on Zookeeper for intra-cluster communication.

* **Metadata Storage** Druid relies on a metadata storage to store metadata about segments and configuration. Services that create segments write new entries to the metadata store
  and the coordinator nodes monitor the metadata store to know when new data needs to be loaded or old data needs to be dropped. The metadata store is not
  involved in the query path. MySQL and PostgreSQL are popular metadata stores for production, but Derby can be used for experimentation when you are running all druid nodes on a single machine.

* **Deep Storage** Deep storage acts as a permanent backup of segments. Services that create segments upload segments to deep storage and historical nodes download
segments from deep storage. Deep storage is not involved in the query path. S3 and HDFS are popular deep storages.

### High Availability Characteristics

Druid is designed to have no single point of failure. Different node types are able to fail without impacting the services of the other node types. To run a highly available Druid cluster, you should have at least 2 nodes of every node type running.

### Comprehensive Architecture

For a comprehensive look at Druid architecture, please read our [white paper](http://static.druid.io/docs/druid.pdf).

