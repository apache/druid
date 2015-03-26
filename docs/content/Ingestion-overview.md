---
layout: doc_page
---
# Ingestion Overview

There are a couple of different ways to get data into Druid. We hope to unify things in the near future, but for the time being
the method you choose to ingest your data into Druid should be driven by your use case.

## Streaming Data

If you have a continuous stream of data, there are a few options to get your data into Druid. It should be noted that the current state of real-time ingestion in Druid does not guarantee exactly once processing. The real-time pipeline is meant to surface insights on
 events as they are occurring. For an accurate copy of ingested data, an accompanying batch pipeline is required. We are working towards a streaming only word, but for
 the time being, we recommend running a lambda architecture.

### Ingest from a Stream Processor

If you process your data using a stream processor such as Apache Samza or Apache Storm, you can use the [Tranquility](https://github.com/metamx/tranquility) library to manage
your real-time ingestion. This setup requires using the indexing service for ingestion, which is what is used in production by many organizations that use Druid.

### Ingest from Apache Kafka

If you wish to ingest directly from Kafka using Tranquility, you will have to write a consumer that reads from Kafka and passes the data to Tranquility.
The other option is to use [standalone Realtime nodes](./Realtime.html).
It should be noted that standalone realtime nodes use the Kafka high level consumer, which imposes a few restrictions.

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

## Large Batch of Static Data

If you have a large batch of historical data that you want to load all at once into Druid, you should use Druid's built in support for
 Hadoop-based indexing. Hadoop-based indexing for large (> 1G) of batch data is the fastest way to load data into Druid. If you wish to avoid
 the Hadoop dependency, or if you do not have a Hadoop cluster present, you can look at using the [index task](). The index task will be much slower
 than Hadoop indexing for ingesting batch data.

One pattern that we've seen is to store raw events (or processed events) in deep storage (S3, HDFS, etc) and periodically run batch processing jobs over these raw events.
You can, for example, create a directory structure for your raw data, such as the following:

```
/prod/<dataSource>/v=1/y=2015/m=03/d=21/H=20/data.gz
/prod/<dataSource>/v=1/y=2015/m=03/d=21/H=21/data.gz
/prod/<dataSource>/v=1/y=2015/m=03/d=21/H=22/data.gz
```

In this example, hourly raw events are stored in individual gzipped files. Periodic batch processing jobs can then run over these files.

## Lambda Architecture

We recommend running a streaming real-time pipeline to run queries over events as they are occurring and a batch pipeline to perform periodic
cleanups of data.

## Sharding

Multiple segments may exist for the same interval of time for the same datasource. These segments form a `block` for an interval.
Depending on the type of `shardSpec` that is used to shard the data, Druid queries may only complete if a `block` is complete. That is to say, if a block consists of 3 segments, such as:

`sampleData_2011-01-01T02:00:00:00Z_2011-01-01T03:00:00:00Z_v1_0`

`sampleData_2011-01-01T02:00:00:00Z_2011-01-01T03:00:00:00Z_v1_1`

`sampleData_2011-01-01T02:00:00:00Z_2011-01-01T03:00:00:00Z_v1_2`

All 3 segments must be loaded before a query for the interval `2011-01-01T02:00:00:00Z_2011-01-01T03:00:00:00Z` completes.

The exception to this rule is with using linear shard specs. Linear shard specs do not force 'completeness' and queries can complete even if shards are not loaded in the system.
For example, if your real-time ingestion creates 3 segments that were sharded with linear shard spec, and only two of the segments were loaded in the system, queries would return results only for those 2 segments.
