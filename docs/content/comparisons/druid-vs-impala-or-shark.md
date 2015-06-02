---
layout: doc_page
---

Druid vs Impala or Shark
========================

The question of Druid versus Impala or Shark basically comes down to your product requirements and what the systems were designed to do.  

Druid was designed to

1. be an always on service
1. ingest data in real-time
1. handle slice-n-dice style ad-hoc queries

Impala and Shark's primary design concerns (as far as I am aware) were to replace Hadoop MapReduce with another, faster, query layer that is completely generic and plays well with the other ecosystem of Hadoop technologies.  I will caveat this discussion with the statement that I am not an expert on Impala or Shark, nor am I intimately familiar with their roadmaps.  If anything is incorrect on this page, I'd be happy to change it, please send a note to the mailing list.

What does this mean?  We can talk about it in terms of four general areas

1. Fault Tolerance
1. Query Speed
1. Data Ingestion
1. Query Flexibility

## Fault Tolerance

Druid pulls segments down from [Deep Storage](../dependencies/deep-storage.html) before serving queries on top of it.  This means that for the data to exist in the Druid cluster, it must exist as a local copy on a historical node.  If deep storage becomes unavailable for any reason, new segments will not be loaded into the system, but the cluster will continue to operate exactly as it was when the backing store disappeared. 

Impala and Shark, on the other hand, pull their data in from HDFS (or some other Hadoop FileSystem) in response to a query.  This has implications for the operation of queries if you need to take HDFS down for a bit (say a software upgrade).  It's possible that data that has been cached in the nodes is still available when the backing file system goes down, but I'm not sure.

This is just one example, but Druid was built to continue operating in the face of failures of any one of its various pieces.  The [Design](../design/design.html) describes these design decisions from the Druid side in more detail.

## Query Speed

Druid takes control of the data given to it, storing it in a column-oriented fashion, compressing it and adding indexing structures.  All of which add to the speed at which queries can be processed.  The column orientation means that we only look at the data that a query asks for in order to compute the answer.  Compression increases the data storage capacity of RAM and allows us to fit more data into quickly accessible RAM.  Indexing structures mean that as you add boolean filters to your queries, we do less processing and you get your result faster, whereas a lot of processing engines do *more* processing when filters are added.

Impala/Shark can basically be thought of as daemon caching layers on top of HDFS.  They are processes that stay on even if there is no query running (eliminating the JVM startup costs from Hadoop MapReduce) and they have facilities to cache data locally so that it can be accessed and updated quicker.  But, I do not believe they go beyond caching capabilities to actually speed up queries.  So, at the end of the day, they don't change the paradigm from a brute-force, scan-everything query processing paradigm.

## Data Ingestion

Druid is built to allow for real-time ingestion of data.  You can ingest data and query it immediately upon ingestion, the latency between how quickly the event is reflected in the data is dominated by how long it takes to deliver the event to Druid.

Impala/Shark, being based on data in HDFS or some other backing store, are limited in their data ingestion rates by the rate at which that backing store can make data available.  Generally, the backing store is the biggest bottleneck for how quickly data can become available.

## Query Flexibility

Druid supports timeseries and groupBy style queries.  It doesn't have support for joins, which makes it a lot less flexible for generic processing.

Impala/Shark support SQL style queries with full joins.
