---
layout: doc_page
---
###How does Druid compare to Redshift?

In terms of drawing a differentiation, Redshift is essentially ParAccel (Actian) which Amazon is licensing.

Aside from potential performance differences, there are some functional differences:

###Real-time data ingestion

Because Druid is optimized to provide insight against massive quantities of streaming data; it is able to load and aggregate data in real-time.

Generally traditional data warehouses including column stores work only with batch ingestion and are not optimal for streaming data in regularly.

###Druid is a read oriented analytical data store

It’s write semantics aren’t as fluid and does not support joins. ParAccel is a full database with SQL support including joins and insert/update statements.

###Data distribution model

Druid’s data distribution, is segment based which exists on highly available "deep" storage, like S3 or HDFS. Scaling up (or down) does not require massive copy actions or downtime; in fact, losing any number of compute nodes does not result in data loss because new compute nodes can always be brought up by reading data from "deep" storage.

To contrast, ParAccel’s data distribution model is hash-based. Expanding the cluster requires re-hashing the data across the nodes, making it difficult to perform without taking downtime. Amazon’s Redshift works around this issue with a multi-step process:

* set cluster into read-only mode
* copy data from cluster to new cluster that exists in parallel
* redirect traffic to new cluster

###Replication strategy

Druid employs segment-level data distribution meaning that more nodes can be added and rebalanced without having to perform a staged swap. The replication strategy also makes all replicas available for querying.

ParAccel’s hash-based distribution generally means that replication is conducted via hot spares. This puts a numerical limit on the number of nodes you can lose without losing data, and this replication strategy often does not allow the hot spare to help share query load.

###Indexing strategy

Along with column oriented structures, Druid uses indexing structures to speed up query execution when a filter is provided. Indexing structures do increase storage overhead (and make it more difficult to allow for mutation), but they can also significantly speed up queries.

ParAccel does not appear to employ indexing strategies.
