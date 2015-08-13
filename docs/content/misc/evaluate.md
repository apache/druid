---
layout: doc_page
---

Evaluate Druid
==============

This page is meant to help you in evaluating Druid by answering common questions that come up.

## Evaluating on a Single Machine

Most of the tutorials focus on running multiple Druid services on a single machine in an attempt to teach basic Druid concepts, and work out kinks in data ingestion. The configurations in the tutorials are
 very poor choices for an actual production cluster.

## Capacity and Cost Planning

The best way to understand what your cluster will cost is to first understand how much data reduction you will get when you create segments.
We recommend indexing and creating segments from 1G of your data and evaluating the resultant segment size. This will allow you to see how much your data rolls up, and how many segments will be able
to be loaded on the hardware you have at your disposal.

Most of the cost of a Druid cluster is in historical nodes, followed by real-time indexing nodes if you have a high data intake. For high availability, you should have backup
coordination nodes (coordinators and overlords). Coordination nodes should require much cheaper hardware than nodes that serve queries.

## Selecting Hardware

Druid is designed to run on commodity hardware and we've tried to provide some general guidelines on [how things should be tuned]() for various deployments. We've also provided
some [example specs](../configuration/production-cluster.html) for hardware for a production cluster.

## Benchmarking Druid

The best resource to benchmark Druid is to follow the steps outlined in our [blog post](http://druid.io/blog/2014/03/17/benchmarking-druid.html) about the topic.
The code to reproduce the results in the blog post are all open source. The blog post covers Druid queries on TPC-H data, but you should be able to customize
 configuration parameters to your data set. The blog post is a little outdated and uses an older version of Druid, but is still mostly relevant to demonstrate performance.

## Colocating Druid Processes for a POC

Not all Druid node processes need to run on separate machines. You can set up a small cluster with colocated processes to load several gigabytes of data. Please note this cluster is not highly available.

It is recommended you follow the [example production configuration](../configuration/production-cluster.html) for an actual production setup.

The deep storage to use in this POC example can be S3 or HDFS.

1. node1: [Coordinator](../design/coordinator.html) + metadata store + zookeeper. 
Example hardware: EC2 c3.2xlarge node (8 cores, Intel Xeon E5-2680 v2 @ 2.80GHz and 15GB of RAM).

See [here](../configuration/production-cluster.html) for the runtime.properties. Some example JVM configs for this hardware:

```
-server
-Xmx6g
-Xms6g
-XX:NewSize=512m
-XX:MaxNewSize=512m
-XX:+UseConcMarkSweepGC
```

2. node2: [Broker](../design/broker.html)
Example hardware: EC2 c3.2xlarge node (8 cores, Intel Xeon E5-2680 v2 @ 2.80GHz and 15GB of RAM). 
[Example configs](https://github.com/druid-io/druid-benchmark/tree/master/config) (see broker-* files).  

2. node3: [Historical](../design/historical.html).
Example hardware: EC2 m3.2xlarge instances (8 cores, Intel Xeon E5-2670 v2 @ 2.50GHz with 160GB SSD and 30GB of RAM)
[Example configs](https://github.com/druid-io/druid-benchmark/tree/master/config) (see compute-* files).
 
3. node4 (optional): [Real-time](../design/realtime.html) node or [Overlord](../design/indexing-service.html) (depending on how you choose to ingest data).
Example hardware: EC2 c3.2xlarge node (8 cores, Intel Xeon E5-2680 v2 @ 2.80GHz and 15GB of RAM).

For the real-time node, see [here](../configuration/production-cluster.html) for the runtime.properties. Use with the following JVM configs:

```
-server
-Xmx8g
-Xms8g
-XX:NewSize=1g
-XX:MaxNewSize=1g
-XX:+UseConcMarkSweepGC
```

For the overlord running in local mode to do all ingestion, see [here](../configuration/production-cluster.html) for the runtime.properties. Use with the following JVM configs:

```
-server
-Xmx2g
-Xms2g
-XX:NewSize=256m
-XX:MaxNewSize=256m
```

The size of the runner javaOpts can be bumped up:

```
druid.indexer.runner.javaOpts="-server -Xmx6g -Xms6g -XX:NewSize=256m -XX:MaxNewSize=256m"
```

The coordination pieces (coordinator, metadata store, ZK) can be colocated on the same node. These processes do not require many resources, even for reasonably large clusters.
 
You can add more historical nodes if your data doesn't fit on a single machine.

For small ingest workloads, you can run the overlord in local mode to load your data.
