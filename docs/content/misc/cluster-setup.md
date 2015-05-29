---
layout: doc_page
---

# Setting Up a Druid Cluster

A Druid cluster consists of various node types that need to be set up depending on your use case. See our [Design](../design/design.html) docs for a description of the different node types.

Minimum Physical Layout: Absolute Minimum
-----------------------------------------

As a special case, the absolute minimum setup is one of the standalone examples for real-time ingestion and querying; see [Examples](../tutorials/examples.html) that can easily run on one machine with one core and 1GB RAM. This layout can be set up to try some basic queries with Druid.

Minimum Physical Layout: Experimental Testing with 4GB of RAM
-------------------------------------------------------------

This layout can be used to load some data from deep storage onto a Druid historical node for the first time. A minimal physical layout for a 1 or 2 core machine with 4GB of RAM is:

1. node1: [Coordinator](../design/coordinator.html) + metadata service + zookeeper + [Historical](../design/historical.html)
2. transient nodes: [Indexing Service](../design/indexing-service.html)

This setup is only reasonable to prove that a configuration works. It would not be worthwhile to use this layout for performance measurement.

Comfortable Physical Layout: Pilot Project with Multiple Machines
-----------------------------------------------------------------

The machine size "flavors" are using AWS/EC2 terminology for descriptive purposes only and is not meant to imply that AWS/EC2 is required or recommended. Another cloud provider or your own hardware can also work.

A minimal physical layout not constrained by cores that demonstrates parallel querying and realtime, using AWS-EC2 "small"/m1.small (one core, with 1.7GB of RAM) or larger, no real-time, is:

1. node1: [Coordinator](../design/coordinator.html) (m1.small)
2. node2: metadata service (m1.small)
3. node3: zookeeper (m1.small)
4. node4: [Broker](../design/broker.html) (m1.small or m1.medium or m1.large)
5. node5: [Historical](../design/historical.html) (m1.small or m1.medium or m1.large)
6. node6: [Historical](../design/historical.html) (m1.small or m1.medium or m1.large)
7. node7: [Realtime](../design/realtime.html) (m1.small or m1.medium or m1.large)
8. transient nodes: [Indexing Service](../design/indexing-service.html)

This layout naturally lends itself to adding more RAM and core to Historical nodes, and to adding many more Historical nodes. Depending on the actual load, the Coordinator, metadata server, and Zookeeper might need to use larger machines.

High Availability Physical Layout
---------------------------------

The machine size "flavors" are using AWS/EC2 terminology for descriptive purposes only and is not meant to imply that AWS/EC2 is required or recommended. Another cloud provider or your own hardware can also work.

An HA layout allows full rolling restarts and heavy volume:

1. node1: [Coordinator](../design/coordinator.html) (m1.small or m1.medium or m1.large)
2. node2: [Coordinator](../design/coordinator.html) (m1.small or m1.medium or m1.large) (backup)
3. node3: metadata service (c1.medium or m1.large)
4. node4: metadata service (c1.medium or m1.large) (backup)
5. node5: zookeeper (c1.medium)
6. node6: zookeeper (c1.medium)
7. node7: zookeeper (c1.medium)
8. node8: [Broker](../design/broker.html) (m1.small or m1.medium or m1.large or m2.xlarge or m2.2xlarge or m2.4xlarge)
9. node9: [Broker](../design/broker.html) (m1.small or m1.medium or m1.large or m2.xlarge or m2.2xlarge or m2.4xlarge) (backup)
10. node10: [Historical](../design/historical.html) (m1.small or m1.medium or m1.large or m2.xlarge or m2.2xlarge or m2.4xlarge)
11. node11: [Historical](../design/historical.html) (m1.small or m1.medium or m1.large or m2.xlarge or m2.2xlarge or m2.4xlarge)
12. node12: [Realtime](../design/realtime.html) (m1.small or m1.medium or m1.large or m2.xlarge or m2.2xlarge or m2.4xlarge)
13. transient nodes: [Indexing Service](../design/indexing-service.html)

Sizing for Cores and RAM
------------------------

The Historical and Broker nodes will use as many cores as are available, depending on usage, so it is best to keep these on dedicated machines. The upper limit of effectively utilized cores is not well characterized yet and would depend on types of queries, query load, and the schema. Historical daemons should have a heap a size of at least 1GB per core for normal usage, but could be squeezed into a smaller heap for testing. Since in-memory caching is essential for good performance, even more RAM is better. Broker nodes will use RAM for caching, so they do more than just route queries.

The effective utilization of cores by Zookeeper, MySQL, and Coordinator nodes is likely to be between 1 and 2 for each process/daemon, so these could potentially share a machine with lots of cores. These daemons work with heap a size between 500MB and 1GB.

Storage
-------

Indexed segments should be kept in a permanent store accessible by all nodes like AWS S3 or HDFS or equivalent. Refer to [Deep-Storage](../dependencies/deep-storage.html) for more details on supported storage types.

Local disk ("ephemeral" on AWS EC2) for caching is recommended over network mounted storage (example of mounted: AWS EBS, Elastic Block Store) in order to avoid network delays during times of heavy usage. If your data center is suitably provisioned for networked storage, perhaps with separate LAN/NICs just for storage, then mounted might work fine.

Setup
-----

Setting up a cluster is essentially just firing up all of the nodes you want with the proper [configuration](../configuration/index.html). One thing to be aware of is that there are a few properties in the configuration that potentially need to be set individually for each process:

```
druid.server.type=historical|realtime
druid.host=someHostOrIPaddrWithPort
druid.port=8080
```

`druid.server.type` should be set to "historical" for your historical nodes and realtime for the realtime nodes. The Coordinator will only assign segments to a "historical" node and the broker has some intelligence around its ability to cache results when talking to a realtime node. This does not need to be set for the coordinator or the broker.

`druid.host` should be set to the hostname that can be used to talk to the given server process. Basically, someone should be able to send a request to http://${druid.host}:${druid.port}/ and actually talk to the process.

`druid.port` should be set to the port that the server should listen on.

Build/Run
---------

The simplest way to build and run from the repository is to run `mvn package` from the base directory and then take `druid-services/target/druid-services-*-selfcontained.jar` and push that around to your machines; the jar does not need to be expanded, and since it contains the main() methods for each kind of service, it is *not* invoked with java -jar. It can be run from a normal java command-line by just including it on the classpath and then giving it the main class that you want to run. For example one instance of the Historical node/service can be started like this:

```
java -Duser.timezone=UTC -Dfile.encoding=UTF-8 -cp services/target/druid-services-*-selfcontained.jar io.druid.cli.Main server historical
```

All Druid server nodes can be started with:

```
io.druid.cli.Main server <node_type>
```

The table below show the program arguments for the different node types.

|service|program arguments|
|-------|----------------|
|Realtime|realtime|
|Coordinator|coordinator|
|Broker|broker|
|Historical|historical|
