---
layout: default
---
A Druid cluster consists of various node types that need to be set up depending on your use case. See our [Design](Design.html) docs for a description of the different node types.

Setup Scripts
-------------

One of our community members, [housejester](https://github.com/housejester/), contributed some scripts to help with setting up a cluster. Checkout the [github](https://github.com/housejester/druid-test-harness) and [wiki](https://github.com/housejester/druid-test-harness/wiki/Druid-Test-Harness).

Minimum Physical Layout: Absolute Minimum
-----------------------------------------

As a special case, the absolute minimum setup is one of the standalone examples for realtime ingestion and querying; see [Examples](Examples.html) that can easily run on one machine with one core and 1GB RAM. This layout can be set up to try some basic queries with Druid.

Minimum Physical Layout: Experimental Testing with 4GB of RAM
-------------------------------------------------------------

This layout can be used to load some data from deep storage onto a Druid compute node for the first time. A minimal physical layout for a 1 or 2 core machine with 4GB of RAM is:

1.  node1: [Master](Master.html) + metadata service + zookeeper + [Compute](Compute.html)
2.  transient nodes: indexer

This setup is only reasonable to prove that a configuration works. It would not be worthwhile to use this layout for performance measurement.

Comfortable Physical Layout: Pilot Project with Multiple Machines
-----------------------------------------------------------------

*The machine size “flavors” are using AWS/EC2 terminology for descriptive purposes only and is not meant to imply that AWS/EC2 is required or recommended. Another cloud provider or your own hardware can also work.*

A minimal physical layout not constrained by cores that demonstrates parallel querying and realtime, using AWS-EC2 “small”/m1.small (one core, with 1.7GB of RAM) or larger, no realtime, is:

1.  node1: [Master](Master.html) (m1.small)
2.  node2: metadata service (m1.small)
3.  node3: zookeeper (m1.small)
4.  node4: [Broker](Broker.html) (m1.small or m1.medium or m1.large)
5.  node5: [Compute](Compute.html) (m1.small or m1.medium or m1.large)
6.  node6: [Compute](Compute.html) (m1.small or m1.medium or m1.large)
7.  node7: [Realtime](Realtime.html) (m1.small or m1.medium or m1.large)
8.  transient nodes: indexer

This layout naturally lends itself to adding more RAM and core to Compute nodes, and to adding many more Compute nodes. Depending on the actual load, the Master, metadata server, and Zookeeper might need to use larger machines.

High Availability Physical Layout
---------------------------------

*The machine size “flavors” are using AWS/EC2 terminology for descriptive purposes only and is not meant to imply that AWS/EC2 is required or recommended. Another cloud provider or your own hardware can also work.*

An HA layout allows full rolling restarts and heavy volume:

1.  node1: [Master](Master.html) (m1.small or m1.medium or m1.large)
2.  node2: [Master](Master.html) (m1.small or m1.medium or m1.large) (backup)
3.  node3: metadata service (c1.medium or m1.large)
4.  node4: metadata service (c1.medium or m1.large) (backup)
5.  node5: zookeeper (c1.medium)
6.  node6: zookeeper (c1.medium)
7.  node7: zookeeper (c1.medium)
8.  node8: [Broker](Broker.html) (m1.small or m1.medium or m1.large or m2.xlarge or m2.2xlarge or m2.4xlarge)
9.  node9: [Broker](Broker.html) (m1.small or m1.medium or m1.large or m2.xlarge or m2.2xlarge or m2.4xlarge) (backup)
10. node10: [Compute](Compute.html) (m1.small or m1.medium or m1.large or m2.xlarge or m2.2xlarge or m2.4xlarge)
11. node11: [Compute](Compute.html) (m1.small or m1.medium or m1.large or m2.xlarge or m2.2xlarge or m2.4xlarge)
12. node12: [Realtime](Realtime.html) (m1.small or m1.medium or m1.large or m2.xlarge or m2.2xlarge or m2.4xlarge)
13. transient nodes: indexer

Sizing for Cores and RAM
------------------------

The Compute and Broker nodes will use as many cores as are available, depending on usage, so it is best to keep these on dedicated machines. The upper limit of effectively utilized cores is not well characterized yet and would depend on types of queries, query load, and the schema. Compute daemons should have a heap a size of at least 1GB per core for normal usage, but could be squeezed into a smaller heap for testing. Since in-memory caching is essential for good performance, even more RAM is better. Broker nodes will use RAM for caching, so they do more than just route queries.

The effective utilization of cores by Zookeeper, MySQL, and Master nodes is likely to be between 1 and 2 for each process/daemon, so these could potentially share a machine with lots of cores. These daemons work with heap a size between 500MB and 1GB.

Storage
-------

Indexed segments should be kept in a permanent store accessible by all nodes like AWS S3 or HDFS or equivalent. Currently Druid supports S3, but this will be extended soon.

Local disk (“ephemeral” on AWS EC2) for caching is recommended over network mounted storage (example of mounted: AWS EBS, Elastic Block Store) in order to avoid network delays during times of heavy usage. If your data center is suitably provisioned for networked storage, perhaps with separate LAN/NICs just for storage, then mounted might work fine.

Setup
-----

Setting up a cluster is essentially just firing up all of the nodes you want with the proper [configuration](configuration.html). One thing to be aware of is that there are a few properties in the configuration that potentially need to be set individually for each process:

    <code>
    druid.server.type=historical|realtime
    druid.host=someHostOrIPaddrWithPort
    druid.port=8080
    </code>

`druid.server.type` should be set to “historical” for your compute nodes and realtime for the realtime nodes. The master will only assign segments to a “historical” node and the broker has some intelligence around its ability to cache results when talking to a realtime node. This does not need to be set for the master or the broker.

`druid.host` should be set to the hostname and port that can be used to talk to the given server process. Basically, someone should be able to send a request to http://\${druid.host}/ and actually talk to the process.

`druid.port` should be set to the port that the server should listen on. In the vast majority of cases, this port should be the same as what is on `druid.host`.

Build/Run
---------

The simplest way to build and run from the repository is to run `mvn package` from the base directory and then take `druid-services/target/druid-services-*-selfcontained.jar` and push that around to your machines; the jar does not need to be expanded, and since it contains the main() methods for each kind of service, it is **not** invoked with java ~~jar. It can be run from a normal java command-line by just including it on the classpath and then giving it the main class that you want to run. For example one instance of the Compute node/service can be started like this:
\<pre\>
<code>
java~~Duser.timezone=UTC ~~Dfile.encoding=UTF-8~~cp compute/:druid-services/target/druid-services~~\*~~selfcontained.jar com.metamx.druid.http.ComputeMain
</code>

</pre>
The following table shows the possible services and fully qualified class for main().

|service|main class|
|-------|----------|
|[ Realtime ]( Realtime .html)|com.metamx.druid.realtime.RealtimeMain|
|[ Master ]( Master .html)|com.metamx.druid.http.MasterMain|
|[ Broker ]( Broker .html)|com.metamx.druid.http.BrokerMain|
|[ Compute ]( Compute .html)|com.metamx.druid.http.ComputeMain|

