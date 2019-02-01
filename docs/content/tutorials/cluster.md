---
layout: doc_page
title: "Clustering"
---

<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->

# Clustering

Druid is designed to be deployed as a scalable, fault-tolerant cluster.

In this document, we'll set up a simple cluster and discuss how it can be further configured to meet
your needs. 

This simple cluster will feature:
 - A single Master server to host the Coordinator and Overlord processes
 - Scalable, fault-tolerant Data servers running Historical and MiddleManager processes
 - Query servers, hosting Druid Broker processes

In production, we recommend deploying multiple Master servers with Coordinator and Overlord processes in a fault-tolerant configuration as well.

## Select hardware

### Master Server

The Coordinator and Overlord processes can be co-located on a single server that is responsible for handling the metadata and coordination needs of your cluster.
The equivalent of an AWS [m3.xlarge](https://aws.amazon.com/ec2/instance-types/#M3) is sufficient for most clusters. This
hardware offers:

- 4 vCPUs
- 15 GB RAM
- 80 GB SSD storage

### Data Server

Historicals and MiddleManagers can be colocated on a single server to handle the actual data in your cluster. These servers benefit greatly from CPU, RAM,
and SSDs. The equivalent of an AWS [r3.2xlarge](https://aws.amazon.com/ec2/instance-types/#r3) is a
good starting point. This hardware offers:

- 8 vCPUs
- 61 GB RAM
- 160 GB SSD storage

### Query Server

Druid Brokers accept queries and farm them out to the rest of the cluster. They also optionally maintain an
in-memory query cache. These servers benefit greatly from CPU and RAM, and can also be deployed on
the equivalent of an AWS [r3.2xlarge](https://aws.amazon.com/ec2/instance-types/#r3). This hardware
offers:

- 8 vCPUs
- 61 GB RAM
- 160 GB SSD storage

You can consider co-locating any open source UIs or query libraries on the same server that the Broker is running on.

Very large clusters should consider selecting larger servers.

## Select OS

We recommend running your favorite Linux distribution. You will also need:

  * Java 8

Your OS package manager should be able to help for both Java. If your Ubuntu-based OS
does not have a recent enough version of Java, WebUpd8 offers [packages for those
OSes](http://www.webupd8.org/2012/09/install-oracle-java-8-in-ubuntu-via-ppa.html).

## Download the distribution

First, download and unpack the release archive. It's best to do this on a single machine at first,
since you will be editing the configurations and then copying the modified distribution out to all
of your servers.

[Download](https://www.apache.org/dyn/closer.cgi?path=/incubator/druid/#{DRUIDVERSION}/apache-druid-#{DRUIDVERSION}-bin.tar.gz)
the #{DRUIDVERSION} release.

Extract Druid by running the following commands in your terminal:

```bash
tar -xzf apache-druid-#{DRUIDVERSION}-bin.tar.gz
cd apache-druid-#{DRUIDVERSION}
```

In the package, you should find:

* `DISCLAIMER`, `LICENSE`, and `NOTICE` files
* `bin/*` - scripts related to the [single-machine quickstart](quickstart.html)
* `conf/*` - template configurations for a clustered setup
* `extensions/*` - core Druid extensions
* `hadoop-dependencies/*` - Druid Hadoop dependencies
* `lib/*` - libraries and dependencies for core Druid
* `quickstart/*` - files related to the [single-machine quickstart](quickstart.html)

We'll be editing the files in `conf/` in order to get things running.

## Configure deep storage

Druid relies on a distributed filesystem or large object (blob) store for data storage. The most
commonly used deep storage implementations are S3 (popular for those on AWS) and HDFS (popular if
you already have a Hadoop deployment).

### S3

In `conf/druid/_common/common.runtime.properties`,

- Set `druid.extensions.loadList=["druid-s3-extensions"]`.

- Comment out the configurations for local storage under "Deep Storage" and "Indexing service logs".

- Uncomment and configure appropriate values in the "For S3" sections of "Deep Storage" and
"Indexing service logs".

After this, you should have made the following changes:

```
druid.extensions.loadList=["druid-s3-extensions"]

#druid.storage.type=local
#druid.storage.storageDirectory=var/druid/segments

druid.storage.type=s3
druid.storage.bucket=your-bucket
druid.storage.baseKey=druid/segments
druid.s3.accessKey=...
druid.s3.secretKey=...

#druid.indexer.logs.type=file
#druid.indexer.logs.directory=var/druid/indexing-logs

druid.indexer.logs.type=s3
druid.indexer.logs.s3Bucket=your-bucket
druid.indexer.logs.s3Prefix=druid/indexing-logs
```

### HDFS

In `conf/druid/_common/common.runtime.properties`,

- Set `druid.extensions.loadList=["druid-hdfs-storage"]`.

- Comment out the configurations for local storage under "Deep Storage" and "Indexing service logs".

- Uncomment and configure appropriate values in the "For HDFS" sections of "Deep Storage" and
"Indexing service logs".

After this, you should have made the following changes:

```
druid.extensions.loadList=["druid-hdfs-storage"]

#druid.storage.type=local
#druid.storage.storageDirectory=var/druid/segments

druid.storage.type=hdfs
druid.storage.storageDirectory=/druid/segments

#druid.indexer.logs.type=file
#druid.indexer.logs.directory=var/druid/indexing-logs

druid.indexer.logs.type=hdfs
druid.indexer.logs.directory=/druid/indexing-logs
```

Also,

- Place your Hadoop configuration XMLs (core-site.xml, hdfs-site.xml, yarn-site.xml,
mapred-site.xml) on the classpath of your Druid nodes. You can do this by copying them into
`conf/druid/_common/`.

## Configure Tranquility Server (optional)

Data streams can be sent to Druid through a simple HTTP API powered by Tranquility
Server. If you will be using this functionality, then at this point you should [configure
Tranquility Server](../ingestion/stream-ingestion.html#server).

## Configure Tranquility Kafka (optional)

Druid can consuming streams from Kafka through Tranquility Kafka. If you will be
using this functionality, then at this point you should
[configure Tranquility Kafka](../ingestion/stream-ingestion.html#kafka).

## Configure for connecting to Hadoop (optional)

If you will be loading data from a Hadoop cluster, then at this point you should configure Druid to be aware
of your cluster:

- Update `druid.indexer.task.hadoopWorkingPath` in `conf/druid/middleManager/runtime.properties` to
a path on HDFS that you'd like to use for temporary files required during the indexing process.
`druid.indexer.task.hadoopWorkingPath=/tmp/druid-indexing` is a common choice.

- Place your Hadoop configuration XMLs (core-site.xml, hdfs-site.xml, yarn-site.xml,
mapred-site.xml) on the classpath of your Druid nodes. You can do this by copying them into
`conf/druid/_common/core-site.xml`, `conf/druid/_common/hdfs-site.xml`, and so on.

Note that you don't need to use HDFS deep storage in order to load data from Hadoop. For example, if
your cluster is running on Amazon Web Services, we recommend using S3 for deep storage even if you
are loading data using Hadoop or Elastic MapReduce.

For more info, please see [batch ingestion](../ingestion/batch-ingestion.html).

## Configure addresses for Druid coordination

In this simple cluster, you will deploy a single Master server containing the following:
- A single Druid Coordinator process
- A single Druid Overlord process
- A single ZooKeeper istance
- An embedded Derby metadata store

The processes on the cluster need to be configured with the addresses of this ZK instance and the metadata store.

In `conf/druid/_common/common.runtime.properties`, replace
"zk.service.host" with [connection string](https://zookeeper.apache.org/doc/current/zookeeperProgrammers.html)
containing a comma separated list of host:port pairs, each corresponding to a ZooKeeper server
(e.g. "127.0.0.1:4545" or "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"):

- `druid.zk.service.host`

In `conf/druid/_common/common.runtime.properties`, replace
"metadata.storage.*" with the address of the machine that you will use as your metadata store:

- `druid.metadata.storage.connector.connectURI`
- `druid.metadata.storage.connector.host`

<div class="note caution">
In production, we recommend running 2 Master servers, each running a Druid Coordinator process
and a Druid Overlord process. We also recommend running a ZooKeeper cluster on its own dedicated hardware,
as well as replicated <a href = "../dependencies/metadata-storage.html">metadata storage</a>
such as MySQL or PostgreSQL, on its own dedicated hardware.
</div>

## Tune processes on the Data Server

Druid Historicals and MiddleManagers can be co-located on the same hardware. Both Druid processes benefit greatly from
being tuned to the hardware they run on. If you are running Tranquility Server or Kafka, you can also colocate Tranquility with these two Druid processes.
If you are using [r3.2xlarge](https://aws.amazon.com/ec2/instance-types/#r3)
EC2 instances, or similar hardware, the configuration in the distribution is a
reasonable starting point.

If you are using different hardware, we recommend adjusting configurations for your specific
hardware. The most commonly adjusted configurations are:

- `-Xmx` and `-Xms`
- `druid.server.http.numThreads`
- `druid.processing.buffer.sizeBytes`
- `druid.processing.numThreads`
- `druid.query.groupBy.maxIntermediateRows`
- `druid.query.groupBy.maxResults`
- `druid.server.maxSize` and `druid.segmentCache.locations` on Historical Nodes
- `druid.worker.capacity` on MiddleManagers

<div class="note info">
Keep -XX:MaxDirectMemory >= numThreads*sizeBytes, otherwise Druid will fail to start up..
</div>

Please see the Druid [configuration documentation](../configuration/index.html) for a full description of all
possible configuration options.

## Tune Druid Brokers on the Query Server

Druid Brokers also benefit greatly from being tuned to the hardware they
run on. If you are using [r3.2xlarge](https://aws.amazon.com/ec2/instance-types/#r3) EC2 instances,
or similar hardware, the configuration in the distribution is a reasonable starting point.

If you are using different hardware, we recommend adjusting configurations for your specific
hardware. The most commonly adjusted configurations are:

- `-Xmx` and `-Xms`
- `druid.server.http.numThreads`
- `druid.cache.sizeInBytes`
- `druid.processing.buffer.sizeBytes`
- `druid.processing.numThreads`
- `druid.query.groupBy.maxIntermediateRows`
- `druid.query.groupBy.maxResults`

<div class="note caution">
Keep -XX:MaxDirectMemory >= numThreads*sizeBytes, otherwise Druid will fail to start up.
</div>

Please see the Druid [configuration documentation](../configuration/index.html) for a full description of all
possible configuration options.

## Open ports (if using a firewall)

If you're using a firewall or some other system that only allows traffic on specific ports, allow
inbound connections on the following:

### Master Server
- 1527 (Derby metadata store; not needed if you are using a separate metadata store like MySQL or PostgreSQL)
- 2181 (ZooKeeper; not needed if you are using a separate ZooKeeper cluster)
- 8081 (Coordinator)
- 8090 (Overlord)

### Data Server
- 8083 (Historical)
- 8091, 8100&ndash;8199 (Druid Middle Manager; you may need higher than port 8199 if you have a very high `druid.worker.capacity`)

### Query Server
- 8082 (Broker)
- 8088 (Router, if used)

### Other
- 8200 (Tranquility Server, if used)
- 8084 (Standalone Realtime, if used, deprecated)

<div class="note caution">
In production, we recommend deploying ZooKeeper and your metadata store on their own dedicated hardware,
rather than on the Master server.
</div>

## Start Master Server

Copy the Druid distribution and your edited configurations to your Master server. 

If you have been editing the configurations on your local machine, you can use *rsync* to copy them:

```bash
rsync -az apache-druid-#{DRUIDVERSION}/ COORDINATION_SERVER:apache-druid-#{DRUIDVERSION}/
```

Log on to your coordination server and install Zookeeper:

```bash
curl http://www.gtlib.gatech.edu/pub/apache/zookeeper/zookeeper-3.4.11/zookeeper-3.4.11.tar.gz -o zookeeper-3.4.11.tar.gz
tar -xzf zookeeper-3.4.11.tar.gz
cd zookeeper-3.4.11
cp conf/zoo_sample.cfg conf/zoo.cfg
./bin/zkServer.sh start
```

<div class="note caution">
In production, we also recommend running a ZooKeeper cluster on its own dedicated hardware.
</div>

On your coordination server, *cd* into the distribution and start up the coordination services (you should do this in different windows or pipe the log to a file):

```bash
java `cat conf/druid/coordinator/jvm.config | xargs` -cp conf/druid/_common:conf/druid/coordinator:lib/* org.apache.druid.cli.Main server coordinator
java `cat conf/druid/overlord/jvm.config | xargs` -cp conf/druid/_common:conf/druid/overlord:lib/* org.apache.druid.cli.Main server overlord
```

You should see a log message printed out for each service that starts up. You can view detailed logs
for any service by looking in the `var/log/druid` directory using another terminal.

## Start Data Server

Copy the Druid distribution and your edited configurations to your Data servers set aside for the Druid Historicals and MiddleManagers.

On each one, *cd* into the distribution and run this command to start the Data server processes:

```bash
java `cat conf/druid/historical/jvm.config | xargs` -cp conf/druid/_common:conf/druid/historical:lib/* org.apache.druid.cli.Main server historical
java `cat conf/druid/middleManager/jvm.config | xargs` -cp conf/druid/_common:conf/druid/middleManager:lib/* org.apache.druid.cli.Main server middleManager
```

You can add more Data servers with Druid Historicals and MiddleManagers as needed.

<div class="note info">
For clusters with complex resource allocation needs, you can break apart Historicals and MiddleManagers and scale the components individually.
This also allows you take advantage of Druid's built-in MiddleManager
autoscaling facility.
</div>

If you are doing push-based stream ingestion with Kafka or over HTTP, you can also start Tranquility Server on the same
hardware that holds MiddleManagers and Historicals. For large scale production, MiddleManagers and Tranquility Server
can still be co-located. If you are running Tranquility (not server) with a stream processor, you can co-locate
Tranquility with the stream processor and not require Tranquility Server.

```bash
curl -O http://static.druid.io/tranquility/releases/tranquility-distribution-0.8.0.tgz
tar -xzf tranquility-distribution-0.8.0.tgz
cd tranquility-distribution-0.8.0
bin/tranquility <server or kafka> -configFile <path_to_druid_distro>/conf/tranquility/<server or kafka>.json
```

## Start Query Server

Copy the Druid distribution and your edited configurations to your Query servers set aside for the Druid Brokers.

On each Query server, *cd* into the distribution and run this command to start the Broker process (you may want to pipe the output to a log file):

```bash
java `cat conf/druid/broker/jvm.config | xargs` -cp conf/druid/_common:conf/druid/broker:lib/* org.apache.druid.cli.Main server broker
```

You can add more Query servers as needed based on query load.

## Loading data

Congratulations, you now have a Druid cluster! The next step is to learn about recommended ways to load data into
Druid based on your use case. Read more about [loading data](../ingestion/index.html).
