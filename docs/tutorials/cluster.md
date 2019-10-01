---
id: cluster
title: "Clustered deployment"
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


Apache Druid (incubating) is designed to be deployed as a scalable, fault-tolerant cluster.

In this document, we'll set up a simple cluster and discuss how it can be further configured to meet
your needs.

This simple cluster will feature:

 - A Master server to host the Coordinator and Overlord processes
 - Two scalable, fault-tolerant Data servers running Historical and MiddleManager processes
 - A query server, hosting the Druid Broker and Router processes

In production, we recommend deploying multiple Master servers and multiple Query servers in a fault-tolerant configuration based on your specific fault-tolerance needs, but you can get started quickly with one Master and one Query server and add more servers later.

## Select hardware

### Fresh Deployment

If you do not have an existing Druid cluster, and wish to start running Druid in a clustered deployment, this guide provides an example clustered deployment with pre-made configurations.

#### Master server

The Coordinator and Overlord processes are responsible for handling the metadata and coordination needs of your cluster. They can be colocated together on the same server.

In this example, we will be deploying the equivalent of one AWS [m5.2xlarge](https://aws.amazon.com/ec2/instance-types/m5/) instance.

This hardware offers:

- 8 vCPUs
- 31 GB RAM

Example Master server configurations that have been sized for this hardware can be found under `conf/druid/cluster/master`.

#### Data server

Historicals and MiddleManagers can be colocated on the same server to handle the actual data in your cluster. These servers benefit greatly from CPU, RAM,
and SSDs.

In this example, we will be deploying the equivalent of two AWS [i3.4xlarge](https://aws.amazon.com/ec2/instance-types/i3/) instances.

This hardware offers:

- 16 vCPUs
- 122 GB RAM
- 2 * 1.9TB SSD storage

Example Data server configurations that have been sized for this hardware can be found under `conf/druid/cluster/data`.

#### Query server

Druid Brokers accept queries and farm them out to the rest of the cluster. They also optionally maintain an
in-memory query cache. These servers benefit greatly from CPU and RAM.

In this example, we will be deploying the equivalent of one AWS [m5.2xlarge](https://aws.amazon.com/ec2/instance-types/m5/) instance.

This hardware offers:

- 8 vCPUs
- 31 GB RAM

You can consider co-locating any open source UIs or query libraries on the same server that the Broker is running on.

Example Query server configurations that have been sized for this hardware can be found under `conf/druid/cluster/query`.

#### Other Hardware Sizes

The example cluster above is chosen as a single example out of many possible ways to size a Druid cluster.

You can choose smaller/larger hardware or less/more servers for your specific needs and constraints.

If your use case has complex scaling requirements, you can also choose to not co-locate Druid processes (e.g., standalone Historical servers).

The information in the [basic cluster tuning guide](../operations/basic-cluster-tuning.md) can help with your decision-making process and with sizing your configurations.

### Migrating from a single-server deployment

If you have an existing single-server deployment, such as the ones from the [single-server deployment examples](../operations/single-server.md), and you wish to migrate to a clustered deployment of similar scale, the following section contains guidelines for choosing equivalent hardware using the Master/Data/Query server organization.

#### Master server

The main considerations for the Master server are available CPUs and RAM for the Coordinator and Overlord heaps.

Sum up the allocated heap sizes for your Coordinator and Overlord from the single-server deployment, and choose Master server hardware with enough RAM for the combined heaps, with some extra RAM for other processes on the machine.

For CPU cores, you can choose hardware with approximately 1/4th of the cores of the single-server deployment.

#### Data server

When choosing Data server hardware for the cluster, the main considerations are available CPUs and RAM, and using SSD storage if feasible.

In a clustered deployment, having multiple Data servers is a good idea for fault-tolerance purposes.

When choosing the Data server hardware, you can choose a split factor `N`, divide the original CPU/RAM of the single-server deployment by `N`, and deploy `N` Data servers of reduced size in the new cluster.

Instructions for adjusting the Historical/MiddleManager configs for the split are described in a later section in this guide.

#### Query server

The main considerations for the Query server are available CPUs and RAM for the Broker heap + direct memory, and Router heap.

Sum up the allocated memory sizes for your Broker and Router from the single-server deployment, and choose Query server hardware with enough RAM to cover the Broker/Router, with some extra RAM for other processes on the machine.

For CPU cores, you can choose hardware with approximately 1/4th of the cores of the single-server deployment.

The [basic cluster tuning guide](../operations/basic-cluster-tuning.md) has information on how to calculate Broker/Router memory usage.

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

[Download](https://www.apache.org/dyn/closer.cgi?path=/incubator/druid/{{DRUIDVERSION}}/apache-druid-{{DRUIDVERSION}}-bin.tar.gz)
the {{DRUIDVERSION}} release.

Extract Druid by running the following commands in your terminal:

```bash
tar -xzf apache-druid-{{DRUIDVERSION}}-bin.tar.gz
cd apache-druid-{{DRUIDVERSION}}
```

In the package, you should find:

* `DISCLAIMER`, `LICENSE`, and `NOTICE` files
* `bin/*` - scripts related to the [single-machine quickstart](index.html)
* `conf/druid/cluster/*` - template configurations for a clustered setup
* `extensions/*` - core Druid extensions
* `hadoop-dependencies/*` - Druid Hadoop dependencies
* `lib/*` - libraries and dependencies for core Druid
* `quickstart/*` - files related to the [single-machine quickstart](index.html)

We'll be editing the files in `conf/druid/cluster/` in order to get things running.

### Migrating from Single-Server Deployments

In the following sections we will be editing the configs under `conf/druid/cluster`.

If you have an existing single-server deployment, please copy your existing configs to `conf/druid/cluster` to preserve any config changes you have made.

## Configure metadata storage and deep storage

### Migrating from Single-Server Deployments

If you have an existing single-server deployment and you wish to preserve your data across the migration, please follow the instructions at [metadata migration](../operations/metadata-migration.md) and [deep storage migration](../operations/deep-storage-migration.md) before updating your metadata/deep storage configs.

These guides are targeted at single-server deployments that use the Derby metadata store and local deep storage. If you are already using a non-Derby metadata store in your single-server cluster, you can reuse the existing metadata store for the new cluster.

These guides also provide information on migrating segments from local deep storage. A clustered deployment requires distributed deep storage like S3 or HDFS. If your single-server deployment was already using distributed deep storage, you can reuse the existing deep storage for the new cluster.

### Metadata storage

In `conf/druid/cluster/_common/common.runtime.properties`, replace
"metadata.storage.*" with the address of the machine that you will use as your metadata store:

- `druid.metadata.storage.connector.connectURI`
- `druid.metadata.storage.connector.host`

In a production deployment, we recommend running a dedicated metadata store such as MySQL or PostgreSQL with replication, deployed separately from the Druid servers.

The [MySQL extension](../development/extensions-core/mysql.md) and [PostgreSQL extension](../development/extensions-core/postgresql.md) docs have instructions for extension configuration and initial database setup.

### Deep storage

Druid relies on a distributed filesystem or large object (blob) store for data storage. The most
commonly used deep storage implementations are S3 (popular for those on AWS) and HDFS (popular if
you already have a Hadoop deployment).

#### S3

In `conf/druid/cluster/_common/common.runtime.properties`,

- Add "druid-s3-extensions" to `druid.extensions.loadList`.

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

Please see the [S3 extension](../development/extensions-core/s3.md) documentation for more info.

#### HDFS

In `conf/druid/cluster/_common/common.runtime.properties`,

- Add "druid-hdfs-storage" to `druid.extensions.loadList`.

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
mapred-site.xml) on the classpath of your Druid processes. You can do this by copying them into
`conf/druid/cluster/_common/`.

Please see the [HDFS extension](../development/extensions-core/hdfs.md) documentation for more info.

<a name="hadoop"></a>

## Configure for connecting to Hadoop (optional)

If you will be loading data from a Hadoop cluster, then at this point you should configure Druid to be aware
of your cluster:

- Update `druid.indexer.task.hadoopWorkingPath` in `conf/druid/cluster/middleManager/runtime.properties` to
a path on HDFS that you'd like to use for temporary files required during the indexing process.
`druid.indexer.task.hadoopWorkingPath=/tmp/druid-indexing` is a common choice.

- Place your Hadoop configuration XMLs (core-site.xml, hdfs-site.xml, yarn-site.xml,
mapred-site.xml) on the classpath of your Druid processes. You can do this by copying them into
`conf/druid/cluster/_common/core-site.xml`, `conf/druid/cluster/_common/hdfs-site.xml`, and so on.

Note that you don't need to use HDFS deep storage in order to load data from Hadoop. For example, if
your cluster is running on Amazon Web Services, we recommend using S3 for deep storage even if you
are loading data using Hadoop or Elastic MapReduce.

For more info, please see the [Hadoop-based ingestion](../ingestion/hadoop.md) page.

## Configure Zookeeper connection

In a production cluster, we recommend using a dedicated ZK cluster in a quorum, deployed separately from the Druid servers.

In `conf/druid/cluster/_common/common.runtime.properties`, set
`druid.zk.service.host` to a [connection string](https://zookeeper.apache.org/doc/current/zookeeperProgrammers.html)
containing a comma separated list of host:port pairs, each corresponding to a ZooKeeper server in your ZK quorum.
(e.g. "127.0.0.1:4545" or "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002")

You can also choose to run ZK on the Master servers instead of having a dedicated ZK cluster. If doing so, we recommend deploying 3 Master servers so that you have a ZK quorum.

## Configuration Tuning

### Migrating from a Single-Server Deployment

#### Master

If you are using an example configuration from [single-server deployment examples](../operations/single-server.md), these examples combine the Coordinator and Overlord processes into one combined process.

The example configs under `conf/druid/cluster/master/coordinator-overlord` also combine the Coordinator and Overlord processes.

You can copy your existing `coordinator-overlord` configs from the single-server deployment to `conf/druid/cluster/master/coordinator-overlord`.

#### Data

Suppose we are migrating from a single-server deployment that had 32 CPU and 256GB RAM. In the old deployment, the following configurations for Historicals and MiddleManagers were applied:

Historical (Single-server)

```
druid.processing.buffer.sizeBytes=500000000
druid.processing.numMergeBuffers=8
druid.processing.numThreads=31
```

MiddleManager (Single-server)

```
druid.worker.capacity=8
druid.indexer.fork.property.druid.processing.numMergeBuffers=2
druid.indexer.fork.property.druid.processing.buffer.sizeBytes=100000000
druid.indexer.fork.property.druid.processing.numThreads=1
```

In the clustered deployment, we can choose a split factor (2 in this example), and deploy 2 Data servers with 16CPU and 128GB RAM each. The areas to scale are the following:

Historical

- `druid.processing.numThreads`: Set to `(num_cores - 1)` based on the new hardware
- `druid.processing.numMergeBuffers`: Divide the old value from the single-server deployment by the split factor
- `druid.processing.buffer.sizeBytes`: Keep this unchanged

MiddleManager:

- `druid.worker.capacity`: Divide the old value from the single-server deployment by the split factor
- `druid.indexer.fork.property.druid.processing.numMergeBuffers`: Keep this unchanged
- `druid.indexer.fork.property.druid.processing.buffer.sizeBytes`: Keep this unchanged
- `druid.indexer.fork.property.druid.processing.numThreads`: Keep this unchanged

The resulting configs after the split:

New Historical (on 2 Data servers)

```
 druid.processing.buffer.sizeBytes=500000000
 druid.processing.numMergeBuffers=8
 druid.processing.numThreads=31
```

New MiddleManager (on 2 Data servers)

```
druid.worker.capacity=4
druid.indexer.fork.property.druid.processing.numMergeBuffers=2
druid.indexer.fork.property.druid.processing.buffer.sizeBytes=100000000
druid.indexer.fork.property.druid.processing.numThreads=1
```

#### Query

You can copy your existing Broker and Router configs to the directories under `conf/druid/cluster/query`, no modifications are needed, as long as the new hardware is sized accordingly.

### Fresh deployment

If you are using the example cluster described above:
- 1 Master server (m5.2xlarge)
- 2 Data servers (i3.4xlarge)
- 1 Query server (m5.2xlarge)

The configurations under `conf/druid/cluster` have already been sized for this hardware and you do not need to make further modifications for general use cases.

If you have chosen different hardware, the [basic cluster tuning guide](../operations/basic-cluster-tuning.md) can help you size your configurations.

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

> In production, we recommend deploying ZooKeeper and your metadata store on their own dedicated hardware,
> rather than on the Master server.

## Start Master Server

Copy the Druid distribution and your edited configurations to your Master server.

If you have been editing the configurations on your local machine, you can use *rsync* to copy them:

```bash
rsync -az apache-druid-{{DRUIDVERSION}}/ MASTER_SERVER:apache-druid-{{DRUIDVERSION}}/
```

### No Zookeeper on Master

From the distribution root, run the following command to start the Master server:

```
bin/start-cluster-master-no-zk-server
```

### With Zookeeper on Master

If you plan to run ZK on Master servers, first update `conf/zoo.cfg` to reflect how you plan to run ZK. Then log on to your Master servers and install Zookeeper:

```bash
curl http://www.gtlib.gatech.edu/pub/apache/zookeeper/zookeeper-3.4.14/zookeeper-3.4.14.tar.gz -o zookeeper-3.4.14.tar.gz
tar -xzf zookeeper-3.4.14.tar.gz
mv zookeeper-3.4.14 zk
```

If you are running ZK on the Master server, you can start the Master server processes together with ZK using:

```
bin/start-cluster-master-with-zk-server
```

> In production, we also recommend running a ZooKeeper cluster on its own dedicated hardware.

## Start Data Server

Copy the Druid distribution and your edited configurations to your Data servers.

From the distribution root, run the following command to start the Data server:

```
bin/start-cluster-data-server
```

You can add more Data servers as needed.

> For clusters with complex resource allocation needs, you can break apart Historicals and MiddleManagers and scale the components individually.
> This also allows you take advantage of Druid's built-in MiddleManager autoscaling facility.

### Tranquility

If you are doing push-based stream ingestion with Kafka or over HTTP, you can also start Tranquility Server on the Data server.

For large scale production, Data server processes and the Tranquility Server can still be co-located.

If you are running Tranquility (not server) with a stream processor, you can co-locate Tranquility with the stream processor and not require Tranquility Server.

First install Tranquility:

```bash
curl http://static.druid.io/tranquility/releases/tranquility-distribution-0.8.3.tgz -o tranquility-distribution-0.8.3.tgz
tar -xzf tranquility-distribution-0.8.3.tgz
mv tranquility-distribution-0.8.3 tranquility
```

Afterwards, in `conf/supervise/cluster/data.conf`, uncomment out the `tranquility-server` line, and restart the Data server processes.

## Start Query Server

Copy the Druid distribution and your edited configurations to your Query servers.

From the distribution root, run the following command to start the Query server:

```
bin/start-cluster-query-server
```

You can add more Query servers as needed based on query load. If you increase the number of Query servers, be sure to adjust the connection pools on your Historicals and Tasks as described in the [basic cluster tuning guide](../operations/basic-cluster-tuning.md).

## Loading data

Congratulations, you now have a Druid cluster! The next step is to learn about recommended ways to load data into
Druid based on your use case. Read more about [loading data](../ingestion/index.md).
