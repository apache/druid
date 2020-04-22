---
id: index
title: "Quickstart"
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


This quickstart takes you through the steps to get started with Apache Druid. In it, you will install Druid and load some 
sample data, giving you hands-on experience with a few of the basic capabilities of Druid. 

Before beginning the quickstart, it is helpful to read the [general Druid overview](../design/index.md) and the
[ingestion overview](../ingestion/index.md), as the tutorials will refer to concepts discussed on those pages.

## Requirements

You can follow these steps on a relatively small machine, such as a laptop with around 4CPU/16GB RAM. 

Druid comes with several startup configuration profiles that are intended for a range of machine sizes. 
The `micro-quickstart`configuration profile demonstrated here is suitable for evaluation use cases. If 
you want to explore Druid's performance or scaling capabilities, you'll need a larger machine.

The startup configuration profiles included with Druid range from the _Nano-Quickstart_ configuration (1 CPU, 4GB RAM) 
to the _X-large_ configuration (64 CPU, 512GB RAM). For more information, see 
[Single server deployment](operations/single-server). Alternatively, see [Clustered deployment](tutorials/cluster) for 
information on deploying Druid services across clustered machines. 

The software requirements for the target machine are:

* Linux, Mac OS X, or other Unix-like OS (Windows is not supported)
* Java 8, Update 92 or later (8u92+)

> Druid officially supports Java 8 only. Support for later major versions of Java is currently experimental.
>
> If needed, you can specify where to find Java using the environment variables `DRUID_JAVA_HOME` or `JAVA_HOME`. For more details run the verify-java script.


## Step 1. Getting started

After confirming the [requirements](#requirements), follow these steps: 

1. [Download](https://www.apache.org/dyn/closer.cgi?path=/druid/{{DRUIDVERSION}}/apache-druid-{{DRUIDVERSION}}-bin.tar.gz)
the {{DRUIDVERSION}} release.
2. Extract Druid by running the following commands in your terminal:

   ```bash
   tar -xzf apache-druid-{{DRUIDVERSION}}-bin.tar.gz
   cd apache-druid-{{DRUIDVERSION}}
   ```
In the package, you'll find `LICENSE` and `NOTICE` files and directories for executable files, configuration files, sample data and more.

## Step 2: Start up Druid services

The following commands start up Druid services using the `micro-quickstart` single-machine configuration. 

From the apache-druid-{{DRUIDVERSION}} package root, run the following command:

```bash
./bin/start-micro-quickstart
```

This brings up instances of ZooKeeper and the Druid services, all running on the local machine, as indicated in the command response:

```bash
$ ./bin/start-micro-quickstart
[Fri May  3 11:40:50 2019] Running command[zk], logging to[/apache-druid-{{DRUIDVERSION}}/var/sv/zk.log]: bin/run-zk conf
[Fri May  3 11:40:50 2019] Running command[coordinator-overlord], logging to[/apache-druid-{{DRUIDVERSION}}/var/sv/coordinator-overlord.log]: bin/run-druid coordinator-overlord conf/druid/single-server/micro-quickstart
[Fri May  3 11:40:50 2019] Running command[broker], logging to[/apache-druid-{{DRUIDVERSION}}/var/sv/broker.log]: bin/run-druid broker conf/druid/single-server/micro-quickstart
[Fri May  3 11:40:50 2019] Running command[router], logging to[/apache-druid-{{DRUIDVERSION}}/var/sv/router.log]: bin/run-druid router conf/druid/single-server/micro-quickstart
[Fri May  3 11:40:50 2019] Running command[historical], logging to[/apache-druid-{{DRUIDVERSION}}/var/sv/historical.log]: bin/run-druid historical conf/druid/single-server/micro-quickstart
[Fri May  3 11:40:50 2019] Running command[middleManager], logging to[/apache-druid-{{DRUIDVERSION}}/var/sv/middleManager.log]: bin/run-druid middleManager conf/druid/single-server/micro-quickstart
```

All persistent state, such as the cluster metadata store and segments for the services, are kept in the `var` directory under the apache-druid-{{DRUIDVERSION}} package root. Logs for the services are located at `var/sv`.


## Step 3. Open the Druid console 

Once Druid starts up, open the [Druid console](../operations/druid-console.md) at [http://localhost:8888](http://localhost:8888). 

![Druid console](../assets/tutorial-quickstart-01.png "Druid console")

It may take a few seconds for all Druid processes to start up. The console itself is served by the [Druid router process](../design/router.md). If you attempt to open the Druid console before startup is complete, you may see errors in the browser. In this case, wait a few moments and try again. 

To stop Druid, use CTRL-C in the terminal. This exits the `bin/start-micro-quickstart` script and terminates all Druid processes.


## Data loading tutorials

After installing Druid, the first thing you'll likely want to do is load data. The following tutorials demonstrate various methods of loading data, including batch and streaming methods.

All tutorials assume that you are using the `micro-quickstart` single-machine configuration mentioned above.

- [Loading a file](./tutorial-batch.md) – How to perform a batch file load using Druid's native batch ingestion.
- [Loading stream data from Apache Kafka](./tutorial-kafka.md) – How to load streaming data from a Kafka topic.
- [Loading a file using Apache Hadoop](./tutorial-batch-hadoop.md) – How to perform a batch file load, using a remote Hadoop cluster.
- [Writing your own ingestion spec](./tutorial-ingestion-spec.md) – How to write a new ingestion spec and use it to load data.

## Resetting cluster state

After stopping Druid services, you can perform a clean start by deleting the `var` directory from the Druid root directory and 
running the `bin/start-micro-quickstart` script again. 


### Resetting Kafka

If you completed [Tutorial: Loading stream data from Kafka](./tutorial-kafka.md) and want to reset the cluster state, you should additionally clear out any Kafka state.

Shut down the Kafka broker with CTRL-C before stopping ZooKeeper and the Druid services, and then delete the Kafka log directory at `/tmp/kafka-logs`:

```bash
rm -rf /tmp/kafka-logs
```
