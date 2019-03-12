---
layout: doc_page
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

# Druid Quickstart

In this quickstart, we will download Druid and set it up on a single machine. The cluster will be ready to load data
after completing this initial setup.

Before beginning the quickstart, it is helpful to read the [general Druid overview](../design/index.html) and the
[ingestion overview](../ingestion/index.html), as the tutorials will refer to concepts discussed on those pages.

## Prerequisites

You will need:

  * Java 8
  * Linux, Mac OS X, or other Unix-like OS (Windows is not supported)
  * 8G of RAM
  * 2 vCPUs

On Mac OS X, you can use [Oracle's JDK
8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) to install
Java.

On Linux, your OS package manager should be able to help for Java. If your Ubuntu-
based OS does not have a recent enough version of Java, WebUpd8 offers [packages for those
OSes](http://www.webupd8.org/2012/09/install-oracle-java-8-in-ubuntu-via-ppa.html).

## Getting started

[Download](https://www.apache.org/dyn/closer.cgi?path=/incubator/druid/#{DRUIDVERSION}/apache-druid-#{DRUIDVERSION}-bin.tar.gz)
the #{DRUIDVERSION} release.

Extract Druid by running the following commands in your terminal:

```bash
tar -xzf apache-druid-#{DRUIDVERSION}-bin.tar.gz
cd apache-druid-#{DRUIDVERSION}
```

In the package, you should find:

* `DISCLAIMER`, `LICENSE`, and `NOTICE` files
* `bin/*` - scripts useful for this quickstart
* `conf/*` - template configurations for a clustered setup
* `extensions/*` - core Druid extensions
* `hadoop-dependencies/*` - Druid Hadoop dependencies
* `lib/*` - libraries and dependencies for core Druid
* `quickstart/*` - configuration files, sample data, and other files for the quickstart tutorials

## Download Zookeeper

Druid has a dependency on [Apache ZooKeeper](http://zookeeper.apache.org/) for distributed coordination. You'll
need to download and run Zookeeper.

In the package root, run the following commands:

```bash
curl https://archive.apache.org/dist/zookeeper/zookeeper-3.4.11/zookeeper-3.4.11.tar.gz -o zookeeper-3.4.11.tar.gz
tar -xzf zookeeper-3.4.11.tar.gz
mv zookeeper-3.4.11 zk
```

The startup scripts for the tutorial will expect the contents of the Zookeeper tarball to be located at `zk` under the apache-druid-#{DRUIDVERSION} package root.

## Start up Druid services

From the apache-druid-#{DRUIDVERSION} package root, run the following command:

```bash
bin/supervise -c quickstart/tutorial/conf/tutorial-cluster.conf
```

This will bring up instances of Zookeeper and the Druid services, all running on the local machine, e.g.:

```bash
bin/supervise -c quickstart/tutorial/conf/tutorial-cluster.conf
[Wed Feb 27 12:46:13 2019] Running command[zk], logging to[/apache-druid-#{DRUIDVERSION}/var/sv/zk.log]: bin/run-zk quickstart/tutorial/conf
[Wed Feb 27 12:46:13 2019] Running command[coordinator], logging to[/apache-druid-#{DRUIDVERSION}/var/sv/coordinator.log]: bin/run-druid coordinator quickstart/tutorial/conf
[Wed Feb 27 12:46:13 2019] Running command[broker], logging to[/apache-druid-#{DRUIDVERSION}/var/sv/broker.log]: bin/run-druid broker quickstart/tutorial/conf
[Wed Feb 27 12:46:13 2019] Running command[router], logging to[/apache-druid-#{DRUIDVERSION}/var/sv/router.log]: bin/run-druid router quickstart/tutorial/conf
[Wed Feb 27 12:46:13 2019] Running command[historical], logging to[/apache-druid-#{DRUIDVERSION}/var/sv/historical.log]: bin/run-druid historical quickstart/tutorial/conf
[Wed Feb 27 12:46:13 2019] Running command[overlord], logging to[/apache-druid-#{DRUIDVERSION}/var/sv/overlord.log]: bin/run-druid overlord quickstart/tutorial/conf
[Wed Feb 27 12:46:13 2019] Running command[middleManager], logging to[/apache-druid-#{DRUIDVERSION}/var/sv/middleManager.log]: bin/run-druid middleManager quickstart/tutorial/conf
```

All persistent state such as the cluster metadata store and segments for the services will be kept in the `var` directory under the apache-druid-#{DRUIDVERSION} package root. Logs for the services are located at `var/sv`.

Later on, if you'd like to stop the services, CTRL-C to exit the `bin/supervise` script, which will terminate the Druid processes.

### Resetting cluster state

If you want a clean start after stopping the services, delete the `var` directory and run the `bin/supervise` script again.

Once every service has started, you are now ready to load data.

#### Resetting Kafka

If you completed [Tutorial: Loading stream data from Kafka](./tutorial-kafka.html) and wish to reset the cluster state, you should additionally clear out any Kafka state.

Shut down the Kafka broker with CTRL-C before stopping Zookeeper and the Druid services, and then delete the Kafka log directory at `/tmp/kafka-logs`:

```bash
rm -rf /tmp/kafka-logs
```

## Loading Data

### Tutorial Dataset

For the following data loading tutorials, we have included a sample data file containing Wikipedia page edit events that occurred on 2015-09-12.

This sample data is located at `quickstart/tutorial/wikiticker-2015-09-12-sampled.json.gz` from the Druid package root. The page edit events are stored as JSON objects in a text file.

The sample data has the following columns, and an example event is shown below:

  * added
  * channel
  * cityName
  * comment
  * countryIsoCode
  * countryName
  * deleted
  * delta
  * isAnonymous
  * isMinor
  * isNew
  * isRobot
  * isUnpatrolled
  * metroCode
  * namespace
  * page
  * regionIsoCode
  * regionName
  * user

```json
{
  "timestamp":"2015-09-12T20:03:45.018Z",
  "channel":"#en.wikipedia",
  "namespace":"Main",
  "page":"Spider-Man's powers and equipment",
  "user":"foobar",
  "comment":"/* Artificial web-shooters */",
  "cityName":"New York",
  "regionName":"New York",
  "regionIsoCode":"NY",
  "countryName":"United States",
  "countryIsoCode":"US",
  "isAnonymous":false,
  "isNew":false,
  "isMinor":false,
  "isRobot":false,
  "isUnpatrolled":false,
  "added":99,
  "delta":99,
  "deleted":0,
}
```

The following tutorials demonstrate various methods of loading data into Druid, including both batch and streaming use cases.

### [Tutorial: Loading a file](./tutorial-batch.html)

This tutorial demonstrates how to perform a batch file load, using Druid's native batch ingestion.

### [Tutorial: Loading stream data from Kafka](./tutorial-kafka.html)

This tutorial demonstrates how to load streaming data from a Kafka topic.

### [Tutorial: Loading a file using Hadoop](./tutorial-batch-hadoop.html)

This tutorial demonstrates how to perform a batch file load, using a remote Hadoop cluster.

### [Tutorial: Loading data using Tranquility](./tutorial-tranquility.html)

This tutorial demonstrates how to load streaming data by pushing events to Druid using the Tranquility service.

### [Tutorial: Writing your own ingestion spec](./tutorial-ingestion-spec.html)

This tutorial demonstrates how to write a new ingestion spec and use it to load data.
