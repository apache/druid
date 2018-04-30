---
layout: doc_page
---

# Druid Quickstart

In this quickstart, we will download Druid, set up it up on a single machine, load some data, and query the data.

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

To install Druid, issue the following commands in your terminal:

```bash
curl -O http://static.druid.io/artifacts/releases/druid-#{DRUIDVERSION}-bin.tar.gz
tar -xzf druid-#{DRUIDVERSION}-bin.tar.gz
cd druid-#{DRUIDVERSION}
```

In the package, you should find:

* `LICENSE` - the license files.
* `bin/` - scripts useful for this quickstart.
* `conf/*` - template configurations for a clustered setup.
* `conf-quickstart/*` - configurations for this quickstart.
* `extensions/*` - all Druid extensions.
* `hadoop-dependencies/*` - Druid Hadoop dependencies.
* `lib/*` - all included software packages for core Druid.
* `quickstart/*` - files useful for this quickstart.

## Start up Zookeeper

Druid currently has a dependency on [Apache ZooKeeper](http://zookeeper.apache.org/) for distributed coordination. You'll
need to download and run Zookeeper.

```bash
curl http://www.gtlib.gatech.edu/pub/apache/zookeeper/zookeeper-3.4.11/zookeeper-3.4.11.tar.gz -o zookeeper-3.4.11.tar.gz
tar -xzf zookeeper-3.4.11.tar.gz
cd zookeeper-3.4.11
cp conf/zoo_sample.cfg conf/zoo.cfg
./bin/zkServer.sh start
```

## Start up Druid services

With Zookeeper running, return to the druid-#{DRUIDVERSION} directory. In that directory, issue the command:

```bash
bin/init
```

This will setup up some directories for you. Next, you can start up the Druid processes in different terminal windows.
This tutorial runs every Druid process on the same system. In a large distributed production cluster,
many of these Druid processes can still be co-located together.

```bash
java `cat conf-quickstart/druid/historical/jvm.config | xargs` -cp "conf-quickstart/druid/_common:conf-quickstart/druid/historical:lib/*" io.druid.cli.Main server historical
java `cat conf-quickstart/druid/broker/jvm.config | xargs` -cp "conf-quickstart/druid/_common:conf-quickstart/druid/broker:lib/*" io.druid.cli.Main server broker
java `cat conf-quickstart/druid/coordinator/jvm.config | xargs` -cp "conf-quickstart/druid/_common:conf-quickstart/druid/coordinator:lib/*" io.druid.cli.Main server coordinator
java `cat conf-quickstart/druid/overlord/jvm.config | xargs` -cp "conf-quickstart/druid/_common:conf-quickstart/druid/overlord:lib/*" io.druid.cli.Main server overlord
java `cat conf-quickstart/druid/middleManager/jvm.config | xargs` -cp "conf-quickstart/druid/_common:conf-quickstart/druid/middleManager:lib/*" io.druid.cli.Main server middleManager
```

You should see a log message printed out for each service that starts up.

Later on, if you'd like to stop the services, CTRL-C to exit from the running java processes. If you
want a clean start after stopping the services, delete the `var` directory and run the `init` script again.

Once every service has started, you are now ready to load data.

## Load batch data

We've included a sample of Wikipedia edits from September 12, 2015 to get you started.

<div class="note info">
This section shows you how to load data in batches, but you can skip ahead to learn how to <a href="quickstart.html#load-streaming-data">load
streams in real-time</a>. Druid's streaming ingestion can load data
with virtually no delay between events occurring and being available for queries.
</div>

The [dimensions](https://en.wikipedia.org/wiki/Dimension_%28data_warehouse%29) (attributes you can
filter and split on) in the Wikipedia dataset, other than time, are:

  * channel
  * cityName
  * comment
  * countryIsoCode
  * countryName
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

The [measures](https://en.wikipedia.org/wiki/Measure_%28data_warehouse%29), or *metrics* as they are known in Druid (values you can aggregate)
in the Wikipedia dataset are:

  * count
  * added
  * deleted
  * delta
  * user_unique

To load this data into Druid, you can submit an *ingestion task* pointing to the file. We've included
a task that loads the `wikiticker-2015-09-12-sampled.json` file included in the archive. To submit
this task, POST it to Druid in a new terminal window from the druid-#{DRUIDVERSION} directory:

```bash
curl -X 'POST' -H 'Content-Type:application/json' -d @quickstart/wikiticker-index.json localhost:8090/druid/indexer/v1/task
```

Which will print the ID of the task if the submission was successful:

```bash
{"task":"index_hadoop_wikipedia_2013-10-09T21:30:32.802Z"}
```

To view the status of your ingestion task, go to your overlord console:
[http://localhost:8090/console.html](http://localhost:8090/console.html). You can refresh the console periodically, and after
the task is successful, you should see a "SUCCESS" status for the task.

After your ingestion task finishes, the data will be loaded by historical nodes and available for
querying within a minute or two. You can monitor the progress of loading your data in the
coordinator console, by checking whether there is a datasource "wikiticker" with a blue circle
indicating "fully available": [http://localhost:8081/#/](http://localhost:8081/#/).

Once the data is fully available, you can immediately query it&mdash; to see how, skip to the [Query
data](#query-data) section below. Or, continue to the [Load your own data](#load-your-own-data)
section if you'd like to load a different dataset.

## Load streaming data

To load streaming data, we are going to push events into Druid
over a simple HTTP API. To do this we will use [Tranquility], a high level data producer
library for Druid.

To download Tranquility, issue the following commands in your terminal:

```bash
curl -O http://static.druid.io/tranquility/releases/tranquility-distribution-0.8.0.tgz
tar -xzf tranquility-distribution-0.8.0.tgz
cd tranquility-distribution-0.8.0
```

We've included a configuration file in `conf-quickstart/tranquility/server.json` as part of the Druid distribution
for a *metrics* datasource. We're going to start the Tranquility server process, which can be used to push events
directly to Druid.

``` bash
bin/tranquility server -configFile <path_to_druid_distro>/conf-quickstart/tranquility/server.json
```

<div class="note info">
This section shows you how to load data using Tranquility Server, but Druid also supports a wide
variety of <a href="../ingestion/stream-ingestion.html#stream-push">other streaming ingestion options</a>, including from
popular streaming systems like Kafka, Storm, Samza, and Spark Streaming.
</div>

The [dimensions](https://en.wikipedia.org/wiki/Dimension_%28data_warehouse%29) (attributes you can
filter and split on) for this datasource are flexible. It's configured for *schemaless dimensions*,
meaning it will accept any field in your JSON input as a dimension.

The metrics (also called
[measures](https://en.wikipedia.org/wiki/Measure_%28data_warehouse%29); values
you can aggregate) in this datasource are:

  * count
  * value_sum (derived from `value` in the input)
  * value_min (derived from `value` in the input)
  * value_max (derived from `value` in the input)

We've included a script that can generate some random sample metrics to load into this datasource.
To use it, simply run in your Druid distribution repository:

```bash
bin/generate-example-metrics | curl -XPOST -H'Content-Type: application/json' --data-binary @- http://localhost:8200/v1/post/metrics
```

Which will print something like:

```
{"result":{"received":25,"sent":25}}
```

This indicates that the HTTP server received 25 events from you, and sent 25 to Druid. Note that
this may take a few seconds to finish the first time you run it, as Druid resources must be
allocated to the ingestion task. Subsequent POSTs should complete quickly.

Once the data is sent to Druid, you can immediately [query it](#query-data).

## Query data

### Direct Druid queries

Druid supports a rich [family of JSON-based
queries](../querying/querying.html). We've included an example topN query
in `quickstart/wikiticker-top-pages.json` that will find the most-edited articles in this dataset:

```bash
curl -L -H'Content-Type: application/json' -XPOST --data-binary @quickstart/wikiticker-top-pages.json http://localhost:8082/druid/v2/?pretty
```

## Visualizing data

Druid is ideal for power user-facing analytic applications. There are a number of different open source applications to
visualize and explore data in Druid. We recommend trying [Pivot](https://github.com/implydata/pivot),
[Superset](https://github.com/airbnb/superset), or [Metabase](https://github.com/metabase/metabase) to start
visualizing the data you just ingested.

If you installed Pivot for example, you should be able to view your data in your browser at [localhost:9090](http://localhost:9090/).

### SQL and other query libraries

There are many more query tools for Druid than we've included here, including SQL
engines, and libraries for various languages like Python and Ruby. Please see [the list of
libraries](../development/libraries.html) for more information.

## Clustered setup

This quickstart sets you up with all services running on a single machine. The next step is to [load
your own data](ingestion.html). Or, you can skip ahead to [running a distributed cluster](cluster.html).
