---
layout: doc_page
---

# Tutorial: A First Look at Druid
Greetings! This tutorial will help clarify some core Druid concepts. We will use a real-time dataset and issue some basic Druid queries. If you are ready to explore Druid, and learn a thing or two, read on!

Note: If you are behind a corporate firewall, please see our additional [instructions](../tutorials/firewall.html) for running this tutorial.

About the data
--------------

The data source we'll be working with is Wikipedia edits. Each time an edit is made in Wikipedia, an event gets pushed to an IRC channel associated with the language of the Wikipedia page. We scrape IRC channels for several different languages and load this data into Druid.

Each event has a timestamp indicating the time of the edit (in UTC time), a list of dimensions indicating various metadata about the event (such as information about the user editing the page and whether the user is a bot), and a list of metrics associated with the event (such as the number of characters added and deleted).

Specifically. the data schema looks like this:

Dimensions (things to filter on):

```json
"page"
"language"
"user"
"unpatrolled"
"newPage"
"robot"
"anonymous"
"namespace"
"continent"
"country"
"region"
"city"
```

Metrics (things to aggregate over):

```json
"count"
"added"
"delta"
"deleted"
```

Setting Up
----------

To start, we need to get our hands on a Druid build. There are two ways to get Druid: download a tarball, or [Build From Source](../development/build.html). You only need to do one of these.

### Download a Tarball

We've built a tarball that contains everything you'll need. You'll find it [here](http://druid.io/downloads.html). Download this file to a directory of your choosing.

### Build From Source

Follow the [Build From Source](../development/build.html) guide to build from source. Then grab the tarball from distribution/target/druid-<version>-bin.tar.gz.

### Unpack the Tarball

You can extract the content within by issuing:

```
tar -zxvf druid-<version>-bin.tar.gz
```

If you cd into the directory:

```
cd druid-<version>
```

You should see a bunch of files:

* run_example_server.sh
* run_example_client.sh
* LICENSE, config, examples, lib directories
* extensions (This folder contains all the extensions that could be loaded by Druid. Note that extension `mysql-metadata-storage` is packaged in a separate tarball that can be downloaded from [here](http://druid.io/downloads.html). See [Including Extensions](../operations/including-extensions.html) for more information about loading extensions.
* hadoop_dependencies (This folder contains hadoop-client:2.3.0, see [Different Hadoop Versions](../operations/other-hadoop.html) for more information about how Druid picks up Hadoop dependencies)

## External Dependencies

Druid requires 3 external dependencies.

* A "deep storage" that acts as a data repository. This is generally distributed storage like HDFS or S3. For prototyping or experimentation on a single machine, Druid can use the local filesystem.

* A "metadata storage" to hold configuration and metadata information. This is generally a small, shared database like MySQL or PostgreSQL. For prototyping or experimentation on a single machine, Druid can use a local instance of [Apache Derby](http://db.apache.org/derby/).

* [Apache Zookeeper](http://zookeeper.apache.org/) for coordination among different pieces of the cluster.

This tutorial only requires Zookeeper be set up.

#### Set up Zookeeper

* Download zookeeper from [http://www.apache.org/dyn/closer.cgi/zookeeper/](http://www.apache.org/dyn/closer.cgi/zookeeper/).
* Install zookeeper.

```bash
ZOOKEPER_VERSION=zookeeper-3.4.6
curl http://www.gtlib.gatech.edu/pub/apache/zookeeper/$ZOOKEPER_VERSION/$ZOOKEPER_VERSION.tar.gz -o $ZOOKEPER_VERSION.tar.gz
tar xzf $ZOOKEPER_VERSION.tar.gz
cd $ZOOKEPER_VERSION
cp conf/zoo_sample.cfg conf/zoo.cfg
./bin/zkServer.sh start
cd ..
```

Note you might update zookeper version. (see [here](http://www.gtlib.gatech.edu/pub/apache/zookeeper/)).

Running Example Scripts
-----------------------

Let's start by spinning up a server. You can start an example Druid [Realtime](../design/realtime.html) node by issuing:

```
./run_example_server.sh
```

Select the "wikipedia" example.

Note that the first time you start the example, it may take some extra time due to its fetching various dependencies. Once the node starts up you will see a bunch of logs about setting up properties and connecting to the data source. If everything was successful, you should see messages of the form shown below.

```
2015-02-17T21:46:36,804 INFO [main] org.eclipse.jetty.server.ServerConnector - Started ServerConnector@79b6cf95{HTTP/1.1}{0.0.0.0:8084}
2015-02-17T21:46:36,804 INFO [main] org.eclipse.jetty.server.Server - Started @9580ms
2015-02-17T21:46:36,862 INFO [ApiDaemon] io.druid.segment.realtime.firehose.IrcFirehoseFactory - irc connection to server [irc.wikimedia.org] established
2015-02-17T21:46:36,862 INFO [ApiDaemon] io.druid.segment.realtime.firehose.IrcFirehoseFactory - Joining channel #en.wikipedia
2015-02-17T21:46:36,863 INFO [ApiDaemon] io.druid.segment.realtime.firehose.IrcFirehoseFactory - Joining channel #fr.wikipedia
2015-02-17T21:46:36,863 INFO [ApiDaemon] io.druid.segment.realtime.firehose.IrcFirehoseFactory - Joining channel #de.wikipedia
2015-02-17T21:46:36,863 INFO [ApiDaemon] io.druid.segment.realtime.firehose.IrcFirehoseFactory - Joining channel #ja.wikipedia
2015-02-17T21:46:37,009 INFO [ServerInventoryView-0] io.druid.client.BatchServerInventoryView - Inventory Initialized
```

The Druid real-time node ingests events in an in-memory buffer. Periodically, these events will be persisted to disk. If you are interested in the details of our real-time architecture and why we persist indexes to disk, we suggest you read our [White Paper](http://static.druid.io/docs/druid.pdf).

To query the real-time node you've spun up, you can issue:

```
./run_example_client.sh
```

Select "wikipedia" once again. This script issues [TimeBoundary](../querying/timeboundaryquery.html) to the data we've been ingesting. The query looks like this:

```json
{
   "queryType":"timeBoundary",
   "dataSource":"wikipedia"
}
```

The **timeBoundary** query is one of the simplest queries you can make in Druid. It gives you the boundaries of the ingested data.

The result looks something like this (when it's prettified):

```json
[ {
  "timestamp" : "2013-09-04T21:44:00.000Z",
  "result" : {
    "minTime" : "2013-09-04T21:44:00.000Z",
    "maxTime" : "2013-09-04T21:47:00.000Z"
  }
} ]
```

If you are having problems with getting results back, make sure you have [curl](http://curl.haxx.se/) installed. Control+C to break out of the client script.

Querying Druid
--------------

In your favorite editor, create the file:

```
timeseries.json
```

We are going to make a slightly more complicated query, the [TimeseriesQuery](../querying/timeseriesquery.html). Copy and paste the following into the file:

```json
{
    "queryType": "timeseries",
    "dataSource": "wikipedia",
    "intervals": [ "2010-01-01/2020-01-01" ],
    "granularity": "all",
    "aggregations": [
        {"type": "longSum", "fieldName": "count", "name": "edit_count"},
        {"type": "doubleSum", "fieldName": "added", "name": "chars_added"}
    ]
}
```

Our query has now expanded to include a time interval, [Granularities](../querying/granularities.html), and [Aggregations](../querying/aggregations.html). What the query is doing is aggregating a set of metrics over a span of time, and the results are grouped into a single time bucket.
To issue the query and get some results, run the following in your command line:

```
curl -X POST 'http://localhost:8084/druid/v2/?pretty' -H 'content-type: application/json'  -d  @timeseries.json
```

Once again, you should get a JSON blob of text back with your results, that looks something like this:

```json
[ {
 "timestamp" : "2013-09-04T21:44:00.000Z",
 "result" : { "chars_added" : 312670.0, "edit_count" : 733 }
} ]
```

If you issue the query again, you should notice your results updating.

Right now all the results you are getting back are being aggregated into a single timestamp bucket. What if we wanted to see our aggregations on a per minute basis?

We can change granularity for the results to "minute". To specify different granularities to bucket our results, we change our query like so:

```json
{
  "queryType": "timeseries",
  "dataSource": "wikipedia",
  "intervals": [ "2010-01-01/2020-01-01" ],
  "granularity": "minute",
  "aggregations": [
     {"type": "longSum", "fieldName": "count", "name": "edit_count"},
     {"type": "doubleSum", "fieldName": "added", "name": "chars_added"}
  ]
}
```

This gives us results like the following:

```json
[
 {
   "timestamp" : "2013-09-04T21:44:00.000Z",
   "result" : { "chars_added" : 30665.0, "edit_count" : 128 }
 },
 {
   "timestamp" : "2013-09-04T21:45:00.000Z",
   "result" : { "chars_added" : 122637.0, "edit_count" : 167 }
 },
 {
   "timestamp" : "2013-09-04T21:46:00.000Z",
   "result" : { "chars_added" : 78938.0, "edit_count" : 159 }
 },
...
]
```

Solving a Problem
-----------------

One of Druid's main powers is to provide answers to problems, so let's pose a problem. What if we wanted to know what the top pages in the US are, ordered by the number of edits over the last few minutes you've been going through this tutorial? To solve this problem, we can use the [TopN](../querying/topnquery.html).

Let's create the file:

```
topn.json
```

and put the following in there:

```json
{
  "queryType": "topN",
  "dataSource": "wikipedia",
  "granularity": "all",
  "dimension": "page",
  "metric": "edit_count",
  "threshold" : 10,
  "aggregations": [
    {"type": "longSum", "fieldName": "count", "name": "edit_count"}
  ],
  "filter": { "type": "selector", "dimension": "country", "value": "United States" },
  "intervals": ["2012-10-01T00:00/2020-01-01T00"]
}
```

Note that our query now includes [Filters](../querying/filters.html). Filters are like `WHERE` clauses in SQL and help narrow down the data that needs to be scanned.

If you issue the query:

```
curl -X POST 'http://localhost:8084/druid/v2/?pretty' -H 'content-type: application/json'  -d @topn.json
```

You should see an answer to our question. As an example, some results are shown below:

```json
[
 {
   "timestamp" : "2013-09-04T21:00:00.000Z",
   "result" : [
    { "page" : "RTC_Transit", "edit_count" : 6 },
    { "page" : "List_of_Deadly_Women_episodes", "edit_count" : 4 },
    { "page" : "User_talk:David_Biddulph", "edit_count" : 4 },
    ...
   ]
 }
]
```

Feel free to tweak other query parameters to answer other questions you may have about the data. Druid also includes more complex query types such as [groupBy queries](../querying/groupbyquery.html). For more information on querying, see this [link](../querying/querying.html).

Next Steps
----------

This tutorial only covered the basic operations of a single Druid node. For production, you'll likely need a full Druid cluster. Check out our next tutorial [The Druid Cluster](../tutorials/tutorial-the-druid-cluster.html) to learn more.

To learn more about loading streaming data, see [Loading Streaming Data](../tutorials/tutorial-loading-streaming-data.html).

To learn more about loading batch data, see [Loading Batch Data](../tutorials/tutorial-loading-batch-data.html).

Additional Information
----------------------

This tutorial is merely showcasing a small fraction of what Druid can do. If you are interested in more information about Druid, including setting up a more sophisticated Druid cluster, read more of the Druid documentation and blogs found on druid.io.

Hopefully you learned a thing or two about Druid real-time ingestion, querying Druid, and how Druid can be used to solve problems. If you have additional questions, feel free to post in our [google groups page](https://groups.google.com/forum/#!forum/druid-user).
