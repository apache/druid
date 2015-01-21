---
layout: doc_page
---
Examples
========

The examples on this page are setup in order to give you a feel for what Druid does in practice. They are quick demos of Druid based on [CliRealtimeExample](https://github.com/metamx/druid/blob/master/services/src/main/java/io/druid/cli/CliRealtimeExample.java). While you wouldn’t run it this way in production you should be able to see how ingestion works and the kind of exploratory queries that are possible. Everything that can be done on your box here can be scaled out to 10’s of billions of events and terabytes of data per day in a production cluster while still giving the snappy responsive exploratory queries.

Installing Standalone Druid
---------------------------

There are two options for installing standalone Druid. Building from source, and downloading the Druid Standalone Kit (DSK).

### Building from source

Clone Druid and build it:

``` bash
git clone https://github.com/metamx/druid.git druid
cd druid
git fetch --tags
git checkout druid-0.6.171
./build.sh
```

### Downloading the DSK (Druid Standalone Kit)

[Download](http://static.druid.io/artifacts/releases/druid-services-0.6.171-bin.tar.gz) a stand-alone tarball and run it:

``` bash
tar -xzf druid-services-0.X.X-bin.tar.gz
cd druid-services-0.X.X
```

Twitter Example
---------------

For a full tutorial based on the twitter example, check out this [Twitter Tutorial](Twitter-Tutorial.html).

This Example uses a feature of Twitter that allows for sampling of it’s stream. We sample the Twitter stream via our [TwitterSpritzerFirehoseFactory](https://github.com/metamx/druid/blob/master/examples/src/main/java/druid/examples/twitter/TwitterSpritzerFirehoseFactory.java) class and use it to simulate the kinds of data you might ingest into Druid. Then, with the client part, the sample shows what kinds of analytics explorations you can do during and after the data is loaded.

### What you’ll learn
* See how large amounts of data gets ingested into Druid in real-time
* Learn how to do fast, interactive, analytics queries on that real-time data

### What you need
* A build of standalone Druid with the Twitter example (see above)
* A Twitter username and password.

### What you’ll do

See [Twitter Tutorial](Twitter-Tutorial.html)

Rand Example
------------

This uses `RandomFirehoseFactory` which emits a stream of random numbers (outColumn, a positive double) with timestamps along with an associated token (target). This provides a timeseries that requires no network access for demonstration, characterization, and testing. The generated tuples can be thought of as asynchronously produced triples (timestamp, outColumn, target) where the timestamp varies depending on speed of processing.

In a terminal window, (NOTE: If you are using the cloned Github repository these scripts are in ./examples/bin) start the server with:

``` bash
./run_example_server.sh # type rand when prompted
```

In another terminal window:

``` bash
./run_example_client.sh # type rand when prompted
```

The result of the client query is in JSON format. The client makes a REST request using the program `curl` which is usually installed on Linux, Unix, and OSX by default.
