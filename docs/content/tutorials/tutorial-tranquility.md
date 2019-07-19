---
layout: doc_page
title: "Tutorial: Load streaming data with HTTP push"
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

# Tutorial: Load streaming data with HTTP push

## Getting started

This tutorial shows you how to load streaming data into Apache Druid (incubating) using HTTP push via Tranquility Server.

[Tranquility Server](https://github.com/druid-io/tranquility/blob/master/docs/server.md) allows a stream of data to be pushed into Druid using HTTP POSTs.

For this tutorial, we'll assume you've already downloaded Druid as described in
the [quickstart](index.html) using the `micro-quickstart` single-machine configuration and have it
running on your local machine. You don't need to have loaded any data yet.

## Download Tranquility

In the Druid package root, run the following commands:

```bash
curl http://static.druid.io/tranquility/releases/tranquility-distribution-0.8.3.tgz -o tranquility-distribution-0.8.3.tgz
tar -xzf tranquility-distribution-0.8.3.tgz
mv tranquility-distribution-0.8.3 tranquility
```

The startup scripts for the tutorial will expect the contents of the Tranquility tarball to be located at `tranquility` under the apache-druid-#{DRUIDVERSION} package root.

## Enable Tranquility Server

- In your `conf/supervise/single-server/micro-quickstart.conf`, uncomment the `tranquility-server` line.
- Stop your *bin/supervise* command (CTRL-C) and then restart it by again running `bin/supervise -c conf/supervise/single-server/micro-quickstart.conf`.

As part of the output of *supervise* you should see something like:

```bash
Running command[tranquility-server], logging to[/stage/apache-druid-#{DRUIDVERSION}/var/sv/tranquility-server.log]: tranquility/bin/tranquility server -configFile conf/tranquility/server.json -Ddruid.extensions.loadList=[]
```

You can check the log file in `var/sv/tranquility-server.log` to confirm that the server is starting up properly.

## Send data

Let's send the sample Wikipedia edits data to Tranquility:

```bash
gunzip -k quickstart/tutorial/wikiticker-2015-09-12-sampled.json.gz 
curl -XPOST -H'Content-Type: application/json' --data-binary @quickstart/tutorial/wikiticker-2015-09-12-sampled.json http://localhost:8200/v1/post/wikipedia
```

Which will print something like:

```json
{"result":{"received":39244,"sent":39244}}
```

This indicates that the HTTP server received 39,244 events from you, and sent 39,244 to Druid. This
command may generate a "connection refused" error if you run it too quickly after enabling Tranquility
Server, which means the server has not yet started up. It should start up within a few seconds. The command
may also take a few seconds to finish the first time you run it, during which time Druid resources are being
allocated to the ingestion task. Subsequent POSTs will complete quickly once this is done.

Once the data is sent to Druid, you can immediately query it.

If you see a `sent` count of 0, retry the send command until the `sent` count also shows 39244:

```json
{"result":{"received":39244,"sent":0}}
```

## Querying your data

Please follow the [query tutorial](../tutorials/tutorial-query.html) to run some example queries on the newly loaded data.

## Cleanup

If you wish to go through any of the other ingestion tutorials, you will need to shut down the cluster and reset the cluster state by removing the contents of the `var` directory under the druid package, as the other tutorials will write to the same "wikipedia" datasource.

When cleaning up after running this Tranquility tutorial, it is also necessary to recomment the `tranquility-server` line in `conf/supervise/single-server/micro-quickstart.conf` before restarting the cluster.


## Further reading

For more information on Tranquility, please see [the Tranquility documentation](https://github.com/druid-io/tranquility).
