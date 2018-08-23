---
layout: doc_page
---

# Tutorial: Load streaming data with HTTP push

## Getting started

This tutorial shows you how to load streaming data into Druid using HTTP push via Tranquility Server.

[Tranquility Server](https://github.com/druid-io/tranquility/blob/master/docs/server.md) allows a stream of data to be pushed into Druid using HTTP POSTs.

For this tutorial, we'll assume you've already downloaded Druid as described in
the [single-machine quickstart](quickstart.html) and have it running on your local machine. You
don't need to have loaded any data yet.

## Download Tranquility

In the Druid package root, run the following commands:

```bash
curl http://static.druid.io/tranquility/releases/tranquility-distribution-0.8.2.tgz -o tranquility-distribution-0.8.2.tgz
tar -xzf tranquility-distribution-0.8.2.tgz
mv tranquility-distribution-0.8.2 tranquility
```

The startup scripts for the tutorial will expect the contents of the Tranquility tarball to be located at `tranquility` under the druid-#{DRUIDVERSION} package root.

## Enable Tranquility Server

- In your `quickstart/tutorial/conf/tutorial-cluster.conf`, uncomment the `tranquility-server` line.
- Stop your *bin/supervise* command (CTRL-C) and then restart it by again running `bin/supervise -c quickstart/tutorial/conf/tutorial-cluster.conf`.

As part of the output of *supervise* you should see something like:

```bash
Running command[tranquility-server], logging to[/stage/druid-#{DRUIDVERSION}/var/sv/tranquility-server.log]: tranquility/bin/tranquility server -configFile quickstart/tutorial/conf/tranquility/server.json -Ddruid.extensions.loadList=[]
```

You can check the log file in `var/sv/tranquility-server.log` to confirm that the server is starting up properly.

## Send data

Let's send the sample Wikipedia edits data to Tranquility:

```bash
gunzip -k quickstart/wikiticker-2015-09-12-sampled.json.gz 
curl -XPOST -H'Content-Type: application/json' --data-binary @quickstart/wikiticker-2015-09-12-sampled.json http://localhost:8200/v1/post/wikipedia
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

When cleaning up after running this Tranquility tutorial, it is also necessary to recomment the `tranquility-server` line in `quickstart/tutorial/conf/tutorial-cluster.conf` before restarting the cluster.


## Further reading

For more information on Tranquility, please see [the Tranquility documentation](https://github.com/druid-io/tranquility).
