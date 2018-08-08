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

```
curl http://static.druid.io/tranquility/releases/tranquility-distribution-0.8.2.tgz -o tranquility-distribution-0.8.2.tgz
tar -xzf tranquility-distribution-0.8.2.tgz
cd tranquility-distribution-0.8.2
```

## Run Tranquility server

Run the following command:

```
bin/tranquility server -configFile ../examples/conf/tranquility/wikipedia-server.json -Ddruid.extensions.loadList=[]
```

## Send data

Let's send the sample Wikipedia edits data to Tranquility:

```
curl -XPOST -H'Content-Type: application/json' --data-binary @quickstart/wikiticker-2015-09-12-sampled.json http://localhost:8200/v1/post/wikipedia
```

Which will print something like:

```
{"result":{"received":39244,"sent":39244}}
```

This indicates that the HTTP server received 39,244 events from you, and sent 39,244 to Druid. This
command may generate a "connection refused" error if you run it too quickly after enabling Tranquility
Server, which means the server has not yet started up. It should start up within a few seconds. The command
may also take a few seconds to finish the first time you run it, during which time Druid resources are being
allocated to the ingestion task. Subsequent POSTs will complete quickly once this is done.

Once the data is sent to Druid, you can immediately query it.

If you see a `sent` count of 0, retry the send command until the `sent` count also shows 39244:

```
{"result":{"received":39244,"sent":0}}
```

## Querying your data

Please follow the [query tutorial](../tutorial/tutorial-query.html) to run some example queries on the newly loaded data.

## Cleanup

If you wish to go through any of the other ingestion tutorials, you will need to reset the cluster and follow these [reset instructions](index.html#resetting-the-cluster), as the other tutorials will write to the same "wikipedia" datasource.

When cleaning up after running this Tranquility tutorial, it is also necessary to recomment the `tranquility-server` line in `quickstart/tutorial/conf/tutorial-cluster.conf` before restarting the cluster.


## Further reading

For more information on Tranquility, please see [the Tranquility documentation](https://github.com/druid-io/tranquility).
