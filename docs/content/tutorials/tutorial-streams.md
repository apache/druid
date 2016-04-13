---
layout: doc_page
---

# Tutorial: Load your own streaming data

## Getting started

This tutorial shows you how to load your own streams into Druid.

For this tutorial, we'll assume you've already downloaded Druid and Tranquility as described in
the [single-machine quickstart](quickstart.html) and have it running on your local machine. You
don't need to have loaded any data yet.

Once that's complete, you can load your own dataset by writing a custom ingestion spec.

## Writing an ingestion spec

When loading streams into Druid, we recommend using the [stream push](../ingestion/stream-push.html)
process. In this tutorial we'll be using [Tranquility Server](../ingestion/stream-ingestion.html#server) to push
data into Druid over HTTP.

<div class="note info">
This tutorial will show you how to push streams to Druid using HTTP, but Druid additionally supports
a wide variety of batch and streaming loading methods. See the <a href="../ingestion/batch-ingestion.html">Loading files</a>
and <a href="../ingestion/stream-ingestion.html">Loading streams</a> pages for more information about other options,
including from Hadoop, Kafka, Storm, Samza, Spark Streaming, and your own JVM apps.
</div>

You can prepare for loading a new dataset over HTTP by writing a custom Tranquility Server
configuration. The bundled configuration is in `conf-quickstart/tranquility/server.json`, which
you can modify for your own needs.

The most important questions are:

  * What should the dataset be called? This is the "dataSource" field of the "dataSchema".
  * Which field should be treated as a timestamp? This belongs in the "column" field of the "timestampSpec".
  * Which fields should be treated as dimensions? This belongs in the "dimensions" field of the "dimensionsSpec".
  * Which fields should be treated as measures? This belongs in the "metricsSpec" field.

Let's use a small JSON pageviews dataset as an example, with records like:

```json
{"time": "2000-01-01T00:00:00Z", "url": "/foo/bar", "user": "alice", "latencyMs": 32}
```

So the answers to the questions above are:

  * Let's call the dataset "pageviews".
  * The timestamp is the "time" field.
  * Good choices for dimensions are the string fields "url" and "user".
  * Good choices for measures are a count of pageviews, and the sum of "latencyMs". Collecting that
sum when we load the data will allow us to compute an average at query time as well.

Now, edit the existing `conf-quickstart/tranquility/server.json` file by altering these
sections:

  1. Change the key `"metrics"` under `"dataSources"` to `"pageviews"`
  2. Alter these sections under the new `"pageviews"` key:
  ```json
  "dataSource": "pageviews"
  ```

  ```json
  "timestampSpec": {
       "format": "auto",
       "column": "time"
  }
  ```

  ```json
  "dimensionsSpec": {
       "dimensions": ["url", "user"]
  }
  ```

  ```json
  "metricsSpec": [
       {"name": "views", "type": "count"},
       {"name": "latencyMs", "type": "doubleSum", "fieldName": "latencyMs"}
  ]
  ```

## Restarting the server

Restart the server to pick up the new configuration file by stopping Tranquility (CTRL-C) and starting it up again.

## Sending data

Let's send some data! We'll start with these three records:

```json
{"time": "2000-01-01T00:00:00Z", "url": "/foo/bar", "user": "alice", "latencyMs": 32}
{"time": "2000-01-01T00:00:00Z", "url": "/", "user": "bob", "latencyMs": 11}
{"time": "2000-01-01T00:00:00Z", "url": "/foo/bar", "user": "bob", "latencyMs": 45}
```

Druid streaming ingestion requires relatively current messages (relative to a slack time controlled by the
[windowPeriod](../ingestion/stream-push.html#segmentgranularity-and-windowperiod) value), so you should
replace `2000-01-01T00:00:00Z` in these messages with the current time in ISO8601 format. You can
get this by running:

```bash
python -c 'import datetime; print(datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"))'
```

Update the timestamps in the JSON above, and save it to a file named `pageviews.json`. Then send
it to Druid by running:

```bash
curl -XPOST -H'Content-Type: application/json' --data-binary @pageviews.json http://localhost:8200/v1/post/pageviews
```

This will print something like:

```
{"result":{"received":3,"sent":3}}
```

This indicates that the HTTP server received 3 events from you, and sent 3 to Druid. Note that
this may take a few seconds to finish the first time you run it, as Druid resources must be
allocated to the ingestion task. Subsequent POSTs should complete quickly.

If you see `"sent":0` this likely means that your timestamps are not recent enough. Try adjusting
your timestamps and re-sending your data.

## Querying your data

After sending data, you can immediately query it using any of the
[supported query methods](../querying/querying.html).

## Further reading

To read more about loading streams, see our [streaming ingestion documentation](../ingestion/stream-ingestion.html).
