---
layout: doc_page
---

## My Data isn't being loaded

### Realtime Ingestion

If you are trying to stream in historical (not current time) data into Druid and you are using the [serverTime](../ingestion/realtime-ingestion.html) rejection policy in your ingestion spec (the default rejection policy), Druid will not ingest this data as it is outside of the acceptable window period. You can verify this is what is happening by looking at the logs of your real-time process for log lines containing "ingest/events/*". These metrics will indicate the events ingested, rejected, etc. We recommend using batch ingestion methods for historical data in production.

If you are doing a POC, you can use the [messageTime](../ingestion/realtime-ingestion.html) rejection policy, but please be aware of the hand-off caveats. This rejection policy is not recommended in production. 

If you are experimenting with realtime ingestion, you can also use the [none](../ingestion/realtime-ingestion.html) rejection policy to load all incoming events, but hand-off will never occur. 
 
### Batch Ingestion
 
If you are trying to batch load historical data but no events are being loaded, make sure the interval of your ingestion spec actually encapsulates the interval of your data. Events outside this interval are dropped. 

## What types of data does Druid support?

Druid can ingest JSON, CSV, TSV and other delimited data out of the box. Druid supports single dimension values, or multiple dimension values (an array of strings). Druid supports long and float numeric columns.

## Not all of my events were ingested

Druid will reject events outside of a window period. The best way to see if events are being rejected is to check the [Druid ingest metrics](../operations/metrics.html).

If the number of ingested events seem correct, make sure your query is correctly formed. If you included a `count` aggregator in your ingestion spec, you will need to query for the results of this aggregate with a `longSum` aggregator. Issuing a query with a count aggregator will count the number of Druid rows, which includes [roll-up](../design/index.html).

## Where do my Druid segments end up after ingestion?

Depending on what `druid.storage.type` is set to, Druid will upload segments to some [Deep Storage](../dependencies/deep-storage.html). Local disk is used as the default deep storage.

## My realtime node is not handing segments off

First, make sure there are no exceptions in the logs of your node. Also make sure that `druid.storage.type` is set to a deep storage that makes sense.

Other common reasons that hand-off fails are as follows:

1) Druid is unable to write to the metadata storage. Make sure your configuration is correct.

2) Historical nodes are out of capacity and cannot download any more segments. You'll see exceptions in the coordinator logs if this occurs.

3) Segments are corrupt and cannot download. You'll see exceptions in your historical nodes if this occurs.

4) Deep storage is improperly configured. Make sure that your segment actually exists in deep storage and that the coordinator logs have no errors.

## How do I get HDFS to work?

Make sure to include the `druid-hdfs-storage` and all the hadoop configuration, dependencies (that can be obtained by running command `hadoop classpath` on a machine where hadoop has been setup) in the classpath. And, provide necessary HDFS settings as described in [Deep Storage](../dependencies/deep-storage.html) .

## I don't see my Druid segments on my historical nodes
You can check the coordinator console located at `<COORDINATOR_IP>:<PORT>`. Make sure that your segments have actually loaded on [historical nodes](../design/historical.html). If your segments are not present, check the coordinator logs for messages about capacity of replication errors. One reason that segments are not downloaded is because historical nodes have maxSizes that are too small, making them incapable of downloading more data. You can change that with (for example):

```
-Ddruid.segmentCache.locations=[{"path":"/tmp/druid/storageLocation","maxSize":"500000000000"}]
-Ddruid.server.maxSize=500000000000
 ```

## My queries are returning empty results

You can check `<BROKER_IP>:<PORT>/druid/v2/datasources/<YOUR_DATASOURCE>?interval=0/3000` for the dimensions and metrics that have been created for your datasource. Make sure that the name of the aggregators you use in your query match one of these metrics. Also make sure that the query interval you specify match a valid time range where data exists. Note: the broker endpoint will only return valid results on historical segments and not segments served by real-time nodes.

## How can I Reindex existing data in Druid with schema changes?

You can use IngestSegmentFirehose with index task to ingest existing druid segments using a new schema and change the name, dimensions, metrics, rollup, etc. of the segment.
See [Firehose](../ingestion/firehose.html) for more details on IngestSegmentFirehose.
Or, if you use hadoop based ingestion, then you can use "dataSource" input spec to do reindexing. See [batch-ingestion](../ingestion/batch-ingestion.html) for more details.

## How can I change the granularity of existing data in Druid?

In a lot of situations you may want to lower the granularity of older data. Example, any data older than 1 month has only hour level granularity but newer data has minute level granularity. This use case is same as re-indexing.

To do this use the IngestSegmentFirehose and run an indexer task. The IngestSegment firehose will allow you to take in existing segments from Druid and aggregate them and feed them back into druid. It will also allow you to filter the data in those segments while feeding it back in. This means if there are rows you want to delete, you can just filter them away during re-ingestion.
Typically the above will be run as a batch job to say everyday feed in a chunk of data and aggregate it.
Or, if you use hadoop based ingestion, then you can use "dataSource" input spec to do reindexing. See [batch-ingestion](../ingestion/batch-ingestion.html) for more details.

## Real-time ingestion seems to be stuck

There are a few ways this can occur. Druid will throttle ingestion to prevent out of memory problems if the intermediate persists are taking too long or if hand-off is taking too long. If your node logs indicate certain columns are taking a very long time to build (for example, if your segment granularity is hourly, but creating a single column takes 30 minutes), you should re-evaluate your configuration or scale up your real-time ingestion. 

## More information

Getting data into Druid can definitely be difficult for first time users. Please don't hesitate to ask questions in our IRC channel or on our [google groups page](https://groups.google.com/forum/#!forum/druid-user).
