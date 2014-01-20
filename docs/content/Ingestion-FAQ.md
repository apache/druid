---
layout: doc_page
---
## Where do my Druid segments end up after ingestion?

Depending on what `druid.storage.type` is set to, Druid will upload segments to some [Deep Storage](Deep-Storage.html). Local disk is used as the default deep storage.

## My realtime node is not handing segments off

Make sure that the `druid.publish.type` on your real-time nodes is set to "db". Also make sure that `druid.storage.type` is set to a deep storage that makes sense. Some example configs:

```
druid.publish.type=db

druid.db.connector.connectURI=jdbc\:mysql\://localhost\:3306/druid
druid.db.connector.user=druid
druid.db.connector.password=diurd

druid.storage.type=s3
druid.storage.bucket=druid
druid.storage.baseKey=sample
```

## I don't see my Druid segments on my historical nodes
You can check the coordinator console located at <COORDINATOR_IP>:<PORT>/cluster.html. Make sure that your segments have actually loaded on [historical nodes](Historical.html). If your segments are not present, check the coordinator logs for messages about capacity of replication errors. One reason that segments are not downloaded is because historical nodes have maxSizes that are too small, making them incapable of downloading more data. You can change that with (for example):

```
-Ddruid.segmentCache.locations=[{"path":"/tmp/druid/storageLocation","maxSize":"500000000000"}]
-Ddruid.server.maxSize=500000000000
 ```

## My queries are returning empty results

You can check <BROKER_IP>:<PORT>/druid/v2/datasources/<YOUR_DATASOURCE> for the dimensions and metrics that have been created for your datasource. Make sure that the name of the aggregators you use in your query match one of these metrics. Also make sure that the query interval you specify match a valid time range where data exists.

## More information

Getting data into Druid can definitely be difficult for first time users. Please don't hesitate to ask questions in our IRC channel or on our [google groups page](https://groups.google.com/forum/#!forum/druid-development).
