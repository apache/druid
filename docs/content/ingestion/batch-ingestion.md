---
layout: doc_page
---

# Batch Data Ingestion

Druid can load data from static files through a variety of methods described here.

## Native Batch Ingestion

Druid has built-in batch ingestion functionality. See [here](../ingestion/native-batch.html) for more info.

## Hadoop Batch Ingestion

Hadoop can be used for batch ingestion. The Hadoop-based batch ingestion will be faster and more scalable than the native batch ingestion. See [here](../ingestion/hadoop.html) for more details.

## Command Line Hadoop Indexer

If you don't want to use a full indexing service to use Hadoop to get data into Druid, you can also use the standalone command line Hadoop indexer. 
See [here](../ingestion/command-line-hadoop-indexer.html) for more info.

Having Problems?
----------------
Getting data into Druid can definitely be difficult for first time users. Please don't hesitate to ask questions in our IRC channel or on our [google groups page](https://groups.google.com/forum/#!forum/druid-user).
