---
layout: doc_page
---
# Integrating Druid With Other Technologies

This page discusses how we can integrate Druid with other technologies. 

## Integrating with Open Source Streaming Technologies

Event streams can be stored in a distributed message bus such as Kafka and further processed via a distributed stream  
processor system such as Storm, Samza, or Spark Streaming. Data processed by the stream processor can feed into Druid using 
the [Tranquility](https://github.com/druid-io/tranquility) library.

<img src="../../img/druid-production.png" width="800"/>

## Integrating with SQL-on-Hadoop Technologies

Druid should theoretically integrate well with SQL-on-Hadoop technologies such as Apache Drill, Spark SQL, Presto, Impala, and Hive.
