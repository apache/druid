---
layout: doc_page
---

Druid vs General Computing Frameworks (Spark/Hadoop)
====================================================

Druid is much more complementary to general computing frameworks than it is competitive. General compute engines can 
very flexibly process and transform data. Druid acts as a query accelerator to these systems by indexing raw data into a custom 
column format to speed up OLAP queries.

## Druid vs Hadoop (HDFS/MapReduce)
 
Hadoop has shown the world that it’s possible to house your data warehouse on commodity hardware for a fraction of the price 
of typical solutions. As people adopt Hadoop for their data warehousing needs, they find two things.

1.  They can now query all of their data in a fairly flexible manner and answer any question they have
2.  The queries take a long time

The first one is the joy that everyone feels the first time they get Hadoop running. The latter is what they realize after they have used Hadoop interactively for a while because Hadoop is optimized for throughput, not latency. 

Druid is a complementary addition to Hadoop. Hadoop is great at storing and making accessible large amounts of individually low-value data. 
Unfortunately, Hadoop is not great at providing query speed guarantees on top of that data, nor does it have very good 
operational characteristics for a customer-facing production system. Druid, on the other hand, excels at taking high-value 
summaries of the low-value data on Hadoop, making it available in a fast and always-on fashion, such that it could be exposed directly to a customer.

Druid also requires some infrastructure to exist for [deep storage](../dependencies/deep-storage.html). 
HDFS is one of the implemented options for this [deep storage](../dependencies/deep-storage.html).

Please note we are only comparing Druid to base Hadoop here, but we welcome comparisons of Druid vs other systems or combinations 
of systems in the Hadoop ecosystems.

## Druid vs Spark

Spark is a cluster computing framework built around the concept of Resilient Distributed Datasets (RDDs) and
can be viewed as a back-office analytics platform.  RDDs enable data reuse by persisting intermediate results
in memory and enable Spark to provide fast computations for iterative algorithms.
This is especially beneficial for certain work flows such as machine
learning, where the same operation may be applied over and over
again until some result is converged upon.  Spark provides analysts with
the ability to run queries and analyze large amounts of data with a
wide array of different algorithms.

Druid is designed to power analytic applications and focuses on the latencies to ingest data and serve queries
over that data. If you were to build an application where users could
arbitrarily explore data, the latencies seen by using Spark will likely be too slow for an interactive experience.
