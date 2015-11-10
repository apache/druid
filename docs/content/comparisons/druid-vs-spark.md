---
layout: doc_page
---

Druid vs Spark
==============

Druid and Spark are complementary solutions as Druid can be used to accelerate OLAP queries in Spark.

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
