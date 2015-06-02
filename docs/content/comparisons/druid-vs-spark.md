---
layout: doc_page
---

Druid vs Spark
==============

We are not experts on Spark, if anything is incorrect about our portrayal, please let us know on the mailing list or via some other means.

Spark is a cluster computing framework built around the concept of Resilient Distributed Datasets (RDDs) and
can be viewed as a back-office analytics platform.  RDDs enable data reuse by persisting intermediate results
in memory and enable Spark to provide fast computations for iterative algorithms.
This is especially beneficial for certain work flows such as machine
learning, where the same operation may be applied over and over
again until some result is converged upon.  Spark provides analysts with
the ability to run queries and analyze large amounts of data with a
wide array of different algorithms.

Druid is designed to power analytic applications and focuses on the latencies to ingest data and serve queries
over that data. If you were to build a web UI where users could
arbitrarily explore data, the latencies seen by using Spark may be too slow for interactive use cases.
