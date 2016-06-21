---
layout: doc_page
---

Druid vs Spark
==============

Druid and Spark are complementary solutions as Druid can be used to accelerate OLAP queries in Spark.

Spark is a general cluster computing framework initially designed around the concept of Resilient Distributed Datasets (RDDs). 
RDDs enable data reuse by persisting intermediate results 
in memory and enable Spark to provide fast computations for iterative algorithms.
This is especially beneficial for certain work flows such as machine
learning, where the same operation may be applied over and over
again until some result is converged upon. The generality of Spark makes it very suitable as an engine to process (clean or transform) data. 
Although Spark provides the ability to query data through Spark SQL, much like Hadoop, the query latencies are not specifically targeted to be interactive (sub-second).

Druid's focus is on extremely low latency queries, and is ideal for powering applications used by thousands of users, and where each query must 
return fast enough such that users can interactively explore through data. Druid fully indexes all data, and can act as a middle layer between Spark and your application. 
One typical setup seen in production is to process data in Spark, and load the processed data into Druid for faster access.

For more information about using Druid and Spark together, including benchmarks of the two systems, please see:

<https://www.linkedin.com/pulse/combining-druid-spark-interactive-flexible-analytics-scale-butani>
