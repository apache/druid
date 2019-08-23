---
id: druid-vs-spark
title: "Apache Druid vs Spark"
---

<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->


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
