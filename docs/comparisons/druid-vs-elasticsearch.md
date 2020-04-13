---
id: druid-vs-elasticsearch
title: "Apache Druid vs Elasticsearch"
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


We are not experts on search systems, if anything is incorrect about our portrayal, please let us know on the mailing list or via some other means.

Elasticsearch is a search system based on Apache Lucene. It provides full text search for schema-free documents
and provides access to raw event level data. Elasticsearch is increasingly adding more support for analytics and aggregations.
[Some members of the community](https://groups.google.com/forum/#!msg/druid-development/nlpwTHNclj8/sOuWlKOzPpYJ) have pointed out
the resource requirements for data ingestion and aggregation in Elasticsearch is much higher than those of Druid.

Elasticsearch also does not support data summarization/roll-up at ingestion time, which can compact the data that needs to be
stored up to 100x with real-world data sets. This leads to Elasticsearch having greater storage requirements.

Druid focuses on OLAP work flows. Druid is optimized for high performance (fast aggregation and ingestion) at low cost,
and supports a wide range of analytic operations. Druid has some basic search support for structured event data, but does not support
full text search. Druid also does not support completely unstructured data. Measures must be defined in a Druid schema such that
summarization/roll-up can be done.
