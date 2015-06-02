---
layout: doc_page
---

Druid vs Elasticsearch
======================

We are not experts on Elasticsearch, if anything is incorrect about our portrayal, please let us know on the mailing list or via some other means.

Elasticsearch is a search server based on Apache Lucene. It provides full text search for schema-free documents and provides access to raw event level data. Elasticsearch also provides support for analytics and aggregations. Based on [user testimony](https://groups.google.com/forum/#!msg/druid-development/nlpwTHNclj8/sOuWlKOzPpYJ), the resource requirements for data ingestion and aggregation in Elasticsearch are higher than those of Druid.

Druid focuses on OLAP work flows. Druid is optimized for high performance (fast aggregation and ingestion) at low cost, and supports a wide range of analytic operations. Druid has some basic search support for structured event data.
