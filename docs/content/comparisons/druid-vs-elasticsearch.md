---
layout: doc_page
---

Druid vs Elasticsearch
======================

We are not experts on search systems, if anything is incorrect about our portrayal, please let us know on the mailing list or via some other means.

Elasticsearch is a search systems based on Apache Lucene. It provides full text search for schema-free documents 
and provides access to raw event level data. Elasticsearch is increasingly adding more support for analytics and aggregations. 
[Some members of the community](https://groups.google.com/forum/#!msg/druid-development/nlpwTHNclj8/sOuWlKOzPpYJ) have pointed out  
the resource requirements for data ingestion and aggregation in Elasticsearch is much higher than those of Druid.

Elasticsearch also does not support data summarization/roll-up at ingestion time, which can compact the data that needs to be 
stored up to 100x with real-world data sets. This leads to Elasticsearch having greater storage requirements.

Druid focuses on OLAP work flows. Druid is optimized for high performance (fast aggregation and ingestion) at low cost, 
and supports a wide range of analytic operations. Druid has some basic search support for structured event data, but does not support 
full text search. Druid also does not support completely unstructured data. Measures must be defined in a Druid schema such that 
summarization/roll-up can be done.
