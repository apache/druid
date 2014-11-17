---
layout: doc_page
---

Druid vs. Cassandra
===================


We are not experts on Cassandra, if anything is incorrect about our portrayal, please let us know on the mailing list or via some other means.  We will fix this page.

Druid is highly optimized for scans and aggregations, it supports arbitrarily deep drill downs into data sets without the need to pre-compute, and it can ingest event streams in real-time and allow users to query events as they come in. Cassandra is a great key-value store and it has some features that allow you to use it to do more interesting things than what you can do with a pure key-value store. But, it is not built for the same use cases that Druid handles, namely regularly scanning over billions of entries per query.

Furthermore, Druid is fully read-consistent. Druid breaks down a data set into immutable chunks known as segments. All replicants always present the exact same view for the piece of data they are holding and we don’t have to worry about data synchronization. The tradeoff is that Druid has limited semantics for write and update operations. Cassandra, similar to Amazon’s Dynamo, has an eventually consistent data model. Writes are always supported but updates to data may take some time before all replicas sync up (data reconciliation is done at read time). This model favors availability and scalability over consistency.
