---
layout: doc_page
---

Druid vs. Key/Value Stores (HBase/Cassandra/OpenTSDB)
====================================================

Druid is highly optimized for scans and aggregations, it supports arbitrarily deep drill downs into data sets. This same functionality 
is supported in key/value stores in 2 ways:

1. Pre-compute all permutations of possible user queries
2. Range scans on event data

When pre-computing results, the key is the exact parameters of the query, and the value is the result of the query.  
The queries return extremely quickly, but at the cost of flexibility, as ad-hoc exploratory queries are not possible with 
pre-computing every possible query permutation. Pre-computing all permutations of all ad-hoc queries leads to result sets 
that grow exponentially with the number of columns of a data set, and pre-computing queries for complex real-world data sets 
can require hours of pre-processing time.

The other approach to using key/value stores for aggregations to use the dimensions of an event as the key and the event measures as the value. 
Aggregations are done by issuing range scans on this data. Timeseries specific databases such as OpenTSDB use this approach. 
One of the limitations here is that the key/value storage model does not have indexes for any kind of filtering other than prefix ranges, 
which can be used to filter a query down to a metric and time range, but cannot resolve complex predicates to narrow the exact data to scan. 
When the number of rows to scan gets large, this limitation can greatly reduce performance. It is also harder to achieve good 
locality with key/value stores because most don’t support pushing down aggregates to the storage layer.

For arbitrary exploration of data (flexible data filtering), Druid's custom column format enables ad-hoc queries without pre-computation. The format 
also enables fast scans on columns, which is important for good aggregation performance.
