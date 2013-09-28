---
layout: doc_page
---
Concepts and Terminology
========================

* **Aggregators**: A mechanism for combining records during realtime incremental indexing, Hadoop batch indexing, and in queries.
* **DataSource**: A table-like view of data; specified in a "specFile" and in a query.
* **Granularity**: The time interval corresponding to aggregation by time.
    * **indexGranularity**: specifies the granularity used to bucket timestamps within a segment.
    * **segmentGranularity**: specifies the granularity of the segment, i.e. the amount of time a segment will represent
* **Segment**: A collection of (internal) records that are stored and processed together.
* **Shard**: A sub-partition of the data in a segment.  It is possible to have multiple segments represent all data for a given segmentGranularity.
* **specFile**: is specification for services in JSON format; see [Realtime](Realtime.html) and [Batch-ingestion](Batch-ingestion.html)
