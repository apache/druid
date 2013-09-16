---
layout: default
---
Concepts and Terminology
========================

-   **Aggregators:** A mechanism for combining records during realtime incremental indexing, Hadoop batch indexing, and in queries.
-   **DataSource:** A table-like view of data; specified in a “specFile” and in a query.
-   **Granularity:** The time interval corresponding to aggregation by time.
    -   The *indexGranularity* setting in a schema is used to aggregate input (ingest) records within an interval into a single output (internal) record.
    -   The *segmentGranularity* is the interval specifying how internal records are stored together in a single file.

-   **Segment:** A collection of (internal) records that are stored and processed together.
-   **Shard:** A unit of partitioning data across machine. TODO: clarify; by time or other dimensions?
-   **specFile** is specification for services in JSON format; see [[Realtime]] and [[Batch-ingestion]]
