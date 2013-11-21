---
layout: doc_page
---
Concepts and Terminology
========================

The following definitions are with respect to the Druid data store. They are intended to help you better understand the Druid documentation, where the defined terms are used. While reading the definitions in order isn't necessary, some entries do build on previous definitions.

More definitions are also available on the [architecture design page](Design).

## Data

* **Timeseries Data** Data points which are ordered in time. The closing value of a financial index or the number of tweets per hour with a certain hashtag are examples of timeseries data.

* **Timestamp** An absolute position on a timeline, given in a standard alpha-numerical format such as with UTC time. Timeseries data points can be ordered by timestamp, and in Druid, they are.

* **Columns** The format for storing records (as opposed to rows). Druid stores records in columns rather than using the classic row-oriented format of traditional RDBMS. This columnar format allows for performing analytics at speeds magnitudes faster than on row-oriented data.

* **Dimensions** Aspects or categories of data, such as languages or locations. For example, with *language* and *country* as the type of dimension, values could be "English" or "Mandarin" for language, or "USA" or "China" for country. In Druid, dimensions can serve as filters for narrowing down hits (for example, language = "English" or country = "China").

* **Metrics** Countable data that can be aggregated. Metrics, for example, can be the number of visitors to a website, number of tweets per day, or average revenue.

* **Segment** A collection of (internal) records that are stored and processed together. Druid chunks data into segments representing a time interval, and these are stored and manipulated in the cluster.

* **Shard** A sub-partition of the data, allowing multiple segments to represent the data in a certain time interval. Sharding occurs along time partitions to better handle amounts of data that exceed certain limits on segment size, although sharding along dimensions may also occur to optimize efficiency.


## Ingestion

* **Aggregation** The summarizing of data meeting certain specifications. Druid aggregates timeseries data, which in effect compacts the data. Time intervals (set in configuration) are used to create buckets, while timestamps determine which buckets data is sent to.

* **Granularity** The time interval corresponding to aggregation by time. Druid configuration settings specify the granularity of timestamp buckets in a segment (for example, by minute or by hour), as well as the granularity of the segment itself. The latter is essentially the overall range of absolute time covered by the segment.



## Queries

* **Aggregators** A mechanism for combining records during realtime incremental indexing, Hadoop batch indexing, and in queries.

* **specFile** The specification for services in JSON format; see [Realtime](Realtime.html) and [Batch-ingestion](Batch-ingestion.html)

* **DataSource** A table-like view of data; specified in specFiles and in queries.
