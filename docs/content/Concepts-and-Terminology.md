---
layout: doc_page
---
Concepts and Terminology
========================

The following definitions are given with respect to the Druid data store. They are intended to help you better understand the Druid documentation, where these  terms and concepts occur.

More definitions are available on the [design page](Design.html).

* **Aggregation** The summarizing of data meeting certain specifications. Druid aggregates [timeseries data](#timeseries), which in effect compacts the data. Time intervals (set in configuration) are used to create buckets, while [timestamps](#timestamp) determine which buckets data aggregated in.

* **Aggregators** A mechanism for combining records during realtime incremental indexing, Hadoop batch indexing, and in queries.

* **DataSource** A table-like view of data; specified in [specFiles](#specfile) and in queries. A dataSource specifies the source of data being ingested and ultimately stored in [segments](#segment).

* **Dimensions** Aspects or categories of data, such as languages or locations. For example, with *language* and *country* as the type of dimension, values could be "English" or "Mandarin" for language, or "USA" or "China" for country. In Druid, dimensions can serve as filters for narrowing down hits (for example, language = "English" or country = "China").

* **Ephemeral Node** A Zookeeper node (or "znode") that exists for as long as the session that created the znode is active. More info [here](http://zookeeper.apache.org/doc/r3.2.1/zookeeperProgrammers.html#Ephemeral+Nodes). In a Druid cluster, ephemeral nodes are typically used for commands (such as assigning [segments](#segment) to certain nodes).

* **Granularity** The time interval corresponding to aggregation by time. Druid configuration settings specify the granularity of [timestamp](#timestamp) buckets in a [segment](#segment) (for example, by minute or by hour), as well as the granularity of the segment itself. The latter is essentially the overall range of absolute time covered by the segment. In queries, granularity settings control the summarization of findings.

* **Ingestion** The pulling and initial storing and processing of data. Druid supports realtime and batch ingestion of data, and applies indexing in both cases.

* **Metrics** Countable data that can be aggregated. Metrics, for example, can be the number of visitors to a website, number of tweets per day, or average revenue.

* **Rollup** The aggregation of data that occurs at one or more stages, based on settings in a [configuration file](#specFile). 

  <a name="segment"></a>
* **Segment** A collection of (internal) records that are stored and processed together. Druid chunks data into segments representing a time interval, and these are stored and manipulated in the cluster.

* **Shard** A sub-partition of the data, allowing multiple [segments](#segment) to represent the data in a certain time interval. Sharding occurs along time partitions to better handle amounts of data that exceed certain limits on segment size, although sharding along dimensions may also occur to optimize efficiency.

  <a name="specfile"></a>
* **specFile** The specification for services in JSON format; see [Realtime](Realtime.html) and [Batch-ingestion](Batch-ingestion.html)

  <a name="timeseries"></a>
* **Timeseries Data** Data points which are ordered in time. The closing value of a financial index or the number of tweets per hour with a certain hashtag are examples of timeseries data.

  <a name="timestamp"></a>
* **Timestamp** An absolute position on a timeline, given in a standard alpha-numerical format such as with UTC time. [Timeseries data](#timeseries) points can be ordered by timestamp, and in Druid, they are.
