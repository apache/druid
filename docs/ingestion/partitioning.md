---
id: partitioning
title: Partitioning
sidebar_label: Partitioning
description: 
---

Optimal partitioning and sorting of segments within your datasources can have substantial impact on footprint and
performance.

Druid datasources are always partitioned by time into _time chunks_, and each time chunk contains one or more segments.
This partitioning happens for all ingestion methods, and is based on the `segmentGranularity` parameter of your
ingestion spec's `dataSchema`.

The segments within a particular time chunk may also be partitioned further, using options that vary based on the
ingestion type you have chosen. In general, doing this secondary partitioning using a particular dimension will
improve locality, meaning that rows with the same value for that dimension are stored together and can be accessed
quickly.

You will usually get the best performance and smallest overall footprint by partitioning your data on some "natural"
dimension that you often filter by, if one exists. This will often improve compression - users have reported threefold
storage size decreases - and it also tends to improve query performance as well.

> Partitioning and sorting are best friends! If you do have a "natural" partitioning dimension, you should also consider
> placing it first in the `dimensions` list of your `dimensionsSpec`, which tells Druid to sort rows within each segment
> by that column. This will often improve compression even more, beyond the improvement gained by partitioning alone.
>
> However, note that currently, Druid always sorts rows within a segment by timestamp first, even before the first
> dimension listed in your `dimensionsSpec`. This can prevent dimension sorting from being maximally effective. If
> necessary, you can work around this limitation by setting `queryGranularity` equal to `segmentGranularity` in your
> [`granularitySpec`](#granularityspec), which will set all timestamps within the segment to the same value, and by saving
> your "real" timestamp as a [secondary timestamp](schema-design.md#secondary-timestamps). This limitation may be removed
> in a future version of Druid.

### How to set up partitioning

Not all ingestion methods support an explicit partitioning configuration, and not all have equivalent levels of
flexibility. As of current Druid versions, If you are doing initial ingestion through a less-flexible method (like
Kafka) then you can use [reindexing](data-management.md#reingesting-data) or [compaction](compaction.md) to repartition your data after it
is initially ingested. This is a powerful technique: you can use it to ensure that any data older than a certain
threshold is optimally partitioned, even as you continuously add new data from a stream.

The following table shows how each ingestion method handles partitioning:

|Method|How it works|
|------|------------|
|[Native batch](native-batch.md)|Configured using [`partitionsSpec`](native-batch.md#partitionsspec) inside the `tuningConfig`.|
|[Hadoop](hadoop.md)|Configured using [`partitionsSpec`](hadoop.md#partitionsspec) inside the `tuningConfig`.|
|[Kafka indexing service](../development/extensions-core/kafka-ingestion.md)|Partitioning in Druid is guided by how your Kafka topic is partitioned. You can also [reindex](data-management.md#reingesting-data) or [compact](compaction.md) to repartition after initial ingestion.|
|[Kinesis indexing service](../development/extensions-core/kinesis-ingestion.md)|Partitioning in Druid is guided by how your Kinesis stream is sharded. You can also [reindex](data-management.md#reingesting-data) or [compact](compaction.md) to repartition after initial ingestion.|

> Note that, of course, one way to partition data is to load it into separate datasources. This is a perfectly viable
> approach and works very well when the number of datasources does not lead to excessive per-datasource overheads. If
> you go with this approach, then you can ignore this section, since it is describing how to set up partitioning
> _within a single datasource_.
>
> For more details on splitting data up into separate datasources, and potential operational considerations, refer
> to the [Multitenancy considerations](../querying/multitenancy.md) page.
