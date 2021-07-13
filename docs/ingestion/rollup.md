---
id: rullup
title: "Data rollup"
sidebar_label: Data rollup
description: 
---
Druid can roll up data ingestion time to reduce the amount of raw data to  store on disk. Rollup is a form of summarization or pre-aggregation. Rolling up data your can dramatically reduce the size of data to be store and reducing row counts by potentially orders of magnitude. The trade off for the effiency of rollup means you lose the ability to query individual events.


At ingestion time, you control rollup with the `rollup` setting in the [`granularitySpec`](#granularityspec). Rollup is enabled by default. This means Druid combines into a single row any rows that have identical [dimensions](#dimensions) values and [timestamp](#primary-timestamp) values after [`queryGranularity`-based truncation](#granularityspec)).

When you disable rollup, Druid loads each row as-is without doing any form of pre-aggregation. This mode is similar to databases that do not support a rollup feature. Set `rollup` to `false` if you want Druid to store each record as-is, without any rollup summarization.

## Maximizing rollup ratio

You can measure the rollup ratio of a datasource by comparing the number of rows in Druid with the number of ingested
events. The higher this number, the more benefit you are gaining from rollup. One way to do this is with a
[Druid SQL](../querying/sql.md) query like:

```sql
SELECT SUM("cnt") / COUNT(*) * 1.0 FROM datasource
```

In this query, `cnt` should refer to a "count" type metric specified at ingestion time. See
[Counting the number of ingested events](schema-design.md#counting) on the "Schema design" page for more details about
how counting works when rollup is enabled.

Tips for maximizing rollup:

- Generally, the fewer dimensions you have, and the lower the cardinality of your dimensions, the better rollup ratios
you will achieve.
- Use [sketches](schema-design.md#sketches) to avoid storing high cardinality dimensions, which harm rollup ratios.
- Adjusting `queryGranularity` at ingestion time (for example, using `PT5M` instead of `PT1M`) increases the
likelihood of two rows in Druid having matching timestamps, and can improve your rollup ratios.
- It can be beneficial to load the same data into more than one Druid datasource. Some users choose to create a "full"
datasource that has rollup disabled (or enabled, but with a minimal rollup ratio) and an "abbreviated" datasource that
has fewer dimensions and a higher rollup ratio. When queries only involve dimensions in the "abbreviated" set, using
that datasource leads to much faster query times. This can often be done with just a small increase in storage
footprint, since abbreviated datasources tend to be substantially smaller.
- If you are using a [best-effort rollup](#perfect-rollup-vs-best-effort-rollup) ingestion configuration that does not guarantee perfect
rollup, you can potentially improve your rollup ratio by switching to a guaranteed perfect rollup option, or by
[reindexing](data-management.md#reingesting-data) or [compacting](compaction.md) your data in the background after initial ingestion.

## Perfect rollup vs Best-effort rollup

Some Druid ingestion methods guarantee _perfect rollup_, meaning that input data are perfectly aggregated at ingestion
time. Others offer _best-effort rollup_, meaning that input data might not be perfectly aggregated and thus there could
be multiple segments holding rows with the same timestamp and dimension values.

In general, ingestion methods that offer best-effort rollup do this because they are either parallelizing ingestion
without a shuffling step (which would be required for perfect rollup), or because they are finalizing and publishing
segments before all data for a time chunk has been received, which we call _incremental publishing_. In both of these
cases, records that could theoretically be rolled up may end up in different segments. All types of streaming ingestion
run in this mode.

Ingestion methods that guarantee perfect rollup do it with an additional preprocessing step to determine intervals
and partitioning before the actual data ingestion stage. This preprocessing step scans the entire input dataset, which
generally increases the time required for ingestion, but provides information necessary for perfect rollup.

The following table shows how each method handles rollup:

|Method|How it works|
|------|------------|
|[Native batch](native-batch.md)|`index_parallel` and `index` type may be either perfect or best-effort, based on configuration.|
|[Hadoop](hadoop.md)|Always perfect.|
|[Kafka indexing service](../development/extensions-core/kafka-ingestion.md)|Always best-effort.|
|[Kinesis indexing service](../development/extensions-core/kinesis-ingestion.md)|Always best-effort.|

## Learn more
See the following topics for more information:
* [Rollup tutorial](../tutorials/tutorial-rollup.md) for an example of how to configure rollup, and of how the feature modifies your data.
.