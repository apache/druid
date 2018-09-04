---
layout: doc_page
---
# Multitenancy Considerations

Druid is often used to power user-facing data applications, where multitenancy is an important requirement. This
document outlines Druid's multitenant storage and querying features.

## Shared datasources or datasource-per-tenant?

A datasource is the Druid equivalent of a database table. Multitenant workloads can either use a separate datasource
for each tenant, or can share one or more datasources between tenants using a "tenant_id" dimension. When deciding
which path to go down, consider that each path has pros and cons.

Pros of datasources per tenant:

- Each datasource can have its own schema, its own backfills, its own partitioning rules, and its own data loading
and expiration rules.
- Queries can be faster since there will be fewer segments to examine for a typical tenant's query.
- You get the most flexibility.

Pros of shared datasources:

- Each datasource requires its own JVMs for realtime indexing.
- Each datasource requires its own YARN resources for Hadoop batch jobs.
- Each datasource requires its own segment files on disk.
- For these reasons it can be wasteful to have a very large number of small datasources.

One compromise is to use more than one datasource, but a smaller number than tenants. For example, you could have some
tenants with partitioning rules A and some with partitioning rules B; you could use two datasources and split your
tenants between them.

## Partitioning shared datasources

If your multitenant cluster uses shared datasources, most of your queries will likely filter on a "tenant_id"
dimension. These sorts of queries perform best when data is well-partitioned by tenant. There are a few ways to
accomplish this.

With batch indexing, you can use [single-dimension partitioning](../indexing/batch-ingestion.html#single-dimension-partitioning)
to partition your data by tenant_id. Druid always partitions by time first, but the secondary partition within each
time bucket will be on tenant_id.

With realtime indexing, you have a couple of options.

1. Partition on tenant_id upfront. You'd do this by tweaking the stream you send to Druid. If you're using Kafka then
you can have your Kafka producer partition your topic by a hash of tenant_id. If you're using Tranquility then you can
define a custom [Partitioner](http://static.druid.io/tranquility/api/latest/#com.metamx.tranquility.partition.Partitioner).
2. Reindex your older data periodically. You can do this with the ["dataSource" input spec](../ingestion/hadoop.html#datasource).
You can use this in concert with single-dimension partitioning to repartition your data.

## Customizing data distribution

Druid additionally supports multitenancy by providing configurable means of distributing data. Druid's historical nodes 
can be configured into [tiers](../operations/rule-configuration.html), and [rules](../operations/rule-configuration.html) 
can be set that determines which segments go into which tiers. One use case of this is that recent data tends to be accessed 
more frequently than older data. Tiering enables more recent segments to be hosted on more powerful hardware for better performance. 
A second copy of recent segments can be replicated on cheaper hardware (a different tier), and older segments can also be 
stored on this tier.

## Supporting high query concurrency

Druid's fundamental unit of computation is a [segment](../design/segments.html). Nodes scan segments in parallel and a
given node can scan `druid.processing.numThreads` concurrently. To
process more data in parallel and increase performance, more cores can be added to a cluster. Druid segments
should be sized such that any computation over any given segment should complete in at most 500ms.

Druid internally stores requests to scan segments in a priority queue. If a given query requires scanning
more segments than the total number of available processors in a cluster, and many similarly expensive queries are concurrently
running, we don't want any query to be starved out. Druid's internal processing logic will scan a set of segments from one query and release resources as soon as the scans complete.
This allows for a second set of segments from another query to be scanned. By keeping segment computation time very small, we ensure
that resources are constantly being yielded, and segments pertaining to different queries are all being processed.

Druid queries can optionally set a `priority` flag in the [query context](../querying/query-context.html). Queries known to be
slow (download or reporting style queries) can be de-prioritized and more interactive queries can have higher priority. 

Broker nodes can also be dedicated to a given tier. For example, one set of broker nodes can be dedicated to fast interactive queries, 
and a second set of broker nodes can be dedicated to slower reporting queries. Druid also provides a [router](../development/router.html) 
node that can route queries to different brokers based on various query parameters (datasource, interval, etc.).  
