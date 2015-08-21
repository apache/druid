---
layout: doc_page
---
# Multitenancy Considerations

Druid is often used to power user-facing data applications and has several features built in to better support high 
volumes of concurrent queries.

## Parallelization Model

Druid's fundamental unit of computation is a [segment](../design/segments.html). Nodes scan segments in parallel and a 
given node can scan `druid.processing.numThreads` concurrently. To 
process more data in parallel and increase performance, more cores can be added to a cluster. Druid segments 
should be sized such that any computation over any given segment should complete in at most 500ms.

Druid internally stores requests to scan segments in a priority queue. If a given query requires scanning  
more segments than the total number of available processors in a cluster, and many similarly expensive queries are concurrently  
running, we don't want any query to be starved out. Druid's internal processing logic will scan a set of segments from one query and release resources as soon as the scans complete. 
This allows for a second set of segments from another query to be scanned. By keeping segment computation time very small, we ensure 
that resources are constantly being yielded, and segments pertaining to different queries are all being processed. 

## Data Distribution

Druid additionally supports multitenancy by providing configurable means of distributing data. Druid's historical nodes 
can be configured into [tiers](../operations/rule-configuration.html), and [rules](../operations/rule-configuration.html) 
can be set that determines which segments go into which tiers. One use case of this is that recent data tends to be accessed 
more frequently than older data. Tiering enables more recent segments to be hosted on more powerful hardware for better performance. 
A second copy of recent segments can be replicated on cheaper hardware (a different tier), and older segments can also be 
stored on this tier.

## Query Distribution

Druid queries can optionally set a `priority` flag in the [query context](../querying/query-context.html). Queries known to be 
slow (download or reporting style queries) can be de-prioritized and more interactive queries can have higher priority. 

Broker nodes can also be dedicated to a given tier. For example, one set of broker nodes can be dedicated to fast interactive queries, 
and a second set of broker nodes can be dedicated to slower reporting queries. Druid also provides a [router](../development/router.html) 
node that can route queries to different brokers based on various query parameters (datasource, interval, etc.).  
