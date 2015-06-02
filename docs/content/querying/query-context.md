---
layout: doc_page
---

Query Context
=============

The query context is used for various query configuration parameters.

|property      |default              | description          |
|--------------|---------------------|----------------------|
|timeout       | `0` (no timeout)    | Query timeout in milliseconds, beyond which unfinished queries will be cancelled. |
|priority      | `0`                 | Query Priority. Queries with higher priority get precedence for computational resources.|
|queryId       | auto-generated      | Unique identifier given to this query. If a query ID is set or known, this can be used to cancel the query |
|useCache      | `true`              | Flag indicating whether to leverage the query cache for this query. This may be overridden in the broker or historical node configuration |
|populateCache | `true`              | Flag indicating whether to save the results of the query to the query cache. Primarily used for debugging. This may be overriden in the broker or historical node configuration |
|bySegment     | `false`             | Return "by segment" results. Primarily used for debugging, setting it to `true` returns results associated with the data segment they came from |
|finalize      | `true`              | Flag indicating whether to "finalize" aggregation results. Primarily used for debugging. For instance, the `hyperUnique` aggregator will return the full HyperLogLog sketch instead of the estimated cardinality when this flag is set to `false` |
|chunkPeriod   | `0` (off)           | At the broker node level, long interval queries (of any type) may be broken into shorter interval queries, reducing the impact on resources. Use ISO 8601 periods. For example, if this property is set to `P1M` (one month), then a query covering a year would be broken into 12 smaller queries. All the query chunks will be processed asynchronously inside query processing executor service. Make sure "druid.processing.numThreads" is configured appropriately on the broker. |

