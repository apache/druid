---
layout: doc_page
---

Recommendations
===============

# Use UTC Timezone

We recommend using UTC timezone for all your events and across on your nodes, not just for Druid, but for all data infrastructure. This can greatly mitigate potential query problems with inconsistent timezones. To query in a non-UTC timezone see [query granularities](../querying/granularities.html#period-granularities)

# SSDs

SSDs are highly recommended for historical and real-time nodes if you are not running a cluster that is entirely in memory. SSDs can greatly mitigate the time required to page data in and out of memory.

# Use Timeseries and TopN Queries Instead of GroupBy Where Possible

Timeseries and TopN queries are much more optimized and significantly faster than groupBy queries for their designed use cases. Issuing multiple topN or timeseries queries from your application can potentially be more efficient than a single groupBy query.

# Segment sizes matter

Segments should generally be between 300MB-700MB in size. Too many small segments results in inefficient CPU utilizations and 
too many large segments impacts query performance, most notably with TopN queries.

# Read FAQs

You should read common problems people have here:

1) [Ingestion-FAQ](../ingestion/faq.html)

2) [Performance-FAQ](../operations/performance-faq.html)
