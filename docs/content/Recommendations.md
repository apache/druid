---
layout: doc_page
---

Recommendations
===============

# Use UTC Timezone

We recommend using UTC timezone for all your events and across on your nodes, not just for Druid, but for all data infrastructure. This can greatly mitigate potential query problems with inconsistent timezones.

# Use Lowercase Strings for Column Names

Druid is not perfect in how it handles mix-cased dimension and metric names. This will hopefully change very soon but for the time being, lower casing your column names is recommended.

# SSDs

SSDs are highly recommended for historical and real-time nodes if you are not running a cluster that is entirely in memory. SSDs can greatly mitigate the time required to page data in and out of memory.
 
# Provide Columns Names in Lexicographic Order

Although Druid supports schema-less ingestion of dimensions, because of [https://github.com/metamx/druid/issues/658](https://github.com/metamx/druid/issues/658), you may sometimes get bigger segments than necessary. To ensure segments are as compact as possible, providing dimension names in lexicographic order is recommended. 
 
 
# Use Timeseries and TopN Queries Instead of GroupBy Where Possible
 
Timeseries and TopN queries are much more optimized and significantly faster than groupBy queries for their designed use cases. Issuing multiple topN or timeseries queries from your application can potentially be more efficient than a single groupBy query.  
 
# Read FAQs

You should read common problems people have here:

1) [Ingestion-FAQ](Ingestion-FAQ.html)

2) [Performance-FAQ](Performance-FAQ.html)