---
layout: doc_page
---

Druid vs Vertica
================


How does Druid compare to Vertica?

Vertica is similar to ParAccel/Redshift ([Druid-vs-Redshift](Druid-vs-Redshift.html)) described above in that it wasn’t built for real-time streaming data ingestion and it supports full SQL.

The other big difference is that instead of employing indexing, Vertica tries to optimize processing by leveraging run-length encoding (RLE) and other compression techniques along with a "projection" system that creates materialized copies of the data in a different sort order (to maximize the effectiveness of RLE).

We are unclear about how Vertica handles data distribution and replication, so we cannot speak to if/how Druid is different.
