---
layout: doc_page
---

Hadoop has shown the world that it’s possible to house your data warehouse on commodity hardware for a fraction of the price of typical solutions. As people adopt Hadoop for their data warehousing needs, they find two things.

1.  They can now query all of their data in a fairly flexible manner and answer any question they have
2.  The queries take a long time

The first one is the joy that everyone feels the first time they get Hadoop running. The latter is what they realize after they have used Hadoop interactively for a while because Hadoop is optimized for throughput, not latency. 

Druid is a complementary addition to Hadoop. Hadoop is great at storing and making accessible large amounts of individually low-value data. Unfortunately, Hadoop is not great at providing query speed guarantees on top of that data, nor does it have very good operational characteristics for a customer-facing production system. Druid, on the other hand, excels at taking high-value summaries of the low-value data on Hadoop, making it available in a fast and always-on fashion, such that it could be exposed directly to a customer.

Druid also requires some infrastructure to exist for [deep storage](Deep-Storage.html). HDFS is one of the implemented options for this [deep storage](Deep-Storage.html).
