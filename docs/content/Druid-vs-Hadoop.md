---
layout: doc_page
---
Druid is a complementary addition to Hadoop. Hadoop is great at storing and making accessible large amounts of individually low-value data. Unfortunately, Hadoop is not great at providing query speed guarantees on top of that data, nor does it have very good operational characteristics for a customer-facing production system. Druid, on the other hand, excels at taking high-value summaries of the low-value data on Hadoop, making it available in a fast and always-on fashion, such that it could be exposed directly to a customer.

Druid also requires some infrastructure to exist for [deep storage](Deep-Storage.html). HDFS is one of the implemented options for this [deep storage](Deep-Storage.html).
