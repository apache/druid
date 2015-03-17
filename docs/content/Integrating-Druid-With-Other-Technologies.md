---
layout: doc_page
---
# Integrating Druid With Other Technologies
This page discusses how we can integrate druid with other technologies. Event streams can be stored in a distributed queue like Kafka, then it can be streamed to a distributed realtime computation system like Twitter Storm / Samza and then it can be feed into Druid via Tranquility plugin. With Tranquility, Middlemanager & Peons will act as a realtime node and they handle realtime queries, segment handoff and realtime indexing.

<img src="../img/druid-production.png" width="800"/>