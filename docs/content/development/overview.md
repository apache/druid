---
layout: doc_page
---

# Developing on Druid

Druid's codebase consists of several major components. For developers interested in learning the code, this document provides 
a high level overview of the main components that make up Druid and the relevant classes to start from to learn the code.

## Storage Format

Data in Druid is stored in a custom column format known as a [segment](../design/segments.html). Segments are composed of 
different types of columns. `Column.java` and the classes that extend it is a great place to looking into the storage format.

## Segment Creation

Raw data is ingested in `IncrementalIndex.java`, and segments are created in `IndexMerger.java`.

## Storage Engine

Druid segments are memory mapped in `IndexIO.java` to be exposed for querying.

## Query Engine

Most of the logic related to Druid queries can be found in the Query* classes. Druid leverages query runners to run queries. 
Query runners often embed other query runners and each query runner adds on a layer of logic. A good starting point to trace 
the query logic is to start from `QueryResource.java`.

## Coordination

Most of the coordination logic for historical nodes is on the Druid coordinator. The starting point here is `DruidCoordinator.java`.  
Most of the coordination logic for (real-time) ingestion is in the Druid indexing service. The starting point here is `OverlordResource.java`.

## Real-time Ingestion

Druid loads data through `FirehoseFactory.java` classes. Firehoses often wrap other firehoses, where, similar to the design of the  
query runners, each firehose adds a layer of logic. Much of the core management logic is in `RealtimeManager.java` and the 
persist and hand-off logic is in `RealtimePlumber.java`.

## Hadoop-based Batch Ingestion

The two main Hadoop indexing classes are `HadoopDruidDetermineConfigurationJob.java` for the job to determine how many Druid 
segments to create, and `HadoopDruidIndexerJob.java`, which creates Druid segments.

At some point in the future, we may move the Hadoop ingestion code out of core Druid.

## Internal UIs

Druid currently has two internal UIs. One is for the Coordinator and one is for the Overlord.

At some point in the future, we will likely move the internal UI code out of core Druid.

## Client Libraries

We welcome contributions for new client libraries to interact with Druid. See client 
[libraries](../development/libraries.html) for existing client libraries.
