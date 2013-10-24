---
layout: doc_page
---

# Data Flow

The diagram below illustrates how different Druid nodes download data and respond to queries:

<img src="../img/druid-dataflow-2x.png" width="800"/>

### Real-time Nodes

Real-time nodes ingest streaming data and announce themselves and the segments they are serving in Zookeeper on start up. During the segment hand-off stage, real-time nodes create a segment metadata entry in MySQL for the segment to hand-off. This segment is uploaded to Deep Storage. Real-time nodes use Zookeeper to monitor when historical nodes complete downloading the segment (indicating hand-off completion) so that it can forget about it. Real-time nodes also respond to query requests from broker nodes and also return query results to the broker nodes.

### Deep Storage

Batch indexed segments and segments created by real-time nodes are uploaded to deep storage. Historical nodes download these segments to serve for queries.

### MySQL

Real-time nodes and batch indexing create new segment metadata entries for the new segments they've created. Coordinator nodes read this metadata table to determine what segments should be loaded in the cluster.

### Coordinator Nodes

Coordinator nodes read segment metadata information from MySQL to determine what segments should be loaded in the cluster. Coordinator nodes user Zookeeper to determine what historical nodes exist, and also create Zookeeper entries to tell historical nodes to load and drop new segments.

### Zookeeper

Real-time nodes announce themselves and the segments they are serving in Zookeeper and also use Zookeeper to monitor segment hand-off. Coordinator nodes use Zookeeper to determine what historical nodes exist in the cluster and create new entries to communicate to historical nodes to load or drop new data. Historical nodes announce themselves and the segments they serve in Zookeeper. Historical nodes also monitor Zookeeper for new load or drop requests. Broker nodes use Zookeeper to determine what historical and real-time nodes exist in the cluster.

### Historical Nodes

Historical nodes announce themselves and the segments they are serving in Zookeeper. Historical nodes also use Zookeeper to monitor for signals to load or drop new segments. Historical nodes download segments from deep storage, respond to the queries from broker nodes about these segments, and return results to the broker nodes.

### Broker Nodes

Broker nodes receive queries from external clients and forward those queries down to real-time and historical nodes. When the individual nodes return their results, broker nodes merge these results and returns them to the caller. Broker nodes use Zookeeper to determine what real-time and historical nodes exist.
