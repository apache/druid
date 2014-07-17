---
layout: doc_page
---


Rolling Updates
===============

For rolling Druid cluster updates with no downtime, we recommend updating Druid nodes in the following order:

1. Historical Nodes
2. Indexing Service/Real-time Nodes
3. Broker Nodes
4. Coordinator Nodes

## Historical Nodes

Historical nodes can be updated one at a time. Each historical node has a startup time to memory map all the segments it was serving before the update. The startup time typically takes a few seconds to a few minutes, depending on the hardware of the node. As long as each historical node is updated with a sufficient delay (greater than the time required to start a single node), you can rolling update the entire historical cluster.

## Standalone Real-time nodes

Standalone real-time nodes can be updated one at a time in a rolling fashion.

## Indexing Service

### With Autoscaling

Overlord nodes will try to launch new middle manager nodes and terminate old ones without dropping data. This process is based on the configuration `druid.indexer.runner.minWorkerVersion=#{VERSION}`. Each time you update your overlord node, the `VERSION` value should be increased.

The config `druid.indexer.autoscale.workerVersion=#{VERSION}` also needs to be set.

### Without Autoscaling

Middle managers can be updated in a rolling fashion based on API.

To prepare a middle manager for update, send a POST request to `<MiddleManager_IP:PORT>/druid/worker/v1/disable`. The overlord will now no longer send tasks to this middle manager.

Current tasks will still try to complete. To view all existing tasks, send a GET request to `<MiddleManager_IP:PORT>/druid/worker/v1/tasks`. When this list is empty, the middle manager can be updated. After the middle manager is updated, it is automatically enabled again. You can also manually enable middle managers POSTing to `<MiddleManager_IP:PORT>/druid/worker/v1/enable`.

## Broker Nodes

Broker nodes can be updated one at a time in a rolling fashion. There needs to be some delay between updating each node as brokers must load the entire state of the cluster before they return valid results.

## Coordinator Nodes

Coordinator nodes can be updated in a rolling fashion.