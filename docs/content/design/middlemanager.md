---
layout: doc_page
---

Middle Manager Node
------------------

### Configuration

For Middlemanager Node Configuration, see [Indexing Service Configuration](../configuration/index.html#middlemanager-and-peons).

### HTTP Endpoints

For a list of API endpoints supported by the MiddleManager, please see the [API reference](../operations/api-reference.html#middlemanager).

### Overview

The middle manager node is a worker node that executes submitted tasks. Middle Managers forward tasks to peons that run in separate JVMs.
The reason we have separate JVMs for tasks is for resource and log isolation. Each [Peon](../design/peons.html) is capable of running only one task at a time, however, a middle manager may have multiple peons.

### Running

```
io.druid.cli.Main server middleManager
```


