---
layout: doc_page
---

Middle Manager Node
------------------

For Middlemanager Node Configuration, see [Indexing Service Configuration](Indexing-Service-Config.html).

The middle manager node is a worker node that executes submitted tasks. Middle Managers forward tasks to peons that run in separate JVMs.
The reason we have separate JVMs for tasks is for resource and log isolation. Each [Peon](Peons.html) is capable of running only one task at a time, however, a middle manager may have multiple peons.

Running
-------

```
io.druid.cli.Main server middleManager
```

HTTP Endpoints
--------------

### GET

* `/status`

Returns the Druid version, loaded extensions, memory used, total memory and other useful information about the node.
