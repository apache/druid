---
layout: doc_page
title: "Web Consoles"
---

<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->

# Web Consoles

Druid has a few console UIs that provide cluster management functionality, described below.

## Coordinator Console

The Druid coordinator exposes a web GUI for displaying cluster information and rule configuration. After the coordinator starts, the console can be accessed at:

```
http://<COORDINATOR_IP>:<COORDINATOR_PORT>
```

There exists a full cluster view (which shows only the realtime and historical nodes), as well as views for individual historical nodes, datasources and segments themselves. Segment information can be displayed in raw JSON form or as part of a sortable and filterable table.

The coordinator console also exposes an interface to creating and editing rules. All valid datasources configured in the segment database, along with a default datasource, are available for configuration. Rules of different types can be added, deleted or edited.


## Overlord Console

The overlord console can be used to view pending tasks, running tasks, available workers, and recent worker creation and termination. The console can be accessed at:

```
http://<OVERLORD_IP>:<OVERLORD_PORT>/console.html
```

## Unified Console

Druid also provides a unified console that combines the functionality of the coordinator and overlord consoles described above. 

To use this console, it is necessary to run a [Router](../development/router.html) process.

In addition, the following cluster settings must be enabled:

- the Router's [management proxy](../development/router.html#enabling-the-management-proxy) must be enabled.
- the Broker processes in the cluster must have [Druid SQL](../querying/sql.html) enabled.

After enabling Druid SQL on the Brokers and deploying a Router with the managment proxy enabled, the unified console can be accessed at:

```
http://<ROUTER_IP>:<ROUTER_PORT>
```
