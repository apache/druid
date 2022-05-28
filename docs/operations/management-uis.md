---
id: management-uis
title: "Legacy Management UIs"
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


## Legacy consoles

Druid provides a console for managing datasources, segments, tasks, data processes (Historicals and MiddleManagers), and coordinator dynamic configuration. The user can also run SQL and native Druid queries within the console.

For more information on the Druid Console, have a look at the [Druid Console overview](./druid-console.md)

The Druid Console contains all of the functionality provided by the older consoles described below, which are still available if needed. The legacy consoles may be replaced by the Druid Console in the future.

These older consoles provide a subset of the functionality of the Druid Console. We recommend using the Druid Console if possible.

### Coordinator consoles

#### Version 2

The Druid Coordinator exposes a web console for displaying cluster information and rule configuration. After the Coordinator starts, the console can be accessed at:

```
http://<COORDINATOR_IP>:<COORDINATOR_PORT>
```

There exists a full cluster view (which shows indexing tasks and Historical processes), as well as views for individual Historical processes, datasources and segments themselves. Segment information can be displayed in raw JSON form or as part of a sortable and filterable table.

The Coordinator console also exposes an interface to creating and editing rules. All valid datasources configured in the segment database, along with a default datasource, are available for configuration. Rules of different types can be added, deleted or edited.

#### Version 1

The oldest version of Druid's Coordinator console is still available for backwards compatibility at:

```
http://<COORDINATOR_IP>:<COORDINATOR_PORT>/old-console
```

### Overlord console

The Overlord console can be used to view pending tasks, running tasks, available workers, and recent worker creation and termination. The console can be accessed at:

```
http://<OVERLORD_IP>:<OVERLORD_PORT>/console.html
```
