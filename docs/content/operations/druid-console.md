---
layout: doc_page
title: "Druid console"
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

# Druid console

The Druid Console is hosted by the [Router](../development/router.html) process.

In addition, the following cluster settings must be enabled:

- the Router's [management proxy](../development/router.html#enabling-the-management-proxy) must be enabled.
- the Broker processes in the cluster must have [Druid SQL](../querying/sql.html) enabled.

After enabling Druid SQL on the Brokers and deploying a Router with the managment proxy enabled, the Druid console can be accessed at:

```
http://<ROUTER_IP>:<ROUTER_PORT>
```

Below is a description of the high-level features and functionality of the Druid Console

## Home

The home view provide a high level overview of the cluster. Each card is clickable and links to the appropriate view. The legacy menu allows you to go to the [legacy coordinator and overlord consoles](./management-uis#legacy-consoles) should you need them.

![home-view](./img/01-home-view.png)

## Datasources

The datasources view shows all the currently enabled datasources. From this view you can see the sizes and availability of the different datasources. You can edit the retention rules and drop data (as well as issue kill tasks).
Like any view that is powered by a DruidSQL query you can click “Go to SQL” to run the underlying SQL query directly.

![datasources](./img/02-datasources.png)

You can view and edit retention rules to determine the general availability of a datasource.

![retention](./img/03-retention.png)

## Segments

The segment view shows every single segment in the cluster. Each segment can be expanded to provide more information. The Segment ID is also conveniently broken down into Datasource, Start, End, Version, and Partition columns for ease of filtering and sorting.

![segments](./img/04-segments.png)

## Tasks and supervisors

The task view is also the home of supervisors. From this view you can check the status of existing supervisors as well as suspend and resume them. You can also submit new supervisors by entering their JSON spec.

![tasks-1](./img/05-tasks-1.png)

The tasks table let’s you see the currently running and recently completed tasks. From this table you can monitor individual tasks and also submit new tasks by entering their JSON spec.

![tasks-2](./img/06-tasks-2.png)

Since there will likely be a lot of tasks you can group the tasks by their type, datasource, or status to make navigation easier.

![tasks-3](./img/07-tasks-3.png)

## Servers

The data servers tab lets you see the current status of the historical nodes and MiddleManager (indexer) processes. Note that currently only historical nodes that are actively serving segments will be shown in this view.

![servers](./img/08-servers.png)

## SQL

The SQL view lets you issue direct DruidSQL queries and display the results as a simple table. Note that despite the name this view also allows you to enter native Druid queries in Hjson format.

![sql](./img/09-sql.png)

