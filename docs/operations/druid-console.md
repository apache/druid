---
id: druid-console
title: "Web console"
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


The Druid Console is hosted by the [Router](../design/router.md) process.

In addition, the following cluster settings must be enabled:

- the Router's [management proxy](../design/router.html#enabling-the-management-proxy) must be enabled.
- the Broker processes in the cluster must have [Druid SQL](../querying/sql.md) enabled.

After enabling Druid SQL on the Brokers and deploying a Router with the management proxy enabled, the Druid console can be accessed at:

```
http://<ROUTER_IP>:<ROUTER_PORT>
```

Below is a description of the high-level features and functionality of the Druid Console

## Home

The home view provides a high level overview of the cluster.
Each card is clickable and links to the appropriate view.
The legacy menu allows you to go to the [legacy coordinator and overlord consoles](./management-uis.html#legacy-consoles) should you need them.

![home-view](../assets/web-console-01-home-view.png)

## Data loader

The data loader view allows you to load data by building an ingestion spec with a step-by-step wizard.

![data-loader-1](../assets/web-console-02-data-loader-1.png)

After selecting the location of your data just follow the series for steps that will show you incremental previews of the data as it will be ingested.
After filling in the required details on every step you can navigate to the next step by clicking the `Next` button.
You can also freely navigate between the steps from the top navigation.

Navigating with the top navigation will leave the underlying spec unmodified while clicking the `Next` button will attempt to fill in the subsequent steps with appropriate defaults.

![data-loader-2](../assets/web-console-03-data-loader-2.png)

## Datasources

The datasources view shows all the currently enabled datasources.
From this view you can see the sizes and availability of the different datasources.
You can edit the retention rules, configure automatic compaction, and drop data.
Like any view that is powered by a DruidSQL query you can click `View SQL query for table` from the `...` menu to run the underlying SQL query directly.

![datasources](../assets/web-console-04-datasources.png)

You can view and edit retention rules to determine the general availability of a datasource.

![retention](../assets/web-console-05-retention.png)

## Segments

The segment view shows all the segments in the cluster.
Each segment can be has a detail view that provides more information.
The Segment ID is also conveniently broken down into Datasource, Start, End, Version, and Partition columns for ease of filtering and sorting.

![segments](../assets/web-console-06-segments.png)

## Tasks and supervisors

From this view you can check the status of existing supervisors as well as suspend, resume, and reset them.
The tasks table allows you see the currently running and recently completed tasks.
To make managing a lot of tasks more accessible, you can group the tasks by their `Type`, `Datasource`, or `Status` to make navigation easier.

![supervisors](../assets/web-console-07-supervisors.png)

Click on the magnifying glass for any supervisor to see detailed reports of its progress.

![supervisor-status](../assets/web-console-08-supervisor-status.png)

Click on the magnifying glass for any task to see more detail about it.

![tasks-status](../assets/web-console-09-task-status.png)

## Servers

The servers tab lets you see the current status of the nodes making up your cluster.
You can group the nodes by type or by tier to get meaningful summary statistics. 

![servers](../assets/web-console-10-servers.png)

## Query

The query view lets you issue [DruidSQL](../querying/sql.md) queries and display the results as a table.
The view will attempt to infer your query and let you modify via contextual actions such as adding filters and changing the sort order when possible.

![query-sql](../assets/web-console-11-query-sql.png)

The query view can also issue queries in Druid's [native query format](../querying/querying.md), which is JSON over HTTP.
To send a native Druid query, you must start your query with `{` and format it as JSON.

![query-rune](../assets/web-console-12-query-rune.png)

## Lookups

You can create and edit query time lookups via the lookup view.

![lookups](../assets/web-console-13-lookups.png)
