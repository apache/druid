---
id: high-availability
title: "High availability"
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


Apache ZooKeeper, metadata store, the coordinator, the overlord, and brokers are recommended to set up a high availability environment.

- For highly-available ZooKeeper, you will need a cluster of 3 or 5 ZooKeeper nodes.
We recommend either installing ZooKeeper on its own hardware, or running 3 or 5 Master servers (where overlords or coordinators are running)
and configuring ZooKeeper on them appropriately. See the [ZooKeeper admin guide](https://zookeeper.apache.org/doc/current/zookeeperAdmin.html) for more details.
- For highly-available metadata storage, we recommend MySQL or PostgreSQL with replication and failover enabled.
See [MySQL HA/Scalability Guide](https://dev.mysql.com/doc/mysql-ha-scalability/en/)
and [PostgreSQL's High Availability, Load Balancing, and Replication](https://www.postgresql.org/docs/9.5/high-availability.html) for MySQL and PostgreSQL, respectively.
- For highly-available Apache Druid Coordinators and Overlords, we recommend to run multiple servers.
If they are all configured to use the same ZooKeeper cluster and metadata storage,
then they will automatically failover between each other as necessary.
Only one will be active at a time, but inactive servers will redirect to the currently active server.
- Druid Brokers can be scaled out and all running servers will be active and queryable.
We recommend placing them behind a load balancer.
