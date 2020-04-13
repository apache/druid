---
id: single-server
title: "Single server deployment"
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


Druid includes a set of reference configurations and launch scripts for single-machine deployments:

- `nano-quickstart`
- `micro-quickstart`
- `small`
- `medium`
- `large`
- `xlarge`

The `micro-quickstart` is sized for small machines like laptops and is intended for quick evaluation use-cases.

The `nano-quickstart` is an even smaller configuration, targeting a machine with 1 CPU and 4GB memory. It is meant for limited evaluations in resource constrained environments, such as small Docker containers.

The other configurations are intended for general use single-machine deployments. They are sized for hardware roughly based on Amazon's i3 series of EC2 instances.

The startup scripts for these example configurations run a single ZK instance along with the Druid services. You can choose to deploy ZK separately as well.

The example configurations run the Druid Coordinator and Overlord together in a single process using the optional configuration `druid.coordinator.asOverlord.enabled=true`, described in the [Coordinator configuration documentation](../configuration/index.html#coordinator-operation).

While example configurations are provided for very large single machines, at higher scales we recommend running Druid in a [clustered deployment](../tutorials/cluster.md), for fault-tolerance and reduced resource contention.

## Single server reference configurations

### Nano-Quickstart: 1 CPU, 4GB RAM

- Launch command: `bin/start-nano-quickstart`
- Configuration directory: `conf/druid/single-server/nano-quickstart`

### Micro-Quickstart: 4 CPU, 16GB RAM

- Launch command: `bin/start-micro-quickstart`
- Configuration directory: `conf/druid/single-server/micro-quickstart`

### Small: 8 CPU, 64GB RAM (~i3.2xlarge)

- Launch command: `bin/start-small`
- Configuration directory: `conf/druid/single-server/small`

### Medium: 16 CPU, 128GB RAM (~i3.4xlarge)

- Launch command: `bin/start-medium`
- Configuration directory: `conf/druid/single-server/medium`

### Large: 32 CPU, 256GB RAM (~i3.8xlarge)

- Launch command: `bin/start-large`
- Configuration directory: `conf/druid/single-server/large`

### X-Large: 64 CPU, 512GB RAM (~i3.16xlarge)

- Launch command: `bin/start-xlarge`
- Configuration directory: `conf/druid/single-server/xlarge`

