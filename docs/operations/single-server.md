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
- `start-druid`

The `micro-quickstart` is sized for small machines like laptops and is intended for quick evaluation use-cases.

The `nano-quickstart` is an even smaller configuration, targeting a machine with 1 CPU and 4GiB memory. It is meant for limited evaluations in resource constrained environments, such as small Docker containers.

The other configurations are intended for general use single-machine deployments. They are sized for hardware roughly based on Amazon's i3 series of EC2 instances.

The startup scripts for these example configurations run a single ZK instance along with the Druid services. You can choose to deploy ZK separately as well.

The example configurations run the Druid Coordinator and Overlord together in a single process using the optional configuration `druid.coordinator.asOverlord.enabled=true`, described in the [Coordinator configuration documentation](../configuration/index.md#coordinator-operation).

The `start-druid` is a generic launch script for starting druid services on single server, it accepts optional arguments like services, memory and config.
All reference configurations can be acheived by passing appropriate arguments to this script. 
If memory argument isn't specified, services will use upto 80% system memory. 
Existing launch scripts are deprecated and will be removed in the next release. 


While example configurations are provided for very large single machines, at higher scales we recommend running Druid in a [clustered deployment](../tutorials/cluster.md), for fault-tolerance and reduced resource contention.

## Single server reference configurations

### Nano: 1 CPU, 4GiB RAM

- Launch command: `bin/start-druid`
- Configuration directory: `conf/druid/single-server/quickstart`

### Micro: 4 CPU, 16GiB RAM

- Launch command: `bin/start-druid`
- Configuration directory: `conf/druid/single-server/quickstart`

### Small: 8 CPU, 64GiB RAM (~i3.2xlarge)

- Launch command: `bin/start-druid`
- Configuration directory: `conf/druid/single-server/quickstart`

### Medium: 16 CPU, 128GiB RAM (~i3.4xlarge)

- Launch command: `bin/start-druid`
- Configuration directory: `conf/druid/single-server/quickstart`

### Large: 32 CPU, 256GiB RAM (~i3.8xlarge)

- Launch command: `bin/start-druid`
- Configuration directory: `conf/druid/single-server/quickstart`

### X-Large: 64 CPU, 512GiB RAM (~i3.16xlarge)

- Launch command: `bin/start-druid`
- Configuration directory: `conf/druid/single-server/quickstart`

The amount of memory used for Druid can be limited by passing memory argument in the launch command, `bin/start-druid --memory=value` 