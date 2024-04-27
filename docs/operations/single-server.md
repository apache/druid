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

Druid includes a launch script, `bin/start-druid` that automatically sets various memory-related parameters based on available processors and memory.
It accepts optional arguments such as list of services, total memory, and a config directory to override default JVM arguments and service-specific runtime properties.

By default, the services started by `bin/start-druid`:

- use all processors
- can use up to 80% memory on the system
- apply the configuration files in `conf/druid/auto` for all other settings.

For details about possible arguments, run `bin/start-druid --help`.

## Single server reference configurations (deprecated)

Druid includes a set of reference configurations and launch scripts for single-machine deployments.
These start scripts are deprecated in favor of the `bin/start-druid` script documented above.
These configuration bundles are located in `conf/druid/single-server/`.

| Configuration      |Sizing|Launch command|Configuration directory|
|--------------------|-----------|----------|------------|
| `nano-quickstart`  |1 CPU, 4GiB RAM|`bin/start-nano-quickstart`|`conf/druid/single-server/nano-quickstart`|
| `micro-quickstart` |4 CPU, 16GiB RAM|`bin/start-micro-quickstart`|`conf/druid/single-server/micro-quickstart`|
| `small`            |8 CPU, 64GiB RAM (~i3.2xlarge)|`bin/start-small`|`conf/druid/single-server/small`|
| `medium`           |16 CPU, 128GiB RAM (~i3.4xlarge)|`bin/start-medium`|`conf/druid/single-server/medium`|
| `large`            |32 CPU, 256GiB RAM (~i3.8xlarge)|`bin/start-large`|`conf/druid/single-server/large`|
| `xlarge`           |64 CPU, 512GiB RAM (~i3.16xlarge)|`bin/start-xlarge`|`conf/druid/single-server/xlarge`|

The `micro-quickstart` is sized for small machines like laptops and is intended for quick evaluation use-cases.

The `nano-quickstart` is an even smaller configuration, targeting a machine with 1 CPU and 4GiB memory. It is meant for limited evaluations in resource constrained environments, such as small Docker containers.

The other configurations are intended for general use single-machine deployments. They are sized for hardware roughly based on Amazon's i3 series of EC2 instances.

The startup scripts for these example configurations run a single ZK instance along with the Druid services. You can choose to deploy ZK separately as well.
