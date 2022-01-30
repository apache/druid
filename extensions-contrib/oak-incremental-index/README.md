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

# Oak Incremental Index
This extension improves the CPU and memory efficiency of Druid's ingestion.
The same performance is achieved with 60% less memory and 50% less CPU time.
Ingestion-throughput was nearly doubled with the same memory budget,
and throughput was increased by 75% with the same CPU budget.
Full details of the experimental setup and results are available [here](https://github.com/liran-funaro/druid/wiki/Evaluation).

It uses [OakMap open source library](https://github.com/yahoo/Oak) to store the keys and values outside the JVM heap.

# Documentation
More information can be found in the [extension documentation](../../docs/development/extensions-contrib/oak-incremental-index.md).

# Credits
This module is a result of feedback and work done by following people.

* https://github.com/liran-funaro
* https://github.com/sanastas
* https://github.com/ebortnik
* https://github.com/eshcar
