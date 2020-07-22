---
id: byte-unit
title: "Byte Configuration Reference"
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


This page documents all configuration properties related to bytes.

These properties can be configured through 2 ways: 
1. a simple number in byte
2. a number with a unit suffix

## A number in byte

Given the cache size is 3G, there's a configuration as below

````
druid.cache.sizeInBytes=3000000000 #3 * 1000_000_000
````


## A number with a unit suffix

Sometimes value in bytes could be very large in Druid, it's counterintuitive to set value of these properties as above.
Here comes another way, a number with a unit suffix.

Given the total size of disks is 1T, the configuration can be

````
druid.segmentCache.locations=[{"path":"/segment-cache","maxSize":"1g"}]
````

### Supported Units
In the world of computer, a unit like `K` is ambigious. It means 1000 or 1024 in different contexts, for more information please see [Here](https://en.wikipedia.org/wiki/Binary_prefix).

To make it clear, the base of units are defined as below

| Unit | Description | Base |
|---|---|---|
| K | Kilo Decimal Byte | 1000 |
| M | Mega Decimal Byte | 1000_000 |
| G | Giga Decimal Byte | 1000_000_000 |
| T | Tera Decimal Byte | 1000_000_000_000 |
| P | Peta Decimal Byte | 1000_000_000_000_000 |
| KiB | Kilo Binary Byte | 1024 |
| MiB  | Mega Binary Byte | 1024 * 1024 |
| GiB | Giga Binary Byte | 1024 * 1024 * 1024 |
| TiB  | Tera Binary Byte | 1024 * 1024 * 1024 * 1024 |
| PiB  | Peta Binary Byte | 1024 * 1024 * 1024 * 1024 * 1024 |

Unit is case-insensitive. `k`, `kib`, `KiB`, `kiB` are all acceptable.

Here're two examples

````
druid.cache.sizeInBytes=1g #1 * 1000_000_000 bytes
````

````
druid.cache.sizeInBytes=256MiB #256 * 1024 * 1024 bytes
````


 
