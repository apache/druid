---
id: human-readable-byte
title: "Human-readable Byte Configuration Reference"
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


This page documents configuration properties related to bytes.

These properties can be configured through 2 ways: 
1. a simple number in bytes
2. a number with a unit suffix

## A number in bytes

Given that cache size is 3G, there's a configuration as below

```properties
# 3G bytes = 3_000_000_000 bytes
druid.cache.sizeInBytes=3000000000 
```


## A number with a unit suffix

When you have to put a large number for some configuration as above, it is easy to make a mistake such as extra or missing 0s. Druid supports a better way, a number with a unit suffix.

Given a disk of 1T, the configuration can be

```properties
druid.segmentCache.locations=[{"path":"/segment-cache-00","maxSize":"1t"},{"path":"/segment-cache-01","maxSize":"1200g"}]
```

Note: in above example, both `1t` and `1T` are acceptable since it's case-insensitive.
Also, only integers are valid as the number part. For example, you can't replace `1200g` with `1.2t`.

### Supported Units
In the world of computer, a unit like `K` is ambiguous. It means 1000 or 1024 in different contexts, for more information please see [Here](https://en.wikipedia.org/wiki/Binary_prefix).

To make it clear, the base of units are defined in Druid as below

| Unit | Description | Base |
|---|---|---|
| K | Kilo Decimal Byte | 1_000 |
| M | Mega Decimal Byte | 1_000_000 |
| G | Giga Decimal Byte | 1_000_000_000 |
| T | Tera Decimal Byte | 1_000_000_000_000 |
| P | Peta Decimal Byte | 1_000_000_000_000_000 |
| Ki | Kilo Binary Byte | 1024 |
| Mi  | Mega Binary Byte | 1024 * 1024 |
| Gi | Giga Binary Byte | 1024 * 1024 * 1024 |
| Ti  | Tera Binary Byte | 1024 * 1024 * 1024 * 1024 |
| Pi  | Peta Binary Byte | 1024 * 1024 * 1024 * 1024 * 1024 |
| KiB | Kilo Binary Byte | 1024 |
| MiB  | Mega Binary Byte | 1024 * 1024 |
| GiB | Giga Binary Byte | 1024 * 1024 * 1024 |
| TiB  | Tera Binary Byte | 1024 * 1024 * 1024 * 1024 |
| PiB  | Peta Binary Byte | 1024 * 1024 * 1024 * 1024 * 1024 |

Unit is case-insensitive. `k`, `kib`, `ki`, `KiB`, `Ki`, `kiB` are all acceptable.

Here are some examples

```properties
# 1G bytes = 1_000_000_000 bytes
druid.cache.sizeInBytes=1g 
```

```properties
# 256MiB bytes = 256 * 1024 * 1024 bytes
druid.cache.sizeInBytes=256MiB 
```

```properties
# 256Mi = 256MiB = 256 * 1024 * 1024 bytes
druid.cache.sizeInBytes=256Mi
```


 
