---
id: datasketches-extension
title: "DataSketches extension"
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


Apache Druid (incubating) aggregators based on [datasketches](https://datasketches.github.io/) library. Sketches are data structures implementing approximate streaming mergeable algorithms. Sketches can be ingested from the outside of Druid or built from raw data at ingestion time. Sketches can be stored in Druid segments as additive metrics.

To use the datasketches aggregators, make sure you [include](../../development/extensions.md#loading-extensions) the extension in your config file:

```
druid.extensions.loadList=["druid-datasketches"]
```

The following modules are available:

* [Theta sketch](datasketches-theta.html) - approximate distinct counting with set operations (union, intersection and set difference).
* [Tuple sketch](datasketches-tuple.html) - extension of Theta sketch to support values associated with distinct keys (arrays of numeric values in this specialized implementation).
* [Quantiles sketch](datasketches-quantiles.html) - approximate distribution of comparable values to obtain ranks, quantiles and histograms. This is a specialized implementation for numeric values.
* [HLL sketch](datasketches-hll.html) - approximate distinct counting using very compact HLL sketch.
