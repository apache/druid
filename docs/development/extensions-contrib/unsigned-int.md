---
id: unsigned-int
title: "Unsigned Integer Column Type"
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

Apache Druid Extension to utilize an unsigned int column.  

Consider this an [EXPERIMENTAL](../experimental.md) feature mostly because it has not been tested yet on a wide variety of long-running Druid clusters.

## How it works

Only store a 4 byte unsigned integer instead of a long column when you need your columns don't require you to store a long.  Aggregations still happen as long values, only the data stored on disk is as an unsigned integer.

## Configuration

To use this extension please make sure to  [include](../extensions.md#loading-extensions)`unsigned-int-extensions` in the extensions load list.

Example Usage: 

```
"metricsSpec": [
        {
          "type": "unsigned_int",
          "name": "value",
          "fieldName": "value"
        },
```

