---
id: tutorial-extern
title: Export query results
sidebar_label: Export results
description: How to use EXTERN to export query results.
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

This tutorial demonstrates how to use the [EXTERN](..multi-stage-query/reference#extern-function) function Apache Druid&circledR; to export data.


## Prerequisites

Before you follow the steps in this tutorial, download Druid as described in the [Local quickstart](index.md) and have it running on your local machine. You don't need to load any data into the Druid cluster.

You should be familiar with data querying in Druid. If you haven't already, go through the [Query data](../tutorials/tutorial-query.md) tutorial first.

## Configure Druid local export directory 

```
sed -i -e $'$a\\\n\\\n\\\n#\\\n###Local export\\\n#\\\ndruid.export.storage.baseDir=/tmp/druid/' conf/druid/auto/_common/common.runtime.properties
```

This adds the following to the Druid configuration:

```
#
###Local export
#
druid.export.storage.baseDir=/tmp/druid/
```

## Start Druid

## Load data

## Export data

```sql
INSERT INTO
  EXTERN(
    local(exportPath => '/tmp/druid/query1')
        )
AS CSV
SELECT APPROX_COUNT_DISTINCT_DS_THETA(theta_uid) FILTER(WHERE "show" = 'Bridgerton') AS users
FROM ts_tutorial
```

## Learn more

See the following topics for more information:

* [Update data](./tutorial-update-data.md) for a tutorial on updating data in Druid.
* [Data updates](../data-management/update.md) for an overview of updating data in Druid.
