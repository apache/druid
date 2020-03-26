---
id: druid-aws-rds
title: "Druid AWS RDS Module"
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

This module provides AWS RDS token [password provider](../../operations/password-provider.md) provides temp token for accessing AWS RDS DB cluster.

```json
{ "type": "awsrdstoken", "user": "USER", "host": "HOST", "port": PORT, "region": "AWS_REGION" }
```

Before using this password provider, please make sure that you have connected all dots for db user to connect using token.
See [AWS Guide](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/UsingWithRDS.IAMDBAuth.html).

To use this extension, make sure you [include](../../development/extensions.md#loading-extensions) it in your config file:

```
druid.extensions.loadList=["druid-aws-rds-extensions"]
```
