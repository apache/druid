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

[AWS RDS](https://aws.amazon.com/rds/) is a managed service to operate relation databases such as PostgreSQL, Mysql etc. These databases could be accessed using static db password mechanism or via [AWS IAM](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.html) temporary tokens. This module provides AWS RDS token [password provider](../../operations/password-provider.md) implementation to be used with [mysql-metadata-store](mysql.md) or [postgresql-metadata-store](postgresql.md) when mysql/postgresql is operated using AWS RDS.

```json
{ "type": "aws-rds-token", "user": "USER", "host": "HOST", "port": PORT, "region": "AWS_REGION" }
```

Before using this password provider, please make sure that you have connected all dots for db user to connect using token.
See [AWS Guide](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/UsingWithRDS.IAMDBAuth.html).

To use this extension, make sure you [include](../../development/extensions.md#loading-extensions) it in your config file along with other extensions e.g.

```
druid.extensions.loadList=["druid-aws-rds-extensions", "postgresql-metadata-storage", ...]
```
