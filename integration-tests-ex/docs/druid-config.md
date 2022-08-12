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

# Druid Configuration

In a normal install, Druid obtains configuration from properties files:

* `<base>/_common/common.runtime.properties`
* `<base>/<service>/runtime.properties`

In the container, Druid uses the same mechanism, though the common properties
file is empty. The container could simply mount the `runtime.properties` file.
However, doing so runs into the normal issues with Druid configuration: Druid
provides no form of inheritance: we'd have to copy/paste the same properties
over and over, which would be a maintenance headache.

Instead, the images use the same technique as the
[production Docker image](https://druid.apache.org/docs/latest/tutorials/docker.html):
we pass in a large number of environment variables.

The test configuration extends the production set to include extra
variables. Thus there are two kinds:

* General configuration (capitalized)
* Druid configuration file settings (lower case)

## Configuration Flow

We use `docker-compose` to gather up the variables. From most specific
(highest priority) to most general, configuration comes from:

* An environment variable set by the script which launches Docker Compose.
  (Use sparingly, only for different test "modes" such as choosing the
  DB driver, when we will use a different mode across diffrerent test runs.)
* As in-line settings in the `environment` section in the Docker Compose
  definition for each service.
* In the service-specific `compose/environment-configs/<service>.env` file.
* In the common `compose/environment-configs/common.env` file.

Make test-specific changes in the test-specific Docker compose file. Make
changes to the `*.env` files only if you are certain that the change should
apply to all tests. An example is when we change something in our product
configs.

The set of defined environment variables starts with the
`druid/conf/single-server/micro-quickstart` settings. It would be great to generate
these files directly from the latest quickstart files. For now, it is a manual
process to keep the definitions in sync.

These are defined in a hierarchy:

* `common.env` - roughly equivalent to the `_common` configuration area in Druid:
  contains definitions common to all Druid services. Services can override any
  of the definitions.
* `<service>.env` - base definitions for each service, assume it runs stand-alone.
  Adjust if test cluster runs multiple instances. Rougly equivalent to the
  service-specific `runtime.properties` file.
* `docker-compose.yaml` - test-specific settings.

The `launch.sh` script converts these variables to config files in
`/tmp/conf/druid`. Those files are then added to the class path.

## Druid Config Settings

To set a Druid config variable, replace dots in the name with underscores.

In the usual properties file:

```text
druid.auth.basic.common.maxSyncRetries=20
```

In an environment config file:

```text
druid_auth_basic_common_maxSyncRetries=20
```

```text
    environment:
      - druid_auth_basic_common_maxSyncRetries=20
```

For everyone's sanity, please include a comment to explain the reason for
the setting if it differs from the Quickstart defaults.

## Special Config Variables

The test configuration goes beyond the production Druid image configuration
to add several extensions specfically for tests. These are variables which
handle some specific part of the configuration to avoid what would otherwise
require redundant copy/paste. See the [Docker section](docker.md) for the
details.

## Shared Directory

Druid configuration includes not just the config files, but also items
on the Druid class path. These are provided via a `shared` directory mounted
into the container at `/shared`.
The shared directory is built in the `target/<category>` folder for each test
category.

The `launch.sh` script fills in a number of implicit configuration items:

| Item | Description |
| ---- | ----------- |
| Heap dump path | Set to `${SHARED}/logs/<instance>` |
| Log4J config | Optional at `${SHARED}/conf/log4j.xml` |
| Hadoop config | Optional at `${SHARED}/hadoop-xml` |
| Extra libraries | Optional at `${SHARED}/lib` |
| Extra resources | Optional at `${SHARED}/resources` |

`${SHARED}/resources` is the place to put things like a custom `log4j2.xml`
file.

## Security Setup

Tests can run with or without security enabled. (Security setup is a work in progress,
the prior integration tests enabled security for all tests.)

* `auth.env` - Additional definitions to create a secure cluster. Also requires that
  the client certificates be created. Add this to tests which test security.
