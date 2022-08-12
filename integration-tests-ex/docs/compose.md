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

# Docker Compose Configuration

The integration tests use Docker Compose to launch Druid clusters. Each
test defines its own cluster
depending on what is to be tested. Since a large amount of the definition is
common, we use inheritance to simplify cluster definition.

Tests are split into categories so that they can run in parallel. Some of
these categories use the same cluster configuration. To further reduce
redundancy, test categories can share cluster configurations.

See also:

* [Druid configuration](druid-config.md) which is done via Compose.
* [Test configuration](test-config.md) which tells tests about the
  cluster configuration.
* [Docker compose specification](https://github.com/compose-spec/compose-spec/blob/master/spec.md)

## File Structure

Docker Compose files live in the `druid-integration-test-cases` module (`test-cases` folder)
in the `cluster` directory. There is a separate subdirectory for each cluster type
(subset of test categories), plus a `Common` folder for shared files.

## Shared Directory

Each test has a "shared" directory that is mounted into each container to hold things
like logs, security files, etc. The directory is known as `/shared` within the container,
and resides in `target/<category>`. Even if two categories share a cluster configuration,
they will have separate local versions of the shared directory. This is important to
keep log files separate for each category.

## Base Configurations

Test clusters run some number of third-party "infrastructure" containers,
and some number of Druid service containers. For the most part, each of
these services (in Compose terms) is similar from test to test. Compose
provides [an inheritance feature](
https://github.com/compose-spec/compose-spec/blob/master/spec.md#extends)
that we use to define base configurations.

* `cluster/Common/dependencies.yaml` defines external dependencis (MySQL, Kafka, ZK
  etc.)
* `cluster/Common/druid.yaml` defines typical settings for each Druid service.

Test-specific configurations extend and customize the above.

## Test-Specific Cluster

Each test has a directory named `cluster/<category>`. Docker Compose uses this name
as the cluster name which appears in the Docker desktop UI. The folder contains
the `docker-compose.yaml` file that defines the test cluster.

In the simplest case, the file just lists the services to run as extensions
of the base services:

```text
services:
  zookeeper:
    extends:
      file: ../Common/dependencies.yaml
      service: zookeeper

  broker:
    extends:
      file: ../Common/compose/druid.yaml
      service: broker
...
```

## Cluster Configuration

If a test wants to run two of some service (say Coordinator), then it
can use the "standard" definition for only one of them and must fill in
the details (especially distinct port numbers) for the second.
(See `HighAvilability` for an example.)

By default, the container and internal host name is the same as the service
name. Thus, a `broker` service resides in a `broker` container known as
host `broker` on the Docker overlay network.
The service name is also usually the log file name. Thus `broker` logs
to `/target/<category>/logs/broker.log`.

An environment variable `DRUID_INSTANCE` adds a suffix to the service
name and causes the log file to be `broker-one.log` if the instance
is `one`. The service name should have the full name `broker-one`.

Druid configuration comes from the common and service-specific environment
files in `/compose/environment-config`. A test-specific service configuration
can override any of these settings using the `environment` section.
(See [Druid Configuration](druid-config.md) for details.)
For special cases, the service can define its configuration in-line and
not load the standard settings at all.

Each service can override the Java options. However, in practice, the
only options that actually change are those for memory. As a result,
the memory settings reside in `DRUID_SERVICE_JAVA_OPTS`, which you can
easily change on a service-by-service or test-by-test basis.

Debugging is enabled on port 8000 in the container. Each service that
wishes to expose debugging must map that container port to a distinct
host port.

The easiest way understand the above is to look at a few examples.

## Service Names

The Docker Compose file sets up an "overlay" network to connect the containers.
Each is known via a host name taken from the service name. Thus "zookeeper" is
the name of the ZK service and of the container that runs ZK. Use these names
in configuration within each container.

### Host Ports

Outside of the application network, containers are accessible only via the
host ports defined in the Docker Compose files. Thus, ZK is known as `localhost:2181`
to tests and other code running outside of Docker.

## Define a Test Cluster

To define a test cluster, do the following:

* Define the overlay network.
* Extend the third-party services required (at least ZK and MySQL).
* Extend each Druid service needed. Add a `depends_on` for `zookeeper` and,
  for the Coordinator and Overlord, `metadata`.
* If you need multiple instances of the same service, extend that service
  twice, and define distinct names and port numbers.
* Add any test-specific environment configuration required.
