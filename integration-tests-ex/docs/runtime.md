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

## Test Runtime Behavior

This section explains how the various configuration pieces come together
to run tests.

See also:

* [Docker setup](docker.md)
* [Docker Compose setup](compose.md)
* [Test setup](test-config.md)

## Shared Resources

This module has a number of folders that are used by all tests:

* `compose`: A collection of Docker Compose scripts that define the basics
  of the cluster. Each test "inherits" the bits that it needs.
* `compose/environment-configs`: Files which define, as environment variables,
  the runtime properties for each service in the cluster. (See below
  for details.)
* `assets`: The `log4j2.xml` file used by images for logging.

## Container Runtime Structure

The container itself is a bit of a hybrid. The Druid distribution, along
with some test-specific extensions, is reused. The container also contains
libraries for Kafka, MySQL and MariaDB.

Druid configuration is passed into the container as environment variables,
and then converted to a `runtime.properties` file by the container launch
script. Though a bit of a [Rube Goldberg](https://en.wikipedia.org/wiki/Rube_Goldberg)
mechanism, it does have one important advantage over the usual Druid configs:
we can support inheritance and overrides. The various `<service>.env` files
provide the standard configurations. Test-specific Docker Compose files can
modify any setting.

The container mounts a shared volume, defined in the `target/shared` directory
of each test module. This volume can provide extra libraries and class path
items. The one made available by default is `log4j2.xml`, but tests can add
more as needed.

Container "output" also goes into the shared folder: logs, "cold storage"
and so on.

Each container exposes the Java debugger on port 8000, mapped to a different
host port for each container.

Each container exposes the usual Druid ports so you can work with the
container as you would a local cluster. Two handy tools are the Druid
console and the scriptable [Python client](https://github.com/paul-rogers/druid-client).

## Test Execution

Tests run using the Maven [failsafe](https://maven.apache.org/surefire/maven-failsafe-plugin/)
plugin which is designed for integration tests. The Maven phases are:

* `pre-integration-test`: Starts the test cluster with `cluster.sh up` using Docker Compose.
* `integration-test`: Runs tests that start or end with `IT`.
* `post-integration-test`: Stops the test cluster using `cluster.sh down`
* `verify`: Checks the result of the integration tests.

See [this example](https://maven.apache.org/surefire/maven-failsafe-plugin/examples/junit.html)
for JUnit setup with failsafe.

The basic process for running a test group (sub-module) is:

* Cluser startup builds a `target/shared` directory with items to be mounted
  into the containers, such as the `log4j2.xml` file, sample data, etc.
  The shared directory also holds log files, Druid persistent storage,
  the metastore (MySQL) DB, etc. See `test-image/README.md` for details.
* The test is configured via a `druid-cluster/compose.yaml` file.
  This file defines the services to run and their configuration.
* The `cluster.sh up` script builds the shared directory, loads the env vars
  defined when the image was created and starts the cluster.
* Tests run on the local host within JUnit.
* The `Initialization` class loads the cluster configuration (see below),
  optionally populates the Druid metadata storage, and is used to
  inject instances into the test.
* The individual tests run.
* The `cluster.sh down` script shuts down the cluster.

`cluster.sh` uses the generated `test-image/target/env.sh` for versions and
and other environment variables. This ensures that tests run with the same
versions used to build the image. It also simplifies the Maven boilerplate to
be copy/pasted into each test sub-project.

