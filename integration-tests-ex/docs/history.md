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


## History

This revision of the integration test Docker scripts is based on a prior
integration test version, which is, in turn, based on
the build used for the public Docker image used in the Druid tutorial. If you are familiar
with the prior structure, here are some of the notable changes.

* Use of "official" images for third-party dependencies, rather than adding them
  to the Druid image. (Results in *far* faster image builds.)
* This project splits the prior `druid-integration-tests` project into several parts. This
  project holds the Druid Docker image, while sibling projects hold the cluster definition
  and test for each test group.
  This allows the projects to better utilize the standard Maven build phases, and allows
  better partial build support.
* The prior approach built the Docker image in the `pre-integration-test` phase. Here, since
  the project is separate, we can use the Maven `install` phase.
* The prior structure ran *before* the Druid `distribution` module, hence the Druid artifacts
  were not available, and the scripts did its own build, which could end up polluting the
  Maven build cache. This version runs after `distribution` so it can reuse the actual build
  artifacts.
* The `pom.xml` file in this project does some of the work that that `build_run_cluster.sh`
  previously did, such as passing Maven versions into Docker.
* The work from the prior Dockerfile and `base-setup.sh` are combined into the revised
  `base-setup.sh` here so that the work is done in the target container.
* Since the prior approach was "all-in-one", it would pass test configuration options into
  the container build process so that the container is specific to the test options. This
  project attempts to create a generic container and instead handle test-specific options
  at container run time.
* The detailed launch commands formerly in the Dockerfile now reside in
  `$DRUID_HOME/launch.sh`.
* The prior version used a much-extended version of the public launch script. Those
  extensions moved into `launch.sh` with the eventual goal of using the same launch
  scripts in both cases.
* The various `generate_*_cert.sh` scripts wrote into the source directory. The revised
  scripts write into `target/shared/tls`.
* The shared directory previously was in `~/shared`, but that places the directory outside
  of the Maven build tree. The new location is `$DRUID_DEV/docker/base-docker/target/shared`.
  As a result, the directory is removed and rebuild on each Maven build. The old location was
  removed via scripts, but the new one is very clearly a Maven artifact, and thus to be
  removed on a Maven `clean` operation.
* The prior approach had security enabled for all tests, which makes debugging hard.
  This version makes security optional, it should be enabled for just a security test.
* The orginal design was based on TestNG. Revised tests are based on JUnit.
* The original tests had "test groups" within the single directory. This version splits
  the former groups into projects, so each can have its own tailored cluster definition.
* Prior images would set up MySQL inline in the container by starting the MySQL engine.
  This led to some redundancy (all images would do the same thing) and also some lost
  work (since the DBs in each container are not those used when running.) Here, MySQL
  is in its own image. Clients can update MySQL as needed using JDBC.
* Prior code used Supervisor to launch tasks. This version uses Docker directly and
  runs one process per container (except for Middle Manager, which runs Peons.)

## History

The current work builds on the prior integration tests, with changes to
simplify and speed up the process.

* The prior tests required a separate Docker build for each test "group"
  Here, the former groups are sub-projects. All use the same Docker image.
* The prior code used the long-obsolte TestNG. Tests here use JUnit.
* The prior test used a TestNG suite to create test intances and inject
  various items using Guice. This version uses an `Initializer` class to
  do roughly the same job.
* The prior tests required test configuration be passed in on the command
  line, which is tedious when debugging. This version uses a cluster
  configuation file instead.
* The prior version perfomed MySQL initialization in the Docker container.
  But, since each test would launch multiple containers, that work was
  done multiple times. Here the work is done by the test itself.
* The prior version had a single "shared" directory for all tests in
  `~/shared`. This version creates a separate shared folder for each
  test module, in `<module>/target/shared`. This ensures that Maven will
  delete everything between test runs.
* This version removes many of the `druid-` prefixes on the container
  names. We assume that the cluster runs as the only Docker app locally,
  so the extra naming just clutters things.