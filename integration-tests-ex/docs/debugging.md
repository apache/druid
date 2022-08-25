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

# Debugging the Druid Image and Integration Tests

The integration setup has as a primary goal the ability to quickly debug
the Druid image and any individual tests. A first step is to move the
image build into a separate project. A second step is to ensure each
test can run in JUnit in an IDE against a cluster you start by hand.

This section discusses how to use the various debugging features.

See:

* [Docker Configuration](docker.md) for information on debugging
  docker builds.

## General Debug Process

Ease of debugging is a key goal of the revised structure.

* Rebuild the Docker image only when the Druid code changes.
  Do a normal distribution build, then build a new image.
* Reuse the same image over and over if you only change tests
  (such as when adding a new test.)
* Reuse the same `shared` directory when the test does not
  make permanent changes.
* Change Druid configuration by changing the Docker compose
  files, no need to rebuild the image.
* Work primarily in the IDE when debugging tests.
* To add more logging, change the `log4j2.xml` file in the shared
  directory to increase the logging level.
* Remote debug Druid services if needed.

## Exploring the Test Cluster

When run in Docker Compose, the endpoints known to Druid nodes differ from
those needed by a client sitting outside the cluster. We could provide an
explicit mapping. Better is to use the
[Router](https://druid.apache.org/docs/latest/design/router.html#router-as-management-proxy)
to proxy requests. Fortunately, the Druid Console already does this.

## Docker Build Output

Modern Docker seems to hide the output of commands, which is a hassle to debug
a build. Oddly, the details appear for a failed build, but not for success.
Use the followig to see at least some output:

```bash
export DOCKER_BUILDKIT=0
```

Once the base container is built, you can run it, log in and poke around. First
identify the name. See the last line of the container build:

```text
Successfully tagged org.apache.druid/test:<version>
```

Or ask Docker:

```bash
docker images
```

## Debug the Docker Image

You can log into the Docker image and poke around to see what's what:


```bash
docker run --rm -it --entrypoint bash org.apache.druid/test:<version>
```

Quite a few environment variables are provided by Docker and the setup scripts
to see them, within the container, use:

```bash
env
```

## Debug an Integration Test

To debug an integration test, you need a Docker image with the latest Druid.
To get that, you need a full Druid build. So, we break the debugging process
down into steps that depend on the state of your code. Assume `DRUID_DEV`
points to your Druid development area.

### On Each Druid Build

If you need to rebuild Druid (because you fixed something), do:

* Do a distribution build of Druid:
* Build the test image.

See [quickstart](quickstart.md) for the commands.

### Start the Test Cluster

* Pick a test "group" to use.
* Start a test cluster configured for this test.
* Run a test from the command line:

Again, see [quickstart](quickstart.md) for the commands.

### Debug the Test

To run from your IDE, find the test to run and run it as a JUnit test (with the
cluster up.)

Depending on the test, you may be able to run the test over and over against the
same cluster. (In fact, you should try to design your tests so that this is true:
clean up after each run.)

The tests are just plain old JUnit tests that happen to reach out to the
test cluster and/or Docker to do their work. You can set breakpoints and debug
in the usual way.

Each test will first verify that the cluster is fully up before it starts, so
you can launch your debug session immediately after starting the cluster: the tests
will wait as needed.

### Stop the Test Cluster

When done, stop the cluster: [quickstart](quickstart.md) again for details.

## Typical Issues

For the most part, you can stop and restart the Druid services as often
as you like and Druid will just work. There are a few combinations that
can lead to trouble, however.

* Services won't start: When doing a new build, stop the existing cluster
  before doing the build. The build removes and rebuilds the shared
  directory: services can't survive that.
* Metastore failure: The metastore container will recreate the DB on
  each restart. This will fail if your shared directory already contains
  a DB. Do a `rm -r target/<category>/db` before restarting the DB container.
* Coordinator fails with DB errors. The Coordinator will create the Druid
  tables when it starts. This means the DB has to be created. If the DB
  is removed after the Coordinator starts (to fix the above issue, say)
  then you have to restart the Coordinator so it can create the needed
  tables.
