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

# Maven Structure

The integration tests are built and run as part of Druid's Maven script.
Maven itself is used by hand, and as part of the [Travis](travis.md) build
proces. Running integration tests in maven is a multi-part process.

* Build the product `distribution`.
* Build the test image.  The tests run against the Maven-created Druid build,
  and so appear in the root `pom.xml` file *after* the `distribution`
  project which builds the Druid tarball.
* Run one or more ITs. Each Maven run includes a single test category and its
  required Druid cluster.

Travis orchestrates the above process to run the ITs in parallel. When you
run tests locally, you do the above steps one by one. You can, of course, reuse
the same disribution for multiple image builds, and the same image for multiple
test runs.

## Build the Distribution and Image

Use the following command to run the ITs, assuming `DRUID_DEV` points
to your Druid development directory:

```bash
cd $DRUID_DEV
mvn clean package -P dist,test-image,skip-static-checks \
    -Dmaven.javadoc.skip=true -DskipUTs=true
```

The various pieces are:

* `clean`: Remove any existing artifacts, and any existing Docker image.
* `install`: Build the Druid code and write it to the local Maven repo.
* `-P dist`: Create the Druid distribution tarball by pulling jars from
  the local Maven repo.
* `-P test-image`: Build the Docker images by grabbing the Druid tarball
  and pulling additional dependencies into the local repo, then stage them
  for Docker.
* Everything else: ignore parts of the build not needed for the ITs, such
  as static checks, unit tests, Javadoc, etc.

Once you've done the above once, you can do just the specific part you want
to repeat during development. See below for details.

See [quickstart](quickstart.md) for how to run the two steps separately.

## Run Each Integration Test Category

Each pass through Maven runs a single test category. Running a test category
has three parts, spelled out in Maven:

* Launch the required cluster.
* Run the test category.
* Shut down the cluster.

Again, see [quickstart](quickstart.md) for how to run the three steps separately,
and how to run the tests in an IDE.

To do the task via Maven:

```bash
cd $DRUID_DEV
mvn verify -P docker-tests,skip-static-checks,IT-<category> \
    -Dmaven.javadoc.skip=true -DskipUTs=true
```

The various pieces are:

* `verify`: Run the steps up to the one that checks the output of the ITs. Because of
  the extra cluster step in an IT, the build does not fail if an IT failse. Instead,
  it continues on to clean up the cluster, and only after that does it check test
  sucess in the `verify` step.
* `<category`: The name of the test category as listed in [tests](tests.md).
* Everything else: as explained above.

## FailSafe

The revised integration tests use the [Maven failsafe plugin]
(https://maven.apache.org/surefire/maven-failsafe-plugin/) which shares
code with the [Maven Surefire plugin](
https://maven.apache.org/surefire/maven-surefire-plugin/)
used to run unit tests. Failsafe handles the special steps unique to integration
tests.

Since we use JUnit categories, we must use the `surefire-junit47` provider. Omitting
the provider seems to want to use the TestNG provider. Using just the `surefire-junit4`
provider cause Surefire to ignore categories.

### Skipping Tests

One shared item is the `skipTests` flag.
Via a bit of [Maven config creativity](
https://stackoverflow.com/questions/6612344/prevent-unit-tests-but-allow-integration-tests-in-maven)
we define extra properties to control the two kinds of tests:

* `-DskipUTs=true` to skip Surefire (unit) tests.
* `-P docker-tests` to enable Failsafe (integration) tests.
* `-DskipTests=true` to skip all tests.

## Modules

The key modules in the above flow include:

* `distribution`: Builds the Druid tarball which the ITs exercise.

The IT process resides in the `integration-tests-ex` folder and consists
of three Maven modules:

* `druid-it-tools`: Testing extensions added to the Docker image.
* `druid-it-image`: Builds the Druid test Docker image.
* `druid-integration-test-cases`: The code for all the ITs, along with
  the supporting framework.

The annoying "druid" prefix occurs to make it easier to separate Apache Druid
modules when users extend Druid with extra user-specific modules.

### Two-level Module Structure

It turns out that, for obscure reasons, we must use a "flat" module
structure under the root Druid `pom.xml` even though the modules
themselves are in folders within `integration-tests-ex`. That is, we cannot
create a `integration-tests-ex` Maven module to hold the IT modules. The
reason has to do with the fact that Maven has no good way to
obtain the directory that contains the root `pom.xml` file. Yet,
we need this directory to find configuration files for some of the
static checking tools. Though there is a [directory plugin](
https://github.com/jdcasey/directory-maven-plugin) that *looks* like
it should work, we then run into a another issue: if we invoke a
goal directly from the `mvn` command line: Maven happily ignores the
`validate` and `initialize` phases where we'd set the directory path.
By using a two-level module structure, we can punt and just always
assume that `${project.parent.basedir}` points to the root Druid
directory. More than you wanted to know, but now you know why there
is no `integration-tests-ex` module as there should be.

As a result, you may have to hunt in your IDE to find the non-project
files in the `integration-tests-ex` directory. Look in the root `druid`
project, in the `integration-tests-ex` folder.

Because of this limitation, all the test code is in one Maven module.
When we tried to create a separate module per category, we ended up with
a great deal of redundancy since we could not have common parent module.
Putting all the code in one module means we can only run one test category
per Maven run, which is actually fine because that's how Travis runs tests
anyway.

## `org.apache.druid.testsEx` Package

The `org.apache.druid.testsEx` is temporary: it holds code from the
`integration-tests` `org.apache.druid.testing` package adapted to work
in the revised environment. Some classes have the same name in both
places. The goal is to merge the `testsEx` package back into
`testing` at some future point when the tests are all upgraded.

The revised ITs themselves are also in this package. Over time, as we
replace the old ITs, the test can migrate to the original package names.

## Profiles

Integration test artifacts are built only if you specifically request them
using a profile.

* `-P test-image` builds the test Docker image.
* `-P docker-tests` enables the integration tests.
* `-P IT-<category>` selects the category to run.

The profiles allow you to build the test image once during debugging,
and reuse it across multiple test runs. (See [Debugging](debugging.md).)

## Dependencies

The Docker image inclues three third-party dependencies not included in the
Druid build:

* MySQL connector
* MariaDB connector
* Kafka Protobuf provider

We use dependency rules in the `test-image/pom.xml` file to cause Maven to download
these dependencies into the Maven cache, then we use the
`maven-dependency-plugin` to copy those dependencies into a Docker directory,
and we use Docker to copy the files into the image. This approach avoids the need
to pull the dependency from a remote repository into the image directly, and thus
both speeds up the build, and is kinder to the upstream repositories.

If you add additional dependencies, please follow the above process. See the
`pom.xml` files for examples.

## Environment Variables

The build environment users environment variables to pass information to Maven.
Maven communicates with Docker and Docker Compose via environment variables
set in the `exec-maven-plugin` of various `pom.xml` files. The environment
variables then flow into either the Docker build script (`Dockerfile`) or the
various Docker Compose scripts (`docker-compose.yaml`). It can be tedious to follow
this flow. A quick outline:

* The build environment (such as Travis) sets environment variables, or passes values
  to maven via the `-d<var>=<value` syntax.
* Maven, via `exec-maven-plugin`, sets environment variables typically from Maven's
  own variables for things like versions.
* `exec-maven-plugin` invokes a script to do the needes shell fiddling. The environment
  variables are visible to the script and implicitly passed to whatever the script
  calls.
* When building, the script passes the environment variables to Docker as build
  arguments.
* `Dockerfile` typically passes the build arguments to the image as environment
  variables of the same name.
* When running, the script passes environment variables implicitly to Docker Compose,
  which uses them in the various service configurations and/or environment variable
  settings passed to each container.

If you find you need a new parameter in either the Docker build or the Docker Compose
configuration:

* First ensure that there is a Maven setting that holds the desired parameter.
* Wire it through the relevant `exec-maven-plugin` sections.
* Pass it to Docker or Docker Compose as above.

The easiest way to test is to insert (or enable, or view) the environment in the
image:

```bash
env
```

The output will typically go into the Docker output or the Docker Compose logs.

## Shortcuts

Since Druid's `pom.xml` file is quite large, Maven can be a bit slow when
all you want to do is to build the Docker image. To speed things up a bit,
you can build just the docker image. See the [Quickstart](docs/quickstart.md)
for how to run tests this way.
Using this trick, creating an image, or launching a cluster, is quite fast.
See [the Docker section](docker.md) for details.
