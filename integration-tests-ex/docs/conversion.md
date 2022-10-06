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

# Test Conversion

Here is the process to convert an existing `integration-test`
group to this new structure.

The tests all go into the `druid-integration-test-cases` module
(sub-directory `test-cases`). Move the tests into the existing
`testsEx` name space so they do not collide with the existing
integration test namespace.

## Cluster Definition

Define a category for your tests. See [tests](tests.md) for the
details. The category is the name of the cluster definition by
default.

Determine if you can use an existing cluster definition, or if you
need to define a new one. See [tests](tests.md) for how to share a
cluster definition. If you share a definition, update `cluster.sh`
to map from your category name to the shared cluster definition
name.

To create a new defnition,
create a `druid-cluster/docker-compose.yaml` file by converting the
previous `docker/docker-compose-<group>.yml` file. Carefully review
each service. Use existing files as a guide.

In `integration-tests` all groups share a set of files with many
conditionals to work out what is to be done. In this system, each
test group stands alone: its Docker Compose file defines the cluster
for that one test. There is some detangling of the existing conditionals
to determine the setup used by each test group.

Create the `yaml/docker.yaml` resource in `/src/test/resources` to
define your cluster for the Java tests.

Determine if the test group populates the metadata store using queries
run in the Docker container. If so, copy those queries into the
`docker.yaml` file in the `metadataInit` section. (In the new structure,
these setup queries run in the test client, not in each Docker service.)
See the former `druid.sh` script to see what SQL was used previously.

###

## Test Runner

ITs require a large amount of setup. All that code is encapsulated in the
`DruidTestRunner` class:

```java
@RunWith(DruidTestRunner.class)
@Category(MyCategory.class)
public class ITMyTest
```

It is helpful to know what the test runner does:

* Loads the cluster configuration from the `docker.yaml` file, and
  resolves any includes.
* Builds up the set of Guice modules needed for the test.
* Creates the Guice injector.
* Uses the injector to inject dependencies into your test class.
* Starts the Druid lifecycle.
* Waits for each Druid service defined in `docker.yaml` to become
  available.
* Runs your test methods.
* Ends the Druid lifecycle.

You can customize the configuration for non-standard cases. See
[tests](tests.md) for details.

## Tests

Convert the individual tests.

### Basics

Copy the existing tests for the target group into the
`druid-it-cases`. For sanity, you may want to do one by one.

When adding tests, leave the original tests in `integration-tests` for
now. (Until we have the new system running in Travis.) Once Travis
runs, you can move, rather than copy, the tests.

While we are copying, copy to the `org.apache.druid.testsEx` package to
prevent name conficts with `org.apache.druid.tests`.

### Maven Dependencies

You may need to add dependencies to `pom.xml`.

The `docker-tests/pom.xml` file includes Maven dependencies for the most
common Druid modules, which transitiviely include the third-party modules
which the Druid modules reference. You test sub-project may need addition
dependencies. To find them, review `integration-tests/pom.xml`. Careful,
however, as that file is a bit of a "kitchen sink" that includes every
possible dependency, even those already available transitively.

If you feel the dependency is one used by multiple tests, go ahead and
add it to `docker-tests/pom.xml`. If, however, it is somehwat unique to
the test group, just add it to that sub-modules `pom.xml` file instead.

Use the following to verify the `pom.xml`:

```bash
mvn dependency:analyze -DoutputXML=true -DignoreNonCompile=true \
    -P skip-static-checks -Dweb.console.skip=true -Dmaven.javadoc.skip=true \
    -P skip-tests
```

Doing it now will save build cycles when submitting your PR.

### Resources and Test Data

The existing tests use the `/src/test/resources/` directory to hold both
JSON specs used by the tests, as well as test data used by the cluster.
To make the data available to tests, we mount the `/src/test/resources`
folder into the Indexer at `/resources`.

In the new version, we separate these two groups of files. Those used by
tests continue to reside in `/src/test/resources` for the individual
tests. Those shared by multiple tests can be in `base-test/src/test/resources`.
Copy the resource files from `integration-tests` into one of these
locations. Try to avoid doing a bulk copy: copy only the files used by
the particular test group being converted.

Then, copy the data into `/data`, keeping the same path. See
`data/README.md` for more information.

#### To Do

It may turn out that data files are shared among tests. In that case, we'd
want to put them in a common location, keeping test-specific data in the
project for that test. But, we can't easily combine two directories into
a single volume mount.

Instead, we can use the `target/shared` folder: create a new `data`
folder, copy in the required files, and mount that at `/resources`.
Or, if we feel energetic, just change the specs to read their data
from `/shared/data`, since `/shared` is already mounted.

### Extensions

You may see build or other code that passes a list of extensions to an old
integration test. Such configuration represents a misunderstanding of how tests (as
clients) actually work. Tests nave no visibility to a Druid installation directory.
As a result, the "extension" concept does not apply. Instead, tests are run from
Maven, and are subject to the usual Maven process for locating jar files. That
means that any extensions which the test wants to use should be listed as dependencies
in the `pom.xml` file, and will be available on the class path. There is no need for,
or use of, the `druid_extensions_loadList` for tests (or, indeed, for any client.)

### Starter Test (Optional)

An optional step is to ease into the new system by doing a simple
"starter test".
Create a ad-hoc test file, say `StarterTest` to hold one of the
tests to be converted. Copy any needed Guice injections. This will
be a JUnit test.

Define your test class like this:

```java
@RunWith(DruidTestRunner.class)
public class StarterTest
```

The test runner handles the required startup, Guice configuration, cluster
validation, and shutdown. Just add your own test cases.

Determine if the test runs queries from `src/test/resources/queries`.
If so, copy those to the new sub-project. Do the same with any other
resources which the test requires.

In this new structure, each group is its own sub-project, so resources
are separted out per test group (sub-project), whereas in
`integration-tests` the resources are all grouped together. If there
are shared resources, put those in the `docker-tests/src/test/resources`
folder so they can be shared. (This may require creating a test-jar.
As an alternative, they can be put in `base-test` which is already
available to all tests.)

Run the one test. This will find bugs in the above. It will also likely
point out that you need Druid modules not in the base set defined by
`Initialization`. Add these modules via the `Builder.modules()` method.
Resolve the other issues which will inevitably appear.

This starter test will ensure that most of the dependency and configuration
issues are resolved.

### Revised Helper Classes

The new test structure adopted shorter container and host names:
`coordinator` instead of `druid-coordinator` etc. This is safe because the
Docker application runs in isolation, we don't have to worry about a
potential `coordinator` from application X.

To handle these changes, there are new versions of several helper classes.
Modify the tests to use the new versions:

* `DruidClusterAdminClient` - interfaces with Docker using hard-coded
  container names.

The old versions are in `org.apache.druid.testing.utils` in
`integration-tests`, the new versions in `org.apache.druid.testing2.utils`
in this project.

### Test Classes

You can now convert the bulk of the tests.
One-by-one, convert existing classes:

* Remove the TestNG annotations and includes. Substitute JUnit includes.
* Add the `@RunWith` annotation.
* Run the test in the debugger to ensure it works.

The test class definition should look like this:

```java
@RunWith(DruidTestRunner.class)
public class ITIndexerTest ...
{
```

Run the entire suite from Maven in the sub-module directory. It should
start the cluster, run the tests, and shut down the cluster.

## Improving Tests

Once the tests work, an optional step is to improvement a bit beyond what
was already done.

### Retries

The `Initializer` takes upon itself the task of ensuring that all services
are up (at least enough that they each report that they are healthy.) So,
it is not necessary for each test case to retry endlessly to handle the case
that it is the first test run on a cluster still coming up. We can remove
retries that don't represent valid server behavior. For example, if the goal
is too ensure that the endpoint `/foo` returns `bar`, then there is no need
to retry: if the server is up, then its `/foo` endpoint should be working, and
so it should return `bar`, assuming that the server is deterministic.

If `bar` represents something that takes time to compute (the result of a
task, say), then retry is valid. If `bar` is deterministic, then retrying won't
fix a bug that causes `bar` to be reported incorrectly.

Use your judgement to determine when retries were added "just to be safe"
(and can thus be removed), and when the represent actual race conditions in
the operation under tests.

### Cluster Client

The tests obviously do a large number of API calls to the server. Some (most)
seem to spell out the code inline, resulting in much copy/paste. An improvement
is to use the cluster client instead: `ClusterClient`. Add methods for endpoints
not yet covered by copying the code from the test in question. (Better, refactor
that code to use the existing lower-level `get()` and similar methods. Then,
use the cluster client method in place of the copy/paste wad of code.

The result is a test that is smaller, easier to undestand, easier to maintain,
and easier debug. Also, future tests are easier to write because they can reuse
the method you added to the cluster client.

You can inject the cluster client into your test:

```java
  @Inject
  private ClusterClient clusterClient;
```

You may find that by using the cluster client, some of the dependencies which
the test needed are now unused. Go ahead and remove them.
