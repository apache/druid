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

# Test Structure

The structure of integration tests is heavily influenced by the existing
integration test structure. In that previous structure:

* Each test group ran as separate Maven build.
* Each would build an image, start a cluster, run the test, and shut down the cluster.
* Tests were created using [TestNG](https://testng.org/doc/), a long-obsolete
  test framework.
* A `IntegrationTestingConfig` is created from system properties (passed in from
  Maven via `-D<key>=<value>` options).
* A TestNG test runner uses a part of the Druid Guice configuration to inject
  test objects into the tests.
* The test then runs.

To minimize test changes, we try to keep much of the "interface" while changing
the "implementation". Basically:

* The same Docker image is used for all tests.
* Each test defines its own test cluster using Docker Compose.
* Maven runs tests one by one, starting and stopping the test-specific cluster for each.
* A `ClusterConfig` object defines the test configuration and creates the
  `IntegrationTestingConfig` object.
* An instance of `Initializer` sets up Guice for each test and injects the
  test objects.
* Tests run as JUnit tests.

The remainder of this section describes the test internals.

## Test Configuration

See [Test Configuration](test-config.md) for details on the `docker.yaml` file
that you create for each test module to tell the tests about the cluster you
have defined.

Test configuration allows inheritance so, as in Docker Compose, we define
standard bits in one place, just providing test-specific information in each
tests `docker.yaml` file.

The test code assumes that the test configuration file is in `src/test/resources/yaml/docker.yaml`
(or, specifically that it is on the class path at `/yaml/docker.yaml`)
and loads it automatically into a `ClusterConfig` instance.

The `ClusterConfig` instance provides the backward-compatible
`IntegrationTestingConfig` instance.

New tests may want to work with `CluserConfig` directly as the older interface
is a bit of a muddle in several areas.

## Initialization

We want the new JUnit form of the integration tests to be as simple as possible
to debug. Rather than use a JUnit test suite as a replacement for the TestNG
test suite, we instead make each test class independent. To do this, we provide 
a custom test runner which initializes Guice from the test configuration:

```java
@RunWith(DruidTestRunner.class)
public class MyTest
{
  @Inject
  private SomeObject myObject;
  ...
  
  @Test
  public void myTest()
  {
    ...
```

`DruidTestRunner` is defined in the `it-base` module, along with a number of
support classes which are mostly hidden from the tests.

Here's what happens:

* JUnit uses the annotation to notice that we've provided a custom
  test runner. (When converting tests, remember to add the required
  annotation.)
* JUnit calls the test class constructor one or more times per test class.
* On the first creation of the test class, `DruidTestRunner`
  loads test configuration, creates the Guice injector,
  injects dependencies into the class instanance, and
  starts the Druid lifecycle.
* JUnit calls one of the test methods in the class.
* On the second creation of the test class in the same JVM, `DruidTestRunner`
  reuses the existing injector to inject dependencies into the test,
  which avoids the large setup overhead.
* During the first configuration, `DruidTestRunner` causes initialization
  to check the health of each service prior to starting the tests.
* The test is now configured just as it would be from TestNG, and is ready to run.
* `DruidTestRunner` ends the lifecycle after the last test within this class runs.

See [this explanation](dependencies.md) for the gory details.

`DruidTestRunner` loads the basic set of Druid modules to run the basic client
code. Tests may wish to load additional modules specific to that test.

TODO: Allow the tests to provide a list of such modules. We'll likely use another
annotation for this when we encounter the needed.

## Test Structure

When working with tests, it is helpful to know a bit more about the "magic"
behind `DruidTestRunner`.

Druid's code is designed to run in a server, not a client. Yet, the tests are
clients. This means that tests want to run code in a way that it was not
intended to be run. The existing ITs have mostly figured out how to make that
happen, but result is not very clean. This is an opportunity for improvement.

Druid uses Guice to initialize code. Initialization assumes that Guice is
configuring a server. The `Initializer` class in `it-base` jumps through hoops
to work around the server assumption. The result isn't pretty, but it works.

Druid uses the `Lifecycle` class to start and stop services. For this to work,
the managed instance must be created *before* the lifecycle starts. There are
a few items that are lazy singletons. When run in the server, they work fine.
But, when run in tests, we run into a race condition: we want to start the
lifecycle once before the tests start, the inject dependencies into each test
class instance as tests run. But, those injections create the insteance we want
the lifecycle to manage, resulting in a muddle. This is why the `DruidTestRunner`
has that odd "first test. vs. subsequent test" logic.

The prior ITs would start running tests immediately. But, it can take up to a
minute or more for a Druid cluster to stabilize as all the services start
running simultaneously. The previous ITs would use a generic retry up to 240
times to work around the fact that any given test could fail due to the cluster
not being ready. This version does that startup check as part if `DruidTestRunner`.
By the time the tests run, the cluster is up and reported itself healthy.
That is, your tests can assume a healthy cluster. If a test fails: it indicates
an actual error or race condition.

Specifically, if tests still randomly fail, those tests are telling you something: something 
in Druid itself is non-deterministic (such as the delay to see changes to the DB, etc.),
or the tests are making invalid assumptions such as assuming an ordering when there
is none, using a time delay to try to synchronize actions when there should be
some specific synchronization, etc. This means that, in general, you should avoid
the use of the generic retry facility: if you have to retry to get your tests to
work, then the Druid user has to also retry. Unless we document the need to retry
in the API documentation, then having to retry should be considered a bug to be fixed
(perhaps by documenting the need to retry, perhaps by fixing a bug, perhaps by adding
a synchronization API.)

Another benefit of the startup check is that the startup and health-check costs are
paid once per test class. This allows you to structure your
tests as a large number of small tests rather than a few big tests.

## `ClusterConfig` and `ResolvedClusterConfig`

The `ClusterConfig` class is the Java representation of the
[test configuration](test-config.md). The instance is available from the
`Initializer` and by Guice injection.

It is a Jackson-serialized class that handles the "raw" form of
configuration.

The `ClusterConfig.resolve()` method expands includes, applies defaults,
validates values, and returns a `ResolvedClusterConfig` instance used
by tests. `ResolvedClusterConfig` is available via Guice injection.
In most cases, however, you'll use it indirecty via the various clients
described below. Each of those uses `IntegrationTestingConfig` class, an
instance of which is created to read from `ResolvedClusterConfig`.

Remember that each host has two names and two ports:

* The external (or "proxy") host and port, as seen by the machine running
  the tests.
* The internal host and port, as seen by the service itself running
  in the Docker cluster.

The various [config files](test-config.md) provide configurations for
the Docker, K8s and local cluster cases. This means that `resolveProxyHost()`
will resolve to the proxy for Docker, but the actual host for a local cluster.

## `ClusterClient`

The integration tests make many REST calls to the Druid cluster. The tests
contain much copy/paste code to make these calls. The `ClusterClient` class
is intended to gather up these calls so we have a single implementation
rather than many copies. Add methods as needed for additional APIs.

The cluster client is "test aware": it uses the information in
`ClusterConfig` to know how to send the requested API. The methods handle
JSON deserialization, so tests can focus simply on making a call and
checking the results.

## `org.apache.druid.testing.clients`

This package in `integration-tests` has clients for most other parts of
Druid. For example, `CoordinatorResourceTestClient` is a
client for Coordinator calls. These clients are also aware of the test
configuration, by way of the `IntegrationTestingConfig` class, an
instance of which is created to read from `ResolvedClusterConfig`.
