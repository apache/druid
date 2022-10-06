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

# Test Creation Guide

You've played with the existing tests and you are ready to create a new test.
This section walks you through the process. If you are converting an existing
test, then see the [conversion guide](conversion.md) instead. The details
of each step are covered in other files, we'll link them from here.

## Category

The first quesetion is: should your new test go into an existing category,
or should you create a new one?

You should use an existing category if:

* Your test is a new case within an obviously-existing category.
* Your test needs the same setup as an existing category, and is quick
  to run. Using the existing category avoids the need to fire up a
  Docker cluster just for your test.

You should create a new category if:

* Your test uses a customized setup: set of services, service
  configuration, set of external dependencies, instead.
* Your test will run for an extended time, and is best run in
  parallel with other tests in a build envrionment. Your test
  can share a cluster configuration with an existing test, but
  the new category allows the test to run by itself.

When your test *can* reuse an existing cluser definition, then the question is
about time. It takes significan time (minutes) to start a Docker cluster. We clearly
don't want to pay that cost for a test that runs for seconds, if we could just add the
test to another category. On the other hand, if you've gone crazy and added a huge
suite of tests that take 20 minutes to run, then there is a huge win to be had by
running the tests in parallel, even if they reuse an existing cluster configuration.
Use your best judgment.

The existing categories are listed in the
`org.apache.druid.testsEx.categories` package. The classes there represent
[JUnit categories](
https://junit.org/junit4/javadoc/4.12/org/junit/experimental/categories/Categories.html).
See [Test Category](tests.md#Test+Category) for details.

If you create a new category, but want to reuse the configuration of
an existing category, add the `@Cluster` annotation as described in the above
link. Note: be sure to link to a "base" category, not to a category that, itself,
has a `@Cluster` annotation.

If you use the `@Cluster` annotation, you must also add a mapping in the
`cluster.sh` file. See the top of the file for an example.

## Cluster Configuration

If you create a new category, you must define a new cluster. There are two parts:

* Docker compose
* Test configuration

### Docker Compose

Create a new folder: `custer/<category>`, then create a `docker-compose.yaml` file
in that folder. Define your cluster by borrowing heavily from existing files.
See [compose](compose.md) for details.

The only trick is if you want to include a new external dependency. The preferred
approach is to use an "official" image. If you must, you can create a custom image
in the `it-image` module. (We've not yet done that, so if you need a custom image,
let us know and we'll figure it out.)

### Test Configuration

Tests need a variety of configuration information. This is, at present, more
complex than we might like. You will at least need:

* Describe the Docker Compose cluster
* Provide test-specific properties

You may also need:

* Test-specific Guice modules
* Environment variable bindings to various properties
* MySQL statements to pre-populate the Druid metastore DB
* And so on.

### Test Config File

The cluster and properties are defined in a config file. Create a folder
`src/test/resources/cluster/<category>`. Then add a file called `docker.yaml`.
Crib the contents from the same category from which you borrowed the Docker
Compose definitions. Strip out properties and metastore statements you don't
need. Add those you do need. See [Test Configuration](test-config.md) for the
gory details of this file.

### Test Config Code

You may also want to customize Guice, environment variable bindings, etc.
This is done in the [test setup](tests.md#Initialization) method in your test.

## Start Simple

There are *many* things that can go wrong. It is best to start simple.

### Verify the Cluster

Start by ensuring your cluster works.

* Define your cluster as described above. Or, pick one to reuse.
* Verify the cluster using `it.sh up <category>`.
* Look at the Docker desktop UI to ensure the cluster says up. if not,
  track down what went wrong. Look at both the Docker (stdout) and
  Druid (`target/<category>/logs/<service>.log`) files.

### Starter Test

Next, create your test file as described above and in [Tests](tests.md).

* Create the test class.
* Add the required annotations.
* Create a simple test function that just prints "hello, world".
* Create your `docker.yaml` file as decribed above.
* Start your cluster, as described above, if not already started.
* Run the test from your IDE.
* Verify that the test "passes" (that is, it prints the message.)

If so, then this means that your test connected to your custer and
verified the health of all the services declared in your `docker.yaml` file.

If something goes wrong, you'll know it is in the basics. Check your
cluster status. Double-check the `docker.yaml` structure. Check ports.
Etc.

### Client

Every test is a Druid client. Determine which service API you need. Find an
existing test client. The `DruidClusterAdminClient` is the "modern" way to
interact with the cluster, but thus far has a limited set of methods. There
are older clients as well, but they tend to be quirky. Feel free to extend
`DruidClusterAdminClient`, or use the older one: whatever works.

Inject the client into your test. See existing tests for how this is done.

Revise your "starter" test to do some trivial operation using the client.
Retest to ensure things work.

### Test Cases

From here, you can start writing tests. Explore the existing mechanisms
(including those in the original `druid-integration-tests` module which may
not yet have been ported to the new framework yet.) For example, there are
ways to store specs as files and parameterize them in tests. There is a
syntax for running queries and specifying expected results.

You may have to create a new tool to help with your test. If you do,
try to use the new mechanisms, such as `ResolvedClusterConfig` rather than
using the old, cumbersome ones. Post questions in Slack so we can help.

### Extensions

Your test may need a "non-default" extension. See [Special Environment Variables](
compose.md#Special+Environment+Variables) for how to specify test-specific
extensions. (Hint: don't copy/paste the full load list!)

Extensions have two aspects in ITs. They act like extensions in the Druid servers
running in Docker. So, the extension must be avaialble in the Docker image. All
standard Druid extensions which are available in the Druid distribution, are also
available in the image. The may not be enabled, however. Hence the need to define
the custom load list.

Your test may use code from the extension. To the *tests*, however, the extension
is just another jar: it must be listed in the `pom.xml` file. There is no such
thing as a "Druid extensions" to the tests themselves.

If you test an extension that is *not* part of the Druid distributeion, then it
has to get into the image. Reach out on the slack mailing list so we can discuss
solutions (such as mounting a directory that contains the extension).

### Retries

The old IT framework was very liberal in its use of retries. Retires were
used to handle:

* the time lag in starting a cluster,
* the latency inherent in events propagaing through a distributed system
  (such as when segments get published),
* random network failures,
* flaky tests.

The new framework takes a stricter view. The framework itself will ensure
service are ready (using the Druid API for that purpose.) If a server reports
itself ready, but still fails on one of your API calls, then we've got a bug
to fix. Don't use retries to work around this issue because users won't know
to do this.

In the new framwork, tests should not be flaky. Flaky tests are a drag on
development; they waste time. If your test is flaky, please fix it. Don't count
on the amount of times things take: a busy build system will run much slower than
your dedicated laptop. And so on.

Ideally, Druid would provide a way to positively confirm that an action has
occurred. Perhaps this might be a test-only API. Otherwise, a retry is fine, but
should be coded into your test. (Or, better, implemented in a client.) Do this only
if we document that, for that API, users should poll. Otherwise, again, users of
the API under test won't know to retry, and so the test shouldn't do so either.

This leaves random failures. The right place to handle those is in the client,
since they are independent of the usage of the API.

The result of the above is that you should not need (or use) the `ITRetryUtil`
mechanism. No reason for your test to retry 240 times if something is really wrong
or your test is flaky.

This is an area under development. If you see a reason to retry, lets discuss it
and put it in the proper place.

### Travis

Run your tests in the IDE. Try them using `it.sh test <category>`. If that passes
add the test to Travis. The details on how to do so are still being worked out.
Likely, you will just copy/paste an existing test "stanza" to define your new
test. Your test will run in parallel with all other IT categories, which is why
we offered the advice above: the test has to have a good reason to fire up yet
another build task.

