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

# Travis Integration

Apache Druid uses Travis to manage builds, including running the integration
tests. You can find the Travis build file at `$DRUID_DEV/.travis.yml`, where
`DRUID_DEV` is the root of your Druid development directory. Information
about Travis can be found at:

* [Documentation](https://docs.travis-ci.com/)
* [Job lifecycle](https://docs.travis-ci.com/user/job-lifecycle/)
* [Environment variables](https://docs.travis-ci.com/user/environment-variables/)
* [Travis file reference](https://config.travis-ci.com/)
* [Travis YAML](https://docs.travis-ci.com/user/build-config-yaml)

## Running ITs In Travis

Travis integration is still experimental. The latest iteration is:

```yaml
    - name: "experimental docker tests"
      stage: Tests - phase 1
      script: ${MVN} install -P test-image,docker-tests -rf :it-tools ${MAVEN_SKIP} -DskipUTs=true
      after_failure:
        - docker-tests/check-results.sh
```

The above is a Travis job definition. The job "inherits" an `install` task defined
earlier in the file. That install task builds all of Druid and creates the distribution
tarball. Since the tests are isolated in specialized Maven profiles, the `install`
task does not build any of the IT-related artifacts.

We've placed the test run in "Phase 1" for debugging convenience. Later, the tests
will run in "Phase 2" along with the other ITs. Once conversion is complete, the
"previous generation" IT tests will be replaced by the newer revisions.

The `script` runs the ITs. The components of the command line are:

* `install` - Run Maven though the install [lifecycle phase](
  https://maven.apache.org/guides/introduction/introduction-to-the-lifecycle.html)
  for each module. This allows us to build and install the "testing tools"
  (see the [Maven notes](maven.md)). The test image is also built during the
  `install` phase. The tests themselves only need the `verify` phase, which occurs
  before `install`. `install` does nothing for ITs.
* `-P test-image,docker-tests` - activates the image to build the image
  (`test-image`) and then runs the ITs (`docker-tests`).
* `-rf :it-tools` - The `it-tools` module is the first of the IT modules: it contains
  the "testing tools" added into the image. Using `-rf` skips all the other projects
  which we already built in the Travis `install` step. Doing so saves the time
  otherwise required for Maven to figure out it has nothing to do for those modules.
* `${MAVEN_SKIP}` - Omits the static checks: they are not needed for ITs.
* `-DskipUTs=true` - The ITs use the [Maven Failsafe plugin](
  https://maven.apache.org/surefire/maven-failsafe-plugin/index.html)
  which shares code with the [Maven Surefire plugin](
  https://maven.apache.org/surefire/maven-surefire-plugin/index.html). We don't want
  to run unit tests. If we did the usual `-DskipTests`, then we'd also disable the
  ITs. The `-DskipUTs=true` uses a bit of [Maven trickery](
  https://stackoverflow.com/questions/6612344/prevent-unit-tests-but-allow-integration-tests-in-maven)
  to skip only the Surefire, but not Faisafe tests.

## Travis Diagnostics

A common failure when running ITs is that they uncover a bug in a Druid service;
typically in the code you added that you want to test. Or, if you are changing the
Docker or Docker Compose infratructure, then the tests will often fail because the
Druid services are mis-configured. (Bad configuration tends to result in services
that don't start, or start and immediately exit.)

The standard way to diagnose such failures is to look at the Druid logs. However,
Travis provides no support for attaching files to a build. The best alternative
seems to be to upload the files somewhere else. As a compromise, the Travis build
will append to the build log a subset of the Druid logs.

Travis has a limit of 4MB per build log, so we can't append the entire log for
every Druid service for every IT. We have to be selective. In most cases, we only
care about the logs for ITs that fail.

Now, it turns out to be *very hard* indeed to capture failues! Eventually, we want
Maven to run many ITs for each test run: we need to know which failed. Each IT
creates its own "shared" directory, so to find the logs, we need to know which IT
failed. Travis does not have this information: Travis only knows that Maven itself
exited with a non-zero status. Maven doesn't know: it only knows that Failsafe
failed the build. Failsafe is designed to run all ITs, then check the results in
the `verify` state, so Maven doesn't even know about the failures.

### Failsafe Error Reports

To work around all this, we mimic Failsafe: we look at the Failsafe error report
in `$DRUID_DEV/docker-tests/<module>/target/failsafe-reports/failsafe-summary.xml`
which looks like this:

```xml
<failsafe-summary ... result="null" timeout="false">
    <completed>3</completed>
    <errors>1</errors>
    <failures>0</failures>
    <skipped>0</skipped>
    <failureMessage xsi:nil="true" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"/>
</failsafe-summary>
```

The above shows one error and no failures. A successful run will show 0 for the
`errors` tag. This example tells us "something didn't work". The corresponding
Druid service logs are candidates for review.

### Druid Service Failures

The Druid logs are in `$DRUID_DEV/docker-tests/<module>/target/shared/logs`.
We could append all of them, but recall the 4MB limit. We generally are
interested only in those services that failed. So, we look at the logs and
see that a successful run is indicated by a normal Lifecycle shutdown:

```text
2022-04-16T20:54:37,997 INFO [Thread-56] org.apache.druid.java.util.common.lifecycle.Lifecycle - Stopping lifecycle [module] stage [INIT]
```

The key bit of text is:

```text
Stopping lifecycle [module] stage [INIT]
```

This says that 1) we're shutting down the lifecycle (which means no exception was thrown),
and 2) that we got all the way to the end (`[INIT]`). Since Druid emits no final
"exited normally" message, we take the above as the next-best thing.

So, we only care about logs that *don't* have the above line. For those, we want to
append the log to the build output. Or, because of the size limit, we append the
last 100 lines.

All of this is encapsulated in the `docker-tests/check-results.sh` script which
is run if the build fails (in the `after_failure`) tag.

### Druid Log Output

For a failed test, the build log will end with something like this:

```text
======= it-high-availability Failed ==========
broker.log logtail ========================
022-04-16T03:53:10,492 INFO [CoordinatorRuleManager-Exec--0] org.apache.druid.discovery.DruidLeaderClient - Request[http://coordinator-one:8081/druid/coordinator/v1/rules] received redirect response to location [http://coordinator-two:8081/druid/coordinator/v1/rules].
...
```

To keep below the limit, on the first failed test is reported.

The above won't catch all cases: maybe the service exited normally, but might still have
log lines of interest. Since all tests run, those lines could be anywhere in the file
and the scripts can't know which might be of interest. To handle that, we either
have to upload all logs somewhere, or you can use the convenience of the new
IT framework to rerun the tests on your development machine.
