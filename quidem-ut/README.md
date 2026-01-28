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

# Quidem UT

Enables to write sql level tests easily.
Can be used to write tests against existing test backends (ComponentSupplier) - by doing so the testcases can be moved closer to the exercised codes.

These tests might come from real usages of Druid by some external tool - by utilizing the capture mode of this module iq tests could be captured and validated later that they retain their results.
By adding tests for those here could act as an early warning that something might have changed.

## Usage

### Install java&maven (if needed)

If you don't have java&maven - one way to set that up is by using sdkman like this:
```bash
# install sdkman
curl -s "https://get.sdkman.io" | bash
# at the end of installation either open a new terminal; or follow the instructions at the end

# install java&maven
sdk install java 11.0.23-zulu
sdk install maven

# run mvn to see if it works
mvn --version

# download druid sources
git clone https://github.com/apache/druid
```


### Running these tests

* CI execution happens by a standard JUnit test `QTest` in this module
* the `dev/quidem` script can be used to run these tests (after the project is built)

### Launching a test generating broker

* make sure to build the project first; one way to do that is:
  ```bash
  mvn install -pl quidem-ut/ -am -DskipTests -Pskip-static-checks
  ```
* launch the broker instance with:
  ```bash
  mvn exec:exec -pl quidem-ut -Dquidem.record.autostart=true
  ```
  * the broker will be running at http://localhost:12345
  * the used test configuration backend can configured by supplying `quidem.uri`
    ```bash
    mvn exec:exec -pl quidem-ut -Dquidem.uri=druidtest:///?componentSupplier=ThetaSketchComponentSupplier
    ``` 
  * new record files can be started by calling http://localhost:12345/quidem/start
    * if `quidem.record.autostart` is omitted recording will not start
* after finished with the usage of the broker ; a new `record-123.iq` file will be in the project's worktree - it will contain all the executed statements
  * running `dev/quidem -Dquidem.overwrite` updates the resultsets of all `iq` files around there
  * rename the testfile to have a more descriptive name



