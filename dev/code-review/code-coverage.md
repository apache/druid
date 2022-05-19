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

# Druid's Code Coverage Enforcement

Druid code repository has an automated way of checking if new code has enough code coverage. 
Druid CI checks are configured to enforce code coverage using JaCoCo. The CI checks will prevent a PR from being merged 
if test coverage of new added code is below the set threshold. The CI checks filters test coverage based on a diff from
your PR and make sure that the thresholds are met. Druid currently enforce branch and line code coverage.

However, do note that our current code coverage checks are merely smoke tests. They only verify that a line or branch 
of code has been called during the test, but not that the functionality has been tested sufficiently. 
Reviewers should still verify that all the different branches are sufficiently tested by reviewing the tests. 

## Running code coverage locally
Code coverage should be run locally to make sure the PR will pass Druid CI checks. 
1. Code coverage on the codebase can be generated directly in [Intellij](../intellij-setup.md#Set-Code-Coverage-Runner). 
2. Code coverage on just the diff of your PR can be generated in your terminal. First, you will have to install
diff-test-coverage by running `npm install @connectis/diff-test-coverage`. Next, run the unit tests
for the module you are working on `mvn -pl <MODULE_TO_CHECK> test jacoco:report` 
(this will create a HTML report in target/site/jacoco/index.html). Finally, run 
`git diff master...HEAD | diff-test-coverage --coverage "**/target/site/jacoco/jacoco.xml" --type jacoco --log-template "full" --`  
