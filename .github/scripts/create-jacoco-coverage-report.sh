#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e
set -x

echo "GITHUB_BASE_REF: ${GITHUB_BASE_REF}"

echo "Setting up git remote"
git remote set-branches --add origin ${GITHUB_BASE_REF}
git fetch

# Compile the project. jacoco:report needs class files along with jacoco.exec files to generate the report.
mvn -B install -DskipTests -P skip-static-checks -Dweb.console.skip=true -Dmaven.javadoc.skip=true

# If there are multiple jacoco.exec files present in any module, merge them into a single jacoco.exec file for that module.
mvn jacoco:merge

mvn jacoco:report

changed_files="$(git diff --name-only origin/${GITHUB_BASE_REF}...HEAD '**/*.java' || [[ $? == 1 ]])"

echo "Changed files:"
for f in ${changed_files}
do
  echo $f
done

mvn com.github.eirslett:frontend-maven-plugin:install-node-and-npm@install-node-and-npm -pl web-console/
PATH+=:web-console/target/node/
npm install @connectis/diff-test-coverage@1.5.3

export FORCE_COLOR=2

if [ -n "${changed_files}" ]
then
  find . -name jacoco.xml | grep . >/dev/null || { echo 'No jacoco.xml found; something must be broken!'; exit 1; }
  git diff origin/${GITHUB_BASE_REF}...HEAD -- ${changed_files} |
  node_modules/.bin/diff-test-coverage \
  --coverage "**/target/site/jacoco/jacoco.xml" \
  --type jacoco \
  --line-coverage 50 \
  --branch-coverage 50 \
  --function-coverage 0 \
  --log-template "coverage-lines-complete" \
  --log-template "coverage-files-complete" \
  --log-template "totals-complete" \
  --log-template "errors" \
  -- ||
  { printf "\n\n****FAILED****\nDiff code coverage check failed. To view coverage report, run 'mvn clean test jacoco:report' and open 'target/site/jacoco/index.html'\nFor more details on how to run code coverage locally, follow instructions here - https://github.com/apache/druid/blob/master/dev/code-review/code-coverage.md#running-code-coverage-locally\n\n" && exit 1; }
fi


echo " * test balancing details"
.github/scripts/test_balancing_calc
