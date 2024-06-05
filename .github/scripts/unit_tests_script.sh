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

unset _JAVA_OPTIONS

# Set MAVEN_OPTS for Surefire launcher.
MAVEN_OPTS='-Xmx2500m' ${MVN} test -pl ${MAVEN_PROJECTS} \
${MAVEN_SKIP} -Ddruid.generic.useDefaultValueForNull=${DRUID_USE_DEFAULT_VALUE_FOR_NULL} \
-DjfrProfilerArgLine="${JFR_PROFILER_ARG_LINE}"
sh -c "dmesg | egrep -i '(oom|out of memory|kill process|killed).*' -C 1 || exit 0"
free -m
${MVN} -pl ${MAVEN_PROJECTS} jacoco:report || { echo "coverage_failure=false" >> "$GITHUB_ENV" && false; }

# Determine the modified files that match the maven projects being tested. We use maven project lists that
# either exclude (starts with "!") or include (does not start with "!"), so both cases need to be handled.
# If the build is triggered by a tag, an error will be printed, but `all_files` will be correctly set to empty
# so that the coverage check is skipped.
all_files="$(git diff --name-only origin/${GITHUB_BASE_REF}...HEAD | grep "\.java$" || [[ $? == 1 ]])"

echo "Changed files:"
for f in ${all_files}
do
  echo $f # for debugging
done

if [[ "${MAVEN_PROJECTS}" = \!* ]]
then
  regex="${MAVEN_PROJECTS:1}"
  regex="^${regex//,\!/\\|^}"
  project_files="$(echo "${all_files}" | grep -v "${regex}" || [[ $? == 1 ]])"
else
  regex="^${MAVEN_PROJECTS//,/\\|^}"
  project_files="$(echo "${all_files}" | grep "${regex}" || [[ $? == 1 ]])"
fi

echo "Changed files in current project:"
for f in ${project_files}
do
  echo $f # for debugging
done

# Check diff code coverage for the maven projects being tested (retry install in case of network error).
# Currently, the function coverage check is not reliable, so it is disabled.
if [ -n "${project_files}" ]
then
  git diff origin/${GITHUB_BASE_REF}...HEAD -- ${project_files} |
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
  { printf "\n\n****FAILED****\nDiff code coverage check failed. To view coverage report, run 'mvn clean test jacoco:report' and open 'target/site/jacoco/index.html'\nFor more details on how to run code coverage locally, follow instructions here - https://github.com/apache/druid/blob/master/dev/code-review/code-coverage.md#running-code-coverage-locally\n\n" && echo "coverage_failure=true" >> "$GITHUB_ENV" && false; }
fi

# Commented out as codecov visualizations are currently not being used in the Druid community
# and cause PR failures due to rate limiting (see https://github.com/apache/druid/pull/16347)
# { for i in 1 2 3; do curl -o codecov.sh -s https://codecov.io/bash && break || sleep 15; done }
# { for i in 1 2 3; do bash codecov.sh -X gcov && break || sleep 15; done }
