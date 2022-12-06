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

#!/bin/bash

set -e

unset _JAVA_OPTIONS

# Set MAVEN_OPTS for Surefire launcher. Skip remoteresources to avoid intermittent connection timeouts when
# resolving the SIGAR dependency.
MAVEN_OPTS='-Xmx2048m' ${MVN} test -pl ${MAVEN_PROJECTS} \
${MAVEN_SKIP} -Dremoteresources.skip=true -Ddruid.generic.useDefaultValueForNull=${DRUID_USE_DEFAULT_VALUE_FOR_NULL}
sh -c "dmesg | egrep -i '(oom|out of memory|kill process|killed).*' -C 1 || exit 0"
free -m
${MVN} -pl ${MAVEN_PROJECTS} jacoco:report

# Add merge target branch to determine diff (see https://github.com/travis-ci/travis-ci/issues/6069).
# This is not needed for build triggered by tags, since there will be no code diff.
echo "GIT_BRANCH=${GIT_BRANCH}"  # for debugging
if [[ ! ${GIT_BRANCH} =~ ^refs\/tags\/v.* ]]
then
  git remote set-branches --add origin ${GIT_BRANCH} && git fetch
fi

# Determine the modified files that match the maven projects being tested. We use maven project lists that
# either exclude (starts with "!") or include (does not start with "!"), so both cases need to be handled.
# If the build is triggered by a tag, an error will be printed, but `all_files` will be correctly set to empty
# so that the coverage check is skipped.
all_files="$(git diff --name-only origin/${GIT_BRANCH}...HEAD | grep "\.java$" || [[ $? == 1 ]])"

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

for f in ${project_files}
do
  echo $f # for debugging
done

# Check diff code coverage for the maven projects being tested (retry install in case of network error).
# Currently, the function coverage check is not reliable, so it is disabled.
if [ -n "${project_files}" ]
then
  { for i in 1 2 3; do npm install @connectis/diff-test-coverage@1.5.3 && break || sleep 15; done }
  git diff origin/${TRAVIS_BRANCH}...HEAD -- ${project_files} |
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
  { printf "\n\n****FAILED****\nDiff code coverage check failed. To view coverage report, run 'mvn clean test jacoco:report' and open 'target/site/jacoco/index.html'\nFor more details on how to run code coverage locally, follow instructions here - https://github.com/apache/druid/blob/master/dev/code-review/code-coverage.md#running-code-coverage-locally\n\n" && false; }
fi

{ for i in 1 2 3; do curl -o codecov.sh -s https://codecov.io/bash && break || sleep 15; done }
{ for i in 1 2 3; do bash codecov.sh -X gcov && break || sleep 15; done }
