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

echo 'Running Maven install...'
${MVN} clean install -q -ff -pl '!distribution' ${MAVEN_SKIP} ${MAVEN_SKIP_TESTS} -T1C
${MVN} install -q -ff -pl 'distribution' ${MAVEN_SKIP} ${MAVEN_SKIP_TESTS}

${MVN} checkstyle:checkstyle --fail-at-end

./.github/scripts/license_checks_script.sh

./.github/scripts/analyze_dependencies_script.sh

${MVN} animal-sniffer:check --fail-at-end

${MVN} enforcer:enforce --fail-at-end

${MVN} forbiddenapis:check forbiddenapis:testCheck --fail-at-end

# TODO: consider adding pmd:cpd-check
${MVN} pmd:check --fail-at-end

${MVN} spotbugs:check --fail-at-end -pl '!benchmarks'
