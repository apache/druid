#!/bin/bash -e
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

mkdir -p target

docs/_bin/generate-license-dependency-reports.py . target --clean-maven-artifact-transfer && docs/_bin/generate-license.py licenses/APACHE2 licenses.yaml LICENSES.BINARY --dependency-reports target/license-reports

MAVEN_OPTS='-Xmx3000m' mvn -DskipTests -Dforbiddenapis.skip=true -Dcheckstyle.skip=true -Dpmd.skip=true -Dmaven.javadoc.skip=true -pl '!benchmarks' -B --fail-at-end clean install -Pdist -Pbundle-contrib-exts
