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

./.github/scripts/setup_generate_license.sh
# This job is the single place in CI that validates everything a release build
# produces: javadoc and source jars, the binary and source distribution
# assemblies, and the license dependency reports. The apache-release profile is
# enabled here so that the Docker test job only needs to build the binary
# tarball. GPG signing and the OWASP dependency check are skipped as they are not
# meaningful in CI. RAT is not run here: apache-rat-plugin is not safe to run in
# a parallel (-T) build, so the single-threaded RAT pass in static-checks-maven.sh
# covers it instead.
./mvnw -B clean install -Papache-release --fail-at-end \
  -pl '!benchmarks, !distribution' -P skip-tests -Dweb.console.skip=false -T1C \
  -Dgpg.skip -Ddependency-check.skip
./mvnw -B install -Papache-release -Pdist -Pbundle-contrib-exts --fail-at-end \
  -pl 'distribution' -P skip-tests -Dweb.console.skip=false -T1C \
  -Dgpg.skip -Ddependency-check.skip
