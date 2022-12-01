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

./.github/scripts/setup_generate_license.sh
${MVN} apache-rat:check -Prat --fail-at-end \
-Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn \
-Drat.consoleOutput=true ${HADOOP_PROFILE}
# Generate dependency reports and checks they are valid.
mkdir -p target
distribution/bin/generate-license-dependency-reports.py . target --clean-maven-artifact-transfer --parallel 2
distribution/bin/check-licenses.py licenses.yaml target/license-reports
