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

JAR_INPUT_FILE="jfr-profiler-1.0.0.jar"
JAR_OUTPUT_FILE="jfr-profiler.jar"
ENV_VAR="JFR_PROFILER_ARG_LINE"

if [ "$#" -ne 5 ]; then
    echo "usage: $0 <jdk_version> <run_id> <run_number> <run_attempt> <module>"
fi

if [[ "$1" -ge "17" ]];
then
  curl https://static.imply.io/cp/$JAR_INPUT_FILE -s -o $JAR_OUTPUT_FILE

  # Run 'java -version' and capture the output
  output=$(java -version 2>&1)

  # Extract the version number using grep and awk
  jvm_version=$(echo "$output" | grep "version" | awk -F '"' '{print $2}')


  echo $ENV_VAR=-javaagent:"$PWD"/$JAR_OUTPUT_FILE \
  -Djfr.profiler.http.username=druid-ci \
  -Djfr.profiler.http.password=w3Fb6PW8LIo849mViEkbgA== \
  -Djfr.profiler.tags.project=druid \
  -Djfr.profiler.tags.run_id=$2 \
  -Djfr.profiler.tags.run_number=$3 \
  -Djfr.profiler.tags.run_attempt=$4 \
  -Djfr.profiler.tags.module=$5 \
  -Djfr.profiler.tags.jvm_version=$jvm_version
else
  echo $ENV_VAR=\"\"
fi


