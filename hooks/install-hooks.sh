#!/bin/bash -eu

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

function cp_if_not_exist(){
  if [ -e "$2" ]
    then
      echo "$2 already exists!"
      exit 1
  else
    cp -r "$1" "$2"
  fi
}

if [ $# != 1 ]
  then
    echo 'usage: program {$DRUID_ROOT}'
    exit 1
fi

DRUID_ROOT=$1

cp_if_not_exist ${DRUID_ROOT}/hooks/run-all-in-dir.py ${DRUID_ROOT}/.git/hooks/run-all-in-dir.py
cp_if_not_exist ${DRUID_ROOT}/hooks/pre-commit ${DRUID_ROOT}/.git/hooks/pre-commit
cp_if_not_exist ${DRUID_ROOT}/hooks/pre-push ${DRUID_ROOT}/.git/hooks/pre-push
cp_if_not_exist ${DRUID_ROOT}/hooks/pre-commits ${DRUID_ROOT}/.git/hooks/pre-commits
cp_if_not_exist ${DRUID_ROOT}/hooks/pre-pushes ${DRUID_ROOT}/.git/hooks/pre-pushes
