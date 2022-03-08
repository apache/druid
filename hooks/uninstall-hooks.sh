#!/bin/bash -eu

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

if [ $# != 1 ]
  then
    echo 'usage: program {$DRUID_ROOT}'
    exit 1
fi

DRUID_ROOT=$1

# This script does not remove .git/hooks/pre-commit, .git/hooks/pre-push, or any other git hook scripts
# because those files may have user-custom hooks.
# Instead, we remove only the files and directories that we are sure it is safe to remove.
rm -f ${DRUID_ROOT}/.git/hooks/run-all-in-dir.py
rm -rf ${DRUID_ROOT}/.git/hooks/pre-commits
rm -rf ${DRUID_ROOT}/.git/hooks/pre-pushes

echo "This script does not remove or modify the git hook script files in .git/hooks, such as 'pre-commit' or 'pre-push'. Those scripts should be removed or modified manually."
