#! /bin/bash

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
#--------------------------------------------------------------------

set -e

TEMPLATE=$0
export MODULE_DIR=$(cd $(dirname $0) && pwd)
CATEGORY=$(basename $MODULE_DIR)

# This test seems to prefer the indexer.
export USE_INDEXER="indexer"

. $MODULE_DIR/../Common/gen-docker.sh

# Override
function gen_header_comment {
	cat << EOF
# Cluster for the Azure deep storage test.
#
# Required env vars:
#
# AZURE_ACCOUNT
# AZURE_KEY
# AZURE_CONTAINER

EOF
}

# Override
function gen_common_env {
	gen_env \
        "druid_test_loadList=druid-azure-extensions" \
        "druid_storage_type=azure" \
        "druid_azure_account=\${AZURE_ACCOUNT}" \
        "druid_azure_key=\${AZURE_KEY}" \
        "druid_azure_container=\${AZURE_CONTAINER}" \
        $*
}

# This test mounts its data from a different location than other tests.
# Override
function gen_indexer_volumes {
    # Test data
	gen_common_volumes \
        "../data:/resources"
}

gen_compose_file $CATEGORY
