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
export DRUID_INTEGRATION_TEST_INDEXER="indexer"

. $MODULE_DIR/../Common/gen-docker.sh

# Override
function gen_header_comment {
	cat << EOF
# Cluster for the Google Cluster Storage (GCS) deep storage test.
#
# Required env vars:
#
# GOOGLE_BUCKET
# GOOGLE_PREFIX
# GOOGLE_APPLICATION_CREDENTIALS - must point to a file that holds the Google
#   credentials. Mounted into each Druid container.

EOF
}

# Override
function gen_common_env {
	gen_env \
      "druid_test_loadList=druid-google-extensions" \
      "druid_storage_type=google" \
      "druid_google_bucket=\${GOOGLE_BUCKET}" \
      "druid_google_prefix=\${GOOGLE_PREFIX}" \
      "GOOGLE_APPLICATION_CREDENTIALS=/resources/credentials.json" \
      $*
}

# Override
function gen_common_volumes {
    # Mount credentials file
	gen_volumes \
        "\${GOOGLE_APPLICATION_CREDENTIALS}:/resources/credentials.json" \
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
