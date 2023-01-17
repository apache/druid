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

# MSQ uses the indexer by default. This should be fixed: it should be MM
# by default, and indexer only if the following variable is set.
USE_INDEXER=indexer

. $MODULE_DIR/../Common/gen-docker.sh


# Override
function gen_indexer_env {
	gen_common_env \
        "druid_msq_intermediate_storage_enable=true" \
        "druid_msq_intermediate_storage_type=local" \
        "druid_msq_intermediate_storage_basePath=/shared/durablestorage/" \
EOF
}

gen_compose_file $CATEGORY
