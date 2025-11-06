#!/bin/bash
#
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
#-------------------------------------------------------------------------

set -e

TASK_ID=$1

echo "Checking Status for task $TASK_ID..."
STATUS=$(curl -s http://druid-tiny-cluster-coordinators.druid.svc:8088/druid/indexer/v1/task/${TASK_ID}/status | jq '.status.status' -r); 
while [ $STATUS == "RUNNING" ]
do 
    sleep 8;
    echo "TASK is "$STATUS "..."
    STATUS=$(curl -s http://druid-tiny-cluster-coordinators.druid.svc:8088/druid/indexer/v1/task/${TASK_ID}/status | jq '.status.status' -r)
done

if [ $STATUS == "SUCCESS" ]
then 
    echo "TASK $TASK_ID COMPLETED SUCCESSFULLY"
    sleep 60 # need time for the segments to become queryable
else 
    echo "TASK $TASK_ID  FAILED !!!!"
    exit 1
fi

echo "Querying Data ... "
echo "Running query SELECT COUNT(*) AS \"Count\" FROM \"wikipedia-2\" WHERE isMinor = 'false'"

cat > query.json <<EOF
{"query":"SELECT COUNT(*) AS \"Count\" FROM \"wikipedia-2\" WHERE isMinor = 'false'","resultFormat":"objectlines"}
EOF

count=`curl -s -XPOST -H'Content-Type: application/json' http://druid-tiny-cluster-routers.druid.svc:8088/druid/v2/sql -d @query.json| jq '.Count'`
echo "count is $count"
if [ $count != "21936" ]
then
    echo "Query failed !!!"
    exit 1
else
    echo "Query Successful !!!"
fi
