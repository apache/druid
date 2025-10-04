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
echo "---------------"
echo "Checking the status of running job $JOB_ID ..."
for (( i=0; i<=9; i++ ))
do  
    sleep 60
    STAT=`kubectl get job  $JOB_ID --template={{.status.succeeded}}`
    if  [ "$STAT" == "<no value>" ]
    then
        echo "Seems to be in progress ..."
    elif [ $STAT == 1 ]
    then
        echo "Job completed Successfully !!!"
        break
    fi
    if [ $i == 9 ]
    then 
        echo "================"
        echo "Task Timeout ..."
        echo "FAILED EXITING !!!"
        echo "================"
        exit 1
    fi
done
