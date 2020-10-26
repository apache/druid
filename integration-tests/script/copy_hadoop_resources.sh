#!/usr/bin/env bash
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

# wait for hadoop namenode to be up
echo "Waiting for hadoop namenode to be up"
docker exec -t druid-it-hadoop sh -c "./usr/local/hadoop/bin/hdfs dfs -mkdir -p /druid"
while [ $? -ne 0 ]
do
   sleep 2
   docker exec -t druid-it-hadoop sh -c "./usr/local/hadoop/bin/hdfs dfs -mkdir -p /druid"
done
echo "Finished waiting for Hadoop namenode"

# Setup hadoop druid dirs
echo "Setting up druid hadoop dirs"
docker exec -t druid-it-hadoop sh -c "./usr/local/hadoop/bin/hdfs dfs -mkdir -p /druid"
docker exec -t druid-it-hadoop sh -c "./usr/local/hadoop/bin/hdfs dfs -mkdir -p /druid/segments"
docker exec -t druid-it-hadoop sh -c "./usr/local/hadoop/bin/hdfs dfs -mkdir -p /quickstart"
docker exec -t druid-it-hadoop sh -c "./usr/local/hadoop/bin/hdfs dfs -chmod 777 /druid"
docker exec -t druid-it-hadoop sh -c "./usr/local/hadoop/bin/hdfs dfs -chmod 777 /druid/segments"
docker exec -t druid-it-hadoop sh -c "./usr/local/hadoop/bin/hdfs dfs -chmod 777 /quickstart"
docker exec -t druid-it-hadoop sh -c "./usr/local/hadoop/bin/hdfs dfs -chmod -R 777 /tmp"
docker exec -t druid-it-hadoop sh -c "./usr/local/hadoop/bin/hdfs dfs -chmod -R 777 /user"
# Copy data files to Hadoop container
docker exec -t druid-it-hadoop sh -c "./usr/local/hadoop/bin/hdfs dfs -put /shared/wikiticker-it/wikiticker-2015-09-12-sampled.json.gz /quickstart/wikiticker-2015-09-12-sampled.json.gz"
docker exec -t druid-it-hadoop sh -c "./usr/local/hadoop/bin/hdfs dfs -put /resources/data/batch_index /batch_index"
echo "Finished setting up druid hadoop dirs"

echo "Copying Hadoop XML files to shared"
docker exec -t druid-it-hadoop sh -c "cp /usr/local/hadoop/etc/hadoop/*.xml /shared/hadoop_xml"
echo "Copied Hadoop XML files to shared"