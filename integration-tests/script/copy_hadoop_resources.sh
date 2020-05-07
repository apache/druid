#!/usr/bin/env bash

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
