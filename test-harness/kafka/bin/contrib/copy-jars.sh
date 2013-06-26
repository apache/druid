#!/bin/bash

if [ $# -lt 1 ];
then
  echo "USAGE: $0 dir"
  exit 1
fi

base_dir=$(dirname $0)/../..

hadoop=${HADOOP_HOME}/bin/hadoop

echo "$hadoop fs -rmr $1"
$hadoop fs -rmr $1

echo "$hadoop fs -mkdir $1"
$hadoop fs -mkdir $1

# include kafka jars
for file in $base_dir/dist/*.jar;
do
   echo "$hadoop fs -put $file $1/"
   $hadoop fs -put $file $1/ 
done

for file in $base_dir/lib/*.jar;
do
   echo "$hadoop fs -put $file $1/"
   $hadoop fs -put $file $1/ 
done


local_dir=$(dirname $0)

# include hadoop-consumer jars
for file in $local_dir/lib/*.jar;
do
   echo "$hadoop fs -put $file $1/"
   $hadoop fs -put $file $1/ 
done

