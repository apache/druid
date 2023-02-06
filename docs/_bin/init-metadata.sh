#!/bin/bash

# Fail fast on any error
set -e

mysql()
{
  cd ${DRUID_ROOT}
  java -classpath "lib/*" -Dlog4j.configurationFile=conf/druid/cluster/_common/log4j2.xml -Ddruid.extensions.directory="extensions" -Ddruid.extensions.loadList="[\"mysql-metadata-storage\"]" -Ddruid.metadata.storage.type=mysql -Ddruid.node.type=metadata-init org.apache.druid.cli.Main tools metadata-init --connectURI=$connectURI --user $username --password $password --base druid
}

postgresql()
{
  cd ${DRUID_ROOT}
  java -classpath "lib/*" -Dlog4j.configurationFile=conf/druid/cluster/_common/log4j2.xml -Ddruid.extensions.directory="extensions" -Ddruid.extensions.loadList="[\"postgresql-metadata-storage\"]" -Ddruid.metadata.storage.type=postgresql -Ddruid.node.type=metadata-init org.apache.druid.cli.Main tools metadata-init --connectURI=$connectURI --user $username --password $password --base druid
}

help()
{
  echo "Tool for creating Druid's metadata tables"
  echo
  echo "Syntax: init-metadata [-d|c|u|p|v]"
  echo "options:"
  echo "d     Metadata storage type eg. mysql, postgresql."
  echo "c     Metadata storage connector connection URI"
  echo "u     Metadata storage connector username"
  echo "p     Metadata storage connector password"
  echo "h     help"
  echo "example useage init-metadata -d mysql -c localhost -u username -p password"
}

while getopts d:c:u:p:h: args
do
            case "${args}" in
                d) database=${OPTARG};;
                c) connectURI=${OPTARG};;
                u) username=${OPTARG};;
                p) password=${OPTARG};;
                h) help=${OPTARG};;
            esac
done

while getopts d:c:u:p:h: args
do
           case "${args}" in
                 d) database=${OPTARG};;
                 c) connectURI=${OPTARG};;
                 u) username=${OPTARG};;
                 p) password=${OPTARG};;
           esac
done

if [ "$database" = "mysql" ]
then
        mysql
elif [ "$database" = "postgresql" ]
then
        postgresql
else
        help
        exit 0
fi



