#!/bin/bash -eu

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

# Tool for creating Druid's metadata tables
# Usage: init-metadata [-d|c|u|p|v]


#params : type loadList 
setup()
{
  cd ${DRUID_ROOT}
  java -classpath "lib/*" -Dlog4j.configurationFile=conf/druid/cluster/_common/log4j2.xml -Ddruid.extensions.directory="extensions" -Ddruid.extensions.loadList="[\"$2\"]" -Ddruid.metadata.storage.type=$1 -Ddruid.node.type=metadata-init org.apache.druid.cli.Main tools metadata-init --connectURI=$connectURI --user $username --password $password --base druid
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
  echo "h     Help"
  echo "example usage init-metadata -d mysql -c localhost -u username -p password"
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
        setup mysql mysql-metadata-storage
elif [ "$database" = "postgresql" ]
then
        setup postgresql postgresql-metadata-storage
else
        help
        exit 0
fi
