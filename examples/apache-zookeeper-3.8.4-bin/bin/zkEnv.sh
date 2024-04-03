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

# This script should be sourced into other zookeeper
# scripts to setup the env variables

# We use ZOOCFGDIR if defined,
# otherwise we use /etc/zookeeper
# or the conf directory that is
# a sibling of this script's directory.
# Or you can specify the ZOOCFGDIR using the
# '--config' option in the command line.

ZOOBINDIR="${ZOOBINDIR:-/usr/bin}"
ZOOKEEPER_PREFIX="${ZOOBINDIR}/.."

#check to see if the conf dir is given as an optional argument
if [ $# -gt 1 ]
then
    if [ "--config" = "$1" ]
	  then
	      shift
	      confdir=$1
	      shift
	      ZOOCFGDIR=$confdir
    fi
fi

if [ "x$ZOOCFGDIR" = "x" ]
then
  if [ -e "${ZOOKEEPER_PREFIX}/conf" ]; then
    ZOOCFGDIR="$ZOOBINDIR/../conf"
  else
    ZOOCFGDIR="$ZOOBINDIR/../etc/zookeeper"
  fi
fi

if [ -f "${ZOOCFGDIR}/zookeeper-env.sh" ]; then
  . "${ZOOCFGDIR}/zookeeper-env.sh"
fi

if [ "x$ZOOCFG" = "x" ]
then
    ZOOCFG="zoo.cfg"
fi

ZOOCFG="$ZOOCFGDIR/$ZOOCFG"

if [ -f "$ZOOCFGDIR/java.env" ]
then
    . "$ZOOCFGDIR/java.env"
fi

if [ "x${ZOO_LOG_DIR}" = "x" ]
then
    ZOO_LOG_DIR="$ZOOKEEPER_PREFIX/logs"
fi

if [[ -n "$JAVA_HOME" ]] && [[ -x "$JAVA_HOME/bin/java" ]];  then
    JAVA="$JAVA_HOME/bin/java"
elif type -p java; then
    JAVA=java
else
    echo "Error: JAVA_HOME is not set and java could not be found in PATH." 1>&2
    exit 1
fi

#add the zoocfg dir to classpath
CLASSPATH="$ZOOCFGDIR:$CLASSPATH"

for i in "$ZOOBINDIR"/../zookeeper-server/src/main/resources/lib/*.jar
do
    CLASSPATH="$i:$CLASSPATH"
done

#make it work in the binary package
#(use array for LIBPATH to account for spaces within wildcard expansion)
if ls "${ZOOKEEPER_PREFIX}"/share/zookeeper/zookeeper-*.jar > /dev/null 2>&1; then 
  LIBPATH=("${ZOOKEEPER_PREFIX}"/share/zookeeper/*.jar)
else
  #release tarball format
  for i in "$ZOOBINDIR"/../zookeeper-*.jar
  do
    CLASSPATH="$i:$CLASSPATH"
  done
  LIBPATH=("${ZOOBINDIR}"/../lib/*.jar)
fi

for i in "${LIBPATH[@]}"
do
    CLASSPATH="$i:$CLASSPATH"
done

#make it work for developers
for d in "$ZOOBINDIR"/../build/lib/*.jar
do
   CLASSPATH="$d:$CLASSPATH"
done

for d in "$ZOOBINDIR"/../zookeeper-server/target/lib/*.jar
do
   CLASSPATH="$d:$CLASSPATH"
done

#make it work for developers
CLASSPATH="$ZOOBINDIR/../build/classes:$CLASSPATH"

#make it work for developers
CLASSPATH="$ZOOBINDIR/../zookeeper-server/target/classes:$CLASSPATH"

case "`uname`" in
    CYGWIN*|MINGW*) cygwin=true ;;
    *) cygwin=false ;;
esac

if $cygwin
then
    CLASSPATH=`cygpath -wp "$CLASSPATH"`
fi

#echo "CLASSPATH=$CLASSPATH"

# default heap for zookeeper server
ZK_SERVER_HEAP="${ZK_SERVER_HEAP:-1000}"
export SERVER_JVMFLAGS="-Xmx${ZK_SERVER_HEAP}m $SERVER_JVMFLAGS"

# default heap for zookeeper client
ZK_CLIENT_HEAP="${ZK_CLIENT_HEAP:-256}"
export CLIENT_JVMFLAGS="-Xmx${ZK_CLIENT_HEAP}m $CLIENT_JVMFLAGS"
