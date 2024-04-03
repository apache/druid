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

#
# If this scripted is run out of /usr/bin or some other system bin directory
# it should be linked to and not copied. Things like java jar files are found
# relative to the canonical path of this script.
#

# use POSIX interface, symlink is followed automatically
ZOOBIN="${BASH_SOURCE-$0}"
ZOOBIN="$(dirname "${ZOOBIN}")"
ZOOBINDIR="$(cd "${ZOOBIN}"; pwd)"

if [ -e "$ZOOBIN/../libexec/zkEnv.sh" ]; then
  . "$ZOOBINDIR"/../libexec/zkEnv.sh
else
  . "$ZOOBINDIR"/zkEnv.sh
fi

usage() {
  # the configfile will be properly formatted as long as the
  # configfile path is less then 40 chars, otw the line will look a
  # bit weird, but otherwise it's fine
  printf "usage: $0 <parameters>
  Optional parameters:
     -h                                                    Display this message
     --help                                                Display this message
     --configfile=%-40s ZooKeeper config file
     --myid=#                                              Set the myid to be used, if any (1-255)
     --force                                               Force creation of the data/txnlog dirs
" "$ZOOCFG"
  exit 1
}

if [ $? != 0 ] ; then
    usage
    exit 1
fi

initialize() {
    if [ ! -e "$ZOOCFG" ]; then
        echo "Unable to find config file at $ZOOCFG"
        exit 1
    fi

    ZOO_DATADIR="$(grep "^[[:space:]]*dataDir" "$ZOOCFG" | sed -e 's/.*=//')"
    ZOO_DATALOGDIR="$(grep "^[[:space:]]*dataLogDir" "$ZOOCFG" | sed -e 's/.*=//')"

    if [ -z "$ZOO_DATADIR" ]; then
        echo "Unable to determine dataDir from $ZOOCFG"
        exit 1
    fi

    if [ $FORCE ]; then
        echo "Force enabled, data/txnlog directories will be re-initialized"
    else
        # we create if version-2 exists (ie real data), not the
        # parent. See comments in following section for more insight
        if [ -d "$ZOO_DATADIR/version-2" ]; then
            echo "ZooKeeper data directory already exists at $ZOO_DATADIR (or use --force to force re-initialization)"
            exit 1
        fi

        if [ -n "$ZOO_DATALOGDIR" ] && [ -d "$ZOO_DATALOGDIR/version-2" ]; then
            echo "ZooKeeper txnlog directory already exists at $ZOO_DATALOGDIR (or use --force to force re-initialization)"
            exit 1
        fi
    fi

    # remove the child files that we're (not) interested in, not the
    # parent. this allows for parent to be installed separately, and
    # permissions to be set based on overarching requirements. by
    # default we'll use the permissions of the user running this
    # script for the files contained by the parent. note also by using
    # -p the parent(s) will be created if it doesn't already exist
    rm -rf "$ZOO_DATADIR/myid" 2>/dev/null >/dev/null
    rm -rf "$ZOO_DATADIR/version-2" 2>/dev/null >/dev/null
    mkdir -p "$ZOO_DATADIR/version-2"

    if [ -n "$ZOO_DATALOGDIR" ]; then
        rm -rf "$ZOO_DATALOGDIR/myid" 2>/dev/null >/dev/null
        rm -rf "$ZOO_DATALOGDIR/version-2" 2>/dev/null >/dev/null
        mkdir -p "$ZOO_DATALOGDIR/version-2"
    fi

    if [ $MYID ]; then
        echo "Using myid of $MYID"
        echo $MYID > "$ZOO_DATADIR/myid"
    else
        echo "No myid provided, be sure to specify it in $ZOO_DATADIR/myid if using non-standalone"
    fi

    touch "$ZOO_DATADIR/initialize"
}

while [ ! -z "$1" ]; do
  case "$1" in
    --configfile)
      ZOOCFG=$2; shift 2
      ;;
    --configfile=?*)
      ZOOCFG=${1#*=}; shift 1
      ;;
    --myid)
      MYID=$2; shift 2
      ;;
    --myid=?*)
      MYID=${1#*=}; shift 1
      ;;
    --force)
      FORCE=1; shift 1
      ;;
    -h)
      usage
      ;; 
    --help)
      usage
      ;; 
    *)
      echo "Unknown option: $1"
      usage
      exit 1 
      ;;
  esac
done
initialize
