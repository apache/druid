#!/bin/bash

#
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
#

set -e

# Check if an argument is provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <path-to-druid-home>"
    exit 1
fi

# Directory containing JAR files
JAR_DIR="$1"

# Declare an associative array to hold the canonical filenames. Works on base version >= 4
declare -A canonical

# Find all JAR files, sort them to ensure duplicates are processed together
while IFS= read -r jar; do
    # Extract the base name and sanitize it to create a valid array key
    key=$(basename "$jar")

    # Check if this is the first occurrence of this file
    if [ -z "${canonical[$key]}" ]; then
        # Mark this file as the canonical one for this basename
        canonical[$key]="$jar"
    else
        # This file is a duplicate, replace it with a symlink to the canonical file
        ln -sf "${canonical[$key]}" "$jar"
        echo "Replaced duplicate $jar with symlink to ${canonical[$key]}"
    fi
# Read in an order that retain core libs as original jars
done < <(find $JAR_DIR -wholename '*/lib/*.jar' | sort ; find $JAR_DIR -wholename '*/extensions/*.jar' | sort ; find $JAR_DIR -wholename '*/hadoop-dependencies/*.jar' | sort)
