#!/bin/bash

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
