#!/usr/bin/env sh
echo "Current directory is $(pwd)"
echo "\n=== SUREFIRE REPORTS ===\n"

for F in target/surefire-reports/*.txt
do
    echo $F
    cat $F
    echo
done
