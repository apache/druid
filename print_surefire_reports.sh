#!/usr/bin/env sh
echo "Current directory is $(pwd)"
echo "\n=== SUREFIRE REPORTS ===\n"

for report in `find . -path '*/surefire-reports/*AsyncQueryForwardingServletTest*.txt' -type f`
do
    echo "\n==== PRINTING REPORT ($report) ======"
    cat $report
    echo
done
