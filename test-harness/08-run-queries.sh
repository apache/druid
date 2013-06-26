#!/bin/bash
(
mkdir -p logs
cd queries
bin/run_client.sh event_counts >> ../logs/queries.log 2>&1 &
)
echo "Queries running...results going to logs/queries.log"
if [ "$1" == "f" ]; then
tail -f logs/queries.log
else
echo "To automatically follow, pass 'f' as the first argument:  './05_run_queries.sh f'"
fi
