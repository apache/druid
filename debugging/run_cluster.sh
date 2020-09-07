#!/bin/bash
#!/bin/bash
set -e

if [ ! -f "~/.druid_gcp_credentials.json" ]; then
  echo "Please create ~/.druid_gcp_credentials.json"
else
  export GOOGLE_APPLICATION_CREDENTIALS=~/.druid_gcp_credentials.json
  echo "Starting master with zk..."
  ../../builds/druid/bin/start-cluster-master-with-zk-server &
  echo "Starting data server 1..."
  ../../builds/druid/bin/start-cluster-data-server &
  echo "Starting data server 2..."
  ../../builds/druid-2/bin/start-cluster-data-server &
  echo "Starting query server..."
  ../../builds/druid/bin/start-cluster-query-server &
fi

