#!/bin/sh
echo "Downloading Index"
wget -q https://raw.githubusercontent.com/apache/druid/master/examples/quickstart/tutorial/wikipedia-index.json

echo "Creating Task"
task_id=$(curl -s -X 'POST' -H 'Content-Type:application/json' -d @wikipedia-index.json http://druid-tiny-cluster-coordinators.druid.svc:8088/druid/indexer/v1/task | jq '.task' -r)
if [ $? == 0 ]
then 
    echo "Task created with ID $task_id"
fi

echo "Checking Status for task ..."
STATUS=$(curl -s http://druid-tiny-cluster-coordinators.druid.svc:8088/druid/indexer/v1/task/$task_id/status | jq '.status.status' -r); 
while [ $STATUS == "RUNNING" ]
do 
    sleep 8;
    echo "TASK is "$STATUS "..."
    STATUS=$(curl -s http://druid-tiny-cluster-coordinators.druid.svc:8088/druid/indexer/v1/task/$task_id/status | jq '.status.status' -r)
done

if [ $STATUS == "SUCCESS" ]
then 
    echo "TASK $task_id COMPLETED SUCCESSFULLY"
    sleep 60 # need time for the segments to become queryable
else 
    echo "TASK $task_id  FAILED !!!!"
fi

echo "Querying Data ... "
echo "Running query SELECT COUNT(*) AS \"Count\" FROM \"wikipedia\" WHERE isMinor = 'false'"

cat > query.json <<EOF
{"query":"SELECT COUNT(*) AS \"Count\" FROM \"wikipedia\" WHERE isMinor = 'false'","resultFormat":"objectlines"}
EOF

count=`curl -s -XPOST -H'Content-Type: application/json' http://druid-tiny-cluster-routers.druid.svc:8088/druid/v2/sql -d @query.json| jq '.Count'`
echo "count is $count"
if [ $count != "21936" ]
then
    echo "Query failed !!!"
    exit 1
else
    echo "Query Successful !!!"
fi
