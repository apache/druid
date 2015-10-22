for node in druid-historical druid-coordinator druid-overlord druid-router druid-broker druid-middlemanager druid-zookeeper-kafka druid-metadata-storage;
do 
docker stop $node
docker rm $node
done
