for node in druid-historical druid-coordinator druid-overlord druid-router druid-router-permissive-tls druid-router-no-client-auth-tls druid-broker druid-middlemanager druid-zookeeper-kafka druid-metadata-storage;
do 
docker stop $node
docker rm $node
done

docker network rm druid-it-net
