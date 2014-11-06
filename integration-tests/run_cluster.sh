
# Add druid jar
cp ../services/target/druid-services-*-selfcontained.jar docker/
cp ../s3-extensions/target/druid-s3-extensions-*.jar docker/ 
cp ../histogram/target/druid-histogram-*.jar docker/ 
# Build Druid Cluster Image
docker build -t druid/cluster docker/

# remove copied jar 
rm docker/*.jar

DOCKERDIR=`pwd`/docker
SHARED_DIR=/tmp/shared
SUPERVISORDIR=/usr/lib/druid/conf
RESOURCEDIR=`pwd`/src/test/resources

# Start zookeeper
docker run -d --name druid-zookeeper -p 2181:2181 -v $SHARED_DIR:/shared -v $DOCKERDIR/zookeeper.conf:$SUPERVISORDIR/zookeeper.conf druid/cluster

# Start MYSQL 
docker run -d --name druid-mysql -v $SHARED_DIR:/shared -v $DOCKERDIR/mysql.conf:$SUPERVISORDIR/mysql.conf druid/cluster

# Start Overlord
docker run -d --name druid-overlord -p 3001:8080 -v $SHARED_DIR:/shared -v $DOCKERDIR/overlord.conf:$SUPERVISORDIR/overlord.conf --link druid-mysql:druid-mysql --link druid-zookeeper:druid-zookeeper druid/cluster

# Start coordinator 
docker run -d --name druid-coordinator -p 3000:8080 -v $SHARED_DIR:/shared -v $DOCKERDIR/coordinator.conf:$SUPERVISORDIR/coordinator.conf --link druid-overlord:druid-overlord --link druid-mysql:druid-mysql --link druid-zookeeper:druid-zookeeper druid/cluster

# Start Historical 
docker run -d --name druid-historical -v $SHARED_DIR:/shared -v $DOCKERDIR/historical.conf:$SUPERVISORDIR/historical.conf --link druid-zookeeper:druid-zookeeper druid/cluster

#Start middlemanger
docker run -d --name druid-middlemanager -p 8100:8100 -p 8101:8101 -p 8102:8102 -p 8103:8103 -p 8104:8104 -p 8105:8105 -v $RESOURCEDIR:/resources -v $SHARED_DIR:/shared -v $DOCKERDIR/middlemanager.conf:$SUPERVISORDIR/middlemanager.conf --link druid-zookeeper:druid-zookeeper --link druid-overlord:druid-overlord druid/cluster

# Start Broker 
docker run -d --name druid-broker -v $SHARED_DIR:/shared -v $DOCKERDIR/broker.conf:$SUPERVISORDIR/broker.conf --link druid-zookeeper:druid-zookeeper --link druid-middlemanager:druid-middlemanager --link druid-historical:druid-historical druid/cluster

# Start Router 
docker run -d --name druid-router -p 3002:8080 -v $SHARED_DIR:/shared -v $DOCKERDIR/router.conf:$SUPERVISORDIR/router.conf --link druid-zookeeper:druid-zookeeper --link druid-coordinator:druid-coordinator --link druid-broker:druid-broker druid/cluster

