# Setup Oracle Java
sudo apt-get update
sudo add-apt-repository -y ppa:webupd8team/java
sudo apt-get update

# Setup yes answer to license question
echo debconf shared/accepted-oracle-license-v1-1 select true | sudo debconf-set-selections
echo debconf shared/accepted-oracle-license-v1-1 seen true | sudo debconf-set-selections
sudo apt-get -y -q install oracle-java7-installer

# Automated Kafka setup
curl http://static.druid.io/artifacts/kafka-0.7.2-incubating-bin.tar.gz -o /tmp/kafka-0.7.2-incubating-bin.tar.gz
tar -xvzf /tmp/kafka-0.7.2-incubating-bin.tar.gz
cd kafka-0.7.2-incubating-bin
cat config/zookeeper.properties
nohup bin/zookeeper-server-start.sh config/zookeeper.properties 2>&1 > /dev/null &
# in a new console
nohup bin/kafka-server-start.sh config/server.properties 2>&1 > /dev/null &

# Install dependencies - mysql must be built from source, as the 12.04 apt-get hangs
export DEBIAN_FRONTEND=noninteractive
sudo debconf-set-selections <<< 'mysql-server-5.5 mysql-server/root_password password diurd'
sudo debconf-set-selections <<< 'mysql-server-5.5 mysql-server/root_password_again password diurd'
sudo apt-get -q -y -V --force-yes --reinstall install mysql-server-5.5

echo "ALL DONE with druid environment setup! Hit CTRL-C to proceed."
exit 0
