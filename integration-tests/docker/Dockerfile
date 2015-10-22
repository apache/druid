FROM ubuntu:14.04

# Add Java 7 repository
RUN apt-get update
RUN apt-get install -y software-properties-common
RUN apt-add-repository -y ppa:webupd8team/java
RUN apt-get update

# Oracle Java 7
RUN echo oracle-java-7-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections
RUN apt-get install -y oracle-java7-installer
RUN apt-get install -y oracle-java7-set-default

# MySQL (Metadata store)
RUN apt-get install -y mysql-server

# Supervisor
RUN apt-get install -y supervisor

# Zookeeper
RUN wget -q -O - http://www.us.apache.org/dist/zookeeper/zookeeper-3.4.6/zookeeper-3.4.6.tar.gz | tar -xzf - -C /usr/local
RUN cp /usr/local/zookeeper-3.4.6/conf/zoo_sample.cfg /usr/local/zookeeper-3.4.6/conf/zoo.cfg
RUN ln -s /usr/local/zookeeper-3.4.6 /usr/local/zookeeper

# Kafka
RUN wget -q -O - http://www.us.apache.org/dist/kafka/0.8.2.0/kafka_2.10-0.8.2.0.tgz | tar -xzf - -C /usr/local
RUN ln -s /usr/local/kafka_2.10-0.8.2.0 /usr/local/kafka
# unless advertised.host.name is set to docker ip, publishing data fails
ADD docker_ip docker_ip
RUN perl -pi -e "s/#advertised.port=.*/advertised.port=9092/; s/#advertised.host.*/advertised.host.name=$(cat docker_ip)/" /usr/local/kafka/config/server.properties

# git
RUN apt-get install -y git

# Druid system user
RUN adduser --system --group --no-create-home druid
RUN mkdir -p /var/lib/druid
RUN chown druid:druid /var/lib/druid

# Add druid jars
ADD lib/* /usr/local/druid/lib/

WORKDIR /

# Setup metadata store
RUN /etc/init.d/mysql start && echo "GRANT ALL ON druid.* TO 'druid'@'%' IDENTIFIED BY 'diurd'; CREATE database druid DEFAULT CHARACTER SET utf8;" | mysql -u root && /etc/init.d/mysql stop

# Add sample data
RUN /etc/init.d/mysql start && java -Ddruid.metadata.storage.type=mysql -cp "/usr/local/druid/lib/*" io.druid.cli.Main tools metadata-init --connectURI="jdbc:mysql://localhost:3306/druid" --user=druid --password=diurd && /etc/init.d/mysql stop
ADD sample-data.sql sample-data.sql
RUN /etc/init.d/mysql start && cat sample-data.sql | mysql -u root druid && /etc/init.d/mysql stop

# Setup supervisord
ADD supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Clean up
RUN apt-get clean && rm -rf /tmp/* /var/tmp/*

# Expose ports:
# - 8081: HTTP (coordinator)
# - 8082: HTTP (broker)
# - 8083: HTTP (historical)
# - 8090: HTTP (overlord)
# - 8091: HTTP (middlemanager)
# - 3306: MySQL
# - 2181 2888 3888: ZooKeeper
# - 8100 8101 8102 8103 8104 : peon ports
EXPOSE 8081
EXPOSE 8082
EXPOSE 8083
EXPOSE 8090
EXPOSE 8091
EXPOSE 3306
EXPOSE 2181 2888 3888
EXPOSE 8100 8101 8102 8103 8104
WORKDIR /var/lib/druid
ENTRYPOINT export HOST_IP="$(resolveip -s $HOSTNAME)" && exec /usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf
