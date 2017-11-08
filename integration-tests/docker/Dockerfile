# This is intended to be a temporary unblocker for Travis CI
# We should revert this when ppa:webupd8team/java repo maintainers fix the issue shown here: https://github.com/druid-io/druid/pull/4970
# Or if we stick to using a non-base Ubuntu image, the custom image should reside in an org repo and not an individual repo
# https://hub.docker.com/r/jonweiimply/ubuntu-j8/
FROM jonweiimply/ubuntu-j8

# MySQL (Metadata store)
RUN apt-get install -y mysql-server

# Supervisor
RUN apt-get install -y supervisor

# Zookeeper
RUN wget -q -O - http://www.us.apache.org/dist/zookeeper/zookeeper-3.4.6/zookeeper-3.4.6.tar.gz | tar -xzf - -C /usr/local \
      && cp /usr/local/zookeeper-3.4.6/conf/zoo_sample.cfg /usr/local/zookeeper-3.4.6/conf/zoo.cfg \
      && ln -s /usr/local/zookeeper-3.4.6 /usr/local/zookeeper

# Kafka
# Match the version to the Kafka client used by KafkaSupervisor
RUN wget -q -O - http://www.us.apache.org/dist/kafka/0.10.2.0/kafka_2.11-0.10.2.0.tgz | tar -xzf - -C /usr/local \
      && ln -s /usr/local/kafka_2.11-0.10.2.0 /usr/local/kafka

# Druid system user
RUN adduser --system --group --no-create-home druid \
      && mkdir -p /var/lib/druid \
      && chown druid:druid /var/lib/druid

# clean up time
RUN apt-get clean \
      && rm -rf /tmp/* \
                /var/tmp/* \
                /var/lib/apt/lists \
                /root/.m2


# Setup metadata store
RUN /etc/init.d/mysql start \
      && echo "GRANT ALL ON druid.* TO 'druid'@'%' IDENTIFIED BY 'diurd'; CREATE database druid DEFAULT CHARACTER SET utf8;" | mysql -u root \
      && /etc/init.d/mysql stop

# Add sample data
RUN /etc/init.d/mysql start \
      && java -cp "/usr/local/druid/lib/*" -Ddruid.metadata.storage.type=mysql io.druid.cli.Main tools metadata-init --connectURI="jdbc:mysql://localhost:3306/druid" --user=druid --password=diurd \
      && /etc/init.d/mysql stop
ADD sample-data.sql sample-data.sql
RUN /etc/init.d/mysql start \
      && cat sample-data.sql | mysql -u root druid \
      && /etc/init.d/mysql stop

# Setup supervisord
ADD supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# unless advertised.host.name is set to docker host ip, publishing data fails
# run this last to avoid rebuilding the image every time the ip changes
ADD docker_ip docker_ip
RUN perl -pi -e "s/#advertised.listeners=.*/advertised.listeners=PLAINTEXT:\/\/$(cat docker_ip):9092/" /usr/local/kafka/config/server.properties

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
