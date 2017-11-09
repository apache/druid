# Base image is built from integration-tests/docker-base in the Druid repo
FROM imply/druiditbase

# Setup metadata store
RUN /etc/init.d/mysql start \
      && echo "GRANT ALL ON druid.* TO 'druid'@'%' IDENTIFIED BY 'diurd'; CREATE database druid DEFAULT CHARACTER SET utf8;" | mysql -u root \
      && /etc/init.d/mysql stop

# Add Druid jars
ADD lib/* /usr/local/druid/lib/

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
