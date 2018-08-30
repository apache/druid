# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Base image is built from integration-tests/docker-base in the Druid repo
FROM imply/druiditbase

# Setup metadata store
# touch is needed because OverlayFS's copy-up operation breaks POSIX standards. See https://github.com/docker/for-linux/issues/72.
RUN find /var/lib/mysql -type f -exec touch {} \; && /etc/init.d/mysql start \
      && echo "GRANT ALL ON druid.* TO 'druid'@'%' IDENTIFIED BY 'diurd'; CREATE database druid DEFAULT CHARACTER SET utf8;" | mysql -u root \
      && /etc/init.d/mysql stop

# Add Druid jars
ADD lib/* /usr/local/druid/lib/

# Add sample data
# touch is needed because OverlayFS's copy-up operation breaks POSIX standards. See https://github.com/docker/for-linux/issues/72.
RUN find /var/lib/mysql -type f -exec touch {} \; && service mysql start \
      && java -cp "/usr/local/druid/lib/*" -Ddruid.metadata.storage.type=mysql org.apache.druid.cli.Main tools metadata-init --connectURI="jdbc:mysql://localhost:3306/druid" --user=druid --password=diurd \
      && /etc/init.d/mysql stop
ADD sample-data.sql sample-data.sql
# touch is needed because OverlayFS's copy-up operation breaks POSIX standards. See https://github.com/docker/for-linux/issues/72.
RUN find /var/lib/mysql -type f -exec touch {} \; && service mysql start \
      && cat sample-data.sql | mysql -u root druid \
      && /etc/init.d/mysql stop

# Setup supervisord
ADD supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# internal docker_ip:9092 endpoint is used to access Kafka from other Docker containers
# external docker ip:9093 endpoint is used to access Kafka from test code
# run this last to avoid rebuilding the image every time the ip changes
ADD docker_ip docker_ip
RUN perl -pi -e "s/#listeners=.*/listeners=INTERNAL:\/\/$(resolveip -s $HOSTNAME):9092,EXTERNAL:\/\/$(resolveip -s $HOSTNAME):9093/" /usr/local/kafka/config/server.properties
RUN perl -pi -e "s/#advertised.listeners=.*/advertised.listeners=INTERNAL:\/\/$(resolveip -s $HOSTNAME):9092,EXTERNAL:\/\/$(cat docker_ip):9093/" /usr/local/kafka/config/server.properties
RUN perl -pi -e "s/#listener.security.protocol.map=.*/listener.security.protocol.map=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT\ninter.broker.listener.name=INTERNAL/" /usr/local/kafka/config/server.properties
RUN perl

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
