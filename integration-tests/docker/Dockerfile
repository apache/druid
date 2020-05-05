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


# This is default value for base image in case DOCKER_IMAGE is not given when building
ARG DOCKER_IMAGE=imply/druiditbase:openjdk-1.8.0_191-1
# Base image is built from integration-tests/docker-base in the Druid repo
FROM $DOCKER_IMAGE

# Verify Java version
ARG DOCKER_IMAGE
ENV DOCKER_IMAGE_USED=$DOCKER_IMAGE
RUN echo "Built using base docker image DOCKER_IMAGE_USED=$DOCKER_IMAGE_USED"
RUN java -version

RUN echo "[mysqld]\ncharacter-set-server=utf8\ncollation-server=utf8_bin\n" >> /etc/mysql/my.cnf

# Setup metadata store
# touch is needed because OverlayFS's copy-up operation breaks POSIX standards. See https://github.com/docker/for-linux/issues/72.
RUN find /var/lib/mysql -type f -exec touch {} \; && /etc/init.d/mysql start \
      && echo "CREATE USER 'druid'@'%' IDENTIFIED BY 'diurd'; GRANT ALL ON druid.* TO 'druid'@'%'; CREATE database druid DEFAULT CHARACTER SET utf8mb4;" | mysql -u root \
      && /etc/init.d/mysql stop

# Add Druid jars
ADD lib/* /usr/local/druid/lib/

# Download the MySQL Java connector
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends apt-utils libmysql-java
RUN ln -sf /usr/share/java/mysql-connector-java.jar /usr/local/druid/lib/mysql-connector-java.jar

# Add sample data
# touch is needed because OverlayFS's copy-up operation breaks POSIX standards. See https://github.com/docker/for-linux/issues/72.
RUN find /var/lib/mysql -type f -exec touch {} \; && service mysql start \
      && java -cp "/usr/local/druid/lib/*" -Ddruid.metadata.storage.type=mysql org.apache.druid.cli.Main tools metadata-init --connectURI="jdbc:mysql://localhost:3306/druid" --user=druid --password=diurd \
      && /etc/init.d/mysql stop
ADD test-data /test-data

# Setup supervisord
ADD supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Add druid configuration setup script
ADD druid.sh /druid.sh

# mysql
ADD run-mysql.sh /run-mysql.sh

# internal docker_ip:9092 endpoint is used to access Kafka from other Docker containers
# external docker ip:9093 endpoint is used to access Kafka from test code
# run this last to avoid rebuilding the image every time the ip changes
ADD docker_ip docker_ip
RUN perl -pi -e "s/#listeners=.*/listeners=INTERNAL:\/\/172.172.172.2:9092,EXTERNAL:\/\/172.172.172.2:9093/" /usr/local/kafka/config/server.properties
RUN perl -pi -e "s/#advertised.listeners=.*/advertised.listeners=INTERNAL:\/\/172.172.172.2:9092,EXTERNAL:\/\/$(cat docker_ip):9093/" /usr/local/kafka/config/server.properties
RUN perl -pi -e "s/#listener.security.protocol.map=.*/listener.security.protocol.map=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT\ninter.broker.listener.name=INTERNAL/" /usr/local/kafka/config/server.properties
RUN perl

# Add directory with TLS support files
ADD tls tls

ADD client_tls client_tls

# Expose ports:
# - 8081, 8281: HTTP, HTTPS (coordinator)
# - 8082, 8282: HTTP, HTTPS (broker)
# - 8083, 8283: HTTP, HTTPS (historical)
# - 8090, 8290: HTTP, HTTPS (overlord)
# - 8091, 8291: HTTP, HTTPS (middlemanager)
# - 8888-8891, 9088-9091: HTTP, HTTPS (routers)
# - 3306: MySQL
# - 2181 2888 3888: ZooKeeper
# - 8100 8101 8102 8103 8104 8105 : peon ports
# - 8300 8301 8302 8303 8304 8305 : peon HTTPS ports
EXPOSE 8081 8281
EXPOSE 8082 8282
EXPOSE 8083 8283
EXPOSE 8090 8290
EXPOSE 8091 8291
EXPOSE 3306
EXPOSE 2181 2888 3888
EXPOSE 8100 8101 8102 8103 8104 8105
EXPOSE 8300 8301 8302 8303 8304 8305
EXPOSE 9092 9093

WORKDIR /var/lib/druid
ENTRYPOINT /tls/generate-server-certs-and-keystores.sh \
            # Create druid service config files with all the config variables
            && . /druid.sh; setupConfig \
            # Some test groups require pre-existing data to be setup
            && . /druid.sh; setupData \
            # Export the service config file path to use in supervisord conf file
            && export DRUID_COMMON_CONF_DIR="$(. /druid.sh; getConfPath ${DRUID_SERVICE})" \
            # Export the common config file path to use in supervisord conf file
            && export DRUID_SERVICE_CONF_DIR="$(. /druid.sh; getConfPath _common)" \
            # Run Druid service using supervisord
            && exec /usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf
