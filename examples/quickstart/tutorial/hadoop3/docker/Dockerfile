# Based on the SequenceIQ hadoop-docker project hosted at
# https://github.com/sequenceiq/hadoop-docker, and modified at
# the Apache Software Foundation (ASF).
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Creates pseudo distributed hadoop 3.3.1 with java 8
FROM centos:7

USER root

# install dev tools
RUN yum clean all \
    && rpm --rebuilddb \
    && yum install -y curl which tar sudo openssh-server openssh-clients rsync yum-plugin-ovl\
    && yum clean all \
    && yum update -y libselinux \
    && yum update -y nss \
    && yum clean all
# update libselinux. see https://github.com/sequenceiq/hadoop-docker/issues/14
# update nss. see https://unix.stackexchange.com/questions/280548/curl-doesnt-connect-to-https-while-wget-does-nss-error-12286

# passwordless ssh
RUN ssh-keygen -q -N "" -t dsa -f /etc/ssh/ssh_host_dsa_key
RUN ssh-keygen -q -N "" -t rsa -f /etc/ssh/ssh_host_rsa_key
RUN ssh-keygen -q -N "" -t rsa -f /root/.ssh/id_rsa
RUN cp /root/.ssh/id_rsa.pub /root/.ssh/authorized_keys

#
# Pull Zulu OpenJDK binaries from official repository:
#

ARG ZULU_REPO_VER=1.0.0-1

RUN rpm --import http://repos.azulsystems.com/RPM-GPG-KEY-azulsystems && \
    curl -sLO https://cdn.azul.com/zulu/bin/zulu-repo-${ZULU_REPO_VER}.noarch.rpm && \
    rpm -ivh zulu-repo-${ZULU_REPO_VER}.noarch.rpm && \
    yum -q -y update && \
    yum -q -y upgrade && \
    yum -q -y install zulu8-jdk && \
    yum clean all && \
    rm -rf /var/cache/yum zulu-repo_${ZULU_REPO_VER}.noarch.rpm

ENV JAVA_HOME=/usr/lib/jvm/zulu8
ENV PATH $PATH:$JAVA_HOME/bin

# hadoop
ARG APACHE_ARCHIVE_MIRROR_HOST=https://archive.apache.org
RUN curl -s ${APACHE_ARCHIVE_MIRROR_HOST}/dist/hadoop/core/hadoop-3.3.1/hadoop-3.3.1.tar.gz | tar -xz -C /usr/local/
RUN cd /usr/local && ln -s ./hadoop-3.3.1 hadoop

ENV HADOOP_HOME /usr/local/hadoop
ENV HADOOP_COMMON_HOME /usr/local/hadoop
ENV HADOOP_HDFS_HOME /usr/local/hadoop
ENV HADOOP_MAPRED_HOME /usr/local/hadoop
ENV HADOOP_YARN_HOME /usr/local/hadoop
ENV HADOOP_CONF_DIR /usr/local/hadoop/etc/hadoop
ENV YARN_CONF_DIR $HADOOP_HOME/etc/hadoop

# in hadoop 3 the example file is nearly empty so we can just append stuff
RUN sed -i '$ a export JAVA_HOME=/usr/lib/jvm/zulu8' $HADOOP_HOME/etc/hadoop/hadoop-env.sh
RUN sed -i '$ a export HADOOP_HOME=/usr/local/hadoop' $HADOOP_HOME/etc/hadoop/hadoop-env.sh
RUN sed -i '$ a export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop/' $HADOOP_HOME/etc/hadoop/hadoop-env.sh
RUN sed -i '$ a export HDFS_NAMENODE_USER=root' $HADOOP_HOME/etc/hadoop/hadoop-env.sh
RUN sed -i '$ a export HDFS_DATANODE_USER=root' $HADOOP_HOME/etc/hadoop/hadoop-env.sh
RUN sed -i '$ a export HDFS_SECONDARYNAMENODE_USER=root' $HADOOP_HOME/etc/hadoop/hadoop-env.sh
RUN sed -i '$ a export YARN_RESOURCEMANAGER_USER=root' $HADOOP_HOME/etc/hadoop/hadoop-env.sh
RUN sed -i '$ a export YARN_NODEMANAGER_USER=root' $HADOOP_HOME/etc/hadoop/hadoop-env.sh

RUN cat $HADOOP_HOME/etc/hadoop/hadoop-env.sh

RUN mkdir $HADOOP_HOME/input
RUN cp $HADOOP_HOME/etc/hadoop/*.xml $HADOOP_HOME/input

# pseudo distributed
ADD core-site.xml.template $HADOOP_HOME/etc/hadoop/core-site.xml.template
RUN sed s/HOSTNAME/localhost/ /usr/local/hadoop/etc/hadoop/core-site.xml.template > /usr/local/hadoop/etc/hadoop/core-site.xml
ADD hdfs-site.xml $HADOOP_HOME/etc/hadoop/hdfs-site.xml
ADD mapred-site.xml $HADOOP_HOME/etc/hadoop/mapred-site.xml
ADD yarn-site.xml $HADOOP_HOME/etc/hadoop/yarn-site.xml

RUN $HADOOP_HOME/bin/hdfs namenode -format

ADD ssh_config /root/.ssh/config
RUN chmod 600 /root/.ssh/config
RUN chown root:root /root/.ssh/config

# # installing supervisord
# RUN yum install -y python-setuptools
# RUN easy_install pip
# RUN curl https://bitbucket.org/pypa/setuptools/raw/bootstrap/ez_setup.py -o - | python
# RUN pip install supervisor
#
# ADD supervisord.conf /etc/supervisord.conf

ADD bootstrap.sh /etc/bootstrap.sh
RUN chown root:root /etc/bootstrap.sh
RUN chmod 700 /etc/bootstrap.sh

ENV BOOTSTRAP /etc/bootstrap.sh

# workingaround docker.io build error
RUN ls -la /usr/local/hadoop/etc/hadoop/*-env.sh
RUN chmod +x /usr/local/hadoop/etc/hadoop/*-env.sh
RUN ls -la /usr/local/hadoop/etc/hadoop/*-env.sh

# Copy additional .jars to classpath
RUN cp /usr/local/hadoop/share/hadoop/tools/lib/*.jar /usr/local/hadoop/share/hadoop/common/lib/

# fix the 254 error code
RUN sed  -i "/^[^#]*UsePAM/ s/.*/#&/"  /etc/ssh/sshd_config
RUN echo "UsePAM no" >> /etc/ssh/sshd_config
RUN echo "Port 2122" >> /etc/ssh/sshd_config

# script for plain sshd start
RUN echo -e \
	'#!/bin/bash\n/usr/sbin/sshd\ntimeout 10 bash -c "until printf \"\" 2>>/dev/null >>/dev/tcp/127.0.0.1/2122; do sleep 0.5; done"' > \
	/usr/local/bin/start_sshd && \
	chmod a+x /usr/local/bin/start_sshd

RUN start_sshd && $HADOOP_HOME/etc/hadoop/hadoop-env.sh && $HADOOP_HOME/sbin/start-dfs.sh
RUN start_sshd && $HADOOP_HOME/etc/hadoop/hadoop-env.sh && $HADOOP_HOME/sbin/start-dfs.sh

CMD ["/etc/bootstrap.sh", "-d"]

# Hdfs ports
EXPOSE 8020 9000 9820 9864 9865 9866 9867 9868 9869 9870 9871 50010 50020 50070 50075 50090
# Mapred ports
EXPOSE 10020 19888
#Yarn ports
EXPOSE 8030 8031 8032 8033 8040 8042 8088
#Other ports
EXPOSE 2122 49707