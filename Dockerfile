# Based on https://github.com/druid-io/docker-druid

FROM ubuntu:xenial

RUN apt-get update \
      && apt-get install -y software-properties-common \
      && apt-add-repository -y ppa:webupd8team/java \
      && apt-get purge --auto-remove -y software-properties-common \
      && apt-get update \
      && echo oracle-java-8-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections \
      && apt-get install -y oracle-java8-installer oracle-java8-set-default \
                            postgresql-client \
                            supervisor \
                            git \
                            netcat \
                            curl \
      && apt-get clean \
      && rm -rf /var/cache/oracle-jdk8-installer \
      && rm -rf /var/lib/apt/lists/*

RUN wget -q -O - http://archive.apache.org/dist/maven/maven-3/3.2.5/binaries/apache-maven-3.2.5-bin.tar.gz | tar -xzf - -C /usr/local \
      && ln -s /usr/local/apache-maven-3.2.5 /usr/local/apache-maven \
      && ln -s /usr/local/apache-maven/bin/mvn /usr/local/bin/mvn

RUN mkdir -p /usr/local/druid/lib \
      && mkdir -p /opt/druid/distribution

WORKDIR /tmp/druid

COPY . .

RUN mvn clean install -DskipTests -Pdist,bundle-contrib-exts --quiet \
      && cp services/target/druid-services-*-selfcontained.jar /usr/local/druid/lib \
      && cp distribution/target/*.tar.gz /opt/druid/distribution \
      && cp -r distribution/target/extensions /usr/local/druid/ \
      && cp -r distribution/target/hadoop-dependencies /usr/local/druid/ \
      && apt-get purge --auto-remove -y git \
      && apt-get clean \
      && rm -rf /var/tmp/* \
                /usr/local/apache-maven-3.2.5 \
                /usr/local/apache-maven \
                /root/.m2

WORKDIR /var/lib/druid
ADD . .

CMD ./cmd.sh