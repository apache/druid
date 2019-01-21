FROM maven:3-jdk-8 as builder

COPY . /src
WORKDIR /src
RUN mvn install -ff -DskipTests -Dforbiddenapis.skip=true -Pdist -Pbundle-contrib-exts
RUN \
 VER=$(mvn -q -Dexec.executable="echo" -Dexec.args='${project.version}' --non-recursive exec:exec) \
 && tar -zxf ./distribution/target/apache-druid-${VER}-bin.tar.gz -C /opt \
 && ln -s /opt/apache-druid-${VER} /opt/druid

RUN cd /opt/druid/extensions/mysql-metadata-storage \
 && wget -O mysql-connector-java-5.1.38.jar http://central.maven.org/maven2/mysql/mysql-connector-java/5.1.38/mysql-connector-java-5.1.38.jar \
 && sha256sum --ignore-missing -c /src/distribution/docker/sha256sums.txt 

RUN addgroup --gid 1000 druid \
 && adduser --home /opt/druid --shell /bin/sh --no-create-home --uid 1000 --gecos '' --gid 1000 --disabled-password druid \
 && mkdir -p /opt/druid/var \
 && chown -R druid:druid /opt/druid

# Use :debug to get a busybox shell so that scripts can run
FROM gcr.io/distroless/java:debug
MAINTAINER Don Bowman <don@agilicus.com>

RUN ["/busybox/busybox", "--install", "/bin"]
COPY --from=builder /etc/passwd /etc/passwd
COPY --from=builder /etc/group /etc/group
COPY --from=builder --chown=druid /opt /opt
COPY distribution/docker/druid.sh /druid.sh
USER druid
VOLUME /opt/druid/var
WORKDIR /opt/druid

ENTRYPOINT ["/druid.sh"]
