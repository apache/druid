#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

ARG DRUID_RELEASE=druid/druid:0.14.0
FROM $DRUID_RELEASE

COPY sha256sums.txt /tmp
RUN wget -O /opt/druid/extensions/mysql-metadata-storage/mysql-connector-java-5.1.38.jar https://repo1.maven.org/maven2/mysql/mysql-connector-java/5.1.38/mysql-connector-java-5.1.38.jar \
 && sed -e '/^#/d' /tmp/sha256sums.txt > /tmp/sha256sums-stripped.txt \
 && sha256sum -c /tmp/sha256sums-stripped.txt \
 && rm -f /opt/druid/lib/mysql-connector-java-5.1.38.jar \
 && ln -s /opt/druid/extensions/mysql-metadata-storage/mysql-connector-java-5.1.38.jar /opt/druid/lib
