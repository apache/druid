# Based on the following projects/files:
# - SequenceIQ hadoop-docker project hosted at https://github.com/sequenceiq/hadoop-docker
# - AdoptOpenJDK openjdk-docker project hosted at https://github.com/AdoptOpenJDK/openjdk-docker
# and modified at the Apache Software Foundation (ASF).
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

FROM ubuntu:16.04

# Install Java JDK 11 (OpenJDK 11.0.5)
# Sourced from AdoptOpenJDK openjdk-docker project (https://github.com/AdoptOpenJDK/openjdk-docker)
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates fontconfig locales \
    && echo "en_US.UTF-8 UTF-8" >> /etc/locale.gen \
    && locale-gen en_US.UTF-8 \
    && rm -rf /var/lib/apt/lists/*

RUN set -eux; \
    ARCH="$(dpkg --print-architecture)"; \
    case "${ARCH}" in \
       armhf) \
         ESUM='c6b1fda3f8807028cbfcc34a4ded2e8a5a6b6239d2bcc1f06673ea6b1530df94'; \
         BINARY_URL='https://github.com/AdoptOpenJDK/openjdk11-binaries/releases/download/jdk-11.0.5%2B10/OpenJDK11U-jdk_arm_linux_hotspot_11.0.5_10.tar.gz'; \
         ;; \
       ppc64el|ppc64le) \
         ESUM='d763481ddc29ac0bdefb24216b3a0bf9afbb058552682567a075f9c0f7da5814'; \
         BINARY_URL='https://github.com/AdoptOpenJDK/openjdk11-binaries/releases/download/jdk-11.0.5%2B10/OpenJDK11U-jdk_ppc64le_linux_hotspot_11.0.5_10.tar.gz'; \
         ;; \
       amd64|x86_64) \
         ESUM='6dd0c9c8a740e6c19149e98034fba8e368fd9aa16ab417aa636854d40db1a161'; \
         BINARY_URL='https://github.com/AdoptOpenJDK/openjdk11-binaries/releases/download/jdk-11.0.5%2B10/OpenJDK11U-jdk_x64_linux_hotspot_11.0.5_10.tar.gz'; \
         ;; \
       *) \
         echo "Unsupported arch: ${ARCH}"; \
         exit 1; \
         ;; \
    esac; \
    curl -LfsSo /tmp/openjdk.tar.gz ${BINARY_URL}; \
    echo "${ESUM} */tmp/openjdk.tar.gz" | sha256sum -c -; \
    mkdir -p /opt/java/openjdk; \
    cd /opt/java/openjdk; \
    tar -xf /tmp/openjdk.tar.gz --strip-components=1; \
    rm -rf /tmp/openjdk.tar.gz;

ENV JAVA_HOME=/opt/java/openjdk \
    PATH="/opt/java/openjdk/bin:$PATH"

# Bundle everything into one script so cleanup can reduce image size.
# Otherwise docker's layered images mean that things are not actually deleted.

COPY setup.sh /root/setup.sh
RUN chmod 0755 /root/setup.sh && /root/setup.sh
