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

FROM ubuntu:16.04

# Install Java JDK 8
RUN apt-get update \
    && apt-get install -y openjdk-8-jdk

# Bundle everything into one script so cleanup can reduce image size.
# Otherwise docker's layered images mean that things are not actually deleted.

COPY setup.sh /root/setup.sh
RUN chmod 0755 /root/setup.sh && /root/setup.sh