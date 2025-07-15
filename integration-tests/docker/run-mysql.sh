#!/bin/bash

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

(mysqld --bind-address=0.0.0.0 || true) & # there might already be a mysqld running, so we ignore the error
/usr/bin/pidproxy /var/run/mysqld/mysqld.pid /usr/bin/mysqld_safe --bind-address=0.0.0.0
echo "Waiting for MySQL to be ready...";
for i in {30..0}; do
  mysqladmin ping --silent && break;
  sleep 1; \
done; \
if [ "$i" = 0 ]; then
  echo "MySQL did not start"; exit 1;
fi;
