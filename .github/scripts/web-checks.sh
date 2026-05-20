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

set -e
set -x

# Install node.js via maven frontend plugin (same approach as jacoco script)
mvn -B com.github.eirslett:frontend-maven-plugin:install-node-and-npm@install-node-and-npm -pl web-console/
PATH+=:web-console/target/node/

# docs
(cd website && npm install)
cd website
npm run build
npm run link-lint
npm run spellcheck
cd ..

# web console
mvn -B test -pl 'web-console'
cd web-console
{ for i in 1 2 3; do npm run codecov && break || sleep 15; done }
cd ..

# web console end-to-end test
./.github/scripts/setup_generate_license.sh
web-console/script/druid build
web-console/script/druid start
(cd web-console && npm run test-e2e)
web-console/script/druid stop
