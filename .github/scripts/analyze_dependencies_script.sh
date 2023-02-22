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

#!bin/bash

${MVN} ${MAVEN_SKIP} dependency:analyze -DoutputXML=true -DignoreNonCompile=true -DfailOnWarning=true ${HADOOP_PROFILE} ||
{ echo "
    The dependency analysis has found a dependency that is either:

    1) Used and undeclared: These are available as a transitive dependency but should be explicitly
    added to the POM to ensure the dependency version. The XML to add the dependencies to the POM is
    shown above.

    2) Unused and declared: These are not needed and removing them from the POM will speed up the build
    and reduce the artifact size. The dependencies to remove are shown above.

    If there are false positive dependency analysis warnings, they can be suppressed:
    https://maven.apache.org/plugins/maven-dependency-plugin/analyze-mojo.html#usedDependencies
    https://maven.apache.org/plugins/maven-dependency-plugin/examples/exclude-dependencies-from-dependency-analysis.html

    For more information, refer to:
    https://maven.apache.org/plugins/maven-dependency-plugin/analyze-mojo.html

    " && false; }
