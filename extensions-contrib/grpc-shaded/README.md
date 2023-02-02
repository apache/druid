<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->

# Shaded Jar file for the grpc-query Module

This module produces a shaded jar that includes gRPC and the version
of Guava which gRPC needs. gRPC needs a newer Guava than Druid.
Guava is notorious for removing methods. Druid must use the version
of Guava which Hadoop 2 needs. That version omits methods which gRPC
needs. But, if we use the gRPC Guava version, then that version omits
methods which Hadoop 2 needs. The solution is to shade rRPC and its
required Guava version.

Once we do that, we realize that we cannot use the shaeded jar to
generate Protobuf code: the generated code refers to Guava in its
unshaded location, causing more version conflicts. To resolve this
we first generate the Java Protobuf code, then include that in the
shaded jar, which relocates the offending Guava references.

The result works as we want, except that it causes grief in an IDE.
Basically, the IDE doesn't know about the shaded jar: it pulls in
the dependencies of this module directly. The result is the very
Guava conflicts we want to avoid. To fix this, we **do not**
include this module in the root-level `pom.xml` file. Instead the
`grpc-query` module uses a Maven exec plugin to do the build of
this module. The result in a full build is the same, with the
benefit that this module is invisible to IDEs.

The one drawback of this odd structure is that this project
will not see requests to run static checks. That should be OK:
the code here is quite small and won't often change. Still, if there
is a better solution, we can switch to it instead.
