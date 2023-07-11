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

# Testing Tools

`it-tools` is a copy of `extensions-core/testing-tools` (module
name `druid-testing-tools`.)

The testing tools are added to the Druid test Docker image. The
`druid-testing-tools` module defines most such additions. However,
`integration-tests` defines a custom node role which also must be
added to the image. `integration-tests` uses a different mechanism
to do that addition.

Here, we want a single extension for all the testing gizmos.
This is a direct copy of the `druid-testing-tools`
extension, along with a copy of the custom node role from
`integration-tests`.

The reason this is a copy, rather than fixing up `druid-testing-tools`
is that the existing `integration-tests` must continue to run and it
is very difficult to change or test them. (Which is the reason for
this parallel approach.) To keep backward compatibility, and to avoid
changing `integration-tests`, we keep the prior approach and make
copies here for the new approach.

The names should never clash: `it-tools` is only ever used
within the `docker-test` project, and the `druid-testing-tools` is
*not* included as a dependency.

Over time, once `integration-tests` are converted, then the
`druid-testing-tools` module can be deprecated in favor of this one.
