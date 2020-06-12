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
  
Comments to various parts of IntelliJ's settings XML files. These comments cannot currently be placed close to the
things that they are about, because IntelliJ keeps removing the comments from settings XML files: see
https://youtrack.jetbrains.com/issue/IDEA-211087. Please vote for this issue to increase the chances that it's fixed
faster. [This Druid's issue](https://github.com/apache/incubator-druid/issues/7549) records the fact that the comments
should be moved when that IntelliJ's issue is fixed.

1) [`inspectionProfiles/Druid.xml`](inspectionProfiles/Druid.xml), `StaticPseudoFunctionalStyleMethod` is turned off
because the current rate of false-positives produced by this inspection is very high, see
https://youtrack.jetbrains.com/issue/IDEA-153047#focus=streamItem-27-3326648.0-0.

2) [`misc.xml`](misc.xml), `ProjectResources` component: this component is needed because IntelliJ verifies XML
documents by the schema. XML documents usually reference those schemas as URLs:
```
<assembly xmlns="http://maven.apache.org/ASSEMBLY/2.0.0"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:schemaLocation="http://maven.apache.org/ASSEMBLY/2.0.0 http://maven.apache.org/xsd/assembly-2.0.0.xsd">
```

But IntelliJ doesn't automatically go to the internet to download the resource. It needs to know what schema corresponds
to what URL, statically. Hence the `ProjectResources` component.