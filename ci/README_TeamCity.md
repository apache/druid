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
  
## Overview  
TeamCity is a continuous integration and deployment server responsible for 
static analysis of Druid source code. Each Github PR request for 
[Druid](https://teamcity.jetbrains.com/project.html?projectId=OpenSourceProjects_Druid) 
is checked by TeamCity automatically.

## Login
One can log in to TeamCity either via credentials or as a guest to check static analysis result of any PR.

## Becoming a Project Administrator
Druid committers shall obtain a status of a [Druid project](
https://teamcity.jetbrains.com/project.html?projectId=OpenSourceProjects_Druid)
administrator. First, the Druid committer needs to log in teamcity.jetbrains.com using his Github account.
Then, somebody who is already a project administrator needs to do the following:

 1. Follow the "Administration" link in the top-right corner of the page
 2. Follow the "Users" link in the "User Management" section in the menu on the left
 3. Type the committer's Github handle in the "Find users" text input, press "Filter"
 4. Select the committer
 5. Press the "Assign roles" button in the bottom of the page
 6. Select "Role: Project administrator" and "Scope: Open-source project -> Druid" in the inputs, press "Assign"

## Restarting a Build
A project administrator could restart a build by pressing the "Run" button on the build page.
