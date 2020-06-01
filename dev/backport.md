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

# Backporting a change
This document contains some examples of how to backport a change from master to another branch. This is not meant to be
an exhaustive list of how to backport a change.

## Using [sqren/backport](https://github.com/sqren/backport)
This is a CLI tool that automates the backport process for you once a change is in master.

To setup, follow the instructions in the [repository](https://github.com/sqren/backport)
A `.backportrc.json` file is maintained in the root of this repository to keep track of the branches that can be
backported to.

[![example](backport-fail.gif)]
