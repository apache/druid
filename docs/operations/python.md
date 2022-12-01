---
id: python
title: "Python Installation"
---

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

Apache Druid startup script requires a python interpreter. This page has steps to install python interpreter and required libraries.

## Python interpreter installation instructions

### MacOS
MacOS comes with a version of Python2, which has been deprecated. Python2 and Python3 can coexist on the same machine without problems.

#### Install the official Python release
* Browse to the [Python Downloads Page](https://www.python.org/downloads/) and download the latest version (3.x.x)
* Verify if Python3 and Pip3 (python package manager) is installed by issuing `python3` and `pip3 -V` commands. 

#### Install with Homebrew
Refer [Installing Python 3 on Mac OS X](https://docs.python-guide.org/starting/install3/osx/)

### Linux

#### Debian
    - `sudo apt update`
    - `sudo apt install -y python3-pip`
#### RHEL 
    - `sudo yum install -y epel-release`
    - `sudo yum install -y python3-pip`

## Required libraries 

### psutil
- pip3 install psutil 

### pathlib 
- pip3 install pathlib 
