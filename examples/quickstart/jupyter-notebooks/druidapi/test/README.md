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
  
# Unit tests for the Druid Python API

These tests are preliminary.

See [https://docs.python.org/3/library/unittest.html] for the unit testing framework used.
`unittest` was selected because it is part of Python, though there are more modern choices.

## Setup

The tests are intended to run in the parent directory of both this `test` module and the
`druidapi` module. To run anywhere else, modify `setup.py` to point to where the Druid API is located.

Most tests require a running Druid instance. The instance can be a quickstart instance, or
a IT cluster started with the `BatchIndex` test category. The tests will create and delete
objects within that cluster.

## Run

To run an individual test:

```bash
python3 -m unittest test/<test file>.py
```
