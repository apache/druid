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

## Dev Dependencies

- Golang 1.20+
- Kubebuilder v3
- It is recommended to install kind since the project's e2e tests are using it.

## Running Operator Locally
We're using Kubebuilder so we are working with its `Makefile` and extra custom commands:
```shell
# If needed, create a kubernetes cluster (requires kind)
make kind

# Install the CRDs
make install

# Run the operator locally
make run
```

## Watch a namespace
```shell
# Watch all namespaces
export WATCH_NAMESPACE=""

# Watch a single namespace
export WATCH_NAMESPACE="mynamespace"

# Watch all namespaces except: kube-system, default
export DENY_LIST="kube-system,default" 
```

## Building The Operator Docker Image
```shell
make docker-build

# In case you want to build it with a custom image:
make docker-build IMG=custom-name:custom-tag 
```

## Testing
Before submitting a PR, make sure the tests are running successfully.
```shell
# Run unit tests
make test 

# Run E2E tests (requires kind)
make e2e
```

## Documentation
If you changed the CRD API, please make sure the documentation is also updated:
```shell
make api-docs 
```

## Help
The `Makefile` should contain all commands with explanations. You can also run:
```shell
# For help
make help
```
