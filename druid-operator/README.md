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

<h2 align="center">
  <br>
  Kubernetes Operator For Apache Druid
</h2>

<div align="center">

</div>

- Druid Operator provisions and manages [Apache Druid](https://druid.apache.org/) cluster on kubernetes.
- Druid Operator is designed to provision and manage [Apache Druid](https://druid.apache.org/) in distributed mode only.
- It is built in Golang using [kubebuilder](https://github.com/kubernetes-sigs/kubebuilder).
- Refer to [Documentation](./docs/README.md) for getting started.

### Newsletter - Monthly updates on running druid on kubernetes.
- [Apache Druid on Kubernetes](https://druidonk8s.substack.com/)

### Talks and Blogs on Druid Operator

- [Druid Summit 2023](https://druidsummit.org/agenda?agendaPath=session/1256850)
- [Dok Community](https://www.youtube.com/live/X4A3lWJRGHk?feature=share)
- [Druid Summit](https://youtu.be/UqPrttXRBDg)
- [Druid Operator Blog](https://www.cloudnatively.com/apache-druid-on-kubernetes/)
- [Druid On K8s Without ZK](https://youtu.be/TRYOvkz5Wuw)
- [Building Apache Druid on Kubernetes: How Dailymotion Serves Partner Data](https://youtu.be/FYFq-tGJOQk)

### Supported CR's

- The operator supports CR's of type ```Druid``` and ```DruidIngestion```.
- ```Druid``` and ```DruidIngestion``` CR belongs to api Group ```druid.apache.org``` and version ```v1alpha1```

### Druid Operator Architecture

![Druid Operator](docs/images/druid-operator.png?raw=true "Druid Operator")

### Notifications

- The project moved to <b>Kubebuilder v3</b> which requires a [manual change](docs/kubebuilder_v3_migration.md) in the operator.
- Users are encourage to use operator version 0.0.9+.
- The operator has moved from HPA apiVersion autoscaling/v2beta1 to autoscaling/v2 API users will need to update there HPA Specs according v2 api in order to work with the latest druid-operator release.
- druid-operator has moved Ingress apiVersion networking/v1beta1 to networking/v1. Users will need to update there Ingress Spec in the druid CR according networking/v1 syntax. In case users are using schema validated CRD, the CRD will also be needed to be updated.
- The v1.0.0 release for druid-operator is compatible with k8s version 1.25. HPA API is kept to version v2beta2.
- Release v1.2.2 had a bug for namespace scoped operator deployments, this is fixed in 1.2.3.

### Kubernetes version compatibility

| druid-operator | 0.0.9 | v1.0.0 | v1.1.0 | v1.2.2 | v1.2.3 | v1.2.4 | v1.2.5 | v1.3.0 |
| :------------- | :-------------: | :-----: | :---: | :---: | :---: | :---: | :---: | :---: |
| kubernetes <= 1.20 | :x:| :x: | :x: | :x: | :x: | :x: | :x: | :x: |
| kubernetes == 1.21 | :white_check_mark:| :x: | :x: | :x: | :x: | :x: | :x: | :x: |
| kubernetes >= 1.22 and <= 1.25 | :white_check_mark: | :white_check_mark: | :white_check_mark: |  :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
| kubernetes > 1.25 and <= 1.33.1 | :x: | :x: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |

