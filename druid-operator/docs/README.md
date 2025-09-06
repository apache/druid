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

# Overview

Druid Operator is a Kubernetes controller that manages the lifecycle of [Apache Druid](https://druid.apache.org/) clusters.
The operator simplifies the management of Druid clusters with its custom logic that is configurable via custom API
(Kubernetes CRD).

## Druid Operator Documentation

* [Getting Started](./getting_started.md)
* API Specifications
  * [Druid API](./api_specifications/druid.md)
* [Supported Features](./features.md)
* [Example Specs](./examples.md)
* [Developer Documentation](./dev_doc.md)
* [Migration To Kubebuilder V3 in the Upcoming Version](./kubebuilder_v3_migration.md)

---

:warning: You won't find any documentation about druid itself in this repository.
If you need details about how to architecture your druid cluster you can consult theses documentations:

* [Druid introduction](<https://druid.apache.org/docs/latest/design/index.html>)
* [Druid architecture](https://druid.apache.org/docs/latest/design/architecture.html)
* [Druid configuration reference](https://druid.apache.org/docs/latest/configuration/index.html)

---

[German company iunera has published their druid cluster spec](https://www.iunera.com/) in github which is used in the context of a software project by the German Ministry for Digital and Transport. The spec have the following features:

* Kubernetes-native Druid
  * K8S jobs instead of Middlemanager with separated [pod-templates](https://github.com/iunera/druid-cluster-config/blob/main/kubernetes/druid/druidcluster/podTemplates/default-task-template.yaml)
  * [Service Discovery by Kubernetes](https://github.com/iunera/druid-cluster-config/blob/main/kubernetes/druid/druidcluster/iuneradruid-cluster.yaml#L172) aka. no zookeeper
  * [HPA for historical nodes](https://github.com/iunera/druid-cluster-config/blob/main/kubernetes/druid/druidcluster/hpa.yaml) / extended [Metrics Exporter](https://github.com/iunera/druid-cluster-config/blob/main/kubernetes/druid/metrics/druid-exporter.helm.yaml)
* Multiple [Authenticator/Authorizer](https://github.com/iunera/druid-cluster-config/blob/main/kubernetes/druid/druidcluster/iuneradruid-cluster.yaml#L88) (Basic Auth and Azure AD Authentication with pac4j)
* [Examples](https://github.com/iunera/druid-cluster-config/tree/main/_authentication-and-authorization-druid) for authorization and authentication
* Based on druid-operator and [flux-cd](https://fluxcd.io/flux/)
* Secrets managed by [SOPS](https://fluxcd.io/flux/guides/mozilla-sops/) and [ingested as Environment Variables](https://github.com/iunera/druid-cluster-config/blob/main/kubernetes/druid/druidcluster/iuneradruid-cluster.yaml#L245)
* Postgres as Metadata Store (incl. [Helmchart Config](https://github.com/iunera/druid-cluster-config/blob/main/kubernetes/druid/postgres/postgres.helm.yaml))
* All endpoints TLS encrypted incl. [Howto](https://github.com/iunera/druid-cluster-config/blob/main/README.md#cluster-internal-tls-encryption)

Link to the complete config file: https://github.com/iunera/druid-cluster-config
