##  Druid CR Spec

- Druid CR has a ```clusterSpec``` which is common for all the druid nodes deployed and a ```nodeSpec``` which is specific to druid nodes.
- Some key values are ```Required```, ie they must be present in the spec to get the cluster deployed. Other's are optional.
- For full details on spec refer to ```pkg/apis/druid/v1alpha1/druid_types.go```
- The operator supports both deployments and statefulsets for druid Nodes. ```kind``` can be specified in the druid NodeSpec's to ```Deployment``` / ```StatefulSet```.
- ```NOTE: The default behavior shall provision all the nodes as statefulsets.```
- The following are cluster scoped and common to all the druid nodes.

```yaml
spec:
  # Enable rolling deploy for druid, not required but suggested for production setup
  # more information in features.md and in druid documentation
  # http://druid.io/docs/latest/operations/rolling-updates.html
  rollingDeploy: true
  # Image for druid, Required Key
  image: apache/druid:25.0.0
  ....
  # Optionally specify image for all nodes. Can be specify on nodes also
  # imagePullSecrets:
  # - name: tutu
  ....
  # Entrypoint for druid cluster, Required Key
  startScript: /druid.sh
  ...
  # Labels populated by pods
  podLabels:
  ....
  # Pod Security Context 
  securityContext:
  ...
  # Service Spec created for all nodes
  services:
  ...
  # Mount Path to mount the common runtime,jvm and log4xml configs inside druid pods. Required Key
  commonConfigMountPath: "/opt/druid/conf/druid/cluster/_common"
  ...
  # JVM Options common for all druid nodes
  jvm.options: |-
  ...
  # log4j.config common for all druid nodes

  log4j.config: |-
  # common runtime properties for all druid nodes
  common.runtime.properties: |
 ```

- The following are specific to a node.

 ```yaml
  nodes:
    # String value, can be anything to define a node name.
    brokers:
      # nodeType can be broker, historical, middleManager, indexer, router, coordinator and overlord.
      # Required Key
      nodeType: "broker"
      # Optionally specify for broker nodes
      # imagePullSecrets:
      # - name: tutu
      # Port for the node
      druid.port: 8088
      # MountPath where are the all node properties get mounted as configMap
      nodeConfigMountPath: "/opt/druid/conf/druid/cluster/query/broker"
      # replica count, required must be greater than > 0.
      replicas: 1
      # Runtime Properties for the node
      # Required Key
      runtime.properties: |
      ...
```

## Authentication Setup

Authentication can be configured to secure communication with the cluster API using credentials stored in Kubernetes secrets.

Currently this is used for compaction, rules, dynamic configs, and ingestion configurations.

This not only applies to the `Druid` CR but also to the `DruidIngestion` CR.

### Configuring Basic Authentication

To use basic authentication, you need to create a Kubernetes secret containing the username and password. This secret is then referenced in the Druid CR.

Steps to Configure Basic Authentication:

1. **Create a Kubernetes Secret:** Store your username and password in a Kubernetes secret. Below is an example of how to define the secret in a YAML file:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: mycluster-admin-operator
  namespace: druid
type: Opaque
data:
  OperatorUserName: <base64-encoded-username>
  OperatorPassword: <base64-encoded-password>
```

Replace <base64-encoded-username> and <base64-encoded-password> with the base64-encoded values of your desired username and password.

2. Define Authentication in the Druid CRD: Reference the secret in your Druid custom resource. Here is an example `Druid`:

```yaml
apiVersion: druid.apache.org/v1alpha1
kind: Druid
metadata:
  name: agent
spec:
  auth:
    secretRef:
      name: mycluster-admin-operator
      namespace: druid
    type: basic-auth
```

This configuration specifies that the Druid cluster should use basic authentication with credentials retrieved from the mycluster-admin-operator secret.
