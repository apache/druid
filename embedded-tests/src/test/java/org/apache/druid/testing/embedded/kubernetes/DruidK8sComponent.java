/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.testing.embedded.kubernetes;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.embedded.TestFolder;
import org.testcontainers.k3s.K3sContainer;

import java.io.File;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Base class for all Druid Kubernetes components. Provides common functionality
 * for Druid services running on Kubernetes with configurable images and RBAC.
 */
public abstract class DruidK8sComponent implements K8sComponent
{
  private static final Logger log = new Logger(DruidK8sComponent.class);
  private static final int POD_READY_CHECK_TIMEOUT_SECONDS = 300;
  public static final int DRUID_PORT = 8088;

  protected final String namespace;
  protected final String druidImage;
  protected final String clusterName;
  private Integer allocatedNodePort;
  protected TestFolder testFolder;

  public static final String DRUID_34_IMAGE = "apache/druid:34.0.0";

  protected DruidK8sComponent(String namespace, String druidImage, String clusterName)
  {
    this.namespace = namespace;
    this.druidImage = druidImage != null ? druidImage : DRUID_34_IMAGE;
    this.clusterName = clusterName;
  }

  /**
   * Set the TestFolder for shared storage across components.
   */
  public void setTestFolder(TestFolder testFolder)
  {
    this.testFolder = testFolder;
  }

  /**
   * Get the shared storage directory for segments and metadata.
   */
  protected String getSharedStorageDirectory()
  {
    if (testFolder == null) {
      throw new IllegalStateException("TestFolder not initialized. Call setTestFolder() first.");
    }

    // Create the main druid-storage directory
    File storageDir = testFolder.getOrCreateFolder("druid-storage");

    // Create subdirectories that Druid components will need
    testFolder.getOrCreateFolder("druid-storage/segments");
    testFolder.getOrCreateFolder("druid-storage/segment-cache");
    testFolder.getOrCreateFolder("druid-storage/metadata");
    testFolder.getOrCreateFolder("druid-storage/indexing-logs");

    return storageDir.getAbsolutePath();
  }

  /**
   * Set the allocated NodePort for this service.
   */
  public void setAllocatedNodePort(int nodePort)
  {
    this.allocatedNodePort = nodePort;
  }

  @Override
  public String getNamespace()
  {
    return namespace;
  }

  public String getDruidImage()
  {
    return druidImage;
  }

  public String getClusterName()
  {
    return clusterName;
  }

  public abstract String getDruidServiceType();

  /**
   * Get the Druid port for this service.
   *
   * @return the port number
   */
  public abstract int getDruidPort();

  /**
   * Get the service URL for this Druid component.
   *
   * @return the service URL in the format suitable for Kubernetes DNS
   */
  public String getServiceUrl()
  {
    return StringUtils.format(
        "http://%s.%s.svc.cluster.local:%d",
        getMetadataName(),
        namespace,
        getDruidPort()
    );
  }

  /**
   * Get unique NodePort for this service type.
   * Uses allocated port from PortAllocator if available, otherwise falls back to hardcoded ports.
   */
  protected int getUniqueNodePort()
  {
    if (allocatedNodePort != null) {
      return allocatedNodePort;
    }

    // Fallback to hardcoded ports for backward compatibility
    switch (getDruidServiceType()) {
      case "coordinator":
        return 30088;
      case "broker":
        return 30089;
      case "router":
        return 30090;
      case "historical":
        return 30091;
      case "overlord":
        return 30092;
      default:
        return 30080 + getDruidServiceType().hashCode() % 10;
    }
  }

  /**
   * Get the external URL for this Druid component accessible from tests.
   * This method uses NodePort services accessible via K3s container.
   *
   * @param k3sContainer the K3s container instance
   * @return the external URL accessible from tests
   */
  public String getExternalUrl(K3sContainer k3sContainer)
  {
    int nodePort = getUniqueNodePort();
    int mappedPort = k3sContainer.getMappedPort(nodePort);
    String host = "0.0.0.0";
    log.info("Using NodePort %d mapped to %s:%d for %s service", nodePort, host, mappedPort, getDruidServiceType());
    return StringUtils.format("http://%s:%d", host, mappedPort);
  }


  /**
   * Get the runtime properties specific to this Druid service.
   *
   * @return the runtime properties as a Properties object
   */
  public abstract Properties getRuntimeProperties();

  /**
   * Convert Properties object to string format suitable for Druid configuration.
   *
   * @param properties the properties to convert
   * @return the properties as a formatted string
   */
  protected String propertiesToString(Properties properties)
  {
    StringWriter writer = new StringWriter();
    try {
      properties.store(writer, null);
      // Remove the generated timestamp comment line and clean up
      String result = writer.toString();
      String[] lines = result.split("\n");
      StringBuilder sb = new StringBuilder();
      for (String line : lines) {
        if (!line.startsWith("#")) {
          sb.append(line).append("\n");
        }
      }
      return sb.toString().trim();
    }
    catch (Exception e) {
      throw new RuntimeException("Failed to convert properties to string", e);
    }
  }

  /**
   * Get the runtime properties as a formatted string.
   *
   * @return the runtime properties as a string
   */
  public String getRuntimePropertiesAsString()
  {
    return propertiesToString(getRuntimeProperties());
  }

  /**
   * Get the JVM options for this service.
   *
   * @return the JVM options
   */
  public abstract String getJvmOptions();

  /**
   * Get the node configuration for this service to be used in the Druid YAML.
   *
   * @return a map representing the node configuration
   */
  public abstract Map<String, Object> getNodeConfig();

  protected Map<String, Object> getCommonNodeConfig()
  {
    Map<String, Object> nodeConfig = new HashMap<>();

    Map<String, Object> serviceSpec = new HashMap<>();
    serviceSpec.put("type", "NodePort");
    serviceSpec.put(
        "ports", List.of(Map.of(
            "name", "http",
            "port", getDruidPort(),
            "targetPort", getDruidPort(),
            "protocol", "TCP",
            "nodePort", getUniqueNodePort()
        ))
    );
    nodeConfig.put("services", List.of(Map.of("spec", serviceSpec)));

    // Add shared storage volume configuration
    if (needsSharedStorage()) {
      nodeConfig.put("volumes", createSharedStorageVolumes());
      nodeConfig.put("volumeMounts", createSharedStorageVolumeMounts());
    }

    return nodeConfig;
  }

  /**
   * Determine if this component needs shared storage.
   * Override in subclasses that don't need shared storage.
   */
  protected boolean needsSharedStorage()
  {
    return true;
  }

  /**
   * Create shared storage volume configuration.
   */
  protected List<Map<String, Object>> createSharedStorageVolumes()
  {
    Map<String, Object> storageVolume = new HashMap<>();
    storageVolume.put("name", "druid-shared-storage");
    storageVolume.put(
        "hostPath", Map.of(
            "path", getSharedStorageDirectory(),
            "type", "DirectoryOrCreate"
        )
    );

    return List.of(storageVolume);
  }

  /**
   * Create shared storage volume mount configuration.
   */
  protected List<Map<String, Object>> createSharedStorageVolumeMounts()
  {
    Map<String, Object> storageMount = new HashMap<>();
    storageMount.put("name", "druid-shared-storage");
    storageMount.put("mountPath", "/druid/data");

    return List.of(storageMount);
  }

  public int getReplicas()
  {
    return 1;
  }

  public int getReadyTimeoutSeconds()
  {
    return POD_READY_CHECK_TIMEOUT_SECONDS;
  }

  /**
   * Get common Druid configuration that applies to all services.
   * For the first cut, this is static
   */
  protected String getCommonDruidProperties()
  {
    return StringUtils.format(
        "# Zookeeper-less Druid Cluster\n"
        +
        "druid.zk.service.enabled=false\n"
        +
        "druid.discovery.type=k8s\n"
        +
        "druid.discovery.k8s.clusterIdentifier=%s\n"
        +
        "druid.serverview.type=http\n"
        +
        "druid.coordinator.loadqueuepeon.type=http\n"
        +
        "druid.indexer.runner.type=httpRemote\n"
        +
        "\n"
        +
        "# Metadata Store (Derby for testing)\n"
        +
        "druid.metadata.storage.type=derby\n"
        +
        "druid.metadata.storage.connector.connectURI=jdbc:derby://localhost:1527/var/druid/metadata.db;create=true\n"
        +
        "druid.metadata.storage.connector.host=localhost\n"
        +
        "druid.metadata.storage.connector.port=1527\n"
        +
        "druid.metadata.storage.connector.createTables=true\n"
        +
        "\n"
        +
        "# Shared storage configuration\n"
        +
        "druid.storage.type=local\n"
        +
        "druid.storage.storageDirectory=/druid/data/segments\n"
        +
        "\n"
        +
        "# Extensions\n"
        +
        "druid.extensions.loadList=[\"druid-kubernetes-overlord-extensions\", \"druid-kubernetes-extensions\", \"druid-datasketches\"]\n"
        +
        "\n"
        +
        "# Service discovery\n"
        +
        "druid.selectors.indexing.serviceName=druid/overlord\n"
        +
        "druid.selectors.coordinator.serviceName=druid/coordinator\n"
        +
        "# Indexing logs\n"
        +
        "druid.indexer.logs.type=file\n"
        +
        "druid.indexer.logs.directory=/druid/data/indexing-logs\n",
        clusterName
    );
  }

  /**
   * Apply the Druid YAML manifest for this component.
   */
  public void applyDruidManifest(KubernetesClient client)
  {
    GenericKubernetesResource druidResource = createGenericDruidResource();
    customizeDruidResource(druidResource);
    client.resource(druidResource).inNamespace(namespace).create();
  }

  protected GenericKubernetesResource createGenericDruidResource()
  {
    GenericKubernetesResource druidCR = new GenericKubernetesResource();

    druidCR.setApiVersion("druid.apache.org/v1alpha1");
    druidCR.setKind("Druid");

    ObjectMeta metadata = new ObjectMeta();
    metadata.setName(getMetadataName());
    metadata.setNamespace(namespace);
    druidCR.setMetadata(metadata);

    Map<String, Object> spec = createDruidSpec();
    druidCR.setAdditionalProperties(Map.of("spec", spec));
    return druidCR;
  }

  /**
   * Create the Druid spec configuration programmatically.
   */
  protected Map<String, Object> createDruidSpec()
  {
    Map<String, Object> spec = new HashMap<>();

    spec.put("image", druidImage);
    spec.put("startScript", "/druid.sh");
    spec.put("scalePvcSts", true);
    spec.put("rollingDeploy", true);
    spec.put("defaultProbes", false);

    spec.put(
        "podLabels", Map.of(
            "environment", "stage",
            "release", "alpha"
        )
    );
    spec.put(
        "podAnnotations", Map.of(
            "dummy", "k8s_extn_needs_atleast_one_annotation"
        )
    );

    spec.put(
        "securityContext", Map.of(
            "fsGroup", 0,
            "runAsUser", 0,
            "runAsGroup", 0
        )
    );
    spec.put(
        "containerSecurityContext", Map.of(
            "privileged", true
        )
    );

    spec.put(
        "services", List.of(Map.of(
            "spec", Map.of(
                "type", "ClusterIP",
                "clusterIP", "None"
            )
        ))
    );

    spec.put("commonConfigMountPath", "/opt/druid/conf/druid/cluster/_common");
    spec.put("log4j.config", createLog4jConfig());
    spec.put("env", createEnvironmentVariables());
    spec.put("common.runtime.properties", getCommonDruidProperties());

    Map<String, Object> nodes = new HashMap<>();
    nodes.put(getNodeName(), getNodeConfig());
    spec.put("nodes", nodes);

    return spec;
  }

  /**
   * Create Log4j configuration.
   */
  protected String createLog4jConfig()
  {
    return "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n" +
           "<Configuration status=\"TRACE\">\n" +
           "    <Appenders>\n" +
           "        <Console name=\"Console\" target=\"SYSTEM_OUT\">\n" +
           "            <JsonLayout properties=\"true\"/>\n" +
           "        </Console>\n" +
           "    </Appenders>\n" +
           "    <Loggers>\n" +
           "        <Root level=\"info\">\n" +
           "            <AppenderRef ref=\"Console\"/>\n" +
           "        </Root>\n" +
           "    </Loggers>\n" +
           "</Configuration>";
  }

  protected List<Map<String, Object>> createEnvironmentVariables()
  {
    return List.of(
        Map.of(
            "name", "POD_NAME",
            "valueFrom", Map.of(
                "fieldRef", Map.of(
                    "fieldPath", "metadata.name"
                )
            )
        ),
        Map.of(
            "name", "POD_NAMESPACE",
            "valueFrom", Map.of(
                "fieldRef", Map.of(
                    "fieldPath", "metadata.namespace"
                )
            )
        )
    );
  }

  /**
   * Customize the Druid resource YAML for this specific component.
   */
  protected void customizeDruidResource(GenericKubernetesResource druidResource)
  {
    Map<String, Object> spec = (Map<String, Object>) druidResource.getAdditionalProperties().get("spec");
    Map<String, Object> nodes = new HashMap<>();
    nodes.put(getNodeName(), getNodeConfig());
    spec.put("nodes", nodes);
  }

  /**
   * Get the metadata name for this Druid custom resource.
   *
   * @return the metadata name
   */
  protected String getMetadataName()
  {
    return clusterName + "-" + getDruidServiceType();
  }

  /**
   * Get the node name for this service type in the YAML configuration.
   *
   * @return the node name
   */
  public String getNodeName()
  {
    return getDruidServiceType();
  }

  @Override
  public void waitUntilReady(KubernetesClient client)
  {
    try {
      String labelValue = getPodLabel();
      String componentName = getMetadataName();

      client.pods()
            .inNamespace(namespace)
            .withLabel("druid_cr", componentName)
            .withLabel("nodeSpecUniqueStr", labelValue)
            .waitUntilReady(getReadyTimeoutSeconds(), TimeUnit.SECONDS);
    }
    catch (Exception e) {
      log.error("Timeout waiting for Druid %s to be ready", getDruidServiceType());
      throw e;
    }
  }

  public String getPodLabel()
  {
    return "druid-" + getMetadataName() + "-" + getDruidServiceType();
  }

  @Override
  public void cleanup(KubernetesClient client)
  {
    try {
      client.genericKubernetesResources("druid.apache.org/v1alpha1", "Druid")
            .inNamespace(namespace)
            .withName(getMetadataName())
            .delete();
    }
    catch (Exception e) {
      log.error("Error during %s cleanup: %s", getDruidServiceType(), e.getMessage());
    }
  }

  @Override
  public String getComponentName()
  {
    return "Druid" + getDruidServiceType().substring(0, 1).toUpperCase(Locale.ENGLISH) + getDruidServiceType().substring(1);
  }

  /**
   * Get default readiness probe configuration using /status/health endpoint.
   * Subclasses can override this if they need different probe configuration.
   */
  protected Map<String, Object> getReadinessProbe()
  {
    Map<String, Object> readinessProbe = new HashMap<>();
    readinessProbe.put("failureThreshold", 20);
    Map<String, Object> readinessHttpGet = new HashMap<>();
    readinessHttpGet.put("path", "/status/health");
    readinessHttpGet.put("port", getDruidPort());
    readinessProbe.put("httpGet", readinessHttpGet);
    readinessProbe.put("initialDelaySeconds", 10);
    readinessProbe.put("periodSeconds", 10);
    readinessProbe.put("successThreshold", 1);
    readinessProbe.put("timeoutSeconds", 5);
    return readinessProbe;
  }

  /**
   * Get default liveness probe configuration using /status/health endpoint.
   * Subclasses can override this if they need different probe configuration.
   */
  protected Map<String, Object> getLivenessProbe()
  {
    Map<String, Object> livenessProbe = new HashMap<>();
    livenessProbe.put("failureThreshold", 5);
    Map<String, Object> livenessHttpGet = new HashMap<>();
    livenessHttpGet.put("path", "/status/health");
    livenessHttpGet.put("port", getDruidPort());
    livenessProbe.put("httpGet", livenessHttpGet);
    livenessProbe.put("initialDelaySeconds", 30);
    livenessProbe.put("periodSeconds", 30);
    livenessProbe.put("successThreshold", 1);
    livenessProbe.put("timeoutSeconds", 5);
    return livenessProbe;
  }

}
