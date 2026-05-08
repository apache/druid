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


package org.apache.druid.k8s.overlord;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.javaprop.JavaPropsMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.druid.guice.IndexingServiceModuleHelper;
import org.apache.druid.java.util.common.RE;
import org.joda.time.Period;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Configuration class for {@link MultipleKubernetesTaskRunner} under this path: druid.indexer.runner
 * This means it shares the SAME configuration prefix with {@link KubernetesTaskRunnerStaticConfig}
 */
public class MultipleKubernetesTaskRunnerConfig extends KubernetesTaskRunnerStaticConfig
{
  /**
   * Per k8s cluster configuration
   */
  public static class KubernetesCluster
  {
    /**
     * The name of the cluster.
     * If it's NOT empty, it will be injected into the task context by {@link MultipleKubernetesTaskRunner} for pod template selection.
     * The name MUST be unique across multiple clusters.
     */
    private final String name;
    private final String kubeconfigPath;
    private final String taskNamespace;

    private final String overlordIdentifier;

    /**
     * Disable schedule new tasks to this cluster, but still monitor existing tasks.
     */
    @JsonProperty
    private final boolean disabled;

    @JsonCreator
    public KubernetesCluster(
        @Nullable @JsonProperty("name") String name,
        @Nonnull @JsonProperty("kubeconfigPath") String kubeconfigPath,
        @Nonnull @JsonProperty("taskNamespace") String taskNamespace,
        @Nonnull @JsonProperty("overlordIdentifier") String overlordIdentifier,
        @Nullable @JsonProperty("disabled") Boolean disabled
    )
    {
      this.name = name == null ? null : name.trim();
      this.kubeconfigPath = kubeconfigPath;
      this.taskNamespace = Preconditions.checkNotNull(taskNamespace, "taskNamespace cannot be null");
      this.overlordIdentifier = overlordIdentifier;

      // default to false if not set
      this.disabled = disabled != null && disabled;
    }

    public String getName()
    {
      return name;
    }

    public String getKubeconfigPath()
    {
      return kubeconfigPath;
    }

    public String getTaskNamespace()
    {
      return taskNamespace;
    }

    public String getOverlordIdentifier()
    {
      return overlordIdentifier;
    }

    public boolean isDisabled()
    {
      return disabled;
    }
  }

  @JsonProperty
  private final List<KubernetesCluster> clusters;

  @JsonProperty
  private final MultipleKubernetesTaskRunner.KubernetesClusterSelector clusterSelector;

  @JsonCreator
  public MultipleKubernetesTaskRunnerConfig(
      @JsonProperty("clusters") List<KubernetesCluster> clusters,
      @JsonProperty("clusterSelector") MultipleKubernetesTaskRunner.KubernetesClusterSelector clusterSelector,
      // Below are properties defined in parent class
      @JsonProperty("namespace") String namespace,
      @JsonProperty("overlordNamespace") String overlordNamespace,
      @JsonProperty("alias") String alias,
      @JsonProperty("debugJobs") Boolean debugJobs,
      @JsonProperty("sidecarSupport") Boolean sidecarSupport,
      @JsonProperty("primaryContainerName") String primaryContainerName,
      @JsonProperty("kubexitImage") String kubexitImage,
      @JsonProperty("graceTerminationPeriodSeconds") Long graceTerminationPeriodSeconds,
      @JsonProperty("disableClientProxy") Boolean disableClientProxy,
      @JsonProperty("maxTaskDuration") Period maxTaskDuration,
      @JsonProperty("taskCleanupDelay") Period taskCleanupDelay,
      @JsonProperty("taskCleanupInterval") Period taskCleanupInterval,
      @JsonProperty("k8sjobLaunchTimeout") Period k8sjobLaunchTimeout,
      @JsonProperty("peonMonitors") List<String> peonMonitors,
      @JsonProperty("javaOptsArray") List<String> javaOptsArray,
      @JsonProperty("cpuCoreInMicro") Integer cpuCoreInMicro,
      @JsonProperty("labels") Map<String, String> labels,
      @JsonProperty("annotations") Map<String, String> annotations,
      @JsonProperty("capacity") Integer capacity,
      @JsonProperty("logSaveTimeout") Period logSaveTimeout,
      @JsonProperty("taskJoinTimeout") Period taskJoinTimeout,
      @JsonProperty("useK8sSharedInformers") Boolean useK8sSharedInformers,
      @JsonProperty("k8sSharedInformerResyncPeriod") Period k8sSharedInformerResyncPeriod
  )
  {
    super(
        namespace,
        overlordNamespace,
        alias,
        debugJobs != null && debugJobs,
        sidecarSupport != null && sidecarSupport,
        primaryContainerName,
        kubexitImage,
        graceTerminationPeriodSeconds,
        disableClientProxy != null && disableClientProxy,
        maxTaskDuration,
        taskCleanupDelay,
        taskCleanupInterval,
        k8sjobLaunchTimeout,
        logSaveTimeout,
        peonMonitors,
        javaOptsArray,
        cpuCoreInMicro != null ? cpuCoreInMicro : 0,
        labels,
        annotations,
        capacity,
        taskJoinTimeout,
        useK8sSharedInformers != null && useK8sSharedInformers,
        k8sSharedInformerResyncPeriod
    );
    Preconditions.checkNotNull(clusters);
    Preconditions.checkState(
        !clusters.isEmpty(),
        "No K8S cluster configuration provided under '%s' configuration properties",
        IndexingServiceModuleHelper.INDEXER_RUNNER_PROPERTY_PREFIX
    );
    this.clusters = clusters;
    this.clusterSelector = clusterSelector == null ? new MultipleKubernetesTaskRunner.RoundRobinSelector() : clusterSelector;
  }

  public List<KubernetesCluster> getClusters()
  {
    return clusters;
  }

  public MultipleKubernetesTaskRunner.KubernetesClusterSelector getClusterSelector()
  {
    return clusterSelector;
  }

  @VisibleForTesting
  static MultipleKubernetesTaskRunnerConfig fromProperties(Properties properties)
  {
    return fromProperties(new ObjectMapper().configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false)
                              .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false),
                          properties);
  }

  /**
   * Using JavaPropsMapper to parse properties so that we can support properties config formats.
   * property config format is much simpler for array/map config.
   * <p>
   * Special handling for labels and annotations properties:
   * <p>
   * Option 1 - JSON format (recommended for complex keys with dots):
   * - If the value starts with '{', it's treated as JSON and parsed directly
   * - This allows complex map keys containing dots and special characters
   * - Example: druid.indexer.runner.labels={"lxcfs-admission-webhook.k8s.io/no-mutating":""}
   * - Can be set via environment variable: druid_indexer_runner_labels='{"key":"value"}'
   * <p>
   * Option 2 - Dot notation (for simple keys without dots):
   * - Use standard dot notation for simple keys
   * - Example: druid.indexer.runner.labels.simpleKey=simpleValue
   * <p>
   * Arrays continue to work with JavaPropsMapper's bracket notation:
   * - Example: druid.indexer.runner.clusters[0].taskNamespace=namespace1
   */
  public static MultipleKubernetesTaskRunnerConfig fromProperties(ObjectMapper jsonMapper, Properties properties)
  {
    String prefix = IndexingServiceModuleHelper.INDEXER_RUNNER_PROPERTY_PREFIX + ".";

    JsonNode labelNode = null;
    JsonNode annotationNode = null;

    Properties props = new Properties();
    for (String prop : properties.stringPropertyNames()) {
      if (prop.startsWith(prefix)) {
        String key = prop.substring(prefix.length());
        String value = properties.getProperty(prop);

        // Special handling for labels: if value starts with '{', treat as JSON
        if ("labels".equals(key) && value.trim().startsWith("{")) {
          try {
            labelNode = jsonMapper.readTree(value);
            // Don't add to props - we'll handle this in the tree
            continue;
          }
          catch (IOException ignored) {
            // If JSON parsing fails, fall back to treating it as a regular property
          }
        }

        // Special handling for annotations: if value starts with '{', treat as JSON
        if ("annotations".equals(key) && value.trim().startsWith("{")) {
          try {
            annotationNode = jsonMapper.readTree(value);
            // Don't add to props - we'll handle this in the tree
            continue;
          }
          catch (IOException ignored) {
            // If JSON parsing fails, fall back to treating it as a regular property
          }
        }

        props.setProperty(key, value);
      }
    }

    JavaPropsMapper propsMapper = (JavaPropsMapper) new JavaPropsMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);

    try {
      // Step 1: Parse properties to JsonNode (tree model)
      JsonNode tree = propsMapper.readPropertiesAs(props, JsonNode.class);

      // Step 2: If we have JSON labels or annotations, merge them into the tree
      if (labelNode != null) {
        mergeJsonNode((ObjectNode) tree, "labels", (ObjectNode) labelNode);
      }

      if (annotationNode != null) {
        mergeJsonNode((ObjectNode) tree, "annotations", (ObjectNode) annotationNode);
      }

      // Step 3: Deserialize the tree to final config object
      // Reuse propsMapper since it already has all necessary modules (e.g., Joda Time) registered
      return jsonMapper.treeToValue(tree, MultipleKubernetesTaskRunnerConfig.class);
    }
    catch (IOException e) {
      throw new RE(e);
    }
  }

  /**
   * Merge JSON object node into the tree at the specified field.
   * If the field already exists in the tree, merge the JSON values into it (JSON takes precedence).
   * If the field doesn't exist, just set it to the JSON node.
   */
  private static void mergeJsonNode(ObjectNode tree, String fieldName, ObjectNode jsonNode)
  {
    if (tree.has(fieldName) && tree.get(fieldName).isObject()) {
      // Merge with existing field from dot notation
      ObjectNode existing = (ObjectNode) tree.get(fieldName);

      // JSON values take precedence
      Iterator<String> fieldNames = jsonNode.fieldNames();
      while (fieldNames.hasNext()) {
        String name = fieldNames.next();
        existing.set(name, jsonNode.get(name));
      }
    } else {
      // No existing field or not an object, just use JSON
      tree.set(fieldName, jsonNode);
    }
  }
}
