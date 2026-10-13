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

package org.apache.druid.k8s.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.DynamicConfigProvider;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * Resolves configuration from the labels of the Kubernetes node that this process runs on. For example, a Kafka
 * supervisor can set {@code client.rack} from the node's zone label, which is not known until the task is scheduled.
 * <p>
 * {@link #getConfig()} reads the node the first time it runs and keeps the answer for the life of the process, so a
 * node relabelled later is not picked up until restart.
 */
public class K8sNodeLabelDynamicConfigProvider implements DynamicConfigProvider<String>
{
  private static final Logger log = new Logger(K8sNodeLabelDynamicConfigProvider.class);

  @VisibleForTesting
  static final String DEFAULT_NODE_NAME_VARIABLE = "HOST_NODE_NAME";

  /**
   * Shared so that the labels of this process's node are read once, however many providers are built.
   */
  private static final NodeLabelReader IN_CLUSTER_READER =
      new CachingNodeLabelReader(ApiNodeLabelReader::readFromCluster);

  private final ImmutableMap<String, String> labels;
  private final String nodeNameVariable;
  private final NodeLabelReader nodeLabelReader;
  private final Function<String, String> env;

  @JsonCreator
  public K8sNodeLabelDynamicConfigProvider(
      @JsonProperty("labels") Map<String, String> labels,
      @JsonProperty("nodeNameVariable") @Nullable String nodeNameVariable
  )
  {
    this(labels, nodeNameVariable, IN_CLUSTER_READER, System::getenv);
  }

  @VisibleForTesting
  K8sNodeLabelDynamicConfigProvider(
      Map<String, String> labels,
      @Nullable String nodeNameVariable,
      NodeLabelReader nodeLabelReader,
      Function<String, String> env
  )
  {
    this.labels = ImmutableMap.copyOf(Preconditions.checkNotNull(labels, "labels"));
    this.nodeNameVariable = nodeNameVariable == null ? DEFAULT_NODE_NAME_VARIABLE : nodeNameVariable;
    this.nodeLabelReader = nodeLabelReader;
    this.env = env;
  }

  @JsonProperty("labels")
  public Map<String, String> getLabels()
  {
    return labels;
  }

  @JsonProperty("nodeNameVariable")
  public String getNodeNameVariable()
  {
    return nodeNameVariable;
  }

  @Override
  public Map<String, String> getConfig()
  {
    final String nodeName = env.apply(nodeNameVariable);
    if (nodeName == null || nodeName.isBlank()) {
      log.warn(
          "Environment variable [%s] is not set, so the node this process runs on is unknown. Skipping %s.",
          nodeNameVariable,
          labels.keySet()
      );
      return Collections.emptyMap();
    }

    final Map<String, String> nodeLabels = nodeLabelReader.readLabels(nodeName);
    if (nodeLabels == null) {
      // The reader has already said why.
      return Collections.emptyMap();
    }

    final Map<String, String> config = new HashMap<>();
    for (Map.Entry<String, String> entry : labels.entrySet()) {
      final String value = nodeLabels.get(entry.getValue());
      // Kubernetes allows a label with an empty value, which marks the node without saying anything about it.
      if (value == null || value.isEmpty()) {
        log.warn("Node [%s] has no value for label [%s], skipping key [%s].", nodeName, entry.getValue(), entry.getKey());
        continue;
      }
      config.put(entry.getKey(), value);
    }
    return config;
  }

  @Override
  public String toString()
  {
    return "K8sNodeLabelDynamicConfigProvider{" +
        "labels=" + labels +
        ", nodeNameVariable='" + nodeNameVariable + '\'' +
        '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    K8sNodeLabelDynamicConfigProvider that = (K8sNodeLabelDynamicConfigProvider) o;

    return Objects.equals(labels, that.labels) && Objects.equals(nodeNameVariable, that.nodeNameVariable);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(labels, nodeNameVariable);
  }
}
