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

package org.apache.druid.k8s.overlord.execution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.Objects;

public class DefaultKubernetesTaskRunnerDynamicConfig implements KubernetesTaskRunnerDynamicConfig
{
  @Nullable
  private final PodTemplateSelectStrategy podTemplateSelectStrategy;

  @Nullable
  private final Integer capacity;

  @JsonCreator
  public DefaultKubernetesTaskRunnerDynamicConfig(
      @JsonProperty("podTemplateSelectStrategy") PodTemplateSelectStrategy podTemplateSelectStrategy,
      @JsonProperty("capacity") Integer capacity
  )
  {
    this.podTemplateSelectStrategy = podTemplateSelectStrategy;
    this.capacity = capacity;
  }

  @Override
  @JsonProperty
  public PodTemplateSelectStrategy getPodTemplateSelectStrategy()
  {
    return podTemplateSelectStrategy;
  }

  @Override
  @JsonProperty
  public Integer getCapacity()
  {
    return capacity;
  }

  @Override
  public KubernetesTaskRunnerDynamicConfig merge(KubernetesTaskRunnerDynamicConfig other)
  {
    if (other == null) {
      return this;
    }
    Integer mergeCapacity = getCapacity();
    if (other.getCapacity() != null) {
      mergeCapacity = other.getCapacity();
    }

    PodTemplateSelectStrategy mergePodTemplateSelectStrategy = getPodTemplateSelectStrategy();
    if (other.getPodTemplateSelectStrategy() != null) {
      mergePodTemplateSelectStrategy = other.getPodTemplateSelectStrategy();
    }
    return new DefaultKubernetesTaskRunnerDynamicConfig(mergePodTemplateSelectStrategy, mergeCapacity);
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
    DefaultKubernetesTaskRunnerDynamicConfig that = (DefaultKubernetesTaskRunnerDynamicConfig) o;
    return Objects.equals(capacity, that.capacity) &&
           Objects.equals(podTemplateSelectStrategy, that.podTemplateSelectStrategy);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(podTemplateSelectStrategy, capacity);
  }

  @Override
  public String toString()
  {
    return "DefaultKubernetesTaskRunnerDynamicConfig{" +
           "podTemplateSelectStrategy=" + podTemplateSelectStrategy +
           ", capacity=" + capacity +
           '}';
  }
}
