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
import com.google.common.base.Preconditions;

import java.util.Objects;

public class DefaultKubernetesTaskRunnerDynamicConfig implements KubernetesTaskRunnerDynamicConfig
{
  private final PodTemplateSelectStrategy podTemplateSelectStrategy;

  @JsonCreator
  public DefaultKubernetesTaskRunnerDynamicConfig(
      @JsonProperty("podTemplateSelectStrategy") PodTemplateSelectStrategy podTemplateSelectStrategy
  )
  {
    Preconditions.checkNotNull(podTemplateSelectStrategy);
    this.podTemplateSelectStrategy = podTemplateSelectStrategy;
  }

  @Override
  @JsonProperty
  public PodTemplateSelectStrategy getPodTemplateSelectStrategy()
  {
    return podTemplateSelectStrategy;
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
    return Objects.equals(podTemplateSelectStrategy, that.podTemplateSelectStrategy);
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(podTemplateSelectStrategy);
  }

  @Override
  public String toString()
  {
    return "DefaultKubernetesTaskRunnerDynamicConfig{" +
           "podTemplateSelectStrategy=" + podTemplateSelectStrategy +
           '}';
  }
}
