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

import com.google.common.base.Supplier;
import org.apache.druid.k8s.overlord.execution.KubernetesTaskRunnerDynamicConfig;
import org.apache.druid.k8s.overlord.execution.PodTemplateSelectStrategy;
import org.joda.time.Period;

import java.util.List;
import java.util.Map;

/**
 * Effective config object that combines static {@link KubernetesTaskRunnerConfig}
 * with dynamic overrides from {@link KubernetesTaskRunnerDynamicConfig}.
 */
public class KubernetesTaskRunnerEffectiveConfig implements KubernetesTaskRunnerConfig
{
  private final KubernetesTaskRunnerStaticConfig staticConfig;
  private final Supplier<KubernetesTaskRunnerDynamicConfig> dynamicConfigSupplier;

  public KubernetesTaskRunnerEffectiveConfig(
      KubernetesTaskRunnerStaticConfig staticConfig,
      Supplier<KubernetesTaskRunnerDynamicConfig> dynamicConfigSupplier
  )
  {
    this.staticConfig = staticConfig;
    this.dynamicConfigSupplier = dynamicConfigSupplier;
  }

  @Override
  public String getNamespace()
  {
    return staticConfig.getNamespace();
  }

  @Override
  public String getOverlordNamespace()
  {
    return staticConfig.getOverlordNamespace();
  }

  @Override
  public String getK8sTaskPodNamePrefix()
  {
    return staticConfig.getK8sTaskPodNamePrefix();
  }

  @Override
  public boolean isDebugJobs()
  {
    return staticConfig.isDebugJobs();
  }

  @Override
  public boolean isSidecarSupport()
  {
    return staticConfig.isSidecarSupport();
  }

  @Override
  public String getPrimaryContainerName()
  {
    return staticConfig.getPrimaryContainerName();
  }

  @Override
  public String getKubexitImage()
  {
    return staticConfig.getKubexitImage();
  }

  @Override
  public Long getGraceTerminationPeriodSeconds()
  {
    return staticConfig.getGraceTerminationPeriodSeconds();
  }

  @Override
  public boolean isDisableClientProxy()
  {
    return staticConfig.isDisableClientProxy();
  }

  @Override
  public Period getTaskTimeout()
  {
    return staticConfig.getTaskTimeout();
  }

  @Override
  public Period getTaskJoinTimeout()
  {
    return staticConfig.getTaskJoinTimeout();
  }

  @Override
  public Period getTaskCleanupDelay()
  {
    return staticConfig.getTaskCleanupDelay();
  }

  @Override
  public Period getTaskCleanupInterval()
  {
    return staticConfig.getTaskCleanupInterval();
  }

  @Override
  public Period getTaskLaunchTimeout()
  {
    return staticConfig.getTaskLaunchTimeout();
  }

  @Override
  public Period getLogSaveTimeout()
  {
    return staticConfig.getLogSaveTimeout();
  }

  @Override
  public List<String> getPeonMonitors()
  {
    return staticConfig.getPeonMonitors();
  }

  @Override
  public List<String> getJavaOptsArray()
  {
    return staticConfig.getJavaOptsArray();
  }

  @Override
  public int getCpuCoreInMicro()
  {
    return staticConfig.getCpuCoreInMicro();
  }

  @Override
  public Map<String, String> getLabels()
  {
    return staticConfig.getLabels();
  }

  @Override
  public Map<String, String> getAnnotations()
  {
    return staticConfig.getAnnotations();
  }

  @Override
  public Integer getCapacity()
  {
    if (dynamicConfigSupplier == null || dynamicConfigSupplier.get() == null || dynamicConfigSupplier.get().getCapacity() == null) {
      return staticConfig.getCapacity();
    }
    return dynamicConfigSupplier.get().getCapacity();
  }

  public PodTemplateSelectStrategy getPodTemplateSelectStrategy()
  {
    if (dynamicConfigSupplier == null || dynamicConfigSupplier.get() == null || dynamicConfigSupplier.get().getPodTemplateSelectStrategy() == null) {
      return KubernetesTaskRunnerDynamicConfig.DEFAULT_STRATEGY;
    }
    return dynamicConfigSupplier.get().getPodTemplateSelectStrategy();
  }
}

