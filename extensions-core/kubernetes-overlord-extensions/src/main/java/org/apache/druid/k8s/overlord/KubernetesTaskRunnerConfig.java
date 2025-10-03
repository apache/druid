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

import org.joda.time.Period;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

import java.util.List;
import java.util.Map;

public interface KubernetesTaskRunnerConfig
{
  String getNamespace();

  String getOverlordNamespace();

  String getK8sTaskPodNamePrefix();

  boolean isDebugJobs();

  @Deprecated
  boolean isSidecarSupport();

  String getPrimaryContainerName();

  String getKubexitImage();

  Long getGraceTerminationPeriodSeconds();

  boolean isDisableClientProxy();

  Period getTaskTimeout();

  Period getTaskJoinTimeout();

  Period getTaskCleanupDelay();

  Period getTaskCleanupInterval();

  Period getTaskLaunchTimeout();

  Period getLogSaveTimeout();

  List<String> getPeonMonitors();

  List<String> getJavaOptsArray();

  int getCpuCoreInMicro();

  Map<String, String> getLabels();

  Map<String, String> getAnnotations();

  Integer getCapacity();

  static Builder builder()
  {
    return new Builder();
  }

  public static class Builder
  {
    private String namespace;
    private String overlordNamespace;
    private String k8sTaskPodNamePrefix;
    private boolean debugJob;
    private boolean sidecarSupport;
    private String primaryContainerName;
    private String kubexitImage;
    private Long graceTerminationPeriodSeconds;
    private boolean disableClientProxy;
    private Period maxTaskDuration;
    private Period taskCleanupDelay;
    private Period taskCleanupInterval;
    private Period k8sjobLaunchTimeout;
    private List<String> peonMonitors;
    private List<String> javaOptsArray;
    private int cpuCoreInMicro;
    private Map<String, String> labels;
    private Map<String, String> annotations;
    private Integer capacity;
    private Period taskJoinTimeout;
    private Period logSaveTimeout;

    public Builder()
    {
    }

    public Builder withNamespace(String namespace)
    {
      this.namespace = namespace;
      return this;
    }

    public Builder withOverlordNamespace(String overlordNamespace)
    {
      this.overlordNamespace = overlordNamespace;
      return this;
    }

    public Builder withK8sTaskPodNamePrefix(String k8sTaskPodNamePrefix)
    {
      this.k8sTaskPodNamePrefix = k8sTaskPodNamePrefix;
      return this;
    }

    public Builder withDebugJob(boolean debugJob)
    {
      this.debugJob = debugJob;
      return this;
    }

    public Builder withSidecarSupport(boolean sidecarSupport)
    {
      this.sidecarSupport = sidecarSupport;
      return this;
    }

    public Builder withPrimaryContainerName(String primaryContainerName)
    {
      this.primaryContainerName = primaryContainerName;
      return this;
    }

    public Builder withKubexitImage(String kubexitImage)
    {
      this.kubexitImage = kubexitImage;
      return this;
    }

    public Builder withGraceTerminationPeriodSeconds(Long graceTerminationPeriodSeconds)
    {
      this.graceTerminationPeriodSeconds = graceTerminationPeriodSeconds;
      return this;
    }

    public Builder withDisableClientProxy(boolean disableClientProxy)
    {
      this.disableClientProxy = disableClientProxy;
      return this;
    }

    public Builder withTaskTimeout(Period taskTimeout)
    {
      this.maxTaskDuration = taskTimeout;
      return this;
    }

    public Builder withTaskCleanupDelay(Period taskCleanupDelay)
    {
      this.taskCleanupDelay = taskCleanupDelay;
      return this;
    }

    public Builder withTaskCleanupInterval(Period taskCleanupInterval)
    {
      this.taskCleanupInterval = taskCleanupInterval;
      return this;
    }

    public Builder withK8sJobLaunchTimeout(Period k8sjobLaunchTimeout)
    {
      this.k8sjobLaunchTimeout = k8sjobLaunchTimeout;
      return this;
    }

    public Builder withPeonMonitors(List<String> peonMonitors)
    {
      this.peonMonitors = peonMonitors;
      return this;
    }

    public Builder withCpuCore(int cpuCore)
    {
      this.cpuCoreInMicro = cpuCore;
      return this;
    }

    public Builder withJavaOptsArray(List<String> javaOptsArray)
    {
      this.javaOptsArray = javaOptsArray;
      return this;
    }

    public Builder withLabels(Map<String, String> labels)
    {
      this.labels = labels;
      return this;
    }

    public Builder withAnnotations(Map<String, String> annotations)
    {
      this.annotations = annotations;
      return this;
    }


    public Builder withCapacity(@Min(0) @Max(Integer.MAX_VALUE) Integer capacity)
    {
      this.capacity = capacity;
      return this;
    }

    public Builder withTaskJoinTimeout(Period taskJoinTimeout)
    {
      this.taskJoinTimeout = taskJoinTimeout;
      return this;
    }

    public Builder withLogSaveTimeout(Period logSaveTimeout)
    {
      this.logSaveTimeout = logSaveTimeout;
      return this;
    }

    public KubernetesTaskRunnerStaticConfig build()
    {
      return new KubernetesTaskRunnerStaticConfig(
          this.namespace,
          this.overlordNamespace,
          this.k8sTaskPodNamePrefix,
          this.debugJob,
          this.sidecarSupport,
          this.primaryContainerName,
          this.kubexitImage,
          this.graceTerminationPeriodSeconds,
          this.disableClientProxy,
          this.maxTaskDuration,
          this.taskCleanupDelay,
          this.taskCleanupInterval,
          this.k8sjobLaunchTimeout,
          this.logSaveTimeout,
          this.peonMonitors,
          this.javaOptsArray,
          this.cpuCoreInMicro,
          this.labels,
          this.annotations,
          this.capacity,
          this.taskJoinTimeout
      );
    }
  }
}
