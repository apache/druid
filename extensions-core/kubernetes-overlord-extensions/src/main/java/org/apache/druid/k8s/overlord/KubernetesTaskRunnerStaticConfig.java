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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.ObjectUtils;
import org.joda.time.Period;

import javax.annotation.Nonnull;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

/**
 * This configuration is populated from runtime properties with the prefix
 * {@code druid.indexer.runner}. It is the base configuration that
 * {@link KubernetesTaskRunnerEffectiveConfig} will use if no dynamic config is provided.
 */
public class KubernetesTaskRunnerStaticConfig implements KubernetesTaskRunnerConfig
{
  @JsonProperty
  @NotNull
  private String namespace;

  @JsonProperty
  private String k8sTaskPodNamePrefix = "";

  // This property is the namespace that the Overlord is running in.
  // For cases where we want task pods to run on different namespace from the overlord, we need to specify the namespace of the overlord here.
  // Else, we can simply leave this field alone.
  @JsonProperty
  private String overlordNamespace = "";

  @JsonProperty
  private boolean debugJobs = false;

  /**
   * Deprecated, please specify adapter type runtime property instead
   * <p>
   * I.E `druid.indexer.runner.k8s.adapter.type: overlordMultiContainer`
   */
  @Deprecated
  @JsonProperty
  private boolean sidecarSupport = false;

  @JsonProperty
  // if this is not set, then the first container in your pod spec is assumed to be the overlord container.
  // usually this is fine, but when you are dynamically adding sidecars like istio, the service mesh could
  // in fact place the istio-proxy container as the first container.  Thus, you would specify this value to
  // the name of your primary container.  e.g. druid-overlord
  private String primaryContainerName = null;

  @JsonProperty
  // for multi-container jobs, we need this image to shut down sidecars after the main container
  // has completed
  private String kubexitImage = "karlkfi/kubexit:v0.3.2";

  // how much time to wait for preStop hooks to execute
  // lower number speeds up pod termination time to release locks
  // faster, defaults to your k8s setup, usually 30 seconds.
  private Long graceTerminationPeriodSeconds = null;

  @JsonProperty
  // disable using http / https proxy environment variables
  private boolean disableClientProxy;

  @JsonProperty
  @NotNull
  private Period maxTaskDuration = new Period("PT4H");

  @JsonProperty
  @NotNull
  // how long to wait for the jobs to be cleaned up.
  private Period taskCleanupDelay = new Period("P2D");

  @JsonProperty
  @NotNull
  // interval for k8s job cleanup to run
  private Period taskCleanupInterval = new Period("PT10m");

  @JsonProperty
  @NotNull
  // how long to wait to join peon k8s jobs on startup
  private Period taskJoinTimeout = new Period("PT1M");


  @JsonProperty
  @NotNull
  // how long to wait for the peon k8s job to launch
  private Period k8sjobLaunchTimeout = new Period("PT1H");

  @JsonProperty
  @NotNull
  // how long to wait for log saving operations to complete
  private Period logSaveTimeout = new Period("PT300S");

  @JsonProperty
  // ForkingTaskRunner inherits the monitors from the MM, in k8s mode
  // the peon inherits the monitors from the overlord, so if someone specifies
  // a TaskCountStatsMonitor in the overlord for example, the peon process
  // fails because it can not inject this monitor in the peon process.
  private List<String> peonMonitors = ImmutableList.of();

  @JsonProperty
  @NotNull
  private List<String> javaOptsArray = ImmutableList.of();

  @JsonProperty
  @NotNull
  private int cpuCoreInMicro = 0;

  @JsonProperty
  @NotNull
  private Map<String, String> labels = ImmutableMap.of();

  @JsonProperty
  @NotNull
  private Map<String, String> annotations = ImmutableMap.of();

  @JsonProperty
  @Min(1)
  @Max(Integer.MAX_VALUE)
  @NotNull
  private Integer capacity = Integer.MAX_VALUE;

  public KubernetesTaskRunnerStaticConfig(
      @Nonnull String namespace,
      String overlordNamespace,
      String k8sTaskPodNamePrefix,
      boolean debugJobs,
      boolean sidecarSupport,
      String primaryContainerName,
      String kubexitImage,
      Long graceTerminationPeriodSeconds,
      boolean disableClientProxy,
      Period maxTaskDuration,
      Period taskCleanupDelay,
      Period taskCleanupInterval,
      Period k8sjobLaunchTimeout,
      Period logSaveTimeout,
      List<String> peonMonitors,
      List<String> javaOptsArray,
      int cpuCoreInMicro,
      Map<String, String> labels,
      Map<String, String> annotations,
      Integer capacity,
      Period taskJoinTimeout
  )
  {
    this.namespace = namespace;
    this.overlordNamespace = ObjectUtils.getIfNull(
        overlordNamespace,
        this.overlordNamespace
    );
    this.k8sTaskPodNamePrefix = k8sTaskPodNamePrefix;
    this.debugJobs = ObjectUtils.getIfNull(
        debugJobs,
        this.debugJobs
    );
    this.sidecarSupport = ObjectUtils.getIfNull(
        sidecarSupport,
        this.sidecarSupport
    );
    this.primaryContainerName = ObjectUtils.getIfNull(
        primaryContainerName,
        this.primaryContainerName
    );
    this.kubexitImage = ObjectUtils.getIfNull(
        kubexitImage,
        this.kubexitImage
    );
    this.graceTerminationPeriodSeconds = ObjectUtils.getIfNull(
        graceTerminationPeriodSeconds,
        this.graceTerminationPeriodSeconds
    );
    this.disableClientProxy = disableClientProxy;
    this.maxTaskDuration = ObjectUtils.getIfNull(
        maxTaskDuration,
        this.maxTaskDuration
    );
    this.taskCleanupDelay = ObjectUtils.getIfNull(
        taskCleanupDelay,
        this.taskCleanupDelay
    );
    this.taskCleanupInterval = ObjectUtils.getIfNull(
        taskCleanupInterval,
        this.taskCleanupInterval
    );
    this.k8sjobLaunchTimeout = ObjectUtils.getIfNull(
        k8sjobLaunchTimeout,
        this.k8sjobLaunchTimeout
    );
    this.taskJoinTimeout = ObjectUtils.getIfNull(
        taskJoinTimeout,
        this.taskJoinTimeout
    );
    this.logSaveTimeout = ObjectUtils.getIfNull(
        logSaveTimeout,
        this.logSaveTimeout
    );
    this.peonMonitors = ObjectUtils.getIfNull(
        peonMonitors,
        this.peonMonitors
    );
    this.javaOptsArray = ObjectUtils.getIfNull(
        javaOptsArray,
        this.javaOptsArray
    );
    this.cpuCoreInMicro = ObjectUtils.getIfNull(
        cpuCoreInMicro,
        this.cpuCoreInMicro
    );
    this.labels = ObjectUtils.getIfNull(
        labels,
        this.labels
    );
    this.annotations = ObjectUtils.getIfNull(
        annotations,
        this.annotations
    );
    this.capacity = ObjectUtils.getIfNull(
        capacity,
        this.capacity
    );
  }

  @Override
  public String getNamespace()
  {
    return namespace;
  }

  @Override
  public String getOverlordNamespace()
  {
    return overlordNamespace;
  }

  @Override
  public String getK8sTaskPodNamePrefix()
  {
    return k8sTaskPodNamePrefix;
  }

  @Override
  public boolean isDebugJobs()
  {
    return debugJobs;
  }

  @Override
  @Deprecated
  public boolean isSidecarSupport()
  {
    return sidecarSupport;
  }

  @Override
  public String getPrimaryContainerName()
  {
    return primaryContainerName;
  }

  @Override
  public String getKubexitImage()
  {
    return kubexitImage;
  }

  @Override
  public Long getGraceTerminationPeriodSeconds()
  {
    return graceTerminationPeriodSeconds;
  }

  @Override
  public boolean isDisableClientProxy()
  {
    return disableClientProxy;
  }

  @Override
  public Period getTaskTimeout()
  {
    return maxTaskDuration;
  }

  @Override
  public Period getTaskJoinTimeout()
  {
    return taskJoinTimeout;
  }


  @Override
  public Period getTaskCleanupDelay()
  {
    return taskCleanupDelay;
  }

  @Override
  public Period getTaskCleanupInterval()
  {
    return taskCleanupInterval;
  }

  @Override
  public Period getTaskLaunchTimeout()
  {
    return k8sjobLaunchTimeout;
  }

  @Override
  public Period getLogSaveTimeout()
  {
    return logSaveTimeout;
  }

  @Override
  public List<String> getPeonMonitors()
  {
    return peonMonitors;
  }

  @Override
  public List<String> getJavaOptsArray()
  {
    return javaOptsArray;
  }

  @Override
  public int getCpuCoreInMicro()
  {
    return cpuCoreInMicro;
  }

  @Override
  public Map<String, String> getLabels()
  {
    return labels;
  }

  @Override
  public Map<String, String> getAnnotations()
  {
    return annotations;
  }

  @Override
  public Integer getCapacity()
  {
    return capacity;
  }
}
