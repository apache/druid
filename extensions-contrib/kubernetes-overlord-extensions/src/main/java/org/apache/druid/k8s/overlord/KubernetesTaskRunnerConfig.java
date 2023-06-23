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

public class KubernetesTaskRunnerConfig
{
  @JsonProperty
  @NotNull
  private String namespace;

  @JsonProperty
  private boolean debugJobs = false;

  /**
   * Deprecated, please specify adapter type runtime property instead
   *
   * I.E `druid.indexer.runner.k8s.adapter.type: overlordMultiContainer`
   */
  @Deprecated
  @JsonProperty
  private boolean sidecarSupport = false;

  @JsonProperty
  // if this is not set, then the first container in your pod spec is assumed to be the overlord container.
  // usually this is fine, but when you are dynamically adding sidecars like istio, the service mesh could
  // in fact place the istio-proxy container as the first container.  Thus you would specify this value to
  // the name of your primary container.  eg) druid-overlord
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
  // how long to wait for the peon k8s job to launch
  private Period k8sjobLaunchTimeout = new Period("PT1H");

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
  private Map<String, String> labels = ImmutableMap.of();

  @JsonProperty
  @NotNull
  private Map<String, String> annotations = ImmutableMap.of();

  @JsonProperty
  @Min(1)
  @Max(Integer.MAX_VALUE)
  @NotNull
  private Integer capacity = Integer.MAX_VALUE;

  public KubernetesTaskRunnerConfig()
  {
  }

  private KubernetesTaskRunnerConfig(
      @Nonnull String namespace,
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
      List<String> peonMonitors,
      List<String> javaOptsArray,
      Map<String, String> labels,
      Map<String, String> annotations,
      Integer capacity
  )
  {
    this.namespace = namespace;
    this.debugJobs = ObjectUtils.defaultIfNull(
        debugJobs,
        this.debugJobs
    );
    this.sidecarSupport = ObjectUtils.defaultIfNull(
        sidecarSupport,
        this.sidecarSupport
    );
    this.primaryContainerName = ObjectUtils.defaultIfNull(
        primaryContainerName,
        this.primaryContainerName
    );
    this.kubexitImage = ObjectUtils.defaultIfNull(
        kubexitImage,
        this.kubexitImage
    );
    this.graceTerminationPeriodSeconds = ObjectUtils.defaultIfNull(
        graceTerminationPeriodSeconds,
        this.graceTerminationPeriodSeconds
    );
    this.disableClientProxy = disableClientProxy;
    this.maxTaskDuration = ObjectUtils.defaultIfNull(
        maxTaskDuration,
        this.maxTaskDuration
    );
    this.taskCleanupDelay = ObjectUtils.defaultIfNull(
        taskCleanupDelay,
        this.taskCleanupDelay
    );
    this.taskCleanupInterval = ObjectUtils.defaultIfNull(
        taskCleanupInterval,
        this.taskCleanupInterval
    );
    this.k8sjobLaunchTimeout = ObjectUtils.defaultIfNull(
        k8sjobLaunchTimeout,
        this.k8sjobLaunchTimeout
    );
    this.peonMonitors = ObjectUtils.defaultIfNull(
        peonMonitors,
        this.peonMonitors
    );
    this.javaOptsArray = ObjectUtils.defaultIfNull(
        javaOptsArray,
        this.javaOptsArray
    );
    this.labels = ObjectUtils.defaultIfNull(
        labels,
        this.labels
    );
    this.annotations = ObjectUtils.defaultIfNull(
        annotations,
        this.annotations
    );
    this.capacity = ObjectUtils.defaultIfNull(
        capacity,
        this.capacity
    );
  }

  public String getNamespace()
  {
    return namespace;
  }

  public boolean isDebugJobs()
  {
    return debugJobs;
  }

  @Deprecated
  public boolean isSidecarSupport()
  {
    return sidecarSupport;
  }

  public String getPrimaryContainerName()
  {
    return primaryContainerName;
  }

  public String getKubexitImage()
  {
    return kubexitImage;
  }

  public Long getGraceTerminationPeriodSeconds()
  {
    return graceTerminationPeriodSeconds;
  }

  public boolean isDisableClientProxy()
  {
    return disableClientProxy;
  }

  public Period getTaskTimeout()
  {
    return maxTaskDuration;
  }

  public Period getTaskCleanupDelay()
  {
    return taskCleanupDelay;
  }

  public Period getTaskCleanupInterval()
  {
    return taskCleanupInterval;
  }

  public Period getTaskLaunchTimeout()
  {
    return k8sjobLaunchTimeout;
  }

  public List<String> getPeonMonitors()
  {
    return peonMonitors;
  }

  public List<String> getJavaOptsArray()
  {
    return javaOptsArray;
  }

  public Map<String, String> getLabels()
  {
    return labels;
  }

  public Map<String, String> getAnnotations()
  {
    return annotations;
  }

  public Integer getCapacity()
  {
    return capacity;
  }

  public static Builder builder()
  {
    return new Builder();
  }

  public static class Builder
  {
    private String namespace;
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
    private Map<String, String> labels;
    private Map<String, String> annotations;
    private Integer capacity;

    public Builder()
    {
    }

    public Builder withNamespace(String namespace)
    {
      this.namespace = namespace;
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

    public KubernetesTaskRunnerConfig build()
    {
      return new KubernetesTaskRunnerConfig(
          this.namespace,
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
          this.peonMonitors,
          this.javaOptsArray,
          this.labels,
          this.annotations,
          this.capacity
      );
    }
  }
}
