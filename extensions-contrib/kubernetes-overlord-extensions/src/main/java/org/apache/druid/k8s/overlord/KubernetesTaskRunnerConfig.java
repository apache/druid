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
import com.google.common.collect.Lists;
import org.joda.time.Period;

import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KubernetesTaskRunnerConfig
{

  @JsonProperty
  @NotNull
  public String namespace;

  @JsonProperty
  public boolean debugJobs = false;

  @JsonProperty
  public boolean sidecarSupport = false;

  @JsonProperty
  // if this is not set, then the first container in your pod spec is assumed to be the overlord container.
  // usually this is fine, but when you are dynamically adding sidecars like istio, the service mesh could
  // in fact place the istio-proxy container as the first container.  Thus you would specify this value to
  // the name of your primary container.  eg) druid-overlord
  public String primaryContainerName = null;

  @JsonProperty
  // for multi-container jobs, we need this image to shut down sidecars after the main container
  // has completed
  public String kubexitImage = "karlkfi/kubexit:v0.3.2";

  // how much time to wait for preStop hooks to execute
  // lower number speeds up pod termination time to release locks
  // faster, defaults to your k8s setup, usually 30 seconds.
  public Long graceTerminationPeriodSeconds = null;

  @JsonProperty
  // disable using http / https proxy environment variables
  public boolean disableClientProxy;

  @JsonProperty
  @NotNull
  public Period maxTaskDuration = new Period("PT4H");

  @JsonProperty
  @NotNull
  // how long to wait for the jobs to be cleaned up.
  public Period taskCleanupDelay = new Period("P2D");

  @JsonProperty
  @NotNull
  // interval for k8s job cleanup to run
  public Period taskCleanupInterval = new Period("PT10m");

  @JsonProperty
  @NotNull
  // how long to wait for the peon k8s job to launch
  public Period k8sjobLaunchTimeout = new Period("PT1H");

  @JsonProperty
  // ForkingTaskRunner inherits the monitors from the MM, in k8s mode
  // the peon inherits the monitors from the overlord, so if someone specifies
  // a TaskCountStatsMonitor in the overlord for example, the peon process
  // fails because it can not inject this monitor in the peon process.
  public List<String> peonMonitors = new ArrayList<>();

  @JsonProperty
  @NotNull
  public List<String> javaOptsArray;

  @JsonProperty
  @NotNull
  public String classpath = System.getProperty("java.class.path");

  @JsonProperty
  @NotNull
  public Map<String, String> labels = new HashMap<>();

  @JsonProperty
  @NotNull
  public Map<String, String> annotations = new HashMap<>();

  @JsonProperty
  @NotNull
  List<String> allowedPrefixes = Lists.newArrayList(
      "com.metamx",
      "druid",
      "org.apache.druid",
      "user.timezone",
      "file.encoding",
      "java.io.tmpdir",
      "hadoop"
  );

  public static long toMilliseconds(Period period)
  {
    return period.toStandardDuration().getMillis();
  }

}
