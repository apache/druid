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

package org.apache.druid.emitter.dropwizard;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.Objects;


public class DropwizardEmitterConfig
{
  // default to 100 Mb
  private static int DEFAULT_METRICS_REGISTRY_SIZE = 100_000_000;
  @JsonProperty
  private final List<DropwizardReporter> reporters;
  @JsonProperty
  private final String prefix;
  @JsonProperty
  private final Boolean includeHost;
  @JsonProperty
  private final String dimensionMapPath;
  @JsonProperty
  private final List<String> alertEmitters;
  @JsonProperty
  private final int maxMetricsRegistrySize;

  @JsonCreator
  public DropwizardEmitterConfig(
      @JsonProperty("reporters") List<DropwizardReporter> reporters,
      @JsonProperty("prefix") String prefix,
      @JsonProperty("includeHost") Boolean includeHost,
      @JsonProperty("dimensionMapPath") String dimensionMapPath,
      @JsonProperty("alertEmitters") List<String> alertEmitters,
      @JsonProperty("maxMetricsRegistrySize") Integer maxMetricsRegistrySize
  )
  {
    Preconditions.checkArgument(reporters != null && !reporters.isEmpty());
    this.reporters = reporters;
    this.prefix = prefix;
    this.alertEmitters = alertEmitters == null ? Collections.emptyList() : alertEmitters;
    this.includeHost = includeHost != null ? includeHost : true;
    this.dimensionMapPath = dimensionMapPath;
    this.maxMetricsRegistrySize = maxMetricsRegistrySize == null ? DEFAULT_METRICS_REGISTRY_SIZE : maxMetricsRegistrySize;
  }

  @JsonProperty
  public List<DropwizardReporter> getReporters()
  {
    return reporters;
  }

  @JsonProperty
  public String getPrefix()
  {
    return prefix;
  }

  @JsonProperty
  public Boolean getIncludeHost()
  {
    return includeHost;
  }

  @JsonProperty
  public String getDimensionMapPath()
  {
    return dimensionMapPath;
  }

  @JsonProperty
  public List<String> getAlertEmitters()
  {
    return alertEmitters;
  }

  @JsonProperty
  public int getMaxMetricsRegistrySize()
  {
    return maxMetricsRegistrySize;
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
    DropwizardEmitterConfig that = (DropwizardEmitterConfig) o;
    return maxMetricsRegistrySize == that.maxMetricsRegistrySize &&
           Objects.equals(reporters, that.reporters) &&
           Objects.equals(prefix, that.prefix) &&
           Objects.equals(includeHost, that.includeHost) &&
           Objects.equals(dimensionMapPath, that.dimensionMapPath) &&
           Objects.equals(alertEmitters, that.alertEmitters);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(reporters, prefix, includeHost, dimensionMapPath, alertEmitters, maxMetricsRegistrySize);
  }

  @Override
  public String toString()
  {
    return "DropwizardEmitterConfig{" +
           "reporters=" + reporters +
           ", prefix='" + prefix + '\'' +
           ", includeHost=" + includeHost +
           ", dimensionMapPath='" + dimensionMapPath + '\'' +
           ", alertEmitters=" + alertEmitters +
           ", maxMetricsRegistrySize=" + maxMetricsRegistrySize +
           '}';
  }
}
