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

package org.apache.druid.k8s.discovery;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.logger.Logger;
import org.joda.time.Duration;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.regex.Pattern;

public class K8sDiscoveryConfig
{
  private static final Logger LOGGER = new Logger(K8sDiscoveryConfig.class);

  public static final Pattern K8S_RESOURCE_NAME_REGEX = Pattern.compile("[a-z0-9][a-z0-9-]*[a-z0-9]");

  @JsonProperty
  @Nonnull
  private final String clusterIdentifier;

  @JsonProperty
  private final String podNameEnvKey;

  @JsonProperty
  private final String podNamespaceEnvKey;

  @JsonProperty
  private final String coordinatorLeaderElectionConfigMapNamespace;

  @JsonProperty
  private final String overlordLeaderElectionConfigMapNamespace;

  @JsonProperty
  private final Duration leaseDuration;

  @JsonProperty
  private final Duration renewDeadline;

  @JsonProperty
  private final Duration retryPeriod;

  @JsonCreator
  public K8sDiscoveryConfig(
      @JsonProperty("clusterIdentifier") String clusterIdentifier,
      @JsonProperty("podNameEnvKey") String podNameEnvKey,
      @JsonProperty("podNamespaceEnvKey") String podNamespaceEnvKey,
      @JsonProperty("coordinatorLeaderElectionConfigMapNamespace") String coordinatorLeaderElectionConfigMapNamespace,
      @JsonProperty("overlordLeaderElectionConfigMapNamespace") String overlordLeaderElectionConfigMapNamespace,
      @JsonProperty("leaseDuration") Duration leaseDuration,
      @JsonProperty("renewDeadline") Duration renewDeadline,
      @JsonProperty("retryPeriod") Duration retryPeriod
  )
  {
    Preconditions.checkArgument(clusterIdentifier != null && !clusterIdentifier.isEmpty(), "null/empty clusterIdentifier");
    Preconditions.checkArgument(
        K8S_RESOURCE_NAME_REGEX.matcher(clusterIdentifier).matches(),
        "clusterIdentifier[%s] is used in k8s resource name and must match regex[%s]",
        clusterIdentifier,
        K8S_RESOURCE_NAME_REGEX.pattern()
    );
    this.clusterIdentifier = clusterIdentifier;

    this.podNameEnvKey = podNameEnvKey == null ? "POD_NAME" : podNameEnvKey;
    this.podNamespaceEnvKey = podNamespaceEnvKey == null ? "POD_NAMESPACE" : podNamespaceEnvKey;

    this.coordinatorLeaderElectionConfigMapNamespace = coordinatorLeaderElectionConfigMapNamespace;
    this.overlordLeaderElectionConfigMapNamespace = overlordLeaderElectionConfigMapNamespace;

    this.leaseDuration = leaseDuration == null ? Duration.millis(60000) : leaseDuration;
    this.renewDeadline = renewDeadline == null ? Duration.millis(47000) : renewDeadline;
    this.retryPeriod = retryPeriod == null ? Duration.millis(5000) : retryPeriod;
  }

  @JsonProperty
  public String getClusterIdentifier()
  {
    return clusterIdentifier;
  }

  @JsonProperty
  public String getPodNameEnvKey()
  {
    return podNameEnvKey;
  }

  @JsonProperty
  public String getPodNamespaceEnvKey()
  {
    return podNamespaceEnvKey;
  }

  @JsonProperty
  public String getCoordinatorLeaderElectionConfigMapNamespace()
  {
    return coordinatorLeaderElectionConfigMapNamespace;
  }

  @JsonProperty
  public String getOverlordLeaderElectionConfigMapNamespace()
  {
    return overlordLeaderElectionConfigMapNamespace;
  }

  @JsonProperty
  public Duration getLeaseDuration()
  {
    return leaseDuration;
  }

  @JsonProperty
  public Duration getRenewDeadline()
  {
    return renewDeadline;
  }

  @JsonProperty
  public Duration getRetryPeriod()
  {
    return retryPeriod;
  }

  @Override
  public String toString()
  {
    return "K8sDiscoveryConfig{" +
           "clusterIdentifier='" + clusterIdentifier + '\'' +
           ", podNameEnvKey='" + podNameEnvKey + '\'' +
           ", podNamespaceEnvKey='" + podNamespaceEnvKey + '\'' +
           ", coordinatorLeaderElectionConfigMapNamespace='" + coordinatorLeaderElectionConfigMapNamespace + '\'' +
           ", overlordLeaderElectionConfigMapNamespace='" + overlordLeaderElectionConfigMapNamespace + '\'' +
           ", leaseDuration=" + leaseDuration +
           ", renewDeadline=" + renewDeadline +
           ", retryPeriod=" + retryPeriod +
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
    K8sDiscoveryConfig that = (K8sDiscoveryConfig) o;
    return clusterIdentifier.equals(that.clusterIdentifier) &&
           Objects.equals(podNameEnvKey, that.podNameEnvKey) &&
           Objects.equals(podNamespaceEnvKey, that.podNamespaceEnvKey) &&
           Objects.equals(
               coordinatorLeaderElectionConfigMapNamespace,
               that.coordinatorLeaderElectionConfigMapNamespace
           ) &&
           Objects.equals(
               overlordLeaderElectionConfigMapNamespace,
               that.overlordLeaderElectionConfigMapNamespace
           ) &&
           Objects.equals(leaseDuration, that.leaseDuration) &&
           Objects.equals(renewDeadline, that.renewDeadline) &&
           Objects.equals(retryPeriod, that.retryPeriod);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        clusterIdentifier,
        podNameEnvKey,
        podNamespaceEnvKey,
        coordinatorLeaderElectionConfigMapNamespace,
        overlordLeaderElectionConfigMapNamespace,
        leaseDuration,
        renewDeadline,
        retryPeriod
    );
  }
}
