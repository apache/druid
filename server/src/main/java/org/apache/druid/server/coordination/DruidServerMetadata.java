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

package org.apache.druid.server.coordination;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 */
public class DruidServerMetadata
{
  private final String name;
  @Nullable
  private final String hostAndPort;
  @Nullable
  private final String hostAndTlsPort;
  private final long maxSize;
  private final long storageSize;
  private final String tier;
  private final ServerType type;
  private final int priority;
  @Nullable
  private final String deploymentGroup;

  public DruidServerMetadata(
      String name,
      @Nullable String hostAndPort,
      @Nullable String hostAndTlsPort,
      long maxSize,
      @Nullable Long storageSize,
      ServerType type,
      String tier,
      int priority
  )
  {
    this(name, hostAndPort, hostAndTlsPort, maxSize, storageSize, type, tier, priority, null);
  }

  // Either hostAndPort or hostAndTlsPort would be null depending on the type of connection.
  @JsonCreator
  public DruidServerMetadata(
      @JsonProperty("name") String name,
      @JsonProperty("host") @Nullable String hostAndPort,
      @JsonProperty("hostAndTlsPort") @Nullable String hostAndTlsPort,
      @JsonProperty("maxSize") long maxSize,
      @JsonProperty("storageSize") @Nullable Long storageSize,
      @JsonProperty("type") ServerType type,
      @JsonProperty("tier") String tier,
      @JsonProperty("priority") int priority,
      @JsonProperty("deploymentGroup") @Nullable String deploymentGroup
  )
  {
    this.name = Preconditions.checkNotNull(name);
    this.hostAndPort = hostAndPort;
    this.hostAndTlsPort = hostAndTlsPort;
    this.maxSize = maxSize;
    // for backwards compatibility, fill in storage size from max size
    this.storageSize = storageSize == null ? maxSize : storageSize;
    this.tier = tier;
    this.type = type;
    this.priority = priority;
    this.deploymentGroup = deploymentGroup;
  }

  @JsonProperty
  public String getName()
  {
    return name;
  }

  public String getHost()
  {
    return getHostAndTlsPort() != null ? getHostAndTlsPort() : getHostAndPort();
  }

  @Nullable
  @JsonProperty("host")
  public String getHostAndPort()
  {
    return hostAndPort;
  }

  @Nullable
  @JsonProperty
  public String getHostAndTlsPort()
  {
    return hostAndTlsPort;
  }

  @JsonProperty
  public long getMaxSize()
  {
    return maxSize;
  }

  @JsonProperty
  public long getStorageSize()
  {
    return storageSize;
  }

  @JsonProperty
  public String getTier()
  {
    return tier;
  }

  @JsonProperty
  public ServerType getType()
  {
    return type;
  }

  @JsonProperty
  public int getPriority()
  {
    return priority;
  }

  /**
   * Operator-set tag identifying the deployment group of this server (e.g. red/black for R/B upgrades).
   * Null means unset. Used by version-aware query routing on the broker.
   * Omitted from JSON when null so older consumers that don't know about this field see unchanged output.
   */
  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getDeploymentGroup()
  {
    return deploymentGroup;
  }

  public boolean isSegmentReplicationTarget()
  {
    return type.isSegmentReplicationTarget();
  }

  public boolean isSegmentBroadcastTarget()
  {
    return type.isSegmentBroadcastTarget();
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

    DruidServerMetadata that = (DruidServerMetadata) o;

    if (!name.equals(that.name)) {
      return false;
    }
    if (!Objects.equals(hostAndPort, that.hostAndPort)) {
      return false;
    }
    if (!Objects.equals(hostAndTlsPort, that.hostAndTlsPort)) {
      return false;
    }
    if (maxSize != that.maxSize) {
      return false;
    }
    if (storageSize != that.storageSize) {
      return false;
    }
    if (!Objects.equals(tier, that.tier)) {
      return false;
    }
    if (type != that.type) {
      return false;
    }
    if (priority != that.priority) {
      return false;
    }
    return Objects.equals(deploymentGroup, that.deploymentGroup);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, hostAndPort, hostAndTlsPort, maxSize, storageSize, tier, type, priority, deploymentGroup);
  }

  @Override
  public String toString()
  {
    return "DruidServerMetadata{" +
           "name='" + name + '\'' +
           ", hostAndPort='" + hostAndPort + '\'' +
           ", hostAndTlsPort='" + hostAndTlsPort + '\'' +
           ", maxSize=" + maxSize +
           ", storageSize=" + storageSize +
           ", tier='" + tier + '\'' +
           ", type=" + type +
           ", priority=" + priority +
           ", deploymentGroup='" + deploymentGroup + '\'' +
           '}';
  }
}
