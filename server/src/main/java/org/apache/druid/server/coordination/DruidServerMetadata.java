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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 */
public class DruidServerMetadata
{
  private final String name;
  private final String hostAndPort;
  @Nullable
  private final String hostAndTlsPort;
  private final long maxSize;
  private final String tier;
  private final ServerType type;
  private final int priority;

  @JsonCreator
  public DruidServerMetadata(
      @JsonProperty("name") String name,
      @JsonProperty("host") String hostAndPort,
      @JsonProperty("hostAndTlsPort") @Nullable String hostAndTlsPort,
      @JsonProperty("maxSize") long maxSize,
      @JsonProperty("type") ServerType type,
      @JsonProperty("tier") String tier,
      @JsonProperty("priority") int priority
  )
  {
    this.name = Preconditions.checkNotNull(name);
    this.hostAndPort = hostAndPort;
    this.hostAndTlsPort = hostAndTlsPort;
    this.maxSize = maxSize;
    this.tier = tier;
    this.type = type;
    this.priority = priority;
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

  public boolean segmentReplicatable()
  {
    return type.isSegmentReplicationTarget();
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
    if (!Objects.equals(tier, that.tier)) {
      return false;
    }
    if (type != that.type) {
      return false;
    }
    return priority == that.priority;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, hostAndPort, hostAndTlsPort, maxSize, tier, type, priority);
  }

  @Override
  public String toString()
  {
    return "DruidServerMetadata{" +
           "name='" + name + '\'' +
           ", hostAndPort='" + hostAndPort + '\'' +
           ", hostAndTlsPort='" + hostAndTlsPort + '\'' +
           ", maxSize=" + maxSize +
           ", tier='" + tier + '\'' +
           ", type=" + type +
           ", priority=" + priority +
           '}';
  }
}
