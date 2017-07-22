/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.coordination;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 */
public class DruidServerMetadata
{
  private final String name;
  private final String hostAndPort;
  private final String hostAndTlsPort;
  private final long maxSize;
  private final String tier;
  private final ServerType type;
  private final int priority;

  @JsonCreator
  public DruidServerMetadata(
      @JsonProperty("name") String name,
      @JsonProperty("host") String hostAndPort,
      @JsonProperty("hostAndTlsPort") String hostAndTlsPort,
      @JsonProperty("maxSize") long maxSize,
      @JsonProperty("type") ServerType type,
      @JsonProperty("tier") String tier,
      @JsonProperty("priority") int priority
  )
  {
    this.name = name;
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

    if (maxSize != that.maxSize) {
      return false;
    }
    if (priority != that.priority) {
      return false;
    }
    if (name != null ? !name.equals(that.name) : that.name != null) {
      return false;
    }
    if (hostAndPort != null ? !hostAndPort.equals(that.hostAndPort) : that.hostAndPort != null) {
      return false;
    }
    if (hostAndTlsPort != null ? !hostAndTlsPort.equals(that.hostAndTlsPort) : that.hostAndTlsPort != null) {
      return false;
    }
    if (tier != null ? !tier.equals(that.tier) : that.tier != null) {
      return false;
    }
    return type == that.type;
  }

  @Override
  public int hashCode()
  {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (hostAndPort != null ? hostAndPort.hashCode() : 0);
    result = 31 * result + (hostAndTlsPort != null ? hostAndTlsPort.hashCode() : 0);
    result = 31 * result + (int) (maxSize ^ (maxSize >>> 32));
    result = 31 * result + (tier != null ? tier.hashCode() : 0);
    result = 31 * result + (type != null ? type.hashCode() : 0);
    result = 31 * result + priority;
    return result;
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
