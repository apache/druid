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
  private final String host;
  private final long maxSize;
  private final String tier;
  private final String type;
  private final int priority;
  private final String service;
  private final String hostText;
  private final int port;

  @JsonCreator
  public DruidServerMetadata(
      @JsonProperty("name") String name,
      @JsonProperty("host") String host,
      @JsonProperty("maxSize") long maxSize,
      @JsonProperty("type") String type,
      @JsonProperty("tier") String tier,
      @JsonProperty("priority") int priority,
      @JsonProperty("service") String service,
      @JsonProperty("hostText") String hostText,
      @JsonProperty("port") int port
  )
  {
    this.name = name;
    this.host = host;
    this.maxSize = maxSize;
    this.tier = tier;
    this.type = type;
    this.priority = priority;
    this.service = service;
    this.hostText = hostText;
    this.port = port;
  }

  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getHost()
  {
    return host;
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
  public String getType()
  {
    return type;
  }

  @JsonProperty
  public int getPriority()
  {
    return priority;
  }

  @JsonProperty
  public String getService()
  {
    return service;
  }

  @JsonProperty
  public String getHostText()
  {
    return hostText;
  }

  @JsonProperty
  public int getPort()
  {
    return port;
  }

  public boolean isAssignable()
  {
    return type.equalsIgnoreCase("historical");
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
    if (port != that.port) {
      return false;
    }
    if (name != null ? !name.equals(that.name) : that.name != null) {
      return false;
    }
    if (host != null ? !host.equals(that.host) : that.host != null) {
      return false;
    }
    if (tier != null ? !tier.equals(that.tier) : that.tier != null) {
      return false;
    }
    if (type != null ? !type.equals(that.type) : that.type != null) {
      return false;
    }
    if (service != null ? !service.equals(that.service) : that.service != null) {
      return false;
    }
    return hostText != null ? hostText.equals(that.hostText) : that.hostText == null;

  }

  @Override
  public int hashCode()
  {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (host != null ? host.hashCode() : 0);
    result = 31 * result + (int) (maxSize ^ (maxSize >>> 32));
    result = 31 * result + (tier != null ? tier.hashCode() : 0);
    result = 31 * result + (type != null ? type.hashCode() : 0);
    result = 31 * result + priority;
    result = 31 * result + (service != null ? service.hashCode() : 0);
    result = 31 * result + (hostText != null ? hostText.hashCode() : 0);
    result = 31 * result + port;
    return result;
  }

  @Override
  public String toString()
  {
    return "DruidServerMetadata{" +
           "name='" + name + '\'' +
           ", host='" + host + '\'' +
           ", maxSize=" + maxSize +
           ", tier='" + tier + '\'' +
           ", type='" + type + '\'' +
           ", priority=" + priority +
           ", service='" + service + '\'' +
           ", hostText='" + hostText + '\'' +
           ", port=" + port +
           '}';
  }
}
