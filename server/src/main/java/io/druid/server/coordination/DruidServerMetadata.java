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

  @JsonCreator
  public DruidServerMetadata(
      @JsonProperty("name") String name,
      @JsonProperty("host") String host,
      @JsonProperty("maxSize") long maxSize,
      @JsonProperty("type") String type,
      @JsonProperty("tier") String tier,
      @JsonProperty("priority") int priority
  )
  {
    this.name = name;
    this.host = host;
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

  public boolean isAssignable()
  {
    return getType().equalsIgnoreCase("historical") || getType().equalsIgnoreCase("bridge");
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

    DruidServerMetadata metadata = (DruidServerMetadata) o;

    if (maxSize != metadata.maxSize) {
      return false;
    }
    if (priority != metadata.priority) {
      return false;
    }
    if (host != null ? !host.equals(metadata.host) : metadata.host != null) {
      return false;
    }
    if (name != null ? !name.equals(metadata.name) : metadata.name != null) {
      return false;
    }
    if (tier != null ? !tier.equals(metadata.tier) : metadata.tier != null) {
      return false;
    }
    if (type != null ? !type.equals(metadata.type) : metadata.type != null) {
      return false;
    }

    return true;
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
           ", priority='" + priority + '\'' +
           '}';
  }
}
