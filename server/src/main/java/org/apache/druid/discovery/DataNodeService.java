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

package org.apache.druid.discovery;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.server.coordination.ServerType;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Metadata announced by any node that serves segments.
 */
public class DataNodeService extends DruidService
{
  public static final String DISCOVERY_SERVICE_KEY = "dataNodeService";
  public static final String SERVER_TYPE_PROP_KEY = "serverType";

  private final String tier;
  private final long maxSize;
  private final ServerType serverType;
  private final int priority;
  private final boolean isDiscoverable;

  @JsonCreator
  public static DataNodeService fromJson(
      @JsonProperty("tier") String tier,
      @JsonProperty("maxSize") long maxSize,
      @JsonProperty(SERVER_TYPE_PROP_KEY) @Nullable ServerType serverType,
      @JsonProperty("priority") int priority
  )
  {
    return new DataNodeService(tier, maxSize, serverType, priority);
  }

  public DataNodeService(
      String tier,
      long maxSize,
      ServerType serverType,
      int priority
  )
  {
    this(tier, maxSize, serverType, priority, true);
  }

  public DataNodeService(
      String tier,
      long maxSize,
      ServerType serverType,
      int priority,
      boolean isDiscoverable
  )
  {
    this.tier = tier;
    this.maxSize = maxSize;
    this.serverType = serverType;
    this.priority = priority;
    this.isDiscoverable = isDiscoverable;
  }

  @JsonIgnore
  @Override
  public String getName()
  {
    return DISCOVERY_SERVICE_KEY;
  }

  @JsonProperty
  public String getTier()
  {
    return tier;
  }

  @JsonProperty
  public long getMaxSize()
  {
    return maxSize;
  }

  @JsonProperty
  public ServerType getServerType()
  {
    return serverType;
  }

  @JsonProperty
  public int getPriority()
  {
    return priority;
  }

  @Override
  @JsonIgnore
  public boolean isDiscoverable()
  {
    return isDiscoverable;
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
    DataNodeService that = (DataNodeService) o;
    return maxSize == that.maxSize &&
           priority == that.priority &&
           Objects.equals(tier, that.tier) &&
           serverType == that.serverType;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(tier, maxSize, serverType, priority);
  }

  @Override
  public String toString()
  {
    return "DataNodeService{" +
           "tier='" + tier + '\'' +
           ", maxSize=" + maxSize +
           ", serverType=" + serverType +
           ", priority=" + priority +
           '}';
  }
}
