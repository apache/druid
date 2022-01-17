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
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.server.coordination.ServerType;

import javax.annotation.Nullable;
import java.util.List;
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

  /**
   * This JSON creator requires the subtype key of {@link DruidService} to appear first
   * in the serialized JSON. Deserialization can fail otherwise. As a result,
   * {@link DiscoveryDruidNode} does not deserialize this class directly.
   * Instead, it deserializes the JSON to {@link org.apache.druid.jackson.StringObjectPairList} first,
   * and then DataNodeService. See {@link DiscoveryDruidNode#toMap(List)} for more details.
   */
  @JsonCreator
  public static DataNodeService fromJson(
      @JsonProperty("tier") String tier,
      @JsonProperty("maxSize") long maxSize,
      @JsonProperty("type") @Deprecated @Nullable ServerType type, // see getType() for deprecation.
      @JsonProperty(SERVER_TYPE_PROP_KEY) @Nullable ServerType serverType,
      @JsonProperty("priority") int priority
  )
  {
    if (type == null && serverType == null) {
      throw new IAE("ServerType is missing");
    }
    final ServerType theServerType = serverType == null ? type : serverType;
    return new DataNodeService(tier, maxSize, theServerType, priority);
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

  /**
   * Use {@link #getServerType()} instead.
   *
   * This method is deprecated as its JSON property key is duplicated with the subtype key of {@link DruidService}.
   * This seems to happen to work because the subtype key "type" always appears first in the serialized JSON,
   * which Jackson always picks up as the subtype key. This is not safe though since it will stop working
   * if Jackson behavior changes in the future. See also {@link DiscoveryDruidNode#toMap(List)}.
   *
   * Since entirely removing this property can break compatibility, we just deprecate it for now.
   * However, we should remove it as soon as possible in a future release.
   */
  @Deprecated
  @JsonProperty
  public ServerType getType()
  {
    return serverType;
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
