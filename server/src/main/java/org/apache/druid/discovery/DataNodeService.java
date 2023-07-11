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
 *
 * Note for JSON serialization and deserialization.
 *
 * This class has a bug that it has the "type" property which is the duplicate name
 * with the subtype key of {@link DruidService}. It seems to happen to work
 * if the "type" subtype key appears first in the serialized JSON since
 * Jackson uses the first "type" property as the subtype key.
 * To always enforce this property order, this class does not use the {@link JsonProperty} annotation for serialization,
 * but uses {@link org.apache.druid.jackson.DruidServiceSerializer}.
 * Since this is a hacky-way to not break compatibility, a new "serverType" field is added
 * to replace the deprecated "type" field. Once we completely remove the "type" field from this class,
 * we can remove DruidServiceSerializer as well.
 *
 * The set of properties to serialize is hard-coded in DruidServiceSerializer.
 * If you want to add a new field in this class before we remove the "type" field,
 * you must add a proper handling of that new field in DruidServiceSerializer as well.
 *
 * For deserialization, DruidServices are deserialized as a part of {@link DiscoveryDruidNode}.
 * To handle the bug of the duplicate "type" key, DiscoveryDruidNode first deserializes
 * the JSON to {@link org.apache.druid.jackson.StringObjectPairList},
 * handles the duplicate "type" keys in the StringObjectPairList,
 * and then finally converts it to a DruidService. See {@link DiscoveryDruidNode#toMap(List)}.
 *
 * @see org.apache.druid.jackson.DruidServiceSerializer
 * @see DiscoveryDruidNode#toMap(List)
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
   * This JSON creator requires for the "type" subtype key of {@link DruidService} to appear before
   * the "type" property of this class in the serialized JSON. Deserialization can fail otherwise.
   * See the Javadoc of this class for more details.
   */
  @JsonCreator
  public static DataNodeService fromJson(
      @JsonProperty("tier") String tier,
      @JsonProperty("maxSize") long maxSize,
      @JsonProperty("type") @Deprecated @Nullable ServerType type,
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

  public String getTier()
  {
    return tier;
  }

  public long getMaxSize()
  {
    return maxSize;
  }

  public ServerType getServerType()
  {
    return serverType;
  }

  public int getPriority()
  {
    return priority;
  }

  // leaving the "JsonIgnore" annotation to remember that "discoverable" is ignored in serialization,
  // even though the annotation is not actually used.
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
