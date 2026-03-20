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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.client.DruidServer;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.DruidNode;
import org.apache.druid.utils.JvmUtils;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

/**
 * Representation of all information related to discovery of a node and all the other metadata associated with
 * the node per nodeRole such as broker, historical etc.
 * Note that one Druid process might announce multiple DiscoveryDruidNode if it acts in multiple {@link NodeRole}s e. g.
 * Coordinator would announce DiscoveryDruidNode for {@link NodeRole#OVERLORD} as well when acting as Overlord.
 */
public class DiscoveryDruidNode
{
  private static final Logger LOG = new Logger(DiscoveryDruidNode.class);
  private static final int UNKNOWN_VALUE = -1;

  private final DruidNode druidNode;
  private final NodeRole nodeRole;
  private final DateTime startTime;
  private final Integer availableProcessors;
  private final Long totalMemory;

  /**
   * Map of service name -> DruidServices.
   * This map has only the DruidServices that are understandable.
   * It means, if there is some DruidService not understandable found while converting rawServices to services,
   * that DruidService will be ignored and not stored in this map.
   *
   * @see DruidNodeDiscoveryProvider#SERVICE_TO_NODE_TYPES
   */
  private final Map<String, DruidService> services = new HashMap<>();

  /**
   * Constructor for tests. In production, the @Inject constructor is used instead.
   */
  @VisibleForTesting
  public DiscoveryDruidNode(
      DruidNode druidNode,
      NodeRole nodeRole,
      Map<String, DruidService> services,
      DateTime startTime
  )
  {
    this(druidNode, nodeRole, services, startTime, Runtime.getRuntime().availableProcessors(), JvmUtils.getTotalMemory());
  }

  public DiscoveryDruidNode(
      DruidNode druidNode,
      NodeRole nodeRole,
      Map<String, DruidService> services
  )
  {
    this(druidNode, nodeRole, services, DateTimes.nowUtc(), Runtime.getRuntime().availableProcessors(), JvmUtils.getTotalMemory());
  }

  public DiscoveryDruidNode(
      DruidNode druidNode,
      NodeRole nodeRole,
      Map<String, DruidService> services,
      DateTime startTime,
      Integer availableProcessors,
      Long totalMemory
  )
  {
    this.druidNode = druidNode;
    this.nodeRole = nodeRole;

    if (services != null && !services.isEmpty()) {
      this.services.putAll(services);
    }
    this.startTime = startTime;

    // Happens if service is running older version of Druid
    this.availableProcessors = availableProcessors != null ? availableProcessors : UNKNOWN_VALUE;
    this.totalMemory = totalMemory != null ? totalMemory : UNKNOWN_VALUE;
  }

  @JsonCreator
  private static DiscoveryDruidNode fromJson(
      @JsonProperty("druidNode") DruidNode druidNode,
      @JsonProperty("nodeType") NodeRole nodeRole,
      @JsonProperty("services") Map<String, Object> rawServices,
      @JsonProperty("startTime") DateTime startTime,
      @JsonProperty("availableProcessors") Integer availableProcessors,
      @JsonProperty("totalMemory") Long totalMemory,
      @JacksonInject ObjectMapper jsonMapper
  )
  {
    Map<String, DruidService> services = new HashMap<>();
    if (rawServices != null && !rawServices.isEmpty()) {
      for (Entry<String, Object> entry : rawServices.entrySet()) {
        try {
          services.put(entry.getKey(), jsonMapper.convertValue(entry.getValue(), DruidService.class));
        }
        catch (RuntimeException e) {
          LOG.warn("Ignore unparseable DruidService for [%s]: %s", druidNode.getHostAndPortToUse(), entry.getValue());
        }
      }
    }
    return new DiscoveryDruidNode(druidNode, nodeRole, services, startTime, availableProcessors, totalMemory);
  }

  @JsonProperty
  public Map<String, DruidService> getServices()
  {
    return services;
  }

  /**
   * Keeping the legacy name 'nodeType' property name for backward compatibility. When the project is updated to
   * Jackson 2.9 it could be changed, see https://github.com/apache/druid/issues/7152.
   */
  @JsonProperty("nodeType")
  public NodeRole getNodeRole()
  {
    return nodeRole;
  }

  @JsonProperty
  public DruidNode getDruidNode()
  {
    return druidNode;
  }

  @JsonProperty
  public DateTime getStartTime()
  {
    return startTime;
  }

  @JsonProperty
  public Integer getAvailableProcessors()
  {
    return availableProcessors;
  }

  @JsonProperty
  public Long getTotalMemory()
  {
    return totalMemory;
  }

  @Nullable
  @JsonIgnore
  public <T extends DruidService> T getService(String key, Class<T> clazz)
  {
    final DruidService o = services.get(key);
    if (o != null && clazz.isAssignableFrom(o.getClass())) {
      //noinspection unchecked
      return (T) o;
    }
    return null;
  }

  public DruidServer toDruidServer()
  {
    final DataNodeService dataNodeService = getService(
        DataNodeService.DISCOVERY_SERVICE_KEY,
        DataNodeService.class
    );

    final DruidNode druidNode = getDruidNode();
    if (dataNodeService == null || druidNode == null) {
      return null;
    }

    return new DruidServer(
        druidNode.getHostAndPortToUse(),
        druidNode.getHostAndPort(),
        druidNode.getHostAndTlsPort(),
        dataNodeService.getMaxSize(),
        dataNodeService.getStorageSize(),
        dataNodeService.getServerType(),
        dataNodeService.getTier(),
        dataNodeService.getPriority()
    );
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
    DiscoveryDruidNode that = (DiscoveryDruidNode) o;
    return Objects.equals(druidNode, that.druidNode) &&
           Objects.equals(nodeRole, that.nodeRole) &&
           Objects.equals(services, that.services) &&
           Objects.equals(availableProcessors, that.availableProcessors) &&
           Objects.equals(totalMemory, that.totalMemory);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(druidNode, nodeRole, services, availableProcessors, totalMemory);
  }

  @Override
  public String toString()
  {
    return "DiscoveryDruidNode{" +
           "druidNode=" + druidNode +
           ", nodeRole='" + nodeRole + '\'' +
           ", services=" + services + '\'' +
           ", startTime=" + startTime +
           ", availableProcessors=" + availableProcessors +
           ", totalMemory=" + totalMemory +
           '}';
  }
}
