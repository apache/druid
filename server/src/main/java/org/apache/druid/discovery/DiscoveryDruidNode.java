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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidTypeIdException;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.DruidNode;

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
  private static final TypeReference<Map<String, Object>> RAW_DRUID_SERVICE_TYPE =
      new TypeReference<Map<String, Object>>()
      {
      };

  private final DruidNode druidNode;
  private final NodeRole nodeRole;

  /**
   * Map of service name -> DruidServices.
   * This map has only the DruidServices that is understandable.
   * It means, if there is some DruidService not understandable found while converting rawServices to services,
   * that DruidService will be ignored and not stored in this map.
   *
   * @see DruidNodeDiscoveryProvider#SERVICE_TO_NODE_TYPES
   */
  private final Map<String, DruidService> services = new HashMap<>();

  public DiscoveryDruidNode(
      DruidNode druidNode,
      NodeRole nodeRole,
      Map<String, DruidService> services
  )
  {
    this.druidNode = druidNode;
    this.nodeRole = nodeRole;

    if (services != null && !services.isEmpty()) {
      this.services.putAll(services);
    }
  }

  @JsonCreator
  private static DiscoveryDruidNode fromJson(
      @JsonProperty("druidNode") DruidNode druidNode,
      @JsonProperty("nodeType") NodeRole nodeRole,
      @JsonProperty("services") Map<String, Map<String, Object>> rawServices,
      @JacksonInject ObjectMapper jsonMapper
  )
  {
    Map<String, DruidService> services = new HashMap<>();
    if (rawServices != null && !rawServices.isEmpty()) {
      for (Entry<String, Map<String, Object>> entry : rawServices.entrySet()) {
        try {
          services.put(entry.getKey(), jsonMapper.convertValue(entry.getValue(), DruidService.class));
        }
        catch (IllegalArgumentException e) {
          if (e.getCause().getClass() == InvalidTypeIdException.class) {
            LOG.info("Ingore unparseable DruidService: %s", entry.getValue());
          } else {
            throw e;
          }
        }
      }
    }
    return new DiscoveryDruidNode(druidNode, nodeRole, services);
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
           Objects.equals(services, that.services);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(druidNode, nodeRole, services);
  }

  @Override
  public String toString()
  {
    return "DiscoveryDruidNode{" +
           "druidNode=" + druidNode +
           ", nodeRole='" + nodeRole + '\'' +
           ", services=" + services +
           '}';
  }
}
