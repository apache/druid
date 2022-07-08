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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import org.apache.druid.jackson.StringObjectPairList;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.NonnullPair;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.DruidNode;

import java.util.HashMap;
import java.util.List;
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

  private final DruidNode druidNode;
  private final NodeRole nodeRole;

  /**
   * Map of service name -> DruidServices.
   * This map has only the DruidServices that are understandable.
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
      @JsonProperty("services") Map<String, StringObjectPairList> rawServices,
      @JacksonInject ObjectMapper jsonMapper
  )
  {
    Map<String, DruidService> services = new HashMap<>();
    if (rawServices != null && !rawServices.isEmpty()) {
      for (Entry<String, StringObjectPairList> entry : rawServices.entrySet()) {
        List<NonnullPair<String, Object>> val = entry.getValue().getPairs();
        try {
          services.put(entry.getKey(), jsonMapper.convertValue(toMap(val), DruidService.class));
        }
        catch (RuntimeException e) {
          LOG.warn("Ignore unparseable DruidService for [%s]: %s", druidNode.getHostAndPortToUse(), val);
        }
      }
    }
    return new DiscoveryDruidNode(druidNode, nodeRole, services);
  }

  /**
   * A JSON of a {@link DruidService} is deserialized to a Map and then converted to aDruidService
   * to ignore any "unknown" DruidServices to the current node. However, directly deserializing a JSON to a Map
   * is problematic for {@link DataNodeService} as it has duplicate "type" keys in its serialized form.
   * Because of the duplicate key, if we directly deserialize a JSON to a Map, we will lose one of the "type" property.
   * This is definitely a bug of DataNodeService, but, since renaming one of those duplicate keys will
   * break compatibility, DataNodeService still has the deprecated "type" property.
   * See the Javadoc of DataNodeService for more details.
   *
   * This function catches such duplicate keys and rewrites the deprecated "type" to "serverType",
   * so that we don't lose any properties.
   *
   * This method can be removed together when we entirely remove the deprecated "type" property from DataNodeService.
   */
  @Deprecated
  private static Map<String, Object> toMap(List<NonnullPair<String, Object>> pairs)
  {
    final Map<String, Object> map = Maps.newHashMapWithExpectedSize(pairs.size());
    for (NonnullPair<String, Object> pair : pairs) {
      final Object prevVal = map.put(pair.lhs, pair.rhs);
      if (prevVal != null) {
        if ("type".equals(pair.lhs)) {
          if (DataNodeService.DISCOVERY_SERVICE_KEY.equals(prevVal)) {
            map.put("type", prevVal);
            // this one is likely serverType.
            map.put(DataNodeService.SERVER_TYPE_PROP_KEY, pair.rhs);
            continue;
          } else if (DataNodeService.DISCOVERY_SERVICE_KEY.equals(pair.rhs)) {
            // this one is likely serverType.
            map.put(DataNodeService.SERVER_TYPE_PROP_KEY, prevVal);
            continue;
          }
        } else if (DataNodeService.SERVER_TYPE_PROP_KEY.equals(pair.lhs)) {
          // Ignore duplicate "serverType" keys since it can happen
          // when the JSON has both "type" and "serverType" keys for serverType.
          continue;
        }

        if (!prevVal.equals(pair.rhs)) {
          throw new IAE("Duplicate key[%s] with different values: [%s] and [%s]", pair.lhs, prevVal, pair.rhs);
        }
      }
    }
    return map;
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
