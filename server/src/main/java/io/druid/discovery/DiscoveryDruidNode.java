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

package io.druid.discovery;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.server.DruidNode;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Representation of all information related to discovery of a node and all the other metadata associated with
 * the node per nodeType such as broker, historical etc.
 * Note that one Druid process might announce multiple DiscoveryDruidNode if it acts as multiple nodeTypes e.g.
 * coordinator would announce DiscoveryDruidNode for overlord nodeType as well when acting as overlord.
 */
public class DiscoveryDruidNode
{
  private final DruidNode druidNode;
  private final String nodeType;

  // Other metadata associated with the node e.g.
  // if its a historical node then lookup information, segment loading capacity etc.
  private final Map<String, DruidService> services = new HashMap<>();

  @JsonCreator
  public DiscoveryDruidNode(
      @JsonProperty("druidNode") DruidNode druidNode,
      @JsonProperty("nodeType") String nodeType,
      @JsonProperty("services") Map<String, DruidService> services
  )
  {
    this.druidNode = druidNode;
    this.nodeType = nodeType;

    if (services != null && !services.isEmpty()) {
      this.services.putAll(services);
    }
  }

  @JsonProperty
  public Map<String, DruidService> getServices()
  {
    return services;
  }

  @JsonProperty
  public String getNodeType()
  {
    return nodeType;
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
           Objects.equals(nodeType, that.nodeType) &&
           Objects.equals(services, that.services);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(druidNode, nodeType, services);
  }

  @Override
  public String toString()
  {
    return "DiscoveryDruidNode{" +
           "druidNode=" + druidNode +
           ", nodeType='" + nodeType + '\'' +
           ", services=" + services +
           '}';
  }
}
