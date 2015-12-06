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

package io.druid.server.coordinator;

import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Ordering;
import io.druid.client.ImmutableDruidServer;

import java.util.Map;

/**
 * Contains a representation of the current state of the cluster by tier.
 * Each tier is mapped to the list of servers for that tier sorted by available space.
 */
public class DruidCluster
{
  private final Map<String, MinMaxPriorityQueue<ServerHolder>> cluster;

  public DruidCluster()
  {
    this.cluster = Maps.newHashMap();
  }

  public DruidCluster(Map<String, MinMaxPriorityQueue<ServerHolder>> cluster)
  {
    this.cluster = cluster;
  }

  public void add(ServerHolder serverHolder)
  {
    ImmutableDruidServer server = serverHolder.getServer();
    MinMaxPriorityQueue<ServerHolder> tierServers = cluster.get(server.getTier());
    if (tierServers == null) {
      tierServers = MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create();
      cluster.put(server.getTier(), tierServers);
    }
    tierServers.add(serverHolder);
  }

  public Map<String, MinMaxPriorityQueue<ServerHolder>> getCluster()
  {
    return cluster;
  }

  public Iterable<String> getTierNames()
  {
    return cluster.keySet();
  }

  public MinMaxPriorityQueue<ServerHolder> getServersByTier(String tier)
  {
    return cluster.get(tier);
  }

  public Iterable<MinMaxPriorityQueue<ServerHolder>> getSortedServersByTier()
  {
    return cluster.values();
  }

  public boolean isEmpty()
  {
    return cluster.isEmpty();
  }

  public boolean hasTier(String tier)
  {
    MinMaxPriorityQueue<ServerHolder> servers = cluster.get(tier);
    return (servers == null) || servers.isEmpty();
  }

  public MinMaxPriorityQueue<ServerHolder> get(String tier)
  {
    return cluster.get(tier);
  }
}
