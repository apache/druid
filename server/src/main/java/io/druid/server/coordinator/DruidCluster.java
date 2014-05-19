/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
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
