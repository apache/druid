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

import com.google.common.annotations.VisibleForTesting;
import io.druid.client.ImmutableDruidServer;
import io.druid.java.util.common.IAE;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

/**
 * Contains a representation of the current state of the cluster by tier.
 * Each tier is mapped to the list of servers for that tier sorted by available space.
 */
public class DruidCluster
{
  private final Set<ServerHolder> realtimes;
  private final Map<String, NavigableSet<ServerHolder>> historicals;

  public DruidCluster()
  {
    this.realtimes = new HashSet<>();
    this.historicals = new HashMap<>();
  }

  @VisibleForTesting
  public DruidCluster(
      @Nullable Set<ServerHolder> realtimes,
      Map<String, NavigableSet<ServerHolder>> historicals
  )
  {
    this.realtimes = realtimes == null ? new HashSet<>() : new HashSet<>(realtimes);
    this.historicals = historicals;
  }

  public void add(ServerHolder serverHolder)
  {
    switch (serverHolder.getServer().getType()) {
      case HISTORICAL:
        addHistorical(serverHolder);
        break;
      case REALTIME:
        addRealtime(serverHolder);
        break;
      case BRIDGE:
        addHistorical(serverHolder);
        break;
      case INDEXER_EXECUTOR:
        throw new IAE("unsupported server type[%s]", serverHolder.getServer().getType());
      default:
        throw new IAE("unknown server type[%s]", serverHolder.getServer().getType());
    }
  }

  private void addRealtime(ServerHolder serverHolder)
  {
    realtimes.add(serverHolder);
  }

  private void addHistorical(ServerHolder serverHolder)
  {
    final ImmutableDruidServer server = serverHolder.getServer();
    final NavigableSet<ServerHolder> tierServers = historicals.computeIfAbsent(
        server.getTier(),
        k -> new TreeSet<>(Collections.reverseOrder())
    );
    tierServers.add(serverHolder);
  }

  public Set<ServerHolder> getRealtimes()
  {
    return realtimes;
  }

  public Map<String, NavigableSet<ServerHolder>> getHistoricals()
  {
    return historicals;
  }

  public Iterable<String> getTierNames()
  {
    return historicals.keySet();
  }

  public NavigableSet<ServerHolder> getHistoricalsByTier(String tier)
  {
    return historicals.get(tier);
  }

  public Collection<ServerHolder> getAllServers()
  {
    final int historicalSize = historicals.values().stream().mapToInt(Collection::size).sum();
    final int realtimeSize = realtimes.size();
    final List<ServerHolder> allServers = new ArrayList<>(historicalSize + realtimeSize);

    historicals.values().forEach(allServers::addAll);
    allServers.addAll(realtimes);
    return allServers;
  }

  public Iterable<NavigableSet<ServerHolder>> getSortedHistoricalsByTier()
  {
    return historicals.values();
  }

  public boolean isEmpty()
  {
    return historicals.isEmpty() && realtimes.isEmpty();
  }

  public boolean hasHistoricals()
  {
    return !historicals.isEmpty();
  }

  public boolean hasRealtimes()
  {
    return !realtimes.isEmpty();
  }

  public boolean hasTier(String tier)
  {
    NavigableSet<ServerHolder> servers = historicals.get(tier);
    return (servers != null) && !servers.isEmpty();
  }
}
