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

package org.apache.druid.server.coordinator;

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.utils.CollectionUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;

/**
 * Contains a representation of the current state of the cluster by tier.
 * Each tier is mapped to the list of servers for that tier sorted by available space.
 */
public class DruidCluster
{
  public static final DruidCluster EMPTY = builder().build();

  private final Set<ServerHolder> realtimes;
  private final Map<String, NavigableSet<ServerHolder>> historicals;
  private final Set<ServerHolder> brokers;
  private final List<ServerHolder> allServers;

  private DruidCluster(
      Set<ServerHolder> realtimes,
      Map<String, Set<ServerHolder>> historicals,
      Set<ServerHolder> brokers
  )
  {
    this.realtimes = Collections.unmodifiableSet(realtimes);
    this.historicals = CollectionUtils.mapValues(
        historicals,
        holders -> CollectionUtils.newTreeSet(Comparator.naturalOrder(), holders)
    );
    this.brokers = Collections.unmodifiableSet(brokers);
    this.allServers = initAllServers();
  }

  public Set<ServerHolder> getRealtimes()
  {
    return realtimes;
  }

  public Map<String, NavigableSet<ServerHolder>> getHistoricals()
  {
    return historicals;
  }

  public Set<ServerHolder> getBrokers()
  {
    return brokers;
  }

  public Iterable<String> getTierNames()
  {
    return historicals.keySet();
  }

  public NavigableSet<ServerHolder> getHistoricalsByTier(String tier)
  {
    return historicals.get(tier);
  }

  public List<ServerHolder> getAllServers()
  {
    return allServers;
  }

  private List<ServerHolder> initAllServers()
  {
    final int historicalSize = historicals.values().stream().mapToInt(Collection::size).sum();
    final int realtimeSize = realtimes.size();
    final List<ServerHolder> allServers = new ArrayList<>(historicalSize + realtimeSize);

    historicals.values().forEach(allServers::addAll);
    allServers.addAll(brokers);
    allServers.addAll(realtimes);
    return allServers;
  }

  public boolean isEmpty()
  {
    return historicals.isEmpty() && realtimes.isEmpty() && brokers.isEmpty();
  }

  public static Builder builder()
  {
    return new Builder();
  }

  public static class Builder
  {
    private final Set<ServerHolder> realtimes = new HashSet<>();
    private final Map<String, Set<ServerHolder>> historicals = new HashMap<>();
    private final Set<ServerHolder> brokers = new HashSet<>();

    public Builder add(ServerHolder serverHolder)
    {
      switch (serverHolder.getServer().getType()) {
        case BRIDGE:
        case HISTORICAL:
          addHistorical(serverHolder);
          break;
        case REALTIME:
        case INDEXER_EXECUTOR:
          realtimes.add(serverHolder);
          break;
        case BROKER:
          brokers.add(serverHolder);
          break;
        default:
          throw new IAE("unknown server type[%s]", serverHolder.getServer().getType());
      }
      return this;
    }

    public Builder addRealtimes(ServerHolder... realtimeServers)
    {
      realtimes.addAll(Arrays.asList(realtimeServers));
      return this;
    }

    public Builder addBrokers(ServerHolder... brokers)
    {
      this.brokers.addAll(Arrays.asList(brokers));
      return this;
    }

    public Builder addTier(String tier, ServerHolder... historicals)
    {
      this.historicals.computeIfAbsent(tier, t -> new HashSet<>())
                      .addAll(Arrays.asList(historicals));
      return this;
    }

    private void addHistorical(ServerHolder serverHolder)
    {
      final String tier = serverHolder.getServer().getTier();
      historicals.computeIfAbsent(tier, t -> new HashSet<>()).add(serverHolder);
    }

    public DruidCluster build()
    {
      return new DruidCluster(realtimes, historicals, brokers);
    }
  }

}
