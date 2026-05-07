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
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Contains a representation of the current state of the cluster by tier.
 * Each tier is mapped to the list of servers for that tier sorted by available space.
 */
public class DruidCluster
{
  public static final DruidCluster EMPTY = builder().build();

  private final Set<ServerHolder> realtimes;
  private final Map<String, NavigableSet<ServerHolder>> historicals;
  private final Map<String, NavigableSet<ServerHolder>> managedHistoricals;
  /**
   * Managed historicals indexed by tier and then by deployment group. Only servers with a non-null
   * deploymentGroup appear here. Used to support per-group replication and handoff enforcement.
   */
  private final Map<String, Map<String, NavigableSet<ServerHolder>>> managedHistoricalsByTierAndGroup;
  private final Set<ServerHolder> brokers;
  private final List<ServerHolder> allManagedServers;

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
    this.managedHistoricals = CollectionUtils.mapValues(
        historicals,
        holders -> {
          List<ServerHolder> managedServers = holders.stream()
                                                     .filter(serverHolder -> !serverHolder.isUnmanaged())
                                                     .collect(Collectors.toList());

          return CollectionUtils.newTreeSet(Comparator.naturalOrder(), managedServers);
        }
    );
    this.managedHistoricalsByTierAndGroup = initManagedHistoricalsByTierAndGroup();
    this.brokers = Collections.unmodifiableSet(brokers);
    this.allManagedServers = initAllManagedServers();
  }

  public Set<ServerHolder> getRealtimes()
  {
    return realtimes;
  }

  /**
   * Return all historicals.
   */
  public Map<String, NavigableSet<ServerHolder>> getHistoricals()
  {
    return historicals;
  }

  /**
   * Returns all managed historicals. Managed historicals are historicals which can participate in segment assignment,
   * drop or balancing.
   */
  public Map<String, NavigableSet<ServerHolder>> getManagedHistoricals()
  {
    return managedHistoricals;
  }

  public Set<ServerHolder> getBrokers()
  {
    return brokers;
  }

  public Iterable<String> getTierNames()
  {
    return historicals.keySet();
  }

  public NavigableSet<ServerHolder> getManagedHistoricalsByTier(String tier)
  {
    return managedHistoricals.get(tier);
  }

  /**
   * Returns the distinct non-null deployment groups present among managed historicals in the given tier.
   */
  public Set<String> getDeploymentGroupsForTier(String tier)
  {
    final Map<String, NavigableSet<ServerHolder>> groupMap = managedHistoricalsByTierAndGroup.get(tier);
    return groupMap == null ? Collections.emptySet() : groupMap.keySet();
  }

  /**
   * Returns managed historicals in the given tier that belong to the given deployment group.
   * Returns an empty set if no servers for that (tier, group) pair exist.
   */
  public NavigableSet<ServerHolder> getManagedHistoricalsByTierAndGroup(String tier, String group)
  {
    final Map<String, NavigableSet<ServerHolder>> groupMap = managedHistoricalsByTierAndGroup.get(tier);
    if (groupMap == null) {
      return Collections.emptyNavigableSet();
    }
    final NavigableSet<ServerHolder> servers = groupMap.get(group);
    return servers == null ? Collections.emptyNavigableSet() : servers;
  }

  /**
   * Returns managed historicals in the given tier whose {@code deploymentGroup} is null or is not
   * present in {@code coordinatingVersions}. These are the "uncoordinated" servers whose replica
   * counts roll up to the tier-wide {@link ReplicaCountKey}; they must still be visited by drop /
   * cancellation passes when the rest of the tier is operating in per-group mode.
   */
  public NavigableSet<ServerHolder> getUncoordinatedManagedHistoricalsByTier(
      String tier,
      Set<String> coordinatingVersions
  )
  {
    final NavigableSet<ServerHolder> all = managedHistoricals.get(tier);
    if (all == null || all.isEmpty()) {
      return Collections.emptyNavigableSet();
    }
    if (coordinatingVersions == null || coordinatingVersions.isEmpty()) {
      return all;
    }
    final NavigableSet<ServerHolder> filtered = new TreeSet<>(Comparator.naturalOrder());
    for (ServerHolder server : all) {
      final String group = server.getServer().getMetadata().getDeploymentGroup();
      if (group == null || !coordinatingVersions.contains(group)) {
        filtered.add(server);
      }
    }
    return filtered;
  }

  private Map<String, Map<String, NavigableSet<ServerHolder>>> initManagedHistoricalsByTierAndGroup()
  {
    final Map<String, Map<String, NavigableSet<ServerHolder>>> result = new HashMap<>();
    managedHistoricals.forEach((tier, servers) -> {
      for (ServerHolder server : servers) {
        final String group = server.getServer().getMetadata().getDeploymentGroup();
        if (group != null) {
          result.computeIfAbsent(tier, t -> new HashMap<>())
                .computeIfAbsent(group, g -> new TreeSet<>(Comparator.naturalOrder()))
                .add(server);
        }
      }
    });
    return Collections.unmodifiableMap(result);
  }

  public List<ServerHolder> getAllManagedServers()
  {
    return allManagedServers;
  }

  private List<ServerHolder> initAllManagedServers()
  {
    final int historicalSize = managedHistoricals.values().stream().mapToInt(Collection::size).sum();
    final int realtimeSize = realtimes.size();
    final List<ServerHolder> allManagedServers = new ArrayList<>(historicalSize + realtimeSize);

    managedHistoricals.values().forEach(allManagedServers::addAll);
    allManagedServers.addAll(brokers);
    allManagedServers.addAll(realtimes);
    return allManagedServers;
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
