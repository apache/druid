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

import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.utils.CollectionUtils;

import javax.annotation.Nullable;
import java.util.ArrayList;
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

/**
 * Contains a representation of the current state of the cluster by tier.
 * Each tier is mapped to the list of servers for that tier sorted by available space.
 */
public class DruidCluster
{
  /** This static factory method must be called only from inside DruidClusterBuilder in tests. */
  @VisibleForTesting
  static DruidCluster createDruidClusterFromBuilderInTest(
      @Nullable Set<ServerHolder> realtimes,
      Map<String, Iterable<ServerHolder>> historicals,
      @Nullable Set<ServerHolder> brokers
  )
  {
    return new DruidCluster(realtimes, historicals, brokers);
  }

  private final Set<ServerHolder> realtimes;
  private final Map<String, NavigableSet<ServerHolder>> historicals;
  private final Set<ServerHolder> brokers;

  public DruidCluster()
  {
    this.realtimes = new HashSet<>();
    this.historicals = new HashMap<>();
    this.brokers = new HashSet<>();
  }

  private DruidCluster(
      @Nullable Set<ServerHolder> realtimes,
      Map<String, Iterable<ServerHolder>> historicals,
      @Nullable Set<ServerHolder> brokers
  )
  {
    this.realtimes = realtimes == null ? new HashSet<>() : new HashSet<>(realtimes);
    this.historicals = CollectionUtils.mapValues(
        historicals,
        holders -> CollectionUtils.newTreeSet(Comparator.reverseOrder(), holders)
    );
    this.brokers = brokers == null ? new HashSet<>() : new HashSet<>(brokers);
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
        addRealtime(serverHolder);
        break;
      case BROKER:
        addBroker(serverHolder);
        break;
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

  private void addBroker(ServerHolder serverHolder)
  {
    brokers.add(serverHolder);
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

  public Collection<ServerHolder> getAllServers()
  {
    final int historicalSize = historicals.values().stream().mapToInt(Collection::size).sum();
    final int realtimeSize = realtimes.size();
    final List<ServerHolder> allServers = new ArrayList<>(historicalSize + realtimeSize);

    historicals.values().forEach(allServers::addAll);
    allServers.addAll(brokers);
    allServers.addAll(realtimes);
    return allServers;
  }

  public Iterable<NavigableSet<ServerHolder>> getSortedHistoricalsByTier()
  {
    return historicals.values();
  }

  public boolean isEmpty()
  {
    return historicals.isEmpty() && realtimes.isEmpty() && brokers.isEmpty();
  }

  public boolean hasHistoricals()
  {
    return !historicals.isEmpty();
  }

  public boolean hasRealtimes()
  {
    return !realtimes.isEmpty();
  }

  public boolean hasBrokers()
  {
    return !brokers.isEmpty();
  }

  public boolean hasTier(String tier)
  {
    NavigableSet<ServerHolder> historicalServers = historicals.get(tier);
    boolean historicalsHasTier = (historicalServers != null) && !historicalServers.isEmpty();
    if (historicalsHasTier) {
      return true;
    }

    return false;
  }
}
