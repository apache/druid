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

package org.apache.druid.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import it.unimi.dsi.fastutil.ints.Int2ObjectRBTreeMap;
import org.apache.druid.client.coordinator.Coordinator;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.client.coordinator.CoordinatorClientImpl;
import org.apache.druid.client.selector.HistoricalFilter;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.error.DruidException;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.query.CloneQueryMode;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.ServiceLocator;
import org.apache.druid.rpc.StandardRetryPolicy;
import org.apache.druid.server.BrokerDynamicConfigResource;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;

import javax.validation.constraints.NotNull;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Broker view of the coordinator dynamic configuration, and its derived values such as target and source clone servers.
 * This class is registered as a managed lifecycle to fetch the coordinator dynamic configuration on startup. Further
 * updates are handled through {@link BrokerDynamicConfigResource}.
 */
public class BrokerViewOfCoordinatorConfig extends BaseBrokerViewOfConfig<CoordinatorDynamicConfig> implements HistoricalFilter
{
  private final CoordinatorClient coordinatorClient;

  @GuardedBy("this")
  private Set<String> targetCloneServers;
  @GuardedBy("this")
  private Set<String> sourceCloneServers;
  @GuardedBy("this")
  private Set<String> turboLoadingNodes;

  @Inject
  public BrokerViewOfCoordinatorConfig(
      @Json final ObjectMapper jsonMapper,
      @EscalatedGlobal final ServiceClientFactory clientFactory,
      @Coordinator final ServiceLocator serviceLocator
  )
  {
    this.coordinatorClient =
        new CoordinatorClientImpl(
            clientFactory.makeClient(
                NodeRole.COORDINATOR.getJsonName(),
                serviceLocator,
                StandardRetryPolicy.builder().maxAttempts(15).build()
            ),
            jsonMapper
        );
  }

  @VisibleForTesting
  public BrokerViewOfCoordinatorConfig(CoordinatorClient coordinatorClient)
  {
    this.coordinatorClient = coordinatorClient;
  }

  @Override
  protected CoordinatorDynamicConfig fetchConfigFromClient() throws Exception
  {
    return coordinatorClient.getCoordinatorDynamicConfig().get();
  }

  @Override
  protected String getConfigTypeName()
  {
    return "coordinator";
  }

  /**
   * Update the config view with a new coordinator dynamic config snapshot. Also updates the source and target clone
   * servers and the turbo-loading nodes based on the new dynamic configuration.
   */
  @Override
  public synchronized void setDynamicConfig(@NotNull CoordinatorDynamicConfig updatedConfig)
  {
    super.setDynamicConfig(updatedConfig);
    final Map<String, String> cloneServers = updatedConfig.getCloneServers();
    this.targetCloneServers = ImmutableSet.copyOf(cloneServers.keySet());
    this.sourceCloneServers = ImmutableSet.copyOf(cloneServers.values());
    this.turboLoadingNodes = ImmutableSet.copyOf(updatedConfig.getTurboLoadingNodes());
  }

  @Override
  public Int2ObjectRBTreeMap<Set<QueryableDruidServer>> getQueryableServers(
      Int2ObjectRBTreeMap<Set<QueryableDruidServer>> historicalServers,
      CloneQueryMode mode
  )
  {
    final Set<String> serversToIgnore;
    final Set<String> turboNodes;
    synchronized (this) {
      serversToIgnore = getCurrentServersToIgnore(mode);
      turboNodes = turboLoadingNodes == null ? Set.of() : turboLoadingNodes;
    }

    if (serversToIgnore.isEmpty() && turboNodes.isEmpty()) {
      return historicalServers;
    }

    // Preserve the comparator from the input map so the tier selection strategy's ordering is respected.
    final Int2ObjectRBTreeMap<Set<QueryableDruidServer>> filteredHistoricals =
        new Int2ObjectRBTreeMap<>(historicalServers.comparator());

    final Set<QueryableDruidServer> deprioritizedTurboHistoricals = new HashSet<>();

    for (int priority : historicalServers.keySet()) {
      final Set<QueryableDruidServer> preferredHistoricals = new HashSet<>();
      for (QueryableDruidServer server : historicalServers.get(priority)) {
        final String host = server.getServer().getHost();
        if (serversToIgnore.contains(host)) {
          // Clone filtering removes the server entirely from the queryable set.
          continue;
        }
        if (turboNodes.contains(host)) {
          // Turbo-loading servers are demoted to a dead-last bucket so queries prefer
          // non-turbo replicas, but still fall back to turbo replicas when no alternative exists.
          deprioritizedTurboHistoricals.add(server);
          continue;
        }
        preferredHistoricals.add(server);
      }
      if (!preferredHistoricals.isEmpty()) {
        filteredHistoricals.put(priority, preferredHistoricals);
      }
    }

    if (!deprioritizedTurboHistoricals.isEmpty()) {
      final int deadLastPriority = computeDeadLastPriority(historicalServers.comparator());
      // If a real tier already occupies MIN/MAX_VALUE, merge rather than overwrite.
      filteredHistoricals.merge(deadLastPriority, deprioritizedTurboHistoricals, (existing, toAdd) -> {
        final Set<QueryableDruidServer> merged = new HashSet<>(existing);
        merged.addAll(toAdd);
        return merged;
      });
    }

    return filteredHistoricals;
  }

  /**
   * Get the set of servers that should not be queried based on the cloneQueryMode parameter.
   */
  @GuardedBy("this")
  private Set<String> getCurrentServersToIgnore(CloneQueryMode cloneQueryMode)
  {
    switch (cloneQueryMode) {
      case PREFERCLONES:
        // Remove servers being cloned targets, so that clones are queried.
        return sourceCloneServers == null ? Set.of() : sourceCloneServers;
      case EXCLUDECLONES:
        // Remove clones, so that only source servers are queried.
        return targetCloneServers == null ? Set.of() : targetCloneServers;
      case INCLUDECLONES:
        // Don't remove either.
        return Set.of();
      default:
        throw DruidException.defensive("Unexpected value of cloneQueryMode[%s]", cloneQueryMode);
    }
  }

  /**
   * Pick a priority value that sorts LAST under the given comparator, so that turbo-loading
   * servers are only considered when no other server has the segment.
   */
  private static int computeDeadLastPriority(Comparator<? super Integer> cmp)
  {
    if (cmp == null) {
      // Natural int ordering (ascending) → MAX_VALUE sorts last.
      return Integer.MAX_VALUE;
    }
    // If the comparator puts higher values first (e.g. reverseOrder), then MIN_VALUE
    // sorts last; otherwise MAX_VALUE sorts last.
    return cmp.compare(1, 0) < 0 ? Integer.MIN_VALUE : Integer.MAX_VALUE;
  }
}
