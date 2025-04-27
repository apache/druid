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

import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import it.unimi.dsi.fastutil.ints.Int2ObjectRBTreeMap;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.client.selector.HistoricalFilter;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.CloneQueryMode;
import org.apache.druid.server.BrokerDynamicConfigResource;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;

import javax.validation.constraints.NotNull;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Broker view of the coordinator dynamic configuration, and its derived values such as target and source clone servers.
 * This class is registered as a managed lifecycle to fetch the coordinator dynamic configuration on startup. Further
 * updates are handled through {@link BrokerDynamicConfigResource}.
 */
public class BrokerViewOfCoordinatorConfig implements HistoricalFilter
{
  private static final Logger log = new Logger(BrokerViewOfCoordinatorConfig.class);
  private final CoordinatorClient coordinatorClient;

  @GuardedBy("this")
  private CoordinatorDynamicConfig config;
  @GuardedBy("this")
  private Set<String> cloneServers;
  @GuardedBy("this")
  private Set<String> serversBeingCloned;

  @Inject
  public BrokerViewOfCoordinatorConfig(CoordinatorClient coordinatorClient)
  {
    this.coordinatorClient = coordinatorClient;
  }

  public synchronized CoordinatorDynamicConfig getDynamicConfig()
  {
    return config;
  }

  /**
   * Update the config view with a new coordinator dynamic config snapshot. Also updates the source and target clone
   * servers based on the new dynamic configuration.
   */
  public synchronized void setDynamicConfig(@NotNull CoordinatorDynamicConfig updatedConfig)
  {
    config = updatedConfig;
    final Map<String, String> cloneServers = config.getCloneServers();
    this.cloneServers = ImmutableSet.copyOf(cloneServers.keySet());
    this.serversBeingCloned = ImmutableSet.copyOf(cloneServers.values());
  }

  @LifecycleStart
  public void start()
  {
    try {
      log.info("Fetching coordinator dynamic configuration.");

      CoordinatorDynamicConfig coordinatorDynamicConfig = coordinatorClient.getCoordinatorDynamicConfig().get();
      setDynamicConfig(coordinatorDynamicConfig);

      log.info("Successfully fetched coordinator dynamic config[%s].", coordinatorDynamicConfig);
    }
    catch (Exception e) {
      // If the fetch fails, the broker should not serve queries. Throw the exception and try again on restart.
      log.error(e, "Failed to initialize coordinator dynamic config");
      throw new RuntimeException("Failed to initialize coordinator dynamic config", e);
    }
  }

  @Override
  public Int2ObjectRBTreeMap<Set<QueryableDruidServer>> getQueryableServers(
      Int2ObjectRBTreeMap<Set<QueryableDruidServer>> historicalServers,
      CloneQueryMode mode
  )
  {
    final Set<String> serversToIgnore = getCurrentServersToIgnore(mode);

    if (serversToIgnore.isEmpty()) {
      return historicalServers;
    }

    final Int2ObjectRBTreeMap<Set<QueryableDruidServer>> filteredHistoricals = new Int2ObjectRBTreeMap<>();
    for (int priority : historicalServers.keySet()) {
      Set<QueryableDruidServer> servers = historicalServers.get(priority);
      filteredHistoricals.put(priority,
                              servers.stream()
                                     .filter(server -> !serversToIgnore.contains(server.getServer().getHost()))
                                     .collect(Collectors.toSet())
      );
    }

    return filteredHistoricals;
  }

  private synchronized Set<String> getCurrentServersToIgnore(CloneQueryMode cloneQueryMode)
  {
    switch (cloneQueryMode) {
      case PREFER_CLONES:
        // Remove servers being cloned targets, so that clones are queried.
        return serversBeingCloned;
      case EXCLUDE_CLONES:
        // Remove clones, so that only source servers are queried.
        return cloneServers;
      case INCLUDE_CLONES:
        // Don't remove either
        return ImmutableSet.of();
      default:
        throw DruidException.defensive("Unexpected value: " + cloneQueryMode);
    }
  }
}
