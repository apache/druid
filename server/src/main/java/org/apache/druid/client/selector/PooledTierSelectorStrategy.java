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

package org.apache.druid.client.selector;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectRBTreeMap;
import org.apache.druid.client.QueryableDruidServer;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.Query;
import org.apache.druid.timeline.DataSegment;

import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * A {@link TierSelectorStrategy} that pools servers with the configured set of priorities from {@link PooledTierSelectorStrategyConfig#getPriorities()}
 * and delegates server selection to the configured {@link ServerSelectorStrategy}.
 * <p>
 * Unlike other {@link TierSelectorStrategy} like {@link CustomTierSelectorStrategy}
 * which has a preference for priority order, this strategy treats all configured priorities equally
 * by combining their servers into a single selection pool and delegates to {@link ServerSelectorStrategy} to do
 * the server selection.
 * <p>
 * Example configuration:
 * <li> <code> druid.broker.select.tier=pooled </code> </li>
 * <li> <code> druid.broker.select.tier.pooled.priorities=[2,1] </code> </li>
 * <p>
 * With this configuration, servers with priority 2 and 1 are pooled together and
 * selection is delegated to the {@link ServerSelectorStrategy}. Servers with other
 * priorities are ignored.
 */
public class PooledTierSelectorStrategy extends AbstractTierSelectorStrategy
{
  private static final EmittingLogger log = new EmittingLogger(PooledTierSelectorStrategy.class);
  public static final String TYPE = "pooled";

  private final ServerSelectorStrategy serverSelectorStrategy;
  private final PooledTierSelectorStrategyConfig config;
  private final Set<Integer> configuredPriorities;

  @JsonCreator
  public PooledTierSelectorStrategy(
      @JacksonInject ServerSelectorStrategy serverSelectorStrategy,
      @JacksonInject PooledTierSelectorStrategyConfig config
  )
  {
    super(serverSelectorStrategy);
    this.serverSelectorStrategy = serverSelectorStrategy;
    this.config = config;
    this.configuredPriorities = config.getPriorities();
  }

  @Override
  public <T> List<QueryableDruidServer> pick(
      Query<T> query,
      Int2ObjectRBTreeMap<Set<QueryableDruidServer>> prioritizedServers,
      DataSegment segment,
      int numServersToPick
  )
  {
    final Set<QueryableDruidServer> serverPool = new LinkedHashSet<>();

    for (Int2ObjectMap.Entry<Set<QueryableDruidServer>> entry : prioritizedServers.int2ObjectEntrySet()) {
      final int priority = entry.getIntKey();
      final Set<QueryableDruidServer> servers = entry.getValue();

      if (configuredPriorities.contains(priority)) {
        serverPool.addAll(servers);
      } else {
        log.debug(
            "Server priority[%d] not in the configured list of priorities[%s] so ignore servers[%s] for query[%s]",
            priority, config.getPriorities(), servers, query
        );
      }
    }

    if (serverPool.isEmpty()) {
      log.makeAlert(
          "No servers found in available servers with priorities[%s] for query[%s] and may return no/partial data. Configured priorities[%s].",
          prioritizedServers.keySet(), query, config.getPriorities()
      ).emit();

      return List.of();
    }

    final List<QueryableDruidServer> selectedServers = serverSelectorStrategy.pick(query, serverPool, segment, numServersToPick);
    log.debug("Selected servers[%s] for query[%s] from given servers[%s] and serverPool[%s]", selectedServers, query, prioritizedServers, serverPool);
    return selectedServers;
  }

  /**
   * @return the natural order of priorities since priority order doesn't matter for this strategy as the configured set of
   * priorities in the pool are treated equally and delegated ot {@link #serverSelectorStrategy}.
   */
  @Override
  public Comparator<Integer> getComparator()
  {
    return Comparator.naturalOrder();
  }

  public PooledTierSelectorStrategyConfig getConfig()
  {
    return config;
  }

  @Override
  public String toString()
  {
    return "PooledTierSelectorStrategy{" +
           "config=" + config +
           '}';
  }
}
