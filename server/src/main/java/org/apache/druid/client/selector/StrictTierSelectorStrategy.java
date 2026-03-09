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
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.Query;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A {@link TierSelectorStrategy} that only considers servers whose priorities
 * are explicitly listed in {@link StrictTierSelectorStrategyConfig#getPriorities()}.
 * <p>
 * Unlike other strategies like {@link CustomTierSelectorStrategy} that falls back to servers with different priorities,
 * this strategy strictly filters the available servers to the configured priorities.
 * If no servers match the configured priorities, an empty server list is returned from {@link #pick(Query, Int2ObjectRBTreeMap, DataSegment, int)},
 * which may cause queries to return partial or no data.
 * <p>
 * When multiple priorities are configured, they are evaluated in the order specified, with earlier priorities
 * preferred over later ones.
 * <p>
 * Example configuration:
 * <li> <code> druid.broker.select.tier=strict </code> </li>
 * <li> <code> druid.broker.select.tier.strict.priorities=[2,1] </code> </li>
 * <p>
 * With this configuration, servers with priority 2 are preferred over servers with priority 1.
 * Servers with any other tier priority are not considered.
 * <p>
 * This strategy is useful when query isolation is required between different server priority tiers. Brokers may still
 * be configured to watch all server tiers, allowing them to retain visibility into the overall cluster state
 * while enforcing isolation at query time.
 */
public class StrictTierSelectorStrategy extends AbstractTierSelectorStrategy
{
  private static final EmittingLogger log = new EmittingLogger(StrictTierSelectorStrategy.class);
  public static final String TYPE = "strict";

  private final StrictTierSelectorStrategyConfig config;
  private final ServiceEmitter emitter;
  private final Map<Integer, Integer> configuredPriorities;
  private final Comparator<Integer> comparator;

  @JsonCreator
  public StrictTierSelectorStrategy(
      @JacksonInject final ServerSelectorStrategy serverSelectorStrategy,
      @JacksonInject final StrictTierSelectorStrategyConfig config,
      @JacksonInject final ServiceEmitter emitter
  )
  {
    super(serverSelectorStrategy);
    this.config = config;
    this.emitter = emitter;

    configuredPriorities = new HashMap<>();
    for (int i = 0; i < config.getPriorities().size(); i++) {
      configuredPriorities.put(config.getPriorities().get(i), i);
    }

    this.comparator = (p1, p2) -> {
      final Integer rank1 = configuredPriorities.get(p1);
      final Integer rank2 = configuredPriorities.get(p2);

      if (rank1 != null && rank2 != null) {
        return Integer.compare(rank1, rank2);
      }
      if (rank1 != null) {
        return -1;
      }
      if (rank2 != null) {
        return 1;
      }

      // Priorities outside configuredPriorities don't matter and won't be selected in pick() for this strategy.
      // This fallback ordering can be anything and is needed because the comparator may be used by ServerSelector/ServerView
      // which maintains views of all servers, including those with unconfigured priorities.
      return Integer.compare(p2, p1);
    };
  }

  @Override
  public <T> List<QueryableDruidServer> pick(
      @Nullable final Query<T> query,
      final Int2ObjectRBTreeMap<Set<QueryableDruidServer>> prioritizedServers,
      final DataSegment segment,
      final int numServersToPick
  )
  {
    final Int2ObjectRBTreeMap<Set<QueryableDruidServer>> candidatePrioritizedServers = new Int2ObjectRBTreeMap<>(getComparator());

    for (Int2ObjectMap.Entry<Set<QueryableDruidServer>> entry : prioritizedServers.int2ObjectEntrySet()) {
      final int priority = entry.getIntKey();
      final Set<QueryableDruidServer> servers = entry.getValue();

      if (configuredPriorities.containsKey(priority)) {
        candidatePrioritizedServers.put(priority, servers);
      } else {
        log.debug(
            "Server priority[%d] not in the configured list of priorities[%s] so ignore servers[%s] for query[%s]",
            priority, config.getPriorities(), servers, query
        );
      }
    }

    if (candidatePrioritizedServers.isEmpty()) {
      if (query == null || query instanceof SegmentMetadataQuery) {
        // Debug logging to reduce logging spam as these are typically system-generated segment metadata queries
        log.debug(
            "No server found for query[%s] from server priorities[%s]. Configured priorities[%s].",
            query, prioritizedServers.keySet(), config.getPriorities()
        );
      } else {
        log.warn(
            "No servers found for query[%s] matching configured priorities[%s]. Available priorities[%s].",
            query, config.getPriorities(), prioritizedServers.keySet()
        );
        emitter.emit(
            ServiceMetricEvent.builder()
                              .setMetric("tierSelector/noServer", 1)
                              .setDimension("dataSource", String.valueOf(query.getDataSource()))
                              .setDimension("tierSelectorType", TYPE)
                              .setDimension("queryType", query.getType())
                              .setDimension("queryPriority", String.valueOf(query.context().getPriority()))
                              .setDimensionIfNotNull("queryId", query.getId())
        );
      }
      return List.of();
    }

    final List<QueryableDruidServer> selectedServers = super.pick(query, candidatePrioritizedServers, segment, numServersToPick);
    log.debug("Selected servers[%s] for query[%s] from given servers[%s] and numServersToPick[%s]", selectedServers, query, prioritizedServers, numServersToPick);
    return selectedServers;
  }

  @Override
  public Comparator<Integer> getComparator()
  {
    return comparator;
  }

  public StrictTierSelectorStrategyConfig getConfig()
  {
    return config;
  }

  @Override
  public String toString()
  {
    return "StrictTierSelectorStrategy{" +
           "config=" + config +
           '}';
  }
}
