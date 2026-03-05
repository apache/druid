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
import org.apache.druid.query.DruidMetrics;
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
 * If no servers match, an empty list is returned, which may result in queries returning partial or no data.
 * <p>
 * <p>
 * Example configuration:
 * <li> <code> druid.broker.select.tier=strict </code> </li>
 * <li> <code> druid.broker.select.tier.strict.priorities=[1] </code> </li>
 * <p>
 * With this configuration, only servers with priority 1 are selected as candidates for queries.
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
  private final Map<Integer, Integer> configuredPriorities;
  private final Comparator<Integer> comparator;
  private final ServiceEmitter emitter;

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

      return Integer.compare(p2, p1);
    };
  }

  @Override
  public <T> List<QueryableDruidServer> pick(
      @Nullable Query<T> query,
      Int2ObjectRBTreeMap<Set<QueryableDruidServer>> prioritizedServers,
      DataSegment segment,
      int numServersToPick
  )
  {
    final Int2ObjectRBTreeMap<Set<QueryableDruidServer>> filteredPrioritizedServers = new Int2ObjectRBTreeMap<>(getComparator());

    // Iterate through entries in tree order to preserve priority ordering
    for (Int2ObjectMap.Entry<Set<QueryableDruidServer>> entry : prioritizedServers.int2ObjectEntrySet()) {
      final int priority = entry.getIntKey();
      final Set<QueryableDruidServer> servers = entry.getValue();

      if (configuredPriorities.containsKey(priority)) {
        filteredPrioritizedServers.put(priority, servers);
      } else {
        log.debug(
            "Server priority[%d] not in the configured list of priorities[%s] so ignore servers[%s] for query[%s]",
            priority, config.getPriorities(), servers, query
        );
      }
    }

    if (filteredPrioritizedServers.isEmpty()) {
      if (query == null || query instanceof SegmentMetadataQuery) {
        log.debug(
            "No server found for query[%s] from server priorities[%s]. Configured priorities[%s].",
            query, prioritizedServers.keySet(), config.getPriorities()
        );
      } else {
        // maybe having one of alert or metric should be enough
        log.makeAlert(
            "No servers found in available servers with priorities[%s] for query[%s] and may return no/partial data. Configured priorities[%s].",
            prioritizedServers.keySet(), query, config.getPriorities()
        ).emit();

        emitter.emit(
            ServiceMetricEvent.builder()
                              .setMetric("tierSelector/noServer", 1)
                              .setDimension(DruidMetrics.DATASOURCE, String.valueOf(query.getDataSource()))
                              .setDimension("queryType", query.getType())
                              .setDimension("queryPriority", String.valueOf(query.context().getPriority()))
                              .setDimension("serverPriorities", prioritizedServers.keySet().toString())
        );
      }
      return List.of();
    }

    final List<QueryableDruidServer> selectedServers = super.pick(query, filteredPrioritizedServers, segment, numServersToPick);
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
