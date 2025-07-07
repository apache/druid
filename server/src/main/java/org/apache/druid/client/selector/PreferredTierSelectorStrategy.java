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
import it.unimi.dsi.fastutil.ints.Int2ObjectRBTreeMap;
import org.apache.druid.client.QueryableDruidServer;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.Query;
import org.apache.druid.timeline.DataSegment;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PreferredTierSelectorStrategy extends AbstractTierSelectorStrategy
{
  private static final Logger log = new Logger(PreferredTierSelectorStrategy.class);

  private final String preferredTier;
  private final TierSelectorStrategy priorityStrategy;

  public PreferredTierSelectorStrategy(
      @JacksonInject ServerSelectorStrategy serverSelectorStrategy,
      @JacksonInject PreferredTierSelectorStrategyConfig config
  )
  {
    super(serverSelectorStrategy);
    this.preferredTier = config.getTier();

    if (config.getPriority() == null) {
      this.priorityStrategy = new HighestPriorityTierSelectorStrategy(serverSelectorStrategy);
    } else {
      if ("highest".equalsIgnoreCase(config.getPriority())) {
        this.priorityStrategy = new HighestPriorityTierSelectorStrategy(serverSelectorStrategy);
      } else if ("lowest".equalsIgnoreCase(config.getPriority())) {
        this.priorityStrategy = new LowestPriorityTierSelectorStrategy(serverSelectorStrategy);
      } else {
        throw new IAE("druid.broker.select.tier.preferred.priority must be either 'highest' or 'lowest'");
      }
    }
  }

  @Override
  public Comparator<Integer> getComparator()
  {
    return priorityStrategy.getComparator();
  }

  @Override
  public <T> List<QueryableDruidServer> pick(
      Query<T> query,
      Int2ObjectRBTreeMap<Set<QueryableDruidServer>> prioritizedServers,
      DataSegment segment,
      int numServersToPick
  )
  {
    if (log.isDebugEnabled()) {
      log.debug(
          "Picking [%d] servers from preferred tier [%s] for segment [%s] with priority [%s]",
          numServersToPick, preferredTier, segment.getId(), this.priorityStrategy.getClass().getSimpleName()
      );
    }

    Int2ObjectRBTreeMap<Set<QueryableDruidServer>> preferred = new Int2ObjectRBTreeMap<>(priorityStrategy.getComparator());
    Int2ObjectRBTreeMap<Set<QueryableDruidServer>> nonPreferred = new Int2ObjectRBTreeMap<>(priorityStrategy.getComparator());
    for (Set<QueryableDruidServer> priorityServers : prioritizedServers.values()) {
      for (QueryableDruidServer server : priorityServers) {
        if (preferredTier.equals(server.getServer().getMetadata().getTier())) {
          preferred.computeIfAbsent(server.getServer().getPriority(), k -> new HashSet<>())
                   .add(server);
        } else {
          nonPreferred.computeIfAbsent(server.getServer().getPriority(), k -> new HashSet<>())
                      .add(server);
        }
      }
    }

    List<QueryableDruidServer> picks = new ArrayList<>(numServersToPick);
    if (!preferred.isEmpty()) {
      // If we have preferred servers, pick them first
      picks.addAll(priorityStrategy.pick(query, preferred, segment, numServersToPick));
    }

    if (picks.size() < numServersToPick && !nonPreferred.isEmpty()) {
      // If we don't have enough preferred servers, pick from the non-preferred ones
      int remaining = numServersToPick - picks.size();
      picks.addAll(priorityStrategy.pick(query, nonPreferred, segment, remaining));
    }

    return picks;
  }
}
