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

    List<QueryableDruidServer> preferred = new ArrayList<>(numServersToPick);
    List<QueryableDruidServer> nonPreferred = new ArrayList<>(numServersToPick);
    for (Set<QueryableDruidServer> priorityServers : prioritizedServers.values()) {
      for (QueryableDruidServer server : priorityServers) {
        if (preferredTier.equals(server.getServer().getMetadata().getTier())) {
          preferred.add(server);
          if (preferred.size() == numServersToPick) {
            return this.serverSelectorStrategy.pick(query, preferred, segment, numServersToPick);
          }
        } else {
          // We have to iterate through all servers even the numbers of the non-preferred servers reach the limit
          // This is because we don't know whether there're preferred servers left in the next priority set
          nonPreferred.add(server);
        }
      }
    }

    // Fill with non-preferred servers if we don't have enough preferred servers
    int fillSize = numServersToPick - preferred.size();
    for (int i = 0; i < fillSize && i < nonPreferred.size(); i++) {
      preferred.add(nonPreferred.get(i));
    }

    return this.serverSelectorStrategy.pick(query, preferred, segment, numServersToPick);
  }
}
