/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.client.selector;

import com.google.common.collect.Iterables;
import io.druid.timeline.DataSegment;
import it.unimi.dsi.fastutil.ints.Int2ObjectRBTreeMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 */
public abstract class AbstractTierSelectorStrategy implements TierSelectorStrategy
{
  private final ServerSelectorStrategy serverSelectorStrategy;

  public AbstractTierSelectorStrategy(ServerSelectorStrategy serverSelectorStrategy)
  {
    this.serverSelectorStrategy = serverSelectorStrategy;
  }

  @Override
  public QueryableDruidServer pick(
      Int2ObjectRBTreeMap<Set<QueryableDruidServer>> prioritizedServers,
      DataSegment segment
  )
  {
    return Iterables.getOnlyElement(pick(prioritizedServers, segment, 1), null);
  }

  @Override
  public List<QueryableDruidServer> pick(
      Int2ObjectRBTreeMap<Set<QueryableDruidServer>> prioritizedServers,
      DataSegment segment,
      int numServersToPick
  )
  {
    List<QueryableDruidServer> result = new ArrayList<>(numServersToPick);
    for (Set<QueryableDruidServer> priorityServers : prioritizedServers.values()) {
      result.addAll(serverSelectorStrategy.pick(priorityServers, segment, numServersToPick - result.size()));
      if (result.size() == numServersToPick) {
        break;
      }
    }
    return result;
  }
}
