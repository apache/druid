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

import io.druid.java.util.common.ISE;
import io.druid.timeline.DataSegment;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

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
      TreeMap<Integer, Set<QueryableDruidServer>> prioritizedServers, DataSegment segment
  )
  {
    final Map.Entry<Integer, Set<QueryableDruidServer>> priorityServers = prioritizedServers.pollFirstEntry();

    if (priorityServers == null) {
      return null;
    }

    final Set<QueryableDruidServer> servers = priorityServers.getValue();
    switch (servers.size()) {
      case 0:
        throw new ISE("[%s] Something hella weird going on here. We should not be here", segment.getIdentifier());
      case 1:
        return priorityServers.getValue().iterator().next();
      default:
        return serverSelectorStrategy.pick(servers, segment);
    }
  }
}
