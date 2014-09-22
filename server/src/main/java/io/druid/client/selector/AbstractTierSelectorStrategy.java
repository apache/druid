/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.client.selector;

import com.metamx.common.ISE;
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
