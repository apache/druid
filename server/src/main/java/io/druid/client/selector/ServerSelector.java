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

import com.google.common.collect.Sets;
import com.metamx.emitter.EmittingLogger;
import io.druid.timeline.DataSegment;

import java.util.Set;
import java.util.TreeMap;

/**
 */
public class ServerSelector implements DiscoverySelector<QueryableDruidServer>
{

  private static final EmittingLogger log = new EmittingLogger(ServerSelector.class);

  private final Set<QueryableDruidServer> servers = Sets.newHashSet();

  private final DataSegment segment;
  private final TierSelectorStrategy strategy;

  public ServerSelector(
      DataSegment segment,
      TierSelectorStrategy strategy
  )
  {
    this.segment = segment;
    this.strategy = strategy;
  }

  public DataSegment getSegment()
  {
    return segment;
  }

  public void addServer(
      QueryableDruidServer server
  )
  {
    synchronized (this) {
      servers.add(server);
    }
  }

  public boolean removeServer(QueryableDruidServer server)
  {
    synchronized (this) {
      return servers.remove(server);
    }
  }

  public boolean isEmpty()
  {
    synchronized (this) {
      return servers.isEmpty();
    }
  }

  public QueryableDruidServer pick()
  {
    synchronized (this) {
      final TreeMap<Integer, Set<QueryableDruidServer>> prioritizedServers = new TreeMap<>(strategy.getComparator());
      for (QueryableDruidServer server : servers) {
        Set<QueryableDruidServer> theServers = prioritizedServers.get(server.getServer().getPriority());
        if (theServers == null) {
          theServers = Sets.newHashSet();
          prioritizedServers.put(server.getServer().getPriority(), theServers);
        }
        theServers.add(server);
      }

      return strategy.pick(prioritizedServers, segment);
    }
  }
}
