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

import com.google.common.primitives.Ints;
import com.metamx.common.ISE;
import io.druid.timeline.DataSegment;

import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class ConnectionCountServerSelectorStrategy implements ServerSelectorStrategy
{
  private static final Comparator<QueryableDruidServer> comparator = new Comparator<QueryableDruidServer>()
  {
    @Override
    public int compare(QueryableDruidServer left, QueryableDruidServer right)
    {
      return Ints.compare(left.getClient().getNumOpenConnections(), right.getClient().getNumOpenConnections());
    }
  };

  @Override
  public QueryableDruidServer pick(
      TreeMap<Integer, Set<QueryableDruidServer>> prioritizedServers, DataSegment segment
  )
  {
    final Map.Entry<Integer, Set<QueryableDruidServer>> highestPriorityServers = prioritizedServers.pollLastEntry();

    if (highestPriorityServers == null) {
      return null;
    }

    final Set<QueryableDruidServer> servers = highestPriorityServers.getValue();
    final int size = servers.size();
    switch (size) {
      case 0:
        throw new ISE("[%s] Something hella weird going on here. We should not be here", segment.getIdentifier());
      case 1:
        return highestPriorityServers.getValue().iterator().next();
      default:
        return Collections.min(servers, comparator);
    }
  }
}
