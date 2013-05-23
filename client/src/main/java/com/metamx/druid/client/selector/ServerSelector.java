/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.client.selector;

import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.client.DruidServer;

import java.util.Collections;
import java.util.Comparator;
import java.util.Set;

/**
 */
public class ServerSelector
{
  private static final Comparator<QueryableDruidServer> comparator = new Comparator<QueryableDruidServer>()
  {
    @Override
    public int compare(QueryableDruidServer left, QueryableDruidServer right)
    {
      return Ints.compare(left.getClient().getNumOpenConnections(), right.getClient().getNumOpenConnections());
    }
  };

  private final Set<QueryableDruidServer> servers = Sets.newHashSet();

  private final DataSegment segment;

  public ServerSelector(
      DataSegment segment
  )
  {
    this.segment = segment;
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
      final int size = servers.size();
      switch (size) {
        case 0: return null;
        case 1: return servers.iterator().next();
        default: return Collections.min(servers, comparator);
      }
    }
  }
}
