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

package io.druid.server.coordinator;

import io.druid.client.ImmutableDruidServer;
import io.druid.timeline.DataSegment;

/**
 */
public class BalancerSegmentHolder
{
  private final ImmutableDruidServer fromServer;
  private final DataSegment segment;

  // This is a pretty fugly hard coding of the maximum lifetime
  private volatile int lifetime = 15;

  public BalancerSegmentHolder(
      ImmutableDruidServer fromServer,
      DataSegment segment
  )
  {
    this.fromServer = fromServer;
    this.segment = segment;
  }

  public ImmutableDruidServer getFromServer()
  {
    return fromServer;
  }

  public DataSegment getSegment()
  {
    return segment;
  }

  public int getLifetime()
  {
    return lifetime;
  }

  public void reduceLifetime()
  {
    lifetime--;
  }
}
