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

package com.metamx.druid.master;

import com.metamx.druid.client.DataSegment;
import com.metamx.druid.client.DruidServer;

/**
 */
public class BalancerSegmentHolder
{
  private final DruidServer fromServer;
  private final DruidServer toServer;
  private final DataSegment segment;

  private volatile int lifetime = 15;

  public BalancerSegmentHolder(
      DruidServer fromServer,
      DruidServer toServer,
      DataSegment segment
  )
  {
    this.fromServer = fromServer;
    this.toServer = toServer;
    this.segment = segment;
  }

  public DruidServer getFromServer()
  {
    return fromServer;
  }

  public DruidServer getToServer()
  {
    return toServer;
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
