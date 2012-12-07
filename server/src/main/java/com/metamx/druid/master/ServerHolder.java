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

import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.client.DruidServer;

/**
 */
public class ServerHolder implements Comparable<ServerHolder>
{
  private static final Logger log = new Logger(ServerHolder.class);
  private final DruidServer server;
  private final LoadQueuePeon peon;

  public ServerHolder(
      DruidServer server,
      LoadQueuePeon peon
  )
  {
    this.server = server;
    this.peon = peon;
  }

  public DruidServer getServer()
  {
    return server;
  }

  public LoadQueuePeon getPeon()
  {
    return peon;
  }

  public Long getMaxSize()
  {
    return server.getMaxSize();
  }

  public Long getCurrServerSize()
  {
    return server.getCurrSize();
  }

  public Long getLoadQueueSize()
  {
    return peon.getLoadQueueSize();
  }

  public Long getSizeUsed()
  {
    return getCurrServerSize() + getLoadQueueSize();
  }

  public Double getPercentUsed()
  {
    return (100 * getSizeUsed().doubleValue()) / getMaxSize();
  }

  public Long getAvailableSize()
  {
    long maxSize = getMaxSize();
    long sizeUsed = getSizeUsed();
    long availableSize = maxSize - sizeUsed;

    log.debug(
        "Server[%s], MaxSize[%,d], CurrSize[%,d], QueueSize[%,d], SizeUsed[%,d], AvailableSize[%,d]",
        server.getName(),
        maxSize,
        getCurrServerSize(),
        getLoadQueueSize(),
        sizeUsed,
        availableSize
    );

    return availableSize;
  }

  public boolean containsSegment(DataSegment segment)
  {
    return (server.getSegment(segment.getIdentifier()) != null || peon.getSegmentsToLoad().contains(segment));
  }

  @Override
  public int compareTo(ServerHolder serverHolder)
  {
    return getAvailableSize().compareTo(serverHolder.getAvailableSize());
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ServerHolder that = (ServerHolder) o;

    if (peon != null ? !peon.equals(that.peon) : that.peon != null) {
      return false;
    }
    if (server != null ? !server.equals(that.server) : that.server != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = server != null ? server.hashCode() : 0;
    result = 31 * result + (peon != null ? peon.hashCode() : 0);
    return result;
  }
}
