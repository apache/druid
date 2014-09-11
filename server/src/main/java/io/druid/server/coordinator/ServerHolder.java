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

import com.metamx.common.logger.Logger;
import io.druid.client.ImmutableDruidServer;
import io.druid.timeline.DataSegment;

/**
 */
public class ServerHolder implements Comparable<ServerHolder>
{
  private static final Logger log = new Logger(ServerHolder.class);
  private final ImmutableDruidServer server;
  private final LoadQueuePeon peon;

  public ServerHolder(
      ImmutableDruidServer server,
      LoadQueuePeon peon
  )
  {
    this.server = server;
    this.peon = peon;
  }

  public ImmutableDruidServer getServer()
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

  public boolean isServingSegment(DataSegment segment)
  {
    return (server.getSegment(segment.getIdentifier()) != null);
  }

  public boolean isLoadingSegment(DataSegment segment)
  {
    return peon.getSegmentsToLoad().contains(segment);
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
