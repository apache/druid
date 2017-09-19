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

package io.druid.server.coordinator;

import io.druid.client.ImmutableDruidServer;
import io.druid.java.util.common.logger.Logger;
import io.druid.timeline.DataSegment;

import java.util.Objects;

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

  public int getNumberOfSegmentsInQueue()
  {
    return peon.getNumberOfSegmentsInQueue();
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

    if (!this.server.getHost().equals(that.server.getHost())) {
      return false;
    }

    if (!this.server.getTier().equals(that.getServer().getTier())) {
      return false;
    }

    return this.server.getType().equals(that.getServer().getType());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(server.getHost(), server.getTier(), server.getType());
  }
}
