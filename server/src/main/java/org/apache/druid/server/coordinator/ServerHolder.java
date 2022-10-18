/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.server.coordinator;

import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public class ServerHolder implements Comparable<ServerHolder>
{
  private final ImmutableDruidServer server;
  private final LoadQueuePeon peon;
  private final boolean isDecommissioning;
  private final int maxSegmentsInLoadQueue;

  private int segmentsQueuedForLoad;
  private long sizeOfLoadingSegments;

  private final ConcurrentMap<SegmentId, SegmentState> queuedSegments = new ConcurrentHashMap<>();

  public ServerHolder(ImmutableDruidServer server, LoadQueuePeon peon)
  {
    this(server, peon, false, 0);
  }

  public ServerHolder(ImmutableDruidServer server, LoadQueuePeon peon, boolean isDecommissioning)
  {
    this(server, peon, isDecommissioning, 0);
  }

  public ServerHolder(
      ImmutableDruidServer server,
      LoadQueuePeon peon,
      boolean isDecommissioning,
      int maxSegmentsInLoadQueue
  )
  {
    this.server = server;
    this.peon = peon;
    this.isDecommissioning = isDecommissioning;
    this.maxSegmentsInLoadQueue = maxSegmentsInLoadQueue;

    peon.getSegmentsInQueue().forEach(this::initializeSegmentState);
  }

  private void initializeSegmentState(DataSegment segment, SegmentAction action)
  {
    switch (action) {
      case DROP:
        startOperation(segment, SegmentState.DROPPING);
        break;
      case PRIORITY_LOAD:
      case LOAD:
        startOperation(segment, SegmentState.LOADING);
        break;
      case MOVE_TO:
        startOperation(segment, SegmentState.MOVING_TO);
        break;
      default:
        break;
    }
  }

  public ImmutableDruidServer getServer()
  {
    return server;
  }

  public LoadQueuePeon getPeon()
  {
    return peon;
  }

  public long getMaxSize()
  {
    return server.getMaxSize();
  }

  public long getCurrServerSize()
  {
    return server.getCurrSize();
  }

  public long getLoadQueueSize()
  {
    return peon.getLoadQueueSize();
  }

  public long getSizeUsed()
  {
    return getCurrServerSize() + sizeOfLoadingSegments;
  }

  public double getPercentUsed()
  {
    return (100.0 * getSizeUsed()) / getMaxSize();
  }

  /**
   * Historical nodes can be 'decommissioned', which instructs Coordinator to move segments from them according to
   * the percent of move operations diverted from normal balancer moves for this purpose by
   * {@link CoordinatorDynamicConfig#getDecommissioningMaxPercentOfMaxSegmentsToMove()}. The mechanism allows draining
   * segments from nodes which are planned for replacement.
   *
   * @return true if the node is decommissioning
   */
  public boolean isDecommissioning()
  {
    return isDecommissioning;
  }

  public long getAvailableSize()
  {
    return getMaxSize() - getSizeUsed();
  }

  /**
   * Checks if the server can load the given segment.
   * <p>
   * A load is possible only if the server meets all of the following criteria:
   * <ul>
   *   <li>is not already serving or loading the segment</li>
   *   <li>is not being decommissioned</li>
   *   <li>has not already exceeded the load queue limit in this run</li>
   *   <li>has available disk space</li>
   * </ul>
   */
  public boolean canLoadSegment(DataSegment segment)
  {
    final SegmentState state = getSegmentState(segment);
    return !isDecommissioning
           && (maxSegmentsInLoadQueue == 0 || maxSegmentsInLoadQueue > segmentsQueuedForLoad)
           && getAvailableSize() >= segment.getSize()
           && state == SegmentState.NONE;
  }

  public SegmentState getSegmentState(DataSegment segment)
  {
    SegmentState state = queuedSegments.get(segment.getId());
    if (state != null) {
      return state;
    }

    return isServingSegment(segment) ? SegmentState.LOADED : SegmentState.NONE;
  }

  public boolean isServingSegment(DataSegment segment)
  {
    return isServingSegment(segment.getId());
  }

  public boolean isLoadingSegment(DataSegment segment)
  {
    return getSegmentState(segment) == SegmentState.LOADING;
  }

  public boolean isDroppingSegment(DataSegment segment)
  {
    return getSegmentState(segment) == SegmentState.DROPPING;
  }

  public boolean startOperation(DataSegment segment, SegmentState newState)
  {
    if (queuedSegments.containsKey(segment.getId())) {
      return false;
    }

    if (newState == SegmentState.LOADING || newState == SegmentState.MOVING_TO) {
      ++segmentsQueuedForLoad;
      sizeOfLoadingSegments += segment.getSize();
    }
    queuedSegments.put(segment.getId(), newState);
    return true;
  }

  public boolean cancelOperation(DataSegment segment, SegmentState currentState)
  {
    SegmentState observedState = queuedSegments.get(segment.getId());
    if (observedState != currentState) {
      return false;
    }

    if (currentState == SegmentState.LOADING || currentState == SegmentState.MOVING_TO) {
      --segmentsQueuedForLoad;
      sizeOfLoadingSegments -= segment.getSize();
    }
    queuedSegments.remove(segment.getId());
    return true;
  }

  public int getNumberOfSegmentsInQueue()
  {
    return peon.getNumberOfSegmentsInQueue();
  }

  public boolean isServingSegment(SegmentId segmentId)
  {
    return server.getSegment(segmentId) != null;
  }

  @Override
  public int compareTo(ServerHolder serverHolder)
  {
    int result = Long.compare(getAvailableSize(), serverHolder.getAvailableSize());
    if (result != 0) {
      return result;
    }

    result = server.getHost().compareTo(serverHolder.server.getHost());
    if (result != 0) {
      return result;
    }

    result = server.getTier().compareTo(serverHolder.server.getTier());
    if (result != 0) {
      return result;
    }

    return server.getType().compareTo(serverHolder.server.getType());
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
