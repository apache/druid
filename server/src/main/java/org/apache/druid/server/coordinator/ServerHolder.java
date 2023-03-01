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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Encapsulates the state of a DruidServer during a single coordinator run.
 */
public class ServerHolder implements Comparable<ServerHolder>
{
  private final ImmutableDruidServer server;
  private final LoadQueuePeon peon;
  private final boolean isDecommissioning;
  private final int maxAssignmentsInRun;

  private int totalAssignmentsInRun;
  private long sizeOfLoadingSegments;

  /**
   * Remove entries from this map only if the operation is cancelled.
   * Do not remove entries on load/drop success or failure during the run.
   */
  private final Map<DataSegment, SegmentAction> queuedSegments = new HashMap<>();

  public ServerHolder(ImmutableDruidServer server, LoadQueuePeon peon)
  {
    this(server, peon, false, 0);
  }

  public ServerHolder(ImmutableDruidServer server, LoadQueuePeon peon, boolean isDecommissioning)
  {
    this(server, peon, isDecommissioning, 0);
  }

  /**
   * Creates a new ServerHolder valid for a single coordinator run.
   *
   * @param server                 Underlying Druid server
   * @param peon                   Load queue peon for this server
   * @param isDecommissioning      Whether the server is decommissioning
   * @param maxSegmentsInLoadQueue Max number of segments that can be present in
   *                               the load queue at any point. If this is 0, the
   *                               load queue can have an unlimited number of segments.
   */
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

    this.maxAssignmentsInRun = maxSegmentsInLoadQueue == 0
                               ? Integer.MAX_VALUE
                               : maxSegmentsInLoadQueue - peon.getNumberOfSegmentsToLoad();

    peon.getSegmentsInQueue().forEach(
        (segment, action) -> {
          queuedSegments.put(segment, simplify(action));
          if (isLoadAction(action)) {
            sizeOfLoadingSegments += segment.getSize();
          }
        }
    );
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

  public long getSizeUsed()
  {
    return server.getCurrSize() + sizeOfLoadingSegments;
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
   *   <li>is not being decommissioned</li>
   *   <li>is not already serving the segment</li>
   *   <li>is not performing any other action on the segment</li>
   *   <li>has not already exceeded the load queue limit in this run</li>
   *   <li>has available disk space</li>
   * </ul>
   */
  public boolean canLoadSegment(DataSegment segment)
  {
    return !isDecommissioning
           && !hasSegmentLoaded(segment.getId())
           && getActionOnSegment(segment) == null
           && totalAssignmentsInRun < maxAssignmentsInRun
           && getAvailableSize() >= segment.getSize();
  }

  public SegmentAction getActionOnSegment(DataSegment segment)
  {
    return queuedSegments.get(segment);
  }

  /**
   * Segments queued for load, drop or move on this server.
   * <ul>
   * <li>Contains segments present in the queue when the current coordinator run started.</li>
   * <li>Contains segments added to the queue during the current run.</li>
   * <li>Maps replicating segments to LOAD rather than REPLICATE for simplicity.</li>
   * <li>Does not contain segments whose actions were cancelled.</li>
   * </ul>
   */
  public Map<DataSegment, SegmentAction> getQueuedSegments()
  {
    return Collections.unmodifiableMap(queuedSegments);
  }

  /**
   * Segments that are expected to be loaded on this server once all the
   * operations in progress have completed.
   *
   * @param includeMoving true if segments moving to this server should also be included.
   */
  public Set<DataSegment> getProjectedSegments(boolean includeMoving)
  {
    final Set<DataSegment> segments = new HashSet<>(server.iterateAllSegments());
    queuedSegments.forEach((segment, action) -> {
      if (action == SegmentAction.LOAD) {
        segments.add(segment);
      } else if (action == SegmentAction.DROP) {
        segments.remove(segment);
      } else if (action == SegmentAction.MOVE_TO && includeMoving) {
        segments.add(segment);
      }
    });

    return segments;
  }


  /**
   * Returns true if this server has the segment loaded and is not dropping it.
   */
  public boolean isServingSegment(DataSegment segment)
  {
    return hasSegmentLoaded(segment.getId()) && getActionOnSegment(segment) == null;
  }

  public boolean isLoadingSegment(DataSegment segment)
  {
    return getActionOnSegment(segment) == SegmentAction.LOAD;
  }

  public boolean isDroppingSegment(DataSegment segment)
  {
    return getActionOnSegment(segment) == SegmentAction.DROP;
  }

  public boolean startOperation(SegmentAction action, DataSegment segment)
  {
    if (queuedSegments.containsKey(segment)) {
      return false;
    }

    if (isLoadAction(action)) {
      ++totalAssignmentsInRun;
      sizeOfLoadingSegments += segment.getSize();
    }
    queuedSegments.put(segment, simplify(action));
    return true;
  }

  public boolean cancelOperation(SegmentAction action, DataSegment segment)
  {
    return queuedSegments.get(segment) == simplify(action)
           && peon.cancelOperation(segment)
           && cleanupState(segment);
  }

  public boolean hasSegmentLoaded(SegmentId segmentId)
  {
    return server.getSegment(segmentId) != null;
  }

  private SegmentAction simplify(SegmentAction action)
  {
    return action == SegmentAction.REPLICATE ? SegmentAction.LOAD : action;
  }

  private boolean isLoadAction(SegmentAction action)
  {
    return action == SegmentAction.LOAD
           || action == SegmentAction.REPLICATE
           || action == SegmentAction.MOVE_TO;
  }

  private boolean cleanupState(DataSegment segment)
  {
    final SegmentAction action = queuedSegments.remove(segment);
    if (isLoadAction(action)) {
      sizeOfLoadingSegments -= segment.getSize();
    }

    return true;
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
