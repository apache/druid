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

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.loadqueue.LoadQueuePeon;
import org.apache.druid.server.coordinator.loadqueue.SegmentAction;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Encapsulates the state of a DruidServer during a single coordinator run.
 */
public class ServerHolder implements Comparable<ServerHolder>
{
  private static final Comparator<ServerHolder> FULL_SERVER_FIRST =
      Comparator.comparing(ServerHolder::getAvailableSize)
                .thenComparing(holder -> holder.getServer().getHost())
                .thenComparing(holder -> holder.getServer().getTier())
                .thenComparing(holder -> holder.getServer().getType());

  private final ImmutableDruidServer server;
  private final LoadQueuePeon peon;
  private final boolean isDecommissioning;
  private final int maxAssignmentsInRun;

  private int totalAssignmentsInRun;
  private long sizeOfLoadingSegments;
  private long sizeOfDroppingSegments;

  /**
   * Remove entries from this map only if the operation is cancelled.
   * Do not remove entries on load/drop success or failure during the run.
   */
  private final Map<DataSegment, SegmentAction> queuedSegments = new HashMap<>();

  private final Map<String, Object2IntOpenHashMap<Interval>> datasourceIntervalToSegmentCount = new HashMap<>();

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

    server.iterateAllSegments()
          .forEach(segment -> updateCountInInterval(segment, true));

    peon.getSegmentsInQueue().forEach(
        (segment, action) -> {
          queuedSegments.put(segment, simplify(action));
          if (isLoadAction(action)) {
            sizeOfLoadingSegments += segment.getSize();
            updateCountInInterval(segment, true);
          } else {
            sizeOfDroppingSegments += segment.getSize();
            updateCountInInterval(segment, false);
          }
        }
    );

    peon.getSegmentsMarkedToDrop().forEach(
        segment -> {
          updateCountInInterval(segment, false);
          queuedSegments.put(segment, SegmentAction.MOVE_FROM);
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
    return server.getCurrSize() + sizeOfLoadingSegments - sizeOfDroppingSegments;
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
    return new HashMap<>(queuedSegments);
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
      } else if (action == SegmentAction.MOVE_FROM && includeMoving) {
        segments.remove(segment);
      }
    });

    return segments;
  }

  /**
   * Segments that are currently in the queue for being loaded on this server.
   * This does not include segments that are being moved to this server.
   */
  public List<DataSegment> getLoadingSegments()
  {
    final List<DataSegment> loadingSegments = new ArrayList<>();
    queuedSegments.forEach((segment, action) -> {
      if (action == SegmentAction.LOAD) {
        loadingSegments.add(segment);
      }
    });

    return loadingSegments;
  }

  public int getNumSegmentsInDatasourceInterval(DataSegment segment)
  {
    Object2IntOpenHashMap<Interval> intervalToCount =
        datasourceIntervalToSegmentCount.get(segment.getDataSource());
    return intervalToCount == null ? 0 : intervalToCount.getInt(segment.getInterval());
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
      updateCountInInterval(segment, true);
    } else {
      sizeOfDroppingSegments += segment.getSize();
      updateCountInInterval(segment, false);
    }

    queuedSegments.put(segment, simplify(action));
    return true;
  }

  public boolean cancelOperation(SegmentAction action, DataSegment segment)
  {
    return queuedSegments.get(segment) == simplify(action)
           && (action == SegmentAction.MOVE_FROM || peon.cancelOperation(segment))
           && cleanupState(segment);
  }

  public boolean hasSegmentLoaded(SegmentId segmentId)
  {
    return server.getSegment(segmentId) != null;
  }

  public boolean isRealtimeServer()
  {
    return server.getType() == ServerType.REALTIME
           || server.getType() == ServerType.INDEXER_EXECUTOR;
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
      updateCountInInterval(segment, false);
    } else {
      sizeOfDroppingSegments -= segment.getSize();
      updateCountInInterval(segment, true);
    }

    return true;
  }

  private void updateCountInInterval(DataSegment segment, boolean add)
  {
    datasourceIntervalToSegmentCount
        .computeIfAbsent(segment.getDataSource(), ds -> new Object2IntOpenHashMap<>())
        .addTo(segment.getInterval(), add ? 1 : -1);
  }

  @Override
  public int compareTo(ServerHolder serverHolder)
  {
    return FULL_SERVER_FIRST.compare(this, serverHolder);
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
    return Objects.equals(server.getHost(), that.server.getHost())
           && Objects.equals(server.getTier(), that.server.getTier())
           && Objects.equals(server.getType(), that.server.getType());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(server.getHost(), server.getTier(), server.getType());
  }
}
