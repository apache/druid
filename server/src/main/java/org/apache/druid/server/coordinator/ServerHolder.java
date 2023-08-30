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
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.loading.LoadQueuePeon;
import org.apache.druid.server.coordinator.loading.SegmentAction;
import org.apache.druid.server.coordinator.loading.SegmentHolder;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Encapsulates the state of a DruidServer during a single coordinator run.
 * <p>
 * ServerHolders are naturally ordered by available size, servers with more
 * available size first.
 */
public class ServerHolder implements Comparable<ServerHolder>
{
  private static final Comparator<ServerHolder> MORE_AVAILABLE_SIZE_SERVER_FIRST =
      Comparator.comparing(ServerHolder::getAvailableSize)
                .thenComparing(holder -> holder.getServer().getHost())
                .thenComparing(holder -> holder.getServer().getTier())
                .thenComparing(holder -> holder.getServer().getType())
                .reversed();

  private static final EmittingLogger log = new EmittingLogger(ServerHolder.class);

  private final ImmutableDruidServer server;
  private final LoadQueuePeon peon;
  private final boolean isDecommissioning;
  private final int maxAssignmentsInRun;
  private final int maxLifetimeInQueue;

  private final int movingSegmentCount;
  private final int loadingReplicaCount;

  private int totalAssignmentsInRun;
  private long sizeOfLoadingSegments;
  private long sizeOfDroppingSegments;

  /**
   * Remove entries from this map only if the operation is cancelled.
   * Do not remove entries on load/drop success or failure during the run.
   */
  private final Map<DataSegment, SegmentAction> queuedSegments = new HashMap<>();

  private final SegmentCountsPerInterval projectedSegments = new SegmentCountsPerInterval();

  public ServerHolder(ImmutableDruidServer server, LoadQueuePeon peon)
  {
    this(server, peon, false, 0, 1);
  }

  public ServerHolder(ImmutableDruidServer server, LoadQueuePeon peon, boolean isDecommissioning)
  {
    this(server, peon, isDecommissioning, 0, 1);
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
   * @param maxLifetimeInQueue     Number of coordinator runs after a which a segment
   *                               in load/drop queue is considered to be stuck.
   */
  public ServerHolder(
      ImmutableDruidServer server,
      LoadQueuePeon peon,
      boolean isDecommissioning,
      int maxSegmentsInLoadQueue,
      int maxLifetimeInQueue
  )
  {
    this.server = server;
    this.peon = peon;
    this.isDecommissioning = isDecommissioning;

    this.maxAssignmentsInRun = maxSegmentsInLoadQueue == 0
                               ? Integer.MAX_VALUE
                               : maxSegmentsInLoadQueue - peon.getSegmentsToLoad().size();
    this.maxLifetimeInQueue = maxLifetimeInQueue;

    final AtomicInteger movingSegmentCount = new AtomicInteger();
    final AtomicInteger loadingReplicaCount = new AtomicInteger();
    initializeQueuedSegments(movingSegmentCount, loadingReplicaCount);

    this.movingSegmentCount = movingSegmentCount.get();
    this.loadingReplicaCount = loadingReplicaCount.get();
  }

  private void initializeQueuedSegments(
      AtomicInteger movingSegmentCount,
      AtomicInteger loadingReplicaCount
  )
  {
    for (DataSegment segment : server.iterateAllSegments()) {
      projectedSegments.addSegment(segment);
    }

    final List<SegmentHolder> expiredSegments = new ArrayList<>();
    for (SegmentHolder holder : peon.getSegmentsInQueue()) {
      int runsInQueue = holder.incrementAndGetRunsInQueue();
      if (runsInQueue > maxLifetimeInQueue) {
        expiredSegments.add(holder);
      }

      final SegmentAction action = holder.getAction();
      addToQueuedSegments(holder.getSegment(), simplify(action));

      if (action == SegmentAction.MOVE_TO) {
        movingSegmentCount.incrementAndGet();
      }
      if (action == SegmentAction.REPLICATE) {
        loadingReplicaCount.incrementAndGet();
      }
    }

    for (DataSegment segment : peon.getSegmentsMarkedToDrop()) {
      addToQueuedSegments(segment, SegmentAction.MOVE_FROM);
    }

    if (!expiredSegments.isEmpty()) {
      List<SegmentHolder> expiredSegmentsSubList =
          expiredSegments.size() > 10 ? expiredSegments.subList(0, 10) : expiredSegments;

      log.makeAlert(
          "Load queue for server [%s], tier [%s] has [%d] segments stuck.",
          server.getName(), server.getTier(), expiredSegments.size()
      )
         .addData("segments", expiredSegmentsSubList.toString())
         .addData("maxLifetime", maxLifetimeInQueue).emit();
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

  public long getSizeUsed()
  {
    return server.getCurrSize() + sizeOfLoadingSegments - sizeOfDroppingSegments;
  }

  public double getPercentUsed()
  {
    return (100.0 * getSizeUsed()) / getMaxSize();
  }

  public boolean isDecommissioning()
  {
    return isDecommissioning;
  }

  public boolean isLoadQueueFull()
  {
    return totalAssignmentsInRun >= maxAssignmentsInRun;
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
   */
  public SegmentCountsPerInterval getProjectedSegments()
  {
    return projectedSegments;
  }

  public boolean isProjectedSegment(DataSegment segment)
  {
    SegmentAction action = getActionOnSegment(segment);
    if (action == null) {
      return hasSegmentLoaded(segment.getId());
    } else {
      return action.isLoad();
    }
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

  /**
   * Segments that are currently loaded on this server.
   */
  public Collection<DataSegment> getServedSegments()
  {
    return server.iterateAllSegments();
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

  public int getNumMovingSegments()
  {
    return movingSegmentCount;
  }

  public int getNumLoadingReplicas()
  {
    return loadingReplicaCount;
  }

  public boolean startOperation(SegmentAction action, DataSegment segment)
  {
    if (queuedSegments.containsKey(segment)) {
      return false;
    }

    if (action.isLoad()) {
      ++totalAssignmentsInRun;
    }

    addToQueuedSegments(segment, simplify(action));
    return true;
  }

  public boolean cancelOperation(SegmentAction action, DataSegment segment)
  {
    // Cancel only if the action is currently in queue
    final SegmentAction queuedAction = queuedSegments.get(segment);
    if (queuedAction != simplify(action)) {
      return false;
    }

    // Try cancelling the operation on the peon
    // MOVE_FROM operations are not sent to the peon, so they can be considered cancelled
    if (queuedAction == SegmentAction.MOVE_FROM || peon.cancelOperation(segment)) {
      removeFromQueuedSegments(segment, queuedAction);
      return true;
    } else {
      return false;
    }
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

  private void addToQueuedSegments(DataSegment segment, SegmentAction action)
  {
    queuedSegments.put(segment, action);

    // Add to projected if load is started, remove from projected if drop has started
    if (action.isLoad()) {
      projectedSegments.addSegment(segment);
      sizeOfLoadingSegments += segment.getSize();
    } else {
      projectedSegments.removeSegment(segment);
      if (action == SegmentAction.DROP) {
        sizeOfDroppingSegments += segment.getSize();
      }
      // MOVE_FROM actions graduate to DROP after the corresponding MOVE_TO has finished
      // Do not consider size delta until then, otherwise we might over-assign the server
    }
  }

  private void removeFromQueuedSegments(DataSegment segment, SegmentAction action)
  {
    queuedSegments.remove(segment);

    if (action.isLoad()) {
      projectedSegments.removeSegment(segment);
      sizeOfLoadingSegments -= segment.getSize();
    } else {
      projectedSegments.addSegment(segment);
      if (action == SegmentAction.DROP) {
        sizeOfDroppingSegments -= segment.getSize();
      }
    }
  }

  @Override
  public int compareTo(ServerHolder serverHolder)
  {
    return MORE_AVAILABLE_SIZE_SERVER_FIRST.compare(this, serverHolder);
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

  @Override
  public String toString()
  {
    return "ServerHolder{" + server.getHost() + "}";
  }
}
