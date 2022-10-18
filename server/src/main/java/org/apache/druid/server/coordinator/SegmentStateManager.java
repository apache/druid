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

import com.google.inject.Inject;
import org.apache.druid.client.ServerInventoryView;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.timeline.DataSegment;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages state of segments being loaded.
 */
public class SegmentStateManager
{
  private final LoadQueueTaskMaster taskMaster;
  private final ServerInventoryView serverInventoryView;
  private final SegmentsMetadataManager segmentsMetadataManager;

  private final ConcurrentHashMap<String, TierLoadingState> currentlyMovingSegments =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, TierLoadingState> currentlyReplicatingSegments
      = new ConcurrentHashMap<>();

  @Inject
  public SegmentStateManager(
      ServerInventoryView serverInventoryView,
      SegmentsMetadataManager segmentsMetadataManager,
      LoadQueueTaskMaster taskMaster
  )
  {
    this.serverInventoryView = serverInventoryView;
    this.segmentsMetadataManager = segmentsMetadataManager;
    this.taskMaster = taskMaster;
  }

  /**
   * Queues load of the segment on the given server.
   */
  public boolean loadSegment(
      DataSegment segment,
      ServerHolder server,
      boolean isFirstLoad,
      ReplicationThrottler throttler
  )
  {
    // Check if this load operation has to be throttled
    final String tier = server.getServer().getTier();
    if (!isFirstLoad && !canLoadReplica(tier, throttler)) {
      throttler.incrementThrottledReplicas(tier);
      return false;
    }

    try {
      if (!server.canLoadSegment(segment)
          || !server.startOperation(segment, SegmentState.LOADING)) {
        return false;
      }

      final LoadPeonCallback callback;
      if (isFirstLoad) {
        callback = null;
      } else {
        throttler.incrementAssignedReplicas(tier);

        final TierLoadingState replicatingInTier = currentlyReplicatingSegments
            .computeIfAbsent(tier, t -> new TierLoadingState(throttler.getMaxLifetime()));
        replicatingInTier.markStarted(segment.getId(), server.getServer().getHost());
        callback = success -> replicatingInTier.markCompleted(segment.getId());
      }

      SegmentAction loadType = isFirstLoad ? SegmentAction.PRIORITY_LOAD : SegmentAction.LOAD;
      server.getPeon().loadSegment(segment, loadType, callback);
      return true;
    }
    catch (Exception e) {
      server.cancelOperation(segment, SegmentState.LOADING);
      return false;
    }
  }

  public boolean dropSegment(DataSegment segment, ServerHolder server)
  {
    try {
      if (!server.startOperation(segment, SegmentState.DROPPING)) {
        return false;
      }

      server.getPeon().dropSegment(segment, null);
      return true;
    }
    catch (Exception e) {
      server.cancelOperation(segment, SegmentState.DROPPING);
      return false;
    }
  }

  public boolean moveSegment(
      DataSegment segment,
      ServerHolder fromServer,
      ServerHolder toServer,
      int maxLifetimeInBalancingQueue
  )
  {
    final TierLoadingState segmentsMovingInTier = currentlyMovingSegments.computeIfAbsent(
        toServer.getServer().getTier(),
        t -> new TierLoadingState(maxLifetimeInBalancingQueue)
    );
    final LoadQueuePeon fromServerPeon = fromServer.getPeon();
    final LoadPeonCallback moveFinishCallback = success -> {
      fromServerPeon.unmarkSegmentToDrop(segment);
      segmentsMovingInTier.markCompleted(segment.getId());
    };

    // mark segment to drop before it is actually loaded on server
    // to be able to account for this information in BalancerStrategy immediately
    toServer.startOperation(segment, SegmentState.MOVING_TO);
    fromServerPeon.markSegmentToDrop(segment);
    segmentsMovingInTier.markStarted(segment.getId(), fromServer.getServer().getHost());

    final LoadQueuePeon toServerPeon = toServer.getPeon();
    final String toServerName = toServer.getServer().getName();
    try {
      toServerPeon.loadSegment(
          segment,
          SegmentAction.MOVE_TO,
          success -> {
            // Drop segment only if:
            // (1) segment load was successful on toServer
            // AND (2) segment not already queued for drop on fromServer
            // AND (3a) loading is http-based
            //     OR (3b) inventory shows segment loaded on toServer

            // Do not check the inventory with http loading as the HTTP
            // response is enough to determine load success or failure
            if (success
                && !fromServerPeon.getSegmentsToDrop().contains(segment)
                && (taskMaster.isHttpLoading()
                    || serverInventoryView.isSegmentLoadedByServer(toServerName, segment))) {
              fromServerPeon.dropSegment(segment, moveFinishCallback);
            } else {
              moveFinishCallback.execute(success);
            }
          }
      );
    }
    catch (Exception e) {
      toServer.cancelOperation(segment, SegmentState.MOVING_TO);
      moveFinishCallback.execute(false);
      throw new RuntimeException(e);
    }

    return true;
  }

  /**
   * Marks the given segment as unused.
   */
  public boolean deleteSegment(DataSegment segment)
  {
    return segmentsMetadataManager.markSegmentAsUnused(segment.getId());
  }

  /**
   * Cancels the segment operation being performed on a server if the actual
   * state of the segment on the server matches the given currentState.
   */
  public boolean cancelOperation(
      SegmentState currentState,
      DataSegment segment,
      ServerHolder server
  )
  {
    if (!server.cancelOperation(segment, currentState)) {
      return false;
    }

    final LoadQueuePeon peon = server.getPeon();
    switch (currentState) {
      case DROPPING:
        return peon.cancelDrop(segment);
      case MOVING_TO:
      case LOADING:
        return peon.cancelLoad(segment);
      default:
        return false;
    }
  }

  /**
   * Reduces the lifetimes of the segments currently being moved in all the tiers,
   * and returns a map from tier names to the corresponding state.
   */
  public Map<String, TierLoadingState> reduceLifetimesOfMovingSegments()
  {
    return reduceLifetimesAndCreateCopy(currentlyMovingSegments);
  }

  /**
   * Reduces the lifetimes of the segments currently being replicated in the tiers,
   * and returns a map from tier names to the corresponding state.
   */
  public Map<String, TierLoadingState> reduceLifetimesOfReplicatingSegments()
  {
    return reduceLifetimesAndCreateCopy(currentlyReplicatingSegments);
  }

  private Map<String, TierLoadingState> reduceLifetimesAndCreateCopy(
      Map<String, TierLoadingState> inFlightSegments
  )
  {
    final Set<String> inactiveTiers = new HashSet<>();
    inFlightSegments.forEach((tier, holder) -> {
      if (holder.getNumProcessingSegments() == 0) {
        inactiveTiers.add(tier);
      }
      holder.reduceLifetime();
    });

    // Reset state for inactive tiers
    inactiveTiers.forEach(inFlightSegments::remove);

    return Collections.unmodifiableMap(inFlightSegments);
  }

  private boolean canLoadReplica(String tier, ReplicationThrottler throttler)
  {
    final TierLoadingState tierState = currentlyReplicatingSegments.get(tier);

    return tierState == null
           || throttler.canAssignReplica(tier, tierState.getNumProcessingSegments());
  }

}
