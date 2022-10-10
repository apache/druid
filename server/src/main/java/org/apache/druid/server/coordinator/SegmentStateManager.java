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

import org.apache.druid.client.ServerInventoryView;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Manages state of segments being loaded.
 */
public class SegmentStateManager
{
  private static final EmittingLogger log = new EmittingLogger(SegmentStateManager.class);

  private final boolean isHttpLoading;
  private final ServerInventoryView serverInventoryView;
  private final SegmentsMetadataManager segmentsMetadataManager;
  private final ReplicationThrottler replicationThrottler = new ReplicationThrottler();

  // This should be merged with currently replicating segments as they use the
  // same lifetime expiration logic
  private final Map<String, ConcurrentHashMap<SegmentId, BalancerSegmentHolder>>
      currentlyMovingSegments = new HashMap<>();

  public SegmentStateManager(
      ServerInventoryView serverInventoryView,
      SegmentsMetadataManager segmentsMetadataManager,
      boolean isHttpLoading
  )
  {
    this.serverInventoryView = serverInventoryView;
    this.segmentsMetadataManager = segmentsMetadataManager;
    this.isHttpLoading = isHttpLoading;
  }

  /**
   * Resets the state of the ReplicationThrottler and updates the lifetime of
   * balancing and replicating segments in the queue.
   */
  public void prepareForRun(DruidCoordinatorRuntimeParams runtimeParams)
  {
    final CoordinatorDynamicConfig dynamicConfig = runtimeParams.getCoordinatorDynamicConfig();
    replicationThrottler.resetParams(
        dynamicConfig.getReplicationThrottleLimit(),
        dynamicConfig.getReplicantLifetime(),
        dynamicConfig.getMaxNonPrimaryReplicantsToLoad()
    );
    replicationThrottler.updateReplicationState();
    updateMovingSegmentLifetimes();
  }

  /**
   * Queues load of a replica of the segment on the given server.
   */
  public boolean loadSegment(DataSegment segment, ServerHolder server, boolean isPrimary)
  {
    final String tier = server.getServer().getTier();
    if (isPrimary) {
      // Primary replicas are not subject to throttling
    } else if (replicationThrottler.canCreateReplicant(tier)) {
      replicationThrottler.registerReplicantCreation(tier, segment.getId(), server.getServer().getHost());
    } else {
      return false;
    }

    try {
      server.startOperation(segment, SegmentState.LOADING);
      server.getPeon().loadSegment(
          segment,
          isPrimary ? SegmentAction.LOAD_AS_PRIMARY : SegmentAction.LOAD_AS_REPLICA,
          success -> replicationThrottler.unregisterReplicantCreation(tier, segment.getId())
      );
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
      server.startOperation(segment, SegmentState.DROPPING);
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
      ServerHolder toServer
  )
  {
    final ConcurrentMap<SegmentId, BalancerSegmentHolder> segmentsMovingInTier = currentlyMovingSegments
        .computeIfAbsent(toServer.getServer().getTier(), t -> new ConcurrentHashMap<>());
    final LoadQueuePeon fromServerPeon = fromServer.getPeon();
    final LoadPeonCallback moveFinishCallback = success -> {
      fromServerPeon.unmarkSegmentToDrop(segment);
      segmentsMovingInTier.remove(segment.getId());
    };

    // mark segment to drop before it is actually loaded on server
    // to be able to account for this information in BalancerStrategy immediately
    toServer.startOperation(segment, SegmentState.MOVING_TO);
    fromServerPeon.markSegmentToDrop(segment);
    segmentsMovingInTier.put(segment.getId(), new BalancerSegmentHolder(fromServer, segment));

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
                && (isHttpLoading
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
   * Gets the number of segments currently being moved in this tier.
   */
  public int getNumMovingSegments(String tier)
  {
    ConcurrentHashMap<SegmentId, BalancerSegmentHolder> segmentsMovingInTier
        = currentlyMovingSegments.get(tier);
    return segmentsMovingInTier == null ? 0 : segmentsMovingInTier.size();
  }

  /**
   * Cancels the segment operation being performed on a server if the state of
   * the segment on this server matches the given currentState.
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
   * Updates the lifetimes of the the segments being moved in all the tiers.
   */
  private void updateMovingSegmentLifetimes()
  {
    for (String tier : currentlyMovingSegments.keySet()) {
      for (BalancerSegmentHolder holder : currentlyMovingSegments.get(tier).values()) {
        holder.reduceLifetime();
        if (holder.getLifetime() <= 0) {
          log.makeAlert("[%s]: Balancer move segments queue has a segment stuck", tier)
             .addData("segment", holder.getSegment().getId())
             .addData("server", holder.getFromServer().getServer().getMetadata())
             .emit();
        }
      }
    }
  }

}
