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

package org.apache.druid.server.coordinator.duty;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.loading.LoadQueueTaskMaster;
import org.apache.druid.timeline.DataSegment;

import java.util.List;

/**
 * Drops the source replica of a move once the Coordinator's inventory view confirms the destination replica.
 * <p>
 * When {@link org.apache.druid.server.coordinator.config.HttpLoadQueuePeonConfig#isConfirmMoveBeforeDrop()} is set,
 * {@link org.apache.druid.server.coordinator.loading.SegmentLoadQueueManager#moveSegment} deliberately does not drop
 * from the source when the destination acknowledges the load. The acknowledgement only means the destination has the
 * segment; Brokers do not find out until their own sync with that server lands. Dropping at ack time means a Broker
 * can process the drop before the load and briefly see no server at all for the segment, silently returning partial
 * results. See apache/druid#18738.
 * <p>
 * So the source is left marked with {@link org.apache.druid.server.coordinator.loading.SegmentAction#MOVE_FROM}
 * and this duty finishes the job on a later run,
 * once some other server is observed to be actually serving the segment. Until then the source keeps serving it, and
 * the MOVE_FROM marker keeps the Coordinator from counting the replica twice.
 */
public class CompletePendingMoves implements CoordinatorDuty
{
  private static final Logger log = new Logger(CompletePendingMoves.class);

  private final LoadQueueTaskMaster taskMaster;

  public CompletePendingMoves(LoadQueueTaskMaster taskMaster)
  {
    this.taskMaster = taskMaster;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    if (!taskMaster.isConfirmMoveBeforeDrop()) {
      return params;
    }

    final List<ServerHolder> allServers = params.getDruidCluster().getAllManagedServers();

    int dropsQueued = 0;
    for (ServerHolder source : allServers) {
      // Read the marks straight off the peon rather than scanning the run's whole queued-segment map. The marks are
      // exactly the pending MOVE_FROMs and there are only ever a handful, whereas the queued map is proportional to
      // load queue depth across every server in the cluster.
      for (DataSegment segment : source.getPeon().getSegmentsMarkedToDrop()) {
        if (isServedByAnyOtherServer(segment, source, allServers)) {
          // dropSegment retires the MOVE_FROM marker atomically, so a failed drop simply leaves the segment on the
          // source for the usual over-replication handling to pick up rather than pinning it here forever.
          source.getPeon().dropSegment(segment, null);
          ++dropsQueued;
        }
      }
    }

    if (dropsQueued > 0) {
      log.debug("Queued [%d] drops for moves whose destination replica is now serving.", dropsQueued);
    }

    return params;
  }

  private boolean isServedByAnyOtherServer(DataSegment segment, ServerHolder source, List<ServerHolder> allServers)
  {
    for (ServerHolder candidate : allServers) {
      // isServingSegment() is true only when the inventory view shows the segment loaded and no action is pending on
      // it, which is exactly the confirmation this duty waits for.
      if (!candidate.equals(source) && candidate.isServingSegment(segment)) {
        return true;
      }
    }
    return false;
  }
}
