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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.coordinator.helper.DruidCoordinatorBalancer;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;

public class DruidCoordinatorBalancerTester extends DruidCoordinatorBalancer
{

  public DruidCoordinatorBalancerTester(DruidCoordinator coordinator)
  {
    super(coordinator);
  }

  @Override
  protected boolean moveSegment(
      final BalancerSegmentHolder segment,
      final ImmutableDruidServer toServer,
      final DruidCoordinatorRuntimeParams params
  )
  {
    final String toServerName = toServer.getName();
    final LoadQueuePeon toPeon = params.getLoadManagementPeons().get(toServerName);

    final String fromServerName = segment.getFromServer().getName();
    final DataSegment segmentToMove = segment.getSegment();
    final SegmentId segmentId = segmentToMove.getId();

    if (!toPeon.getSegmentsToLoad().contains(segmentToMove) &&
        (toServer.getSegment(segmentId) == null) &&
        new ServerHolder(toServer, toPeon).getAvailableSize() > segmentToMove.getSize()) {
      log.info(
          "Moving [%s] from [%s] to [%s]",
          segmentId,
          fromServerName,
          toServerName
      );
      try {
        final LoadQueuePeon loadPeon = params.getLoadManagementPeons().get(toServerName);

        loadPeon.loadSegment(segment.getSegment(), () -> {});

        final LoadQueuePeon dropPeon = params.getLoadManagementPeons().get(fromServerName);
        dropPeon.markSegmentToDrop(segment.getSegment());

        currentlyMovingSegments.get("normal").put(segmentId, segment);
        return true;
      }
      catch (Exception e) {
        log.info(e, StringUtils.format("[%s] : Moving exception", segmentId));
      }
    }
    return false;
  }
}
