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
import io.druid.java.util.common.StringUtils;
import io.druid.server.coordinator.helper.DruidCoordinatorBalancer;
import io.druid.timeline.DataSegment;

public class DruidCoordinatorBalancerTester extends DruidCoordinatorBalancer
{
  public DruidCoordinatorBalancerTester(DruidCoordinator coordinator)
  {
    super(coordinator);
  }

  @Override
  protected void moveSegment(
      final BalancerSegmentHolder segment,
      final ImmutableDruidServer toServer,
      final DruidCoordinatorRuntimeParams params
  )
  {
    final String toServerName = toServer.getName();
    final LoadQueuePeon toPeon = params.getLoadManagementPeons().get(toServerName);

    final String fromServerName = segment.getFromServer().getName();
    final DataSegment segmentToMove = segment.getSegment();
    final String segmentName = segmentToMove.getIdentifier();

    if (!toPeon.getSegmentsToLoad().contains(segmentToMove) &&
        !currentlyMovingSegments.get("normal").containsKey(segmentName) &&
        new ServerHolder(toServer, toPeon).getAvailableSize() > segmentToMove.getSize()) {
      log.info(
          "Moving [%s] from [%s] to [%s]",
          segmentName,
          fromServerName,
          toServerName
      );
      try {
        final LoadQueuePeon loadPeon = params.getLoadManagementPeons().get(toServerName);

        loadPeon.loadSegment(segment.getSegment(), new LoadPeonCallback()
        {
          @Override
          public void execute()
          {
          }
        });

        currentlyMovingSegments.get("normal").put(segmentName, segment);
      }
      catch (Exception e) {
        log.info(e, StringUtils.format("[%s] : Moving exception", segmentName));
      }
    } else {
      currentlyMovingSegments.get("normal").remove(segmentName);
    }
  }
}
