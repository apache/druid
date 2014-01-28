/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.server.coordinator;

import io.druid.client.DruidServer;
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
      final DruidServer toServer,
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
        !toServer.getSegments().containsKey(segmentName) &&
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
        log.info(e, String.format("[%s] : Moving exception", segmentName));
      }
    } else {
      currentlyMovingSegments.get("normal").remove(segment);
    }
  }
}
