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

package io.druid.server.coordinator.helper;

import com.metamx.common.logger.Logger;
import io.druid.server.coordinator.DruidCoordinator;
import io.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import io.druid.timeline.DataSegment;

import java.util.Set;

public class DruidCoordinatorSegmentInfoLoader implements DruidCoordinatorHelper
{
  private final DruidCoordinator coordinator;

  private static final Logger log = new Logger(DruidCoordinatorSegmentInfoLoader.class);

  public DruidCoordinatorSegmentInfoLoader(DruidCoordinator coordinator)
  {
    this.coordinator = coordinator;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    // Display info about all available segments
    final Set<DataSegment> availableSegments = coordinator.getAvailableDataSegments();
    if (log.isDebugEnabled()) {
      log.debug("Available DataSegments");
      for (DataSegment dataSegment : availableSegments) {
        log.debug("  %s", dataSegment);
      }
    }

    return params.buildFromExisting()
                 .withAvailableSegments(availableSegments)
                 .build();
  }
}
