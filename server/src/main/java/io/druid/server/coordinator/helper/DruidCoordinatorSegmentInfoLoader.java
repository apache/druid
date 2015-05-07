/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
    log.info("Starting coordination. Getting available segments.");

    // Display info about all available segments
    final Set<DataSegment> availableSegments = coordinator.getOrderedAvailableDataSegments();
    if (log.isDebugEnabled()) {
      log.debug("Available DataSegments");
      for (DataSegment dataSegment : availableSegments) {
        log.debug("  %s", dataSegment);
      }
    }

    log.info("Found [%,d] available segments.", availableSegments.size());

    return params.buildFromExisting()
                 .withAvailableSegments(availableSegments)
                 .build();
  }
}
