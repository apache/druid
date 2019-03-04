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

package org.apache.druid.server.coordinator.helper;

import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.timeline.DataSegment;

import java.util.TreeSet;

public class DruidCoordinatorSegmentInfoLoader implements DruidCoordinatorHelper
{
  private final DruidCoordinator coordinator;

  private static final EmittingLogger log = new EmittingLogger(DruidCoordinatorSegmentInfoLoader.class);

  public DruidCoordinatorSegmentInfoLoader(DruidCoordinator coordinator)
  {
    this.coordinator = coordinator;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    log.info("Starting coordination. Getting available segments.");

    final TreeSet<DataSegment> availableSegments = DruidCoordinatorRuntimeParams.createAvailableSegmentsSet();
    for (DataSegment segment : coordinator.iterateAvailableDataSegments()) {
      if (segment.getSize() < 0) {
        log.makeAlert("No size on a segment")
           .addData("segment", segment)
           .emit();
      }
      availableSegments.add(segment);
    }

    // Log info about all available segments
    if (log.isDebugEnabled()) {
      log.debug("Available DataSegments");
      for (DataSegment dataSegment : availableSegments) {
        log.debug("  %s", dataSegment);
      }
    }

    log.info("Found [%,d] available segments.", availableSegments.size());

    return params.buildFromExisting()
                 .setAvailableSegments(availableSegments)
                 .build();
  }
}
