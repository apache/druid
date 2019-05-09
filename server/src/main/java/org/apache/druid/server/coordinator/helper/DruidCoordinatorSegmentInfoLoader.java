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

import com.google.common.collect.Iterables;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.timeline.DataSegment;

import java.util.TreeSet;

public class DruidCoordinatorSegmentInfoLoader implements DruidCoordinatorHelper
{
  private static final EmittingLogger log = new EmittingLogger(DruidCoordinatorSegmentInfoLoader.class);

  private final DruidCoordinator coordinator;

  public DruidCoordinatorSegmentInfoLoader(DruidCoordinator coordinator)
  {
    this.coordinator = coordinator;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    log.info("Starting coordination. Getting used segments.");

    final Iterable<DataSegment> usedSegments = coordinator.iterateAllUsedSegments();

    // The following transform() call doesn't actually transform the iterable. It only checks the sizes of the segments
    // and emits alerts if segments with negative sizes are encountered. In other words, semantically it's similar to
    // Stream.peek(). It works as long as DruidCoordinatorRuntimeParams.createUsedSegmentsSet() (which is called
    // below) guarantees to go over the passed iterable exactly once.
    //
    // An iterable returned from iterateAllUsedSegments() is not simply iterated (with size checks) before passing
    // into DruidCoordinatorRuntimeParams.createUsedSegmentsSet() because iterateAllUsedSegments()'s
    // documentation says to strive to avoid iterating the result more than once.
    //
    //noinspection StaticPseudoFunctionalStyleMethod: https://youtrack.jetbrains.com/issue/IDEA-153047
    Iterable<DataSegment> usedSegmentsWithSizeChecking = Iterables.transform(
        usedSegments,
        segment -> {
          if (segment.getSize() < 0) {
            log.makeAlert("No size on a segment")
               .addData("segment", segment)
               .emit();
          }
          return segment;
        }
    );
    final TreeSet<DataSegment> usedSegmentSet =
        DruidCoordinatorRuntimeParams.createUsedSegmentsSet(usedSegmentsWithSizeChecking);

    // Log info about all used segments
    if (log.isDebugEnabled()) {
      log.debug("Used Segments");
      for (DataSegment dataSegment : usedSegmentSet) {
        log.debug("  %s", dataSegment);
      }
    }

    log.info("Found [%,d] used segments.", usedSegmentSet.size());

    return params.buildFromExisting()
                 .setUsedSegments(usedSegmentSet)
                 .build();
  }
}
