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

import com.google.common.base.Optional;
import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.server.coordinator.stats.RowKey;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.Partitions;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.SegmentTimeline;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Marks dangling tombstones not overshadowed by currently served segments as unused.
 * Dangling tombstones are tombstone segments with their start or end interval as eternity. So each datasource
 * can have at most two dangling segments.
 */
public class MarkDanglingTombstonesAsUnused implements CoordinatorDuty
{
  private static final Logger log = new Logger(MarkDanglingTombstonesAsUnused.class);

  private final SegmentDeleteHandler deleteHandler;

  public MarkDanglingTombstonesAsUnused(final SegmentDeleteHandler deleteHandler)
  {
    this.deleteHandler = deleteHandler;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(final DruidCoordinatorRuntimeParams params)
  {
    DataSourcesSnapshot dataSourcesSnapshot = params.getDataSourcesSnapshot();

    final Map<String, Set<SegmentId>> datasourceToDanglingTombstones = determineDanglingTombstones(dataSourcesSnapshot);

    if (datasourceToDanglingTombstones.size() == 0) {
      log.debug("No dangling tombstones found.");
      return params;
    }

    log.debug("Found [%d] datasource dangling tombstones  [%s]",
              datasourceToDanglingTombstones.size(), datasourceToDanglingTombstones
    );

    final CoordinatorRunStats stats = params.getCoordinatorStats();
    datasourceToDanglingTombstones.forEach((datasource, unusedSegments) -> {
      RowKey datasourceKey = RowKey.of(Dimension.DATASOURCE, datasource);
      stats.add(Stats.Segments.DANGLING_TOMBSTONE, datasourceKey, unusedSegments.size());
      int unusedCount = deleteHandler.markSegmentsAsUnused(unusedSegments);
      log.info(
          "Successfully marked [%d] dangling tombstones as unused for datasource[%s].",
          unusedCount,
          datasource
      );
    });

    return params;
  }

  /**
   * Computes the set of dangling tombstones using the datasource snapshot.
   *
   * <li> Determine the set of used and non-overshadowed segments from the used segments' timeline. </li>
   * <li> For each such segment that is dangling, look at the set of overshadowed segments to see if the intervals overlap.
   * There can at most be two such dangling tombstones per datasource. </li>
   * <li> If there is no overlap, add the dangling segment to the result set to be marked as unused. </li>
   * </p>
   *
   * @param dataSourcesSnapshot the datasources snapshot for segments timeline and overshadowed segments
   * @return the set of dangling tombstones grouped by datasource
   */
  private Map<String, Set<SegmentId>> determineDanglingTombstones(final DataSourcesSnapshot dataSourcesSnapshot)
  {
    final Map<String, Set<SegmentId>> datasourceToDanglingTombstones = new HashMap<>();

    dataSourcesSnapshot.getDataSourcesMap().keySet().forEach((datasource) -> {
      final SegmentTimeline usedSegmentsTimeline
          = dataSourcesSnapshot.getUsedSegmentsTimelinesPerDataSource().get(datasource);

      final Optional<Set<DataSegment>> usedNonOvershadowedSegments =
          Optional.fromNullable(usedSegmentsTimeline)
                  .transform(timeline -> timeline.findNonOvershadowedObjectsInInterval(
                      Intervals.ETERNITY,
                      Partitions.ONLY_COMPLETE
                  ));

      if (usedNonOvershadowedSegments.isPresent()) {
        usedNonOvershadowedSegments.get().forEach(usedNonOvershadowedSegment -> {
          if (isDanglingTombstone(usedNonOvershadowedSegment)) {
            boolean overlaps = dataSourcesSnapshot.getOvershadowedSegments().stream()
                                                  .anyMatch(
                                                      overshadowedSegment ->
                                                          datasource.equals(overshadowedSegment.getDataSource()) &&
                                                          isDanglingTombstone(usedNonOvershadowedSegment) &&
                                                          usedNonOvershadowedSegment.getInterval()
                                                                                    .overlaps(overshadowedSegment.getInterval())
                                                  );
            if (!overlaps) {
              datasourceToDanglingTombstones
                  .computeIfAbsent(datasource, ds -> new HashSet<>())
                  .add(usedNonOvershadowedSegment.getId());
            }
          }
        });
      }
    });

    return datasourceToDanglingTombstones;
  }

  private boolean isDanglingTombstone(final DataSegment segment)
  {
    return segment.isTombstone() && (
        Intervals.ETERNITY.getStart().equals(segment.getInterval().getStart()) ||
        Intervals.ETERNITY.getEnd().equals(segment.getInterval().getEnd()));
  }
}
