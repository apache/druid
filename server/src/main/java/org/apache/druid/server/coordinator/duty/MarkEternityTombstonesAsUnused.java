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
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
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
import org.apache.druid.timeline.partition.TombstoneShardSpec;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Mark eternity tombstones not overshadowed by currently served segments as unused. A candidate segment must fit all
 * the criteria:
 * <li> It is a tombstone that starts at {@link DateTimes#MIN} or ends at {@link DateTimes#MAX} </li>
 * <li> It does not overlap with any overshadowed segment in the datasource </li>
 * <li> It has has 0 core partitions i.e., {@link TombstoneShardSpec#getNumCorePartitions()} == 0</li>
 *
 * <p>
 * Only infinite-interval tombstones are considered as candidate segments in this duty because they
 * don't honor the preferred segment granularity specified at ingest time to cover an underlying segment with
 * {@link Granularities#ALL} as it can generate too many segments per time chunk and cause an OOM. The infinite-interval
 * tombstones make it hard to append data on the end of a data set that started out with an {@link Granularities#ALL} eternity
 * and then moved to actual time grains, so the compromise is that the coordinator will remove these segments as long as it
 * doesn't overlap any other segment.
 * </p>
 * <p>
 * The overlapping condition is necessary as a candidate segment can overlap with an overshadowed segment, and the latter
 * needs to be marked as unused first by {@link MarkOvershadowedSegmentsAsUnused} duty before the tombstone candidate
 * can be marked as unused by {@link MarkEternityTombstonesAsUnused} duty.
 *</p>
 * <p>
 * Only tombstones with 0 core partitions is considered as candidate segments. Earlier generation tombstones with 1 core
 * partition (i.e., {@link TombstoneShardSpec#getNumCorePartitions()} == 1) are ignored by this duty because it can potentially
 * cause data loss in a concurrent append and replace scenario and needs to be manually cleaned up. See this
 * <a href="https://github.com/apache/druid/pull/15379">for details</a>.
 * </p>
 */
public class MarkEternityTombstonesAsUnused implements CoordinatorDuty
{
  private static final Logger log = new Logger(MarkEternityTombstonesAsUnused.class);

  private final SegmentDeleteHandler deleteHandler;

  public MarkEternityTombstonesAsUnused(final SegmentDeleteHandler deleteHandler)
  {
    this.deleteHandler = deleteHandler;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(final DruidCoordinatorRuntimeParams params)
  {
    DataSourcesSnapshot dataSourcesSnapshot = params.getDataSourcesSnapshot();

    final Map<String, Set<SegmentId>> datasourceToNonOvershadowedEternityTombstones = 
        determineNonOvershadowedEternityTombstones(
        dataSourcesSnapshot
    );

    if (datasourceToNonOvershadowedEternityTombstones.size() == 0) {
      log.debug("No non-overshadowed eternity tombstones found.");
      return params;
    }

    log.debug("Found [%d] datasource containing non-overshadowed eternity tombstones[%s]",
              datasourceToNonOvershadowedEternityTombstones.size(), datasourceToNonOvershadowedEternityTombstones
    );

    final CoordinatorRunStats stats = params.getCoordinatorStats();
    datasourceToNonOvershadowedEternityTombstones.forEach((datasource, nonOvershadowedEternityTombstones) -> {
      final RowKey datasourceKey = RowKey.of(Dimension.DATASOURCE, datasource);
      stats.add(Stats.Segments.UNNEEDED_ETERNITY_TOMBSTONE, datasourceKey, nonOvershadowedEternityTombstones.size());
      final int unusedCount = deleteHandler.markSegmentsAsUnused(nonOvershadowedEternityTombstones);
      log.info(
          "Successfully marked [%d] non-overshadowed eternity tombstones[%s] of datasource[%s] as unused.",
          unusedCount,
          nonOvershadowedEternityTombstones,
          datasource
      );
    });

    return params;
  }

  /**
   * Computes the set of unneeded eternity tombstones per datasource using the datasources snapshot. The computation is
   * as follows:
   *
   * <li> Determine the set of used and non-overshadowed segments from the used segments' timeline. </li>
   * <li> For each such candidate segment that is a tombstone with an infinite start or end, look at the set of overshadowed
   * segments to see if any of the intervals overlaps with the candidate segment.
   * <li> If there is no overlap, add the candidate segment to the eternity segments result set. </li>
   * There can at most be two such candidate tombstones per datasource  -- one that starts at {@link DateTimes#MIN}
   * and another that ends at {@link DateTimes#MAX}. </li>
   * </p>
   *
   * @param dataSourcesSnapshot the datasources snapshot for segments timeline
   * @return the set of non-overshadowed eternity tombstones grouped by datasource
   */
  private Map<String, Set<SegmentId>> determineNonOvershadowedEternityTombstones(final DataSourcesSnapshot dataSourcesSnapshot)
  {
    final Map<String, Set<SegmentId>> datasourceToNonOvershadowedEternityTombstones = new HashMap<>();

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
        usedNonOvershadowedSegments.get().forEach(candidateSegment -> {
          if (isNewGenerationEternityTombstone(candidateSegment)) {
            boolean overlaps = dataSourcesSnapshot.getOvershadowedSegments().stream()
                                                  .filter(overshadowedSegment ->
                                                              candidateSegment.getDataSource()
                                                                              .equals(overshadowedSegment.getDataSource()))
                                                  .anyMatch(
                                                      overshadowedSegment ->
                                                          candidateSegment.getInterval()
                                                                          .overlaps(overshadowedSegment.getInterval())
                                                  );
            if (!overlaps) {
              datasourceToNonOvershadowedEternityTombstones
                  .computeIfAbsent(datasource, ds -> new HashSet<>())
                  .add(candidateSegment.getId());
            }
          }
        });
      }
    });

    return datasourceToNonOvershadowedEternityTombstones;
  }

  private boolean isNewGenerationEternityTombstone(final DataSegment segment)
  {
    return segment.isTombstone() && segment.getShardSpec().getNumCorePartitions() == 0 && (
        DateTimes.MIN.equals(segment.getInterval().getStart()) ||
        DateTimes.MAX.equals(segment.getInterval().getEnd()));
  }
}
