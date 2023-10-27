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
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.server.coordinator.stats.RowKey;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.SegmentTimeline;

import java.util.Collection;
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

  private final SegmentsMetadataManager segmentsMetadataManager;

  public MarkDanglingTombstonesAsUnused(final SegmentsMetadataManager metadataManager)
  {
    this.segmentsMetadataManager = metadataManager;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(final DruidCoordinatorRuntimeParams params)
  {
    DataSourcesSnapshot dataSourcesSnapshot = params.getDataSourcesSnapshot();

    final Map<String, Set<DataSegment>> datasourceToDanglingTombstones = getDatasourceToNonOvershadowedDanglingTombstones(
        dataSourcesSnapshot.getDataSourcesMap().keySet()
    );

    if (datasourceToDanglingTombstones.size() == 0) {
      log.info("No dangling tombstones found!");
      return params;
    }

    log.info("DatasourceToDanglingSegments size[%d] - [%s]",
             datasourceToDanglingTombstones.size(), datasourceToDanglingTombstones
    );

    final Map<String, Set<SegmentId>> datasourceToUnusedDanglingTombstones = getDatasourceToUnusedDanglingTombstones(
        datasourceToDanglingTombstones,
        params.getDataSourcesSnapshot().getUsedSegmentsTimelinesPerDataSource()
    );

    log.info("DatasourceToUnusedDanglingTombstones size[%d] - [%s]",
             datasourceToUnusedDanglingTombstones.size(), datasourceToUnusedDanglingTombstones
    );

    final CoordinatorRunStats stats = params.getCoordinatorStats();
    datasourceToUnusedDanglingTombstones.forEach((datasource, unusedSegments) -> {
      RowKey datasourceKey = RowKey.of(Dimension.DATASOURCE, datasource);
      stats.add(Stats.Segments.DANGLING_TOMBSTONE, datasourceKey, unusedSegments.size());
      int unusedCount = segmentsMetadataManager.markSegmentsAsUnused(unusedSegments);
      log.info(
          "Successfully marked [%d] dangling tombstones as unused for datasource[%s].",
          unusedCount,
          datasource
      );
    });

    return params;
  }

  private Map<String, Set<DataSegment>> getDatasourceToNonOvershadowedDanglingTombstones(final Set<String> datasources)
  {
    final Map<String, Set<DataSegment>> datasourceToDanglingSegments = new HashMap<>();

    datasources.forEach((datasource) -> {
      Optional<Iterable<DataSegment>> usedNonOvershadowedSegments = segmentsMetadataManager.iterateAllUsedNonOvershadowedSegmentsForDatasourceInterval(
          datasource,
          Intervals.ETERNITY,
          true
      );

      if (usedNonOvershadowedSegments.isPresent()) {
        usedNonOvershadowedSegments.get().forEach(usedNonOvershadowedSegment -> {
          if (isDanglingTombstone(usedNonOvershadowedSegment)) {
            datasourceToDanglingSegments.computeIfAbsent(
                usedNonOvershadowedSegment.getDataSource(),
                ds -> new HashSet<>()
            ).add(usedNonOvershadowedSegment);
          }
        });
      }
    });

    return datasourceToDanglingSegments;
  }

  private Map<String, Set<SegmentId>> getDatasourceToUnusedDanglingTombstones(
      final Map<String, Set<DataSegment>> datasourceToDanglingSegments,
      final Map<String, SegmentTimeline> usedSegmentsTimelinesPerDataSource
  )
  {
    final Map<String, Set<SegmentId>> datasourceToUnusedDanglingSegmentIds = new HashMap<>();

    datasourceToDanglingSegments.forEach((datasource, danglingSegments) -> {
      // We have to look at the used segments set and not just at the overshadowed segments because there's no guarantee
      // that the overshadowed set is constructed. We can only guarantee that the used segments is accurate. For example, if you run
      // an insert and replace in quick succession, the appended segment will be used, but perhaps not marked as overshadowed by the
      // coordinator (as part of the snapshot). So there's a window where dangling segments can be prematurely marked as unused and expose
      // the underlying appended data.
      Collection<DataSegment> usedSegments = usedSegmentsTimelinesPerDataSource.get(datasource).iterateAllObjects();

      danglingSegments.forEach(danglingSegment -> {
        boolean overlaps = usedSegments.stream()
                                       .filter(segment -> !segment.getId().equals(danglingSegment.getId()))
                                       .anyMatch(
                                           nonOvershadowedObject -> nonOvershadowedObject.getInterval()
                                                                                         .overlaps(danglingSegment.getInterval())
                                       );
        if (!overlaps) {
          datasourceToUnusedDanglingSegmentIds
              .computeIfAbsent(danglingSegment.getDataSource(), ds -> new HashSet<>())
              .add(danglingSegment.getId());
        }
      });
    });
    return datasourceToUnusedDanglingSegmentIds;
  }

  private boolean isDanglingTombstone(final DataSegment segment)
  {
    return segment.isTombstone() && (
        Intervals.ETERNITY.getStart().equals(segment.getInterval().getStart()) ||
        Intervals.ETERNITY.getEnd().equals(segment.getInterval().getEnd()));
  }
}
