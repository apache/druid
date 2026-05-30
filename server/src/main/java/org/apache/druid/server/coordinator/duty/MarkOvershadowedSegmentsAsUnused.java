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

import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.java.util.common.Stopwatch;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.server.coordinator.stats.RowKey;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.SegmentTimeline;
import org.joda.time.Duration;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Marks a segment as unused if it is overshadowed by:
 * <ul>
 * <li>a segment served by a historical or broker</li>
 * <li>a segment that has zero required replicas and thus will never be loaded on a server</li>
 * </ul>
 * <p>
 * This duty runs only if the Coordinator has been running long enough to have a
 * refreshed metadata view. This duration is controlled by the dynamic config
 * {@link org.apache.druid.server.coordinator.CoordinatorDynamicConfig#markSegmentAsUnusedDelayMillis}.
 */
public class MarkOvershadowedSegmentsAsUnused implements CoordinatorDuty
{
  private static final Logger log = new Logger(MarkOvershadowedSegmentsAsUnused.class);

  private final MetadataAction.DeleteSegments deleteHandler;
  private final Stopwatch sinceCoordinatorStarted = Stopwatch.createStarted();

  public MarkOvershadowedSegmentsAsUnused(MetadataAction.DeleteSegments deleteHandler)
  {
    this.deleteHandler = deleteHandler;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    final Stopwatch totalTime = Stopwatch.createStarted();

    // Mark overshadowed segments as unused only if the coordinator has been running
    // long enough to have refreshed its metadata view
    final Duration requiredDelay = Duration.millis(
        params.getCoordinatorDynamicConfig().getMarkSegmentAsUnusedDelayMillis()
    );
    if (sinceCoordinatorStarted.hasNotElapsed(requiredDelay)) {
      log.info(
          "Skipping MarkOvershadowedSegmentsAsUnused; required delay[%s] not yet elapsed since coordinator start.",
          requiredDelay
      );
      return params;
    }

    final Set<DataSegment> allOvershadowedSegments = params.getDataSourcesSnapshot().getOvershadowedSegments();
    if (allOvershadowedSegments.isEmpty()) {
      log.info("No overshadowed segments found in metadata snapshot. Total time[%,d]ms.", totalTime.millisElapsed());
      return params;
    }

    // Identify the datasources that actually have overshadowed segments to check.
    // Timelines only need to be built for these datasources.
    final Set<String> relevantDatasources = new HashSet<>();
    for (DataSegment s : allOvershadowedSegments) {
      relevantDatasources.add(s.getDataSource());
    }

    final DruidCluster cluster = params.getDruidCluster();
    final int totalDatasources = params.getDataSourcesSnapshot().getDataSourcesMap().size();

    final Stopwatch timelineBuildTime = Stopwatch.createStarted();
    final Map<String, SegmentTimeline> timelines = new HashMap<>();
    final long[] servedSegmentsCounter = {0};

    cluster.getManagedHistoricals().values().forEach(
        historicals -> historicals.forEach(
            historical -> servedSegmentsCounter[0] += addSegmentsFromServer(historical, timelines, relevantDatasources)
        )
    );
    cluster.getBrokers().forEach(
        broker -> servedSegmentsCounter[0] += addSegmentsFromServer(broker, timelines, relevantDatasources)
    );

    // Include all segments that require zero replicas to be loaded
    final int[] zeroReplicaSegments = {0};
    params.getSegmentAssigner().getSegmentsWithZeroRequiredReplicas().forEach(
        (datasource, segments) -> {
          if (relevantDatasources.contains(datasource)) {
            zeroReplicaSegments[0] += segments.size();
            timelines.computeIfAbsent(datasource, ds -> new SegmentTimeline())
                     .addSegments(segments.iterator());
          }
        }
    );
    timelineBuildTime.stop();

    // Do not include segments served by ingestion services such as tasks or indexers,
    // to prevent unpublished segments from prematurely overshadowing segments.

    // Mark all segments overshadowed by served segments as unused
    final Stopwatch checkTime = Stopwatch.createStarted();
    final Map<String, Set<SegmentId>> datasourceToUnusedSegments = new HashMap<>();
    int verifiedOvershadowed = 0;
    for (DataSegment dataSegment : allOvershadowedSegments) {
      SegmentTimeline timeline = timelines.get(dataSegment.getDataSource());
      if (timeline != null && timeline.isOvershadowed(dataSegment)) {
        datasourceToUnusedSegments.computeIfAbsent(dataSegment.getDataSource(), ds -> new HashSet<>())
                                  .add(dataSegment.getId());
        verifiedOvershadowed++;
      }
    }
    checkTime.stop();

    final Stopwatch dbUpdateTime = Stopwatch.createStarted();
    final CoordinatorRunStats stats = params.getCoordinatorStats();
    final int[] totalMarked = {0};
    datasourceToUnusedSegments.forEach(
        (datasource, unusedSegments) -> {
          RowKey datasourceKey = RowKey.of(Dimension.DATASOURCE, datasource);
          stats.add(Stats.Segments.OVERSHADOWED, datasourceKey, unusedSegments.size());

          final Stopwatch updateTime = Stopwatch.createStarted();
          int updatedCount = deleteHandler.markSegmentsAsUnused(datasource, unusedSegments);
          totalMarked[0] += updatedCount;
          log.info(
              "Marked [%d] segments of datasource[%s] as unused in [%,d]ms.",
              updatedCount, datasource, updateTime.millisElapsed()
          );
        }
    );
    dbUpdateTime.stop();

    log.info(
        "MarkOvershadowedSegmentsAsUnused summary: overshadowedInMetadata[%,d], relevantDatasources[%d/%d],"
        + " servedSegmentsAdded[%,d], zeroReplicaSegmentsAdded[%,d], verifiedOvershadowed[%,d], marked[%,d];"
        + " timelineBuildMs[%,d], checkMs[%,d], dbUpdateMs[%,d], totalMs[%,d].",
        allOvershadowedSegments.size(), relevantDatasources.size(), totalDatasources,
        servedSegmentsCounter[0], zeroReplicaSegments[0], verifiedOvershadowed, totalMarked[0],
        timelineBuildTime.millisElapsed(), checkTime.millisElapsed(), dbUpdateTime.millisElapsed(),
        totalTime.millisElapsed()
    );

    return params;
  }

  private long addSegmentsFromServer(
      ServerHolder serverHolder,
      Map<String, SegmentTimeline> timelines,
      Set<String> relevantDatasources
  )
  {
    final ImmutableDruidServer server = serverHolder.getServer();
    long added = 0;

    for (final ImmutableDruidDataSource dataSource : server.getDataSources()) {
      if (!relevantDatasources.contains(dataSource.getName())) {
        continue;
      }

      final int n = dataSource.getSegments().size();
      added += n;
      timelines
          .computeIfAbsent(dataSource.getName(), dsName -> new SegmentTimeline())
          .addSegments(dataSource.getSegments().iterator());
    }
    return added;
  }
}
