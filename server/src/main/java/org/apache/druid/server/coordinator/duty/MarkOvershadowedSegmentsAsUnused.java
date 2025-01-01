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
    // Mark overshadowed segments as unused only if the coordinator has been running
    // long enough to have refreshed its metadata view
    final Duration requiredDelay = Duration.millis(
        params.getCoordinatorDynamicConfig().getMarkSegmentAsUnusedDelayMillis()
    );
    if (sinceCoordinatorStarted.hasNotElapsed(requiredDelay)) {
      return params;
    }

    final Set<DataSegment> allOvershadowedSegments = params.getDataSourcesSnapshot().getOvershadowedSegments();
    if (allOvershadowedSegments.isEmpty()) {
      return params;
    }

    final DruidCluster cluster = params.getDruidCluster();
    final Map<String, SegmentTimeline> timelines = new HashMap<>();

    cluster.getHistoricals().values().forEach(
        historicals -> historicals.forEach(
            historical -> addSegmentsFromServer(historical, timelines)
        )
    );
    cluster.getBrokers().forEach(
        broker -> addSegmentsFromServer(broker, timelines)
    );

    // Include all segments that require zero replicas to be loaded
    params.getSegmentAssigner().getSegmentsWithZeroRequiredReplicas().forEach(
        (datasource, segments) -> timelines
            .computeIfAbsent(datasource, ds -> new SegmentTimeline())
            .addSegments(segments.iterator())
    );

    // Do not include segments served by ingestion services such as tasks or indexers,
    // to prevent unpublished segments from prematurely overshadowing segments.

    // Mark all segments overshadowed by served segments as unused
    final Map<String, Set<SegmentId>> datasourceToUnusedSegments = new HashMap<>();
    for (DataSegment dataSegment : allOvershadowedSegments) {
      SegmentTimeline timeline = timelines.get(dataSegment.getDataSource());
      if (timeline != null && timeline.isOvershadowed(dataSegment)) {
        datasourceToUnusedSegments.computeIfAbsent(dataSegment.getDataSource(), ds -> new HashSet<>())
                                  .add(dataSegment.getId());
      }
    }

    final CoordinatorRunStats stats = params.getCoordinatorStats();
    datasourceToUnusedSegments.forEach(
        (datasource, unusedSegments) -> {
          RowKey datasourceKey = RowKey.of(Dimension.DATASOURCE, datasource);
          stats.add(Stats.Segments.OVERSHADOWED, datasourceKey, unusedSegments.size());

          final Stopwatch updateTime = Stopwatch.createStarted();
          int updatedCount = deleteHandler.markSegmentsAsUnused(datasource, unusedSegments);
          log.info(
              "Marked [%d] segments of datasource[%s] as unused in [%,d]ms.",
              updatedCount, datasource, updateTime.millisElapsed()
          );
        }
    );

    return params;
  }

  private void addSegmentsFromServer(
      ServerHolder serverHolder,
      Map<String, SegmentTimeline> timelines
  )
  {
    ImmutableDruidServer server = serverHolder.getServer();

    for (ImmutableDruidDataSource dataSource : server.getDataSources()) {
      timelines
          .computeIfAbsent(dataSource.getName(), dsName -> new SegmentTimeline())
          .addSegments(dataSource.getSegments().iterator());
    }
  }
}
