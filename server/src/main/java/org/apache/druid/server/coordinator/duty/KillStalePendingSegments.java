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

import com.google.common.collect.ImmutableList;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Stopwatch;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.server.coordinator.stats.RowKey;
import org.apache.druid.server.coordinator.stats.Stats;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Duty to kill stale pending segments which are not needed anymore. Pending segments
 * are created when appending realtime or batch tasks allocate segments to build
 * incremental indexes. Under normal operation, these pending segments get committed
 * when the task completes and become regular segments. But in case of task failures,
 * some pending segments might be left around and cause clutter in the metadata store.
 * <p>
 * While cleaning up, this duty ensures that the following pending segments are
 * retained for at least {@link #DURATION_TO_RETAIN}:
 * <ul>
 * <li>Pending segments created by any active task (across all datasources)</li>
 * <li>Pending segments created by the latest completed task (across all datasources)</li>
 * </ul>
 */
public class KillStalePendingSegments implements CoordinatorDuty
{
  private static final Logger log = new Logger(KillStalePendingSegments.class);
  private static final Period DURATION_TO_RETAIN = new Period("P1D");

  private final OverlordClient overlordClient;

  public KillStalePendingSegments(OverlordClient overlordClient)
  {
    this.overlordClient = overlordClient;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    final Stopwatch totalTime = Stopwatch.createStarted();

    final Set<String> killDatasources = new HashSet<>(
        params.getUsedSegmentsTimelinesPerDataSource().keySet()
    );
    final int candidates = killDatasources.size();
    killDatasources.removeAll(
        params.getCoordinatorDynamicConfig()
              .getDataSourcesToNotKillStalePendingSegmentsIn()
    );

    final Stopwatch minCreatedTime0 = Stopwatch.createStarted();
    final DateTime minCreatedTime = getMinCreatedTimeToRetain();
    minCreatedTime0.stop();

    final Stopwatch killTime = Stopwatch.createStarted();
    long totalKilled = 0;
    int datasourcesWithKills = 0;
    long maxCallMs = 0;
    String slowestDs = null;
    for (String dataSource : killDatasources) {
      final Stopwatch perCall = Stopwatch.createStarted();
      int pendingSegmentsKilled = FutureUtils.getUnchecked(
          overlordClient.killPendingSegments(
              dataSource,
              new Interval(DateTimes.MIN, minCreatedTime)
          ),
          true
      );
      final long elapsed = perCall.millisElapsed();
      if (elapsed > maxCallMs) {
        maxCallMs = elapsed;
        slowestDs = dataSource;
      }
      if (pendingSegmentsKilled > 0) {
        datasourcesWithKills++;
        totalKilled += pendingSegmentsKilled;
        log.info(
            "Killed [%d] pendingSegments created before [%s] for datasource[%s] in [%,d]ms.",
            pendingSegmentsKilled, minCreatedTime, dataSource, elapsed
        );
        params.getCoordinatorStats().add(
            Stats.Kill.PENDING_SEGMENTS,
            RowKey.of(Dimension.DATASOURCE, dataSource),
            pendingSegmentsKilled
        );
      }
    }
    killTime.stop();

    log.info(
        "KillStalePendingSegments summary: candidates[%d], kept[%d] (skipped via config), datasourcesWithKills[%d],"
        + " totalKilled[%,d], slowestDs[%s] at [%,d]ms;"
        + " minCreatedTimeMs[%,d], killLoopMs[%,d], totalMs[%,d].",
        candidates, candidates - killDatasources.size(), datasourcesWithKills,
        totalKilled, slowestDs, maxCallMs,
        minCreatedTime0.millisElapsed(), killTime.millisElapsed(), totalTime.millisElapsed()
    );

    return params;
  }

  /**
   * Computes the minimum created time of retainable pending segments. Any pending
   * segment created before this time is considered stale and can be safely deleted.
   * The limit is determined to ensure that pending segments created by any active
   * task and the latest completed task (across all datasources) are retained for
   * at least {@link #DURATION_TO_RETAIN}.
   */
  private DateTime getMinCreatedTimeToRetain()
  {
    // Fetch the statuses of all active tasks and the latest completed task
    // (The Overlord API returns complete tasks in descending order of created_date.)
    final List<TaskStatusPlus> statuses = ImmutableList.copyOf(
        FutureUtils.getUnchecked(overlordClient.taskStatuses(null, null, 1), true)
    );

    DateTime earliestActiveTaskStart = DateTimes.nowUtc();
    DateTime latestCompletedTaskStart = null;
    for (TaskStatusPlus status : statuses) {
      if (status.getStatusCode() != null && status.getStatusCode().isComplete()) {
        latestCompletedTaskStart = DateTimes.laterOf(
            latestCompletedTaskStart,
            status.getCreatedTime()
        );
      } else {
        earliestActiveTaskStart = DateTimes.earlierOf(
            earliestActiveTaskStart,
            status.getCreatedTime()
        );
      }
    }

    return DateTimes.earlierOf(latestCompletedTaskStart, earliestActiveTaskStart)
                    .minus(DURATION_TO_RETAIN);
  }
}
