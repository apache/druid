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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.config.KillUnusedSegmentsConfig;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.server.coordinator.stats.RowKey;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.utils.CollectionUtils;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <p>
 * Completely removes information about unused segments who have an interval end that comes before
 * now - {@link #durationToRetain} from the metadata store. {@link #durationToRetain} can be a positive or negative duration,
 * negative meaning the interval end target will be in the future. Also, {@link #durationToRetain} can be ignored if
 * {@link #ignoreDurationToRetain} is enabled, meaning that there is no upper bound to the end interval of segments that
 * will be killed. The umbrella interval of the unused segments per datasource to be killed is determined by
 * {@link #findIntervalForKill(String, DateTime, CoordinatorRunStats)}, which takes into account the configured {@link #bufferPeriod}.
 * However, the kill task needs to check again for max {@link #bufferPeriod} for the unused segments in the widened interval
 * as there can be multiple unused segments with different {@code used_status_last_updated} time.
 * </p>
 * <p>
 * See {@link org.apache.druid.indexing.common.task.KillUnusedSegmentsTask}.
 * </p>
 */
public class KillUnusedSegments implements CoordinatorDuty
{
  public static final String KILL_TASK_TYPE = "kill";
  public static final String TASK_ID_PREFIX = "coordinator-issued";

  private static final Predicate<TaskStatusPlus> IS_AUTO_KILL_TASK =
      status -> null != status
                && (KILL_TASK_TYPE.equals(status.getType()) && status.getId().startsWith(TASK_ID_PREFIX));
  private static final Logger log = new Logger(KillUnusedSegments.class);

  private final Duration period;
  private final Duration durationToRetain;
  private final boolean ignoreDurationToRetain;
  private final int maxSegmentsToKill;
  private final Duration bufferPeriod;

  /**
   * Used to keep track of the last interval end time that was killed for each
   * datasource.
   */
  private final Map<String, DateTime> datasourceToLastKillIntervalEnd;

  /**
   * State that is maintained in the duty to determine if the duty needs to run or not.
   */
  private DateTime lastKillTime;

  /**
   * Round-robin iterator of the datasources to kill. It's updated in every run by the duty.
   */
  private final RoundRobinIterator datasourceIterator;

  private final SegmentsMetadataManager segmentsMetadataManager;
  private final OverlordClient overlordClient;

  public KillUnusedSegments(
      SegmentsMetadataManager segmentsMetadataManager,
      OverlordClient overlordClient,
      KillUnusedSegmentsConfig killConfig
  )
  {
    this(segmentsMetadataManager, overlordClient, killConfig, new RoundRobinIterator());
  }

  @VisibleForTesting
  KillUnusedSegments(
      SegmentsMetadataManager segmentsMetadataManager,
      OverlordClient overlordClient,
      KillUnusedSegmentsConfig killConfig,
      RoundRobinIterator robinUniqueIterator
  )
  {
    this.period = killConfig.getCleanupPeriod();
    this.maxSegmentsToKill = killConfig.getMaxSegments();
    this.ignoreDurationToRetain = killConfig.isIgnoreDurationToRetain();
    this.durationToRetain = killConfig.getDurationToRetain();
    if (this.ignoreDurationToRetain) {
      log.info(
          "druid.coordinator.kill.durationToRetain[%s] will be ignored when discovering segments to kill "
          + "because druid.coordinator.kill.ignoreDurationToRetain is set to true.",
          durationToRetain
      );
    }
    this.bufferPeriod = killConfig.getBufferPeriod();

    log.info(
        "Kill task scheduling enabled with period[%s], durationToRetain[%s], bufferPeriod[%s], maxSegmentsToKill[%s]",
        this.period,
        this.ignoreDurationToRetain ? "IGNORING" : this.durationToRetain,
        this.bufferPeriod,
        this.maxSegmentsToKill
    );

    this.segmentsMetadataManager = segmentsMetadataManager;
    this.overlordClient = overlordClient;
    this.datasourceToLastKillIntervalEnd = new ConcurrentHashMap<>();
    this.datasourceIterator = robinUniqueIterator;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(final DruidCoordinatorRuntimeParams params)
  {
    if (canDutyRun()) {
      return runInternal(params);
    } else {
      log.info(
          "Skipping KillUnusedSegments until period[%s] has elapsed after lastKillTime[%s].",
          period, lastKillTime
      );
      return params;
    }
  }

  private DruidCoordinatorRuntimeParams runInternal(final DruidCoordinatorRuntimeParams params)
  {
    final CoordinatorDynamicConfig dynamicConfig = params.getCoordinatorDynamicConfig();
    final CoordinatorRunStats stats = params.getCoordinatorStats();

    final int availableKillTaskSlots = getAvailableKillTaskSlots(dynamicConfig, stats);
    if (availableKillTaskSlots <= 0) {
      log.info("Skipping KillUnusedSegments because there are no available kill task slots.");
      return params;
    }

    final Set<String> dataSourcesToKill;
    if (!CollectionUtils.isNullOrEmpty(dynamicConfig.getSpecificDataSourcesToKillUnusedSegmentsIn())) {
      dataSourcesToKill = dynamicConfig.getSpecificDataSourcesToKillUnusedSegmentsIn();
    } else {
      // If no datasource has been specified, all are eligible for killing unused segments by default
      dataSourcesToKill = segmentsMetadataManager.retrieveAllDataSourceNames();
    }

    datasourceIterator.updateCandidates(dataSourcesToKill);
    lastKillTime = DateTimes.nowUtc();

    killUnusedSegments(dataSourcesToKill, availableKillTaskSlots, stats);

    // any datasources that are no longer being considered for kill should have their
    // last kill interval removed from map.
    datasourceToLastKillIntervalEnd.keySet().retainAll(dataSourcesToKill);
    return params;
  }

  /**
   * Spawn kill tasks for each datasource in {@code dataSourcesToKill} upto {@code availableKillTaskSlots}.
   */
  private void killUnusedSegments(
      final Set<String> dataSourcesToKill,
      final int availableKillTaskSlots,
      final CoordinatorRunStats stats
  )
  {
    if (CollectionUtils.isNullOrEmpty(dataSourcesToKill)) {
      log.info("Skipping KillUnusedSegments because there are no datasources to kill.");
      stats.add(Stats.Kill.SUBMITTED_TASKS, 0);
      return;
    }
    final Iterator<String> dataSourcesToKillIterator = this.datasourceIterator.getIterator();
    final Set<String> remainingDatasourcesToKill = new HashSet<>(dataSourcesToKill);
    final Set<String> datasourcesKilled = new HashSet<>();

    int submittedTasks = 0;
    while (dataSourcesToKillIterator.hasNext()) {
      if (remainingDatasourcesToKill.size() == 0) {
        log.info(
            "Submitted [%d] kill tasks for [%d] datasources. No more datasource to kill in this cycle.",
            submittedTasks, datasourcesKilled.size()
        );
        break;
      }

      if (submittedTasks >= availableKillTaskSlots) {
        log.info(
            "Submitted [%d] kill tasks for [%d] datasources and reached kill task slot limit [%d].",
            submittedTasks, datasourcesKilled.size(), availableKillTaskSlots
        );
        break;
      }

      final String dataSource = dataSourcesToKillIterator.next();
      final DateTime maxUsedStatusLastUpdatedTime = DateTimes.nowUtc().minus(bufferPeriod);
      final Interval intervalToKill = findIntervalForKill(dataSource, maxUsedStatusLastUpdatedTime, stats);
      if (intervalToKill == null) {
        datasourceToLastKillIntervalEnd.remove(dataSource);
        remainingDatasourcesToKill.remove(dataSource);
        continue;
      }

      try {
        FutureUtils.getUnchecked(
            overlordClient.runKillTask(
                TASK_ID_PREFIX,
                dataSource,
                intervalToKill,
                null,
                maxSegmentsToKill,
                maxUsedStatusLastUpdatedTime
            ),
            true
        );
        ++submittedTasks;
        datasourcesKilled.add(dataSource);
        remainingDatasourcesToKill.remove(dataSource);
        datasourceToLastKillIntervalEnd.put(dataSource, intervalToKill.getEnd());
      }
      catch (Exception ex) {
        log.error(ex, "Failed to submit kill task for dataSource[%s] in interval[%s]", dataSource, intervalToKill);
        if (Thread.currentThread().isInterrupted()) {
          log.warn("Skipping kill task scheduling because thread is interrupted.");
          break;
        }
      }
    }

    log.info(
        "Submitted [%d] kill tasks for [%d] datasources: [%s]. Remaining datasources to kill: [%s]",
        submittedTasks, datasourcesKilled.size(), datasourcesKilled, remainingDatasourcesToKill
    );

    stats.add(Stats.Kill.SUBMITTED_TASKS, submittedTasks);
  }

  @Nullable
  private Interval findIntervalForKill(
      final String dataSource,
      final DateTime maxUsedStatusLastUpdatedTime,
      final CoordinatorRunStats stats
  )
  {
    final DateTime minStartTime = datasourceToLastKillIntervalEnd.get(dataSource);
    final DateTime maxEndTime = ignoreDurationToRetain
                                ? DateTimes.COMPARE_DATE_AS_STRING_MAX
                                : DateTimes.nowUtc().minus(durationToRetain);

    final List<Interval> unusedSegmentIntervals = segmentsMetadataManager.getUnusedSegmentIntervals(
        dataSource,
        minStartTime,
        maxEndTime,
        maxSegmentsToKill,
        maxUsedStatusLastUpdatedTime
    );

    // Each unused segment interval returned above has a 1:1 correspondence with an unused segment. So we can assume
    // these are eligible segments for deletion by the kill task. After the umbrella interval is computed
    // below, we cannot say the same as there can be multiple unused segments with different usedStatusLastUpdatedTime.
    final RowKey datasourceKey = RowKey.of(Dimension.DATASOURCE, dataSource);
    stats.add(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, datasourceKey, unusedSegmentIntervals.size());

    if (CollectionUtils.isNullOrEmpty(unusedSegmentIntervals)) {
      log.info(
          "No segments found to kill for datasource[%s] in [%s/%s].",
          dataSource, minStartTime, maxEndTime
      );
      return null;
    } else if (unusedSegmentIntervals.size() == 1) {
      return unusedSegmentIntervals.get(0);
    } else {
      return JodaUtils.umbrellaInterval(unusedSegmentIntervals);
    }
  }

  private boolean canDutyRun()
  {
    return lastKillTime == null || !DateTimes.nowUtc().isBefore(lastKillTime.plus(period));
  }

  private int getAvailableKillTaskSlots(final CoordinatorDynamicConfig config, final CoordinatorRunStats stats)
  {
    final int killTaskCapacity = Math.min(
        (int) (CoordinatorDutyUtils.getTotalWorkerCapacity(overlordClient) * Math.min(config.getKillTaskSlotRatio(), 1.0)),
        config.getMaxKillTaskSlots()
    );

    final int availableKillTaskSlots = Math.max(
        0,
        killTaskCapacity - CoordinatorDutyUtils.getNumActiveTaskSlots(overlordClient, IS_AUTO_KILL_TASK).size()
    );

    stats.add(Stats.Kill.AVAILABLE_SLOTS, availableKillTaskSlots);
    stats.add(Stats.Kill.MAX_SLOTS, killTaskCapacity);
    return availableKillTaskSlots;
  }
}
