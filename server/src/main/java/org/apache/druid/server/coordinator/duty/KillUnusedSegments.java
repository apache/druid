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
import com.google.common.collect.ImmutableList;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.server.coordinator.DruidCoordinatorConfig;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.utils.CollectionUtils;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <p>
 * Completely removes information about unused segments who have an interval end that comes before
 * now - {@link #durationToRetain} from the metadata store. {@link #durationToRetain} can be a positive or negative duration,
 * negative meaning the interval end target will be in the future. Also, {@link #durationToRetain} can be ignored if
 * {@link #ignoreDurationToRetain} is enabled, meaning that there is no upper bound to the end interval of segments that
 * will be killed. The umbrella interval of the unused segments per datasource to be killed is determined by
 * {@link #findIntervalForKill(String, DateTime)}, which takes into account the configured {@link #bufferPeriod}. However,
 * the kill task needs to check again for max {@link #bufferPeriod} for the unused segments in the widened interval
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
  public static final Predicate<TaskStatusPlus> IS_AUTO_KILL_TASK =
      status -> null != status
                && (KILL_TASK_TYPE.equals(status.getType()) && status.getId().startsWith(TASK_ID_PREFIX));
  private static final Logger log = new Logger(KillUnusedSegments.class);

  private final Duration period;
  private final Duration durationToRetain;
  private final boolean ignoreDurationToRetain;
  private final int maxSegmentsToKill;

  /**
   * Used to keep track of the last interval end time that was killed for each
   * datasource.
   */
  private final Map<String, DateTime> datasourceToLastKillIntervalEnd;
  private DateTime lastKillTime;
  private final long bufferPeriod;

  private final SegmentsMetadataManager segmentsMetadataManager;
  private final OverlordClient overlordClient;

  public KillUnusedSegments(
      SegmentsMetadataManager segmentsMetadataManager,
      OverlordClient overlordClient,
      DruidCoordinatorConfig config
  )
  {
    if (config.getCoordinatorKillPeriod().getMillis() < config.getCoordinatorIndexingPeriod().getMillis()) {
      throw InvalidInput.exception(
          "druid.coordinator.kill.period[%s] must be >= druid.coordinator.period.indexingPeriod[%s]",
          config.getCoordinatorKillPeriod(),
          config.getCoordinatorIndexingPeriod()
      );
    }
    if (config.getCoordinatorKillMaxSegments() < 0) {
      throw InvalidInput.exception(
          "druid.coordinator.kill.maxSegments[%s] is invalid. It must be a positive integer.",
          config.getCoordinatorKillMaxSegments()
      );
    }
    this.period = config.getCoordinatorKillPeriod();
    this.ignoreDurationToRetain = config.getCoordinatorKillIgnoreDurationToRetain();
    this.durationToRetain = config.getCoordinatorKillDurationToRetain();
    if (this.ignoreDurationToRetain) {
      log.debug(
          "druid.coordinator.kill.durationToRetain[%s] will be ignored when discovering segments to kill "
          + "because druid.coordinator.kill.ignoreDurationToRetain is set to true.",
          this.durationToRetain
      );
    }
    this.bufferPeriod = config.getCoordinatorKillBufferPeriod().getMillis();
    this.maxSegmentsToKill = config.getCoordinatorKillMaxSegments();
    datasourceToLastKillIntervalEnd = new ConcurrentHashMap<>();

    log.info(
        "Kill task scheduling enabled with period[%s], durationToRetain[%s], bufferPeriod[%s], maxSegmentsToKill[%s]",
        this.period,
        this.ignoreDurationToRetain ? "IGNORING" : this.durationToRetain,
        this.bufferPeriod,
        this.maxSegmentsToKill
    );

    this.segmentsMetadataManager = segmentsMetadataManager;
    this.overlordClient = overlordClient;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    if (!canDutyRun()) {
      log.debug(
          "Skipping KillUnusedSegments until period[%s] has elapsed after lastKillTime[%s].",
          period,
          lastKillTime
      );
      return params;
    }

    return runInternal(params);
  }

  @VisibleForTesting
  DruidCoordinatorRuntimeParams runInternal(DruidCoordinatorRuntimeParams params)
  {
    TaskStats taskStats = new TaskStats();
    Collection<String> dataSourcesToKill =
        params.getCoordinatorDynamicConfig().getSpecificDataSourcesToKillUnusedSegmentsIn();
    double killTaskSlotRatio = params.getCoordinatorDynamicConfig().getKillTaskSlotRatio();
    int maxKillTaskSlots = params.getCoordinatorDynamicConfig().getMaxKillTaskSlots();
    int killTaskCapacity = getKillTaskCapacity(
        CoordinatorDutyUtils.getTotalWorkerCapacity(overlordClient),
        killTaskSlotRatio,
        maxKillTaskSlots
    );
    int availableKillTaskSlots = getAvailableKillTaskSlots(
        killTaskCapacity,
        CoordinatorDutyUtils.getNumActiveTaskSlots(overlordClient, IS_AUTO_KILL_TASK).size()
    );
    final CoordinatorRunStats stats = params.getCoordinatorStats();

    taskStats.availableTaskSlots = availableKillTaskSlots;
    taskStats.maxSlots = killTaskCapacity;

    if (0 < availableKillTaskSlots) {
      // If no datasource has been specified, all are eligible for killing unused segments
      if (CollectionUtils.isNullOrEmpty(dataSourcesToKill)) {
        dataSourcesToKill = segmentsMetadataManager.retrieveAllDataSourceNames();
      }

      log.debug("Killing unused segments for datasources[%s]", dataSourcesToKill);
      lastKillTime = DateTimes.nowUtc();
      taskStats.submittedTasks = killUnusedSegments(dataSourcesToKill, availableKillTaskSlots);
    }

    // any datasources that are no longer being considered for kill should have their
    // last kill interval removed from map.
    datasourceToLastKillIntervalEnd.keySet().retainAll(dataSourcesToKill);
    addStats(taskStats, stats);
    return params;
  }

  private void addStats(
      TaskStats taskStats,
      CoordinatorRunStats stats
  )
  {
    stats.add(Stats.Kill.AVAILABLE_SLOTS, taskStats.availableTaskSlots);
    stats.add(Stats.Kill.SUBMITTED_TASKS, taskStats.submittedTasks);
    stats.add(Stats.Kill.MAX_SLOTS, taskStats.maxSlots);
  }

  private int killUnusedSegments(
      Collection<String> dataSourcesToKill,
      int availableKillTaskSlots
  )
  {
    int submittedTasks = 0;
    if (0 < availableKillTaskSlots && !CollectionUtils.isNullOrEmpty(dataSourcesToKill)) {
      for (String dataSource : dataSourcesToKill) {
        if (submittedTasks >= availableKillTaskSlots) {
          log.debug(StringUtils.format(
              "Submitted [%d] kill tasks and reached kill task slot limit [%d]. Will resume "
              + "on the next coordinator cycle.", submittedTasks, availableKillTaskSlots));
          break;
        }
        final DateTime maxUsedStatusLastUpdatedTime = DateTimes.nowUtc().minus(bufferPeriod);
        final Interval intervalToKill = findIntervalForKill(dataSource, maxUsedStatusLastUpdatedTime);
        if (intervalToKill == null) {
          datasourceToLastKillIntervalEnd.remove(dataSource);
          continue;
        }

        try {
          FutureUtils.getUnchecked(
              overlordClient.runKillTask(
                  TASK_ID_PREFIX,
                  dataSource,
                  intervalToKill,
                  maxSegmentsToKill,
                  maxUsedStatusLastUpdatedTime
              ),
              true
          );
          ++submittedTasks;
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
    }

    if (log.isDebugEnabled()) {
      log.debug(
          "Submitted [%d] kill tasks for [%d] datasources.%s",
          submittedTasks,
          dataSourcesToKill.size(),
          availableKillTaskSlots < dataSourcesToKill.size()
              ? StringUtils.format(
              " Datasources skipped: %s",
              ImmutableList.copyOf(dataSourcesToKill).subList(submittedTasks, dataSourcesToKill.size())
          )
              : ""
      );
    }

    // report stats
    return submittedTasks;
  }

  /**
   * Calculates the interval for which segments are to be killed in a datasource.
   */
  @Nullable
  private Interval findIntervalForKill(String dataSource, DateTime maxUsedStatusLastUpdatedTime)
  {
    final DateTime maxEndTime = ignoreDurationToRetain
                                ? DateTimes.COMPARE_DATE_AS_STRING_MAX
                                : DateTimes.nowUtc().minus(durationToRetain);
    List<Interval> unusedSegmentIntervals = segmentsMetadataManager
        .getUnusedSegmentIntervals(dataSource, datasourceToLastKillIntervalEnd.get(dataSource), maxEndTime, maxSegmentsToKill, maxUsedStatusLastUpdatedTime);

    if (CollectionUtils.isNullOrEmpty(unusedSegmentIntervals)) {
      return null;
    } else if (unusedSegmentIntervals.size() == 1) {
      return unusedSegmentIntervals.get(0);
    } else {
      return JodaUtils.umbrellaInterval(unusedSegmentIntervals);
    }
  }

  private int getAvailableKillTaskSlots(int killTaskCapacity, int numActiveKillTasks)
  {
    return Math.max(
        0,
        killTaskCapacity - numActiveKillTasks
    );
  }

  private boolean canDutyRun()
  {
    return lastKillTime == null || !DateTimes.nowUtc().isBefore(lastKillTime.plus(period));
  }

  @VisibleForTesting
  static int getKillTaskCapacity(int totalWorkerCapacity, double killTaskSlotRatio, int maxKillTaskSlots)
  {
    return Math.min((int) (totalWorkerCapacity * Math.min(killTaskSlotRatio, 1.0)), maxKillTaskSlots);
  }

  @VisibleForTesting
  Map<String, DateTime> getDatasourceToLastKillIntervalEnd()
  {
    return datasourceToLastKillIntervalEnd;
  }


  static class TaskStats
  {
    int availableTaskSlots;
    int maxSlots;
    int submittedTasks;

    TaskStats()
    {
      availableTaskSlots = 0;
      maxSlots = 0;
      submittedTasks = 0;
    }
  }
}
