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

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.apache.druid.client.indexing.IndexingTotalWorkerCapacityInfo;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.rpc.HttpResponseException;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.server.coordinator.DruidCoordinatorConfig;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.utils.CollectionUtils;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Completely removes information about unused segments who have an interval end that comes before
 * now - {@link #retainDuration} from the metadata store. retainDuration can be a positive or negative duration,
 * negative meaning the interval end target will be in the future. Also, retainDuration can be ignored,
 * meaning that there is no upper bound to the end interval of segments that will be killed. This action is called
 * "to kill a segment".
 * <p>
 * See org.apache.druid.indexing.common.task.KillUnusedSegmentsTask.
 */
public class KillUnusedSegments implements CoordinatorDuty
{
  public static final String KILL_TASK_TYPE = "kill";
  private static final Logger log = new Logger(KillUnusedSegments.class);

  private final long period;
  private final long retainDuration;
  private final boolean ignoreRetainDuration;
  private final int maxSegmentsToKill;
  private long lastKillTime = 0;

  private final SegmentsMetadataManager segmentsMetadataManager;
  private final OverlordClient overlordClient;

  @Inject
  public KillUnusedSegments(
      SegmentsMetadataManager segmentsMetadataManager,
      OverlordClient overlordClient,
      DruidCoordinatorConfig config
  )
  {
    this.period = config.getCoordinatorKillPeriod().getMillis();
    Preconditions.checkArgument(
        this.period > config.getCoordinatorIndexingPeriod().getMillis(),
        "coordinator kill period must be greater than druid.coordinator.period.indexingPeriod"
    );

    this.ignoreRetainDuration = config.getCoordinatorKillIgnoreDurationToRetain();
    this.retainDuration = config.getCoordinatorKillDurationToRetain().getMillis();
    if (this.ignoreRetainDuration) {
      log.debug(
          "druid.coordinator.kill.durationToRetain [%s] will be ignored when discovering segments to kill "
          + "because you have set druid.coordinator.kill.ignoreDurationToRetain to True.",
          this.retainDuration
      );
    }

    this.maxSegmentsToKill = config.getCoordinatorKillMaxSegments();
    Preconditions.checkArgument(this.maxSegmentsToKill > 0, "coordinator kill maxSegments must be > 0");

    log.info(
        "Kill Task scheduling enabled with period [%s], retainDuration [%s], maxSegmentsToKill [%s]",
        this.period,
        this.ignoreRetainDuration ? "IGNORING" : this.retainDuration,
        this.maxSegmentsToKill
    );

    this.segmentsMetadataManager = segmentsMetadataManager;
    this.overlordClient = overlordClient;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    Collection<String> dataSourcesToKill =
        params.getCoordinatorDynamicConfig().getSpecificDataSourcesToKillUnusedSegmentsIn();
    final Double killTaskSlotRatio = params.getCoordinatorDynamicConfig().getKillTaskSlotRatio();

    // If no datasource has been specified, all are eligible for killing unused segments
    if (CollectionUtils.isNullOrEmpty(dataSourcesToKill)) {
      dataSourcesToKill = segmentsMetadataManager.retrieveAllDataSourceNames();
    }

    final long currentTimeMillis = System.currentTimeMillis();
    if (CollectionUtils.isNullOrEmpty(dataSourcesToKill)) {
      log.debug("No eligible datasource to kill unused segments.");
    } else if (lastKillTime + period > currentTimeMillis) {
      log.debug("Skipping kill of unused segments as kill period has not elapsed yet.");
    } else {
      log.debug("Killing unused segments in datasources: %s", dataSourcesToKill);
      lastKillTime = currentTimeMillis;
      killUnusedSegments(dataSourcesToKill, killTaskSlotRatio);
    }

    return params;
  }

  private void killUnusedSegments(Collection<String> dataSourcesToKill, @Nullable Double killTaskSlotRatio)
  {
    int submittedTasks = 0;
    int availableKillTaskSlots = getAvailableKillTaskSlots(killTaskSlotRatio);
    if (0 == availableKillTaskSlots) {
      log.warn("Not killing any unused segments because there are no available kill task slots at this time.");
      return;
    }
    for (String dataSource : dataSourcesToKill) {
      final Interval intervalToKill = findIntervalForKill(dataSource);
      if (intervalToKill == null) {
        continue;
      }

      try {
        FutureUtils.getUnchecked(overlordClient.runKillTask(
            "coordinator-issued",
            dataSource,
            intervalToKill,
            maxSegmentsToKill
        ), true);
        ++submittedTasks;
      }
      catch (Exception ex) {
        log.error(ex, "Failed to submit kill task for dataSource [%s]", dataSource);
        if (Thread.currentThread().isInterrupted()) {
          log.warn("skipping kill task scheduling because thread is interrupted.");
          break;
        }
      }

      if (submittedTasks >= availableKillTaskSlots) {
        log.info("Reached kill task slot limit with pending unused segments to kill. Will resume "
                 + "on the next coordinator cycle.");
        break;
      }
    }

    log.debug("Submitted kill tasks for [%d] datasources.", submittedTasks);
  }

  /**
   * Calculates the interval for which segments are to be killed in a datasource.
   */
  @Nullable
  private Interval findIntervalForKill(String dataSource)
  {
    final DateTime maxEndTime = ignoreRetainDuration
                                ? DateTimes.COMPARE_DATE_AS_STRING_MAX
                                : DateTimes.nowUtc().minus(retainDuration);

    List<Interval> unusedSegmentIntervals = segmentsMetadataManager
        .getUnusedSegmentIntervals(dataSource, maxEndTime, maxSegmentsToKill);

    if (CollectionUtils.isNullOrEmpty(unusedSegmentIntervals)) {
      return null;
    } else if (unusedSegmentIntervals.size() == 1) {
      return unusedSegmentIntervals.get(0);
    } else {
      return JodaUtils.umbrellaInterval(unusedSegmentIntervals);
    }
  }

  private int getAvailableKillTaskSlots(@Nullable Double killTaskSlotRatio)
  {
    return Math.max(0, getKillTaskCapacity(killTaskSlotRatio) - getNumActiveKillTaskSlots());
  }

  private int getNumActiveKillTaskSlots()
  {
    final CloseableIterator<TaskStatusPlus> activeTasks =
        FutureUtils.getUnchecked(overlordClient.taskStatuses(null, null, 0), true);
    // Fetch currently running kill tasks
    int numActiveKillTasks = 0;

    try (final Closer closer = Closer.create()) {
      closer.register(activeTasks);
      while (activeTasks.hasNext()) {
        final TaskStatusPlus status = activeTasks.next();

        // taskType can be null if middleManagers are running with an older version. Here, we consevatively regard
        // the tasks of the unknown taskType as the killTask. This is because it's important to not run
        // killTasks more than the configured limit at any time which might impact to the ingestion
        // performance.
        if (status.getType() == null || KILL_TASK_TYPE.equals(status.getType())) {
          numActiveKillTasks++;
        }
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    return numActiveKillTasks;
  }

  private int getKillTaskCapacity(@Nullable Double killTaskSlotRatio)
  {
    int totalWorkerCapacity;
    try {
      final IndexingTotalWorkerCapacityInfo workerCapacityInfo =
          FutureUtils.get(overlordClient.getTotalWorkerCapacity(), true);
      totalWorkerCapacity = workerCapacityInfo.getMaximumCapacityWithAutoScale();
      if (totalWorkerCapacity < 0) {
        totalWorkerCapacity = workerCapacityInfo.getCurrentClusterCapacity();
      }
    }
    catch (ExecutionException e) {
      // Call to getTotalWorkerCapacity may fail during a rolling upgrade: API was added in 0.23.0.
      if (e.getCause() instanceof HttpResponseException
          && ((HttpResponseException) e.getCause()).getResponse().getStatus().equals(HttpResponseStatus.NOT_FOUND)) {
        log.noStackTrace().warn(e, "Call to getTotalWorkerCapacity failed. Falling back to getWorkers.");
        totalWorkerCapacity =
            FutureUtils.getUnchecked(overlordClient.getWorkers(), true)
                .stream()
                .mapToInt(worker -> worker.getWorker().getCapacity())
                .sum();
      } else {
        throw new RuntimeException(e.getCause());
      }
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }


    return Math.max(
        killTaskSlotRatio == null
            ? totalWorkerCapacity
            : (int) (totalWorkerCapacity * killTaskSlotRatio),
        1
    );
  }
}
