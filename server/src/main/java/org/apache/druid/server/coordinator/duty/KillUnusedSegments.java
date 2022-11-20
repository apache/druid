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
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.server.coordinator.DruidCoordinatorConfig;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.utils.CollectionUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.Collection;
import java.util.List;

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
  private static final Logger log = new Logger(KillUnusedSegments.class);

  private final long period;
  private final long retainDuration;
  private final boolean ignoreRetainDuration;
  private final int maxSegmentsToKill;
  private long lastKillTime = 0;

  private final SegmentsMetadataManager segmentsMetadataManager;
  private final IndexingServiceClient indexingServiceClient;

  @Inject
  public KillUnusedSegments(
      SegmentsMetadataManager segmentsMetadataManager,
      IndexingServiceClient indexingServiceClient,
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
    this.indexingServiceClient = indexingServiceClient;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    Collection<String> dataSourcesToKill =
        params.getCoordinatorDynamicConfig().getSpecificDataSourcesToKillUnusedSegmentsIn();

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
      killUnusedSegments(dataSourcesToKill);
    }

    return params;
  }

  private void killUnusedSegments(Collection<String> dataSourcesToKill)
  {
    int submittedTasks = 0;
    for (String dataSource : dataSourcesToKill) {
      final Interval intervalToKill = findIntervalForKill(dataSource);
      if (intervalToKill == null) {
        continue;
      }

      try {
        indexingServiceClient.killUnusedSegments("coordinator-issued", dataSource, intervalToKill);
        ++submittedTasks;
      }
      catch (Exception ex) {
        log.error(ex, "Failed to submit kill task for dataSource [%s]", dataSource);
        if (Thread.currentThread().isInterrupted()) {
          log.warn("skipping kill task scheduling because thread is interrupted.");
          break;
        }
      }
    }

    log.debug("Submitted kill tasks for [%d] datasources.", submittedTasks);
  }

  /**
   * Calculates the interval for which segments are to be killed in a datasource.
   */
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

}
