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
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.server.coordinator.DruidCoordinatorConfig;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;

/**
 * Completely removes information about unused segments who have an interval end that comes before
 * now - {@link #retainDuration}. retainDuration can be a positive or negative duration, negative meaning the interval
 * end target will be in the future. Also, retainDuration can be ignored, meaning that there is no upper bound to the
 * end interval of segments that will be killed. This action is called "to kill a segment".
 *
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

    // Operators must explicitly set this configuration if they are going to enable Segment Killing and do not set ignoreRetainDuration to true
    if (!ignoreRetainDuration) {
      Preconditions.checkArgument(
          config.getCoordinatorKillDurationToRetain() != null,
          "druid.coordinator.kill.durationToRetain must be non-null if ignoreRetainDuration is false"
      );
      this.retainDuration = config.getCoordinatorKillDurationToRetain().getMillis();
    } else {
      // Provide dummy value that will not be used since ignoreRetainDuration overrides it.
      this.retainDuration = new Duration("PT0S").getMillis();
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
    boolean killAllDataSources = params.getCoordinatorDynamicConfig().isKillUnusedSegmentsInAllDataSources();
    Collection<String> specificDataSourcesToKill =
        params.getCoordinatorDynamicConfig().getSpecificDataSourcesToKillUnusedSegmentsIn();

    if (killAllDataSources && specificDataSourcesToKill != null && !specificDataSourcesToKill.isEmpty()) {
      log.error(
          "killAllDataSources can't be true when specificDataSourcesToKill is non-empty. No kill tasks are scheduled."
      );
      return params;
    }

    Collection<String> dataSourcesToKill = specificDataSourcesToKill;
    if (killAllDataSources) {
      dataSourcesToKill = segmentsMetadataManager.retrieveAllDataSourceNames();
    }

    if (dataSourcesToKill != null &&
        dataSourcesToKill.size() > 0 &&
        (lastKillTime + period) < System.currentTimeMillis()) {
      lastKillTime = System.currentTimeMillis();

      for (String dataSource : dataSourcesToKill) {
        final Interval intervalToKill = findIntervalForKill(dataSource, maxSegmentsToKill);
        if (intervalToKill != null) {
          try {
            indexingServiceClient.killUnusedSegments("coordinator-issued", dataSource, intervalToKill);
          }
          catch (Exception ex) {
            log.error(ex, "Failed to submit kill task for dataSource [%s]", dataSource);
            if (Thread.currentThread().isInterrupted()) {
              log.warn("skipping kill task scheduling because thread is interrupted.");
              break;
            }
          }
        }
      }
    }
    return params;
  }

  /**
   * For a given datasource and limit of segments that can be killed in one task, determine the interval to be
   * submitted with the kill task.
   *
   * @param dataSource dataSource whose unused segments are being killed.
   * @param limit the maximum number of segments that can be included in the kill task.
   * @return {@link Interval} to be used in the kill task.
   */
  @VisibleForTesting
  @Nullable
  Interval findIntervalForKill(String dataSource, int limit)
  {
    List<Interval> unusedSegmentIntervals =
        segmentsMetadataManager.getUnusedSegmentIntervals(dataSource, getEndTimeUpperLimit(), limit);

    if (unusedSegmentIntervals != null && unusedSegmentIntervals.size() > 0) {
      return JodaUtils.umbrellaInterval(unusedSegmentIntervals);
    } else {
      return null;
    }
  }

  /**
   * Calculate the {@link DateTime} that wil form the upper bound when looking for segments that are
   * eligible to be killed. If ignoreDurationToRetain is true, we have no upper bound and return a DateTime object
   * for 9999-12-31T23:59. This static date has to be used because the metastore is not comparing date objects, but rather
   * varchar columns. This means DateTimes.MAX is less than the 21st century and beyond for according to the metastore, due to its
   * year starting with a '1'
   *
   * @return {@link DateTime} representing the upper bound time used when looking for segments to kill.
   */
  @VisibleForTesting
  DateTime getEndTimeUpperLimit()
  {
    return ignoreRetainDuration
           ? DateTimes.of(9999, 12, 31, 23, 59)
           : DateTimes.nowUtc().minus(retainDuration);
  }

  @VisibleForTesting
  Long getRetainDuration()
  {
    return retainDuration;
  }
}
