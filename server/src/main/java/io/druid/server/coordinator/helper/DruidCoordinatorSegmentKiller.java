/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.coordinator.helper;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;

import io.druid.client.indexing.IndexingServiceClient;
import io.druid.common.utils.JodaUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.metadata.MetadataSegmentManager;
import io.druid.server.coordinator.DruidCoordinatorConfig;
import io.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.joda.time.Interval;

import java.util.Collection;
import java.util.List;

/**
 */
public class DruidCoordinatorSegmentKiller implements DruidCoordinatorHelper
{
  private final static Logger log = new Logger(DruidCoordinatorSegmentKiller.class);

  private final long period;
  private final long retainDuration;
  private final int maxSegmentsToKill;
  private long lastKillTime = 0;


  private final MetadataSegmentManager segmentManager;
  private final IndexingServiceClient indexingServiceClient;

  @Inject
  public DruidCoordinatorSegmentKiller(
      MetadataSegmentManager segmentManager,
      IndexingServiceClient indexingServiceClient,
      DruidCoordinatorConfig config
  )
  {
    this.period = config.getCoordinatorKillPeriod().getMillis();
    Preconditions.checkArgument(
        this.period > config.getCoordinatorIndexingPeriod().getMillis(),
        "coordinator kill period must be greater than druid.coordinator.period.indexingPeriod"
    );

    this.retainDuration = config.getCoordinatorKillDurationToRetain().getMillis();
    Preconditions.checkArgument(this.retainDuration >= 0, "coordinator kill retainDuration must be >= 0");

    this.maxSegmentsToKill = config.getCoordinatorKillMaxSegments();
    Preconditions.checkArgument(this.maxSegmentsToKill > 0, "coordinator kill maxSegments must be > 0");

    log.info(
        "Kill Task scheduling enabled with period [%s], retainDuration [%s], maxSegmentsToKill [%s]",
        this.period,
        this.retainDuration,
        this.maxSegmentsToKill
    );

    this.segmentManager = segmentManager;
    this.indexingServiceClient = indexingServiceClient;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    boolean killAllDataSources = params.getCoordinatorDynamicConfig().isKillAllDataSources();
    Collection<String> whitelist = params.getCoordinatorDynamicConfig().getKillDataSourceWhitelist();

    if (killAllDataSources && whitelist != null && !whitelist.isEmpty()) {
      log.error("killAllDataSources can't be true when killDataSourceWhitelist is non-empty, No kill tasks are scheduled.");
      return params;
    }

    if (killAllDataSources) {
      whitelist = segmentManager.getAllDatasourceNames();
    }

    if (whitelist != null && whitelist.size() > 0 && (lastKillTime + period) < System.currentTimeMillis()) {
      lastKillTime = System.currentTimeMillis();

      for (String dataSource : whitelist) {
        final Interval intervalToKill = findIntervalForKillTask(dataSource, maxSegmentsToKill);
        if (intervalToKill != null) {
          try {
            indexingServiceClient.killSegments(dataSource, intervalToKill);
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

  @VisibleForTesting
  Interval findIntervalForKillTask(String dataSource, int limit)
  {
    List<Interval> unusedSegmentIntervals = segmentManager.getUnusedSegmentIntervals(
        dataSource,
        new Interval(
            0,
            System.currentTimeMillis()
            - retainDuration
        ),
        limit
    );

    if (unusedSegmentIntervals != null && unusedSegmentIntervals.size() > 0) {
      return JodaUtils.umbrellaInterval(unusedSegmentIntervals);
    } else {
      return null;
    }
  }
}
