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

package org.apache.druid.server.coordinator;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.server.coordinator.duty.CoordinatorDuty;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

import java.util.ArrayList;
import java.util.List;

public class KillStalePendingSegments implements CoordinatorDuty
{
  private static final Logger log = new Logger(KillStalePendingSegments.class);
  private static final Period KEEP_PENDING_SEGMENTS_OFFSET = new Period("P1D");

  private final OverlordClient overlordClient;

  @Inject
  public KillStalePendingSegments(OverlordClient overlordClient)
  {
    this.overlordClient = overlordClient;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    final List<DateTime> createdTimes = new ArrayList<>();

    // Include one complete status so we can get the time of the last-created complete task. (The Overlord API returns
    // complete tasks in descending order of created_date.)
    final List<TaskStatusPlus> statuses =
        ImmutableList.copyOf(FutureUtils.getUnchecked(overlordClient.taskStatuses(null, null, 1), true));
    createdTimes.add(
        statuses
            .stream()
            .filter(status -> status.getStatusCode() == null || !status.getStatusCode().isComplete())
            .map(TaskStatusPlus::getCreatedTime)
            .min(Comparators.naturalNullsFirst())
            .orElse(DateTimes.nowUtc()) // If there are no active tasks, this returns the current time.
    );

    final TaskStatusPlus completeTaskStatus =
        statuses.stream()
                .filter(status -> status != null && status.getStatusCode().isComplete())
                .findFirst()
                .orElse(null);
    if (completeTaskStatus != null) {
      createdTimes.add(completeTaskStatus.getCreatedTime());
    }
    createdTimes.sort(Comparators.naturalNullsFirst());

    // There should be at least one createdTime because the current time is added to the 'createdTimes' list if there
    // is no running/pending/waiting tasks.
    Preconditions.checkState(!createdTimes.isEmpty(), "Failed to gather createdTimes of tasks");

    // If there is no running/pending/waiting/complete tasks, stalePendingSegmentsCutoffCreationTime is
    // (DateTimes.nowUtc() - KEEP_PENDING_SEGMENTS_OFFSET).
    final DateTime stalePendingSegmentsCutoffCreationTime = createdTimes.get(0).minus(KEEP_PENDING_SEGMENTS_OFFSET);
    for (String dataSource : params.getUsedSegmentsTimelinesPerDataSource().keySet()) {
      if (!params.getCoordinatorDynamicConfig().getDataSourcesToNotKillStalePendingSegmentsIn().contains(dataSource)) {
        final int pendingSegmentsKilled = FutureUtils.getUnchecked(
            overlordClient.killPendingSegments(
                dataSource,
                new Interval(DateTimes.MIN, stalePendingSegmentsCutoffCreationTime)
            ),
            true
        );
        log.info(
            "Killed [%d] pendingSegments created until [%s] for dataSource[%s]",
            pendingSegmentsKilled,
            stalePendingSegmentsCutoffCreationTime,
            dataSource
        );
      }
    }
    return params;
  }
}
