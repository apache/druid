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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.client.indexing.NoopOverlordClient;
import org.apache.druid.indexer.RunnerTaskState;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.server.coordinator.stats.RowKey;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class KillStalePendingSegmentsTest
{
  private TestOverlordClient overlordClient;
  private KillStalePendingSegments killDuty;

  @Before
  public void setup()
  {
    this.overlordClient = new TestOverlordClient();
    this.killDuty = new KillStalePendingSegments(overlordClient);
  }

  @Test
  public void testRetentionStarts1DayBeforeNowWhenNoKnownTask()
  {
    DruidCoordinatorRuntimeParams params = createParamsWithDatasources(DS.WIKI).build();
    killDuty.run(params);

    final Interval observedKillInterval = overlordClient.observedKillIntervals.get(DS.WIKI);
    Assert.assertEquals(DateTimes.MIN, observedKillInterval.getStart());

    // Verify that the cutoff time is no later than 1 day ago from now
    DateTime expectedCutoffTime = DateTimes.nowUtc().minusDays(1);
    Assert.assertTrue(
        expectedCutoffTime.getMillis() - observedKillInterval.getEnd().getMillis() <= 100
    );
  }

  @Test
  public void testRetentionStarts1DayBeforeEarliestActiveTask()
  {
    final DateTime startOfEarliestActiveTask = DateTimes.of("2023-01-01");
    overlordClient.addTaskAndSegment(DS.WIKI, startOfEarliestActiveTask, TaskState.RUNNING);
    overlordClient.addTaskAndSegment(DS.WIKI, startOfEarliestActiveTask.plusHours(2), TaskState.RUNNING);
    overlordClient.addTaskAndSegment(DS.WIKI, startOfEarliestActiveTask.plusDays(1), TaskState.RUNNING);
    overlordClient.addTaskAndSegment(DS.WIKI, startOfEarliestActiveTask.plusHours(3), TaskState.RUNNING);

    DruidCoordinatorRuntimeParams params = createParamsWithDatasources(DS.WIKI).build();
    killDuty.run(params);

    final Interval observedKillInterval = overlordClient.observedKillIntervals.get(DS.WIKI);
    Assert.assertEquals(DateTimes.MIN, observedKillInterval.getStart());
    Assert.assertEquals(startOfEarliestActiveTask.minusDays(1), observedKillInterval.getEnd());
  }

  @Test
  public void testRetentionStarts1DayBeforeLatestCompletedTask()
  {
    final DateTime startOfLatestCompletedTask = DateTimes.of("2023-01-01");
    overlordClient.addTaskAndSegment(DS.WIKI, startOfLatestCompletedTask, TaskState.FAILED);
    overlordClient.addTaskAndSegment(DS.WIKI, startOfLatestCompletedTask.minusHours(2), TaskState.SUCCESS);
    overlordClient.addTaskAndSegment(DS.WIKI, startOfLatestCompletedTask.minusDays(2), TaskState.FAILED);
    overlordClient.addTaskAndSegment(DS.WIKI, startOfLatestCompletedTask.minusDays(3), TaskState.SUCCESS);

    DruidCoordinatorRuntimeParams params = createParamsWithDatasources(DS.WIKI).build();
    killDuty.run(params);

    final Interval observedKillInterval = overlordClient.observedKillIntervals.get(DS.WIKI);
    Assert.assertEquals(DateTimes.MIN, observedKillInterval.getStart());
    Assert.assertEquals(startOfLatestCompletedTask.minusDays(1), observedKillInterval.getEnd());

    final CoordinatorRunStats stats = params.getCoordinatorStats();
    Assert.assertEquals(2, stats.get(Stats.Kill.PENDING_SEGMENTS, RowKey.of(Dimension.DATASOURCE, DS.WIKI)));
  }

  @Test
  public void testRetentionStarts1DayBeforeLatestCompletedOrEarliestActiveTask()
  {
    final DateTime startOfLatestCompletedTask = DateTimes.of("2023-02-01");
    overlordClient.addTaskAndSegment(DS.WIKI, startOfLatestCompletedTask, TaskState.FAILED);

    final DateTime startOfEarliestActiveTask = DateTimes.of("2023-01-01");
    overlordClient.addTaskAndSegment(DS.KOALA, startOfEarliestActiveTask, TaskState.RUNNING);

    DruidCoordinatorRuntimeParams params = createParamsWithDatasources(DS.WIKI, DS.KOALA).build();
    killDuty.run(params);

    DateTime earliestEligibleTask = DateTimes.earlierOf(startOfEarliestActiveTask, startOfLatestCompletedTask);
    final Interval wikiKillInterval = overlordClient.observedKillIntervals.get(DS.WIKI);
    Assert.assertEquals(DateTimes.MIN, wikiKillInterval.getStart());
    Assert.assertEquals(earliestEligibleTask.minusDays(1), wikiKillInterval.getEnd());

    final Interval koalaKillInterval = overlordClient.observedKillIntervals.get(DS.KOALA);
    Assert.assertEquals(DateTimes.MIN, koalaKillInterval.getStart());
    Assert.assertEquals(earliestEligibleTask.minusDays(1), wikiKillInterval.getEnd());
  }

  @Test
  public void testPendingSegmentOfDisallowedDatasourceIsNotDeleted()
  {
    DruidCoordinatorRuntimeParams params =
        createParamsWithDatasources(DS.WIKI, DS.KOALA).withDynamicConfigs(
            CoordinatorDynamicConfig
                .builder()
                .withDatasourcesToNotKillPendingSegmentsIn(
                    Collections.singleton(DS.KOALA)
                )
                .build()
        ).build();

    DateTime startOfLatestCompletedTask = DateTimes.of("2023-01-01");
    overlordClient.addTaskAndSegment(DS.WIKI, startOfLatestCompletedTask, TaskState.SUCCESS);
    overlordClient.addTaskAndSegment(DS.WIKI, startOfLatestCompletedTask.minusDays(3), TaskState.SUCCESS);
    overlordClient.addTaskAndSegment(DS.WIKI, startOfLatestCompletedTask.minusDays(5), TaskState.SUCCESS);
    overlordClient.addTaskAndSegment(DS.KOALA, startOfLatestCompletedTask, TaskState.SUCCESS);
    overlordClient.addTaskAndSegment(DS.KOALA, startOfLatestCompletedTask.minusDays(3), TaskState.SUCCESS);
    overlordClient.addTaskAndSegment(DS.KOALA, startOfLatestCompletedTask.minusDays(5), TaskState.SUCCESS);

    killDuty.run(params);

    // Verify that stale pending segments are killed in "wiki" but not in "koala"
    final CoordinatorRunStats stats = params.getCoordinatorStats();
    Assert.assertTrue(overlordClient.observedKillIntervals.containsKey(DS.WIKI));
    Assert.assertEquals(2, stats.get(Stats.Kill.PENDING_SEGMENTS, RowKey.of(Dimension.DATASOURCE, DS.WIKI)));

    Assert.assertFalse(overlordClient.observedKillIntervals.containsKey(DS.KOALA));
    Assert.assertEquals(0, stats.get(Stats.Kill.PENDING_SEGMENTS, RowKey.of(Dimension.DATASOURCE, DS.KOALA)));
  }

  private DruidCoordinatorRuntimeParams.Builder createParamsWithDatasources(String... datasources)
  {
    DruidCoordinatorRuntimeParams.Builder builder = DruidCoordinatorRuntimeParams.newBuilder(DateTimes.nowUtc());

    // Create a dummy for each of the datasources so that they get added to the timeline
    Set<DataSegment> usedSegments = new HashSet<>();
    for (String datasource : datasources) {
      usedSegments.add(
          DataSegment.builder().dataSource(datasource).interval(Intervals.ETERNITY)
                     .version("v1").shardSpec(new NumberedShardSpec(0, 1)).size(100).build()
      );
    }

    return builder.withUsedSegments(usedSegments);
  }

  private static class DS
  {
    static final String WIKI = "wiki";
    static final String KOALA = "koala";
  }

  /**
   * Simulates an Overlord with a configurable list of tasks and pending segments.
   */
  private static class TestOverlordClient extends NoopOverlordClient
  {
    private final List<TaskStatusPlus> taskStatuses = new ArrayList<>();
    private final Map<String, List<DateTime>> datasourceToPendingSegments = new HashMap<>();

    private final Map<String, Interval> observedKillIntervals = new HashMap<>();

    private int taskIdSuffix = 0;

    void addTaskAndSegment(String datasource, DateTime createdTime, TaskState state)
    {
      taskStatuses.add(
          new TaskStatusPlus(
              datasource + "__" + taskIdSuffix++,
              null, null, createdTime, createdTime, state,
              state.isComplete() ? RunnerTaskState.NONE : RunnerTaskState.RUNNING,
              100L, TaskLocation.unknown(), datasource, null
          )
      );

      // Add a pending segment with created time 5 minutes after the task was created
      datasourceToPendingSegments.computeIfAbsent(datasource, ds -> new ArrayList<>())
                                 .add(createdTime.plusMinutes(5));
    }

    @Override
    public ListenableFuture<CloseableIterator<TaskStatusPlus>> taskStatuses(
        @Nullable String state,
        @Nullable String dataSource,
        @Nullable Integer maxCompletedTasks
    )
    {
      return Futures.immediateFuture(
          CloseableIterators.wrap(taskStatuses.iterator(), null)
      );
    }

    @Override
    public ListenableFuture<Integer> killPendingSegments(String dataSource, Interval interval)
    {
      observedKillIntervals.put(dataSource, interval);

      List<DateTime> pendingSegments = datasourceToPendingSegments.remove(dataSource);
      if (pendingSegments == null || pendingSegments.isEmpty()) {
        return Futures.immediateFuture(0);
      }

      List<DateTime> remainingPendingSegments = new ArrayList<>();
      int numDeletedPendingSegments = 0;
      for (DateTime createdTime : pendingSegments) {
        if (createdTime.isBefore(interval.getEnd())) {
          ++numDeletedPendingSegments;
        } else {
          remainingPendingSegments.add(createdTime);
        }
      }

      if (remainingPendingSegments.size() > 0) {
        datasourceToPendingSegments.put(dataSource, remainingPendingSegments);
      }

      return Futures.immediateFuture(numDeletedPendingSegments);
    }
  }
}
