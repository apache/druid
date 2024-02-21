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

package org.apache.druid.indexing.common.actions;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class RetrieveSegmentsActionsTest
{
  private static final Interval INTERVAL = Intervals.of("2017-10-01/2017-10-15");

  @ClassRule
  public static TaskActionTestKit actionTestKit = new TaskActionTestKit();

  private static Task task;
  private static Set<DataSegment> expectedUnusedSegments;
  private static Set<DataSegment> expectedUsedSegments;

  @BeforeClass
  public static void setup() throws IOException
  {
    task = NoopTask.create();

    actionTestKit.getTaskLockbox().add(task);

    expectedUnusedSegments = new HashSet<>();
    expectedUnusedSegments.add(createSegment(Intervals.of("2017-10-05/2017-10-06"), "1"));
    expectedUnusedSegments.add(createSegment(Intervals.of("2017-10-06/2017-10-07"), "1"));
    expectedUnusedSegments.add(createSegment(Intervals.of("2017-10-07/2017-10-08"), "1"));

    actionTestKit.getMetadataStorageCoordinator()
                 .commitSegments(expectedUnusedSegments);

    expectedUnusedSegments.forEach(s -> actionTestKit.getTaskLockbox().unlock(task, s.getInterval()));

    expectedUsedSegments = new HashSet<>();
    expectedUsedSegments.add(createSegment(Intervals.of("2017-10-05/2017-10-06"), "2"));
    expectedUsedSegments.add(createSegment(Intervals.of("2017-10-06/2017-10-07"), "2"));
    expectedUsedSegments.add(createSegment(Intervals.of("2017-10-07/2017-10-08"), "2"));

    actionTestKit.getMetadataStorageCoordinator()
                 .commitSegments(expectedUsedSegments);

    expectedUsedSegments.forEach(s -> actionTestKit.getTaskLockbox().unlock(task, s.getInterval()));

    expectedUnusedSegments.forEach(s -> actionTestKit.getSegmentsMetadataManager().markSegmentAsUnused(s.getId()));
  }

  private static DataSegment createSegment(Interval interval, String version)
  {
    return new DataSegment(
        task.getDataSource(),
        interval,
        version,
        null,
        ImmutableList.of("dim1", "dim2"),
        ImmutableList.of("met1", "met2"),
        NoneShardSpec.instance(),
        Integer.valueOf(version),
        1
    );
  }

  @Test
  public void testRetrieveUsedSegmentsAction()
  {
    final RetrieveUsedSegmentsAction action =
        new RetrieveUsedSegmentsAction(task.getDataSource(), ImmutableList.of(INTERVAL));
    final Set<DataSegment> resultSegments = new HashSet<>(action.perform(task, actionTestKit.getTaskActionToolbox()));
    Assert.assertEquals(expectedUsedSegments, resultSegments);
  }

  @Test
  public void testRetrieveUnusedSegmentsAction()
  {
    final RetrieveUnusedSegmentsAction action = new RetrieveUnusedSegmentsAction(task.getDataSource(), INTERVAL, null, null);
    final Set<DataSegment> resultSegments = new HashSet<>(action.perform(task, actionTestKit.getTaskActionToolbox()));
    Assert.assertEquals(expectedUnusedSegments, resultSegments);
  }

  @Test
  public void testRetrieveUnusedSegmentsActionWithMinUsedLastUpdatedTime()
  {
    final RetrieveUnusedSegmentsAction action = new RetrieveUnusedSegmentsAction(task.getDataSource(), INTERVAL, null, DateTimes.MIN);
    final Set<DataSegment> resultSegments = new HashSet<>(action.perform(task, actionTestKit.getTaskActionToolbox()));
    Assert.assertEquals(ImmutableSet.of(), resultSegments);
  }

  @Test
  public void testRetrieveUnusedSegmentsActionWithNowUsedLastUpdatedTime()
  {
    final RetrieveUnusedSegmentsAction action = new RetrieveUnusedSegmentsAction(task.getDataSource(), INTERVAL, null, DateTimes.nowUtc());
    final Set<DataSegment> resultSegments = new HashSet<>(action.perform(task, actionTestKit.getTaskActionToolbox()));
    Assert.assertEquals(expectedUnusedSegments, resultSegments);
  }
}
