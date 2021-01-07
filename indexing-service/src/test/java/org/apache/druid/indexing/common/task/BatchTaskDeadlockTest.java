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

package org.apache.druid.indexing.common.task;

import com.google.common.collect.ImmutableList;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.List;

public class BatchTaskDeadlockTest extends IngestionTestBase
{
  private static final Granularity SEGMENT_GRANULARITY = Granularities.DAY;

  @Test
  public void testRunTasksWorkingOnOverlappingIntervalsRunOneByOne() throws Exception
  {
    final TestTask task1 = new TestTask("t1", Intervals.of("2021-01/P1M"));
    final TestTask task2 = new TestTask("t2", Intervals.of("2021-01-31/P1M"));
    prepareTaskForLocking(task1);
    prepareTaskForLocking(task2);
    final TaskActionClient actionClient1 = createActionClient(task1);
    final TaskActionClient actionClient2 = createActionClient(task2);
    Assert.assertTrue(task1.isReady(actionClient1));
    Assert.assertFalse(task2.isReady(actionClient2));
    Assert.assertTrue(getLockbox().findLocksForTask(task2).isEmpty());

    shutdownTask(task1);

    final TestTask task3 = new TestTask("t3", Intervals.of("2021-01-15/P1M"));
    prepareTaskForLocking(task3);
    final TaskActionClient actionClient3 = createActionClient(task3);

    Assert.assertTrue(task3.isReady(actionClient3));
    shutdownTask(task3);

    Assert.assertTrue(task2.isReady(actionClient2));
  }

  private static class TestTask extends AbstractBatchIndexTask
  {
    private final Interval interval;

    private TestTask(String id, Interval interval)
    {
      super(id, "datasource", null);
      this.interval = interval;
    }

    @Override
    public boolean isReady(TaskActionClient taskActionClient) throws Exception
    {
      return tryTimeChunkLock(taskActionClient, ImmutableList.of(interval));
    }

    @Override
    public TaskStatus runTask(TaskToolbox toolbox)
    {
      return TaskStatus.success(getId());
    }

    @Override
    public boolean requireLockExistingSegments()
    {
      return false;
    }

    @Override
    public List<DataSegment> findSegmentsToLock(TaskActionClient taskActionClient, List<Interval> intervals)
    {
      return null;
    }

    @Override
    public boolean isPerfectRollup()
    {
      return false;
    }

    @Nullable
    @Override
    public Granularity getSegmentGranularity()
    {
      return SEGMENT_GRANULARITY;
    }

    @Override
    public String getType()
    {
      return "test";
    }
  }
}