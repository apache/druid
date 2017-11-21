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

package io.druid.indexing.common.actions;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskLockType;
import io.druid.indexing.common.config.TaskStorageConfig;
import io.druid.indexing.common.task.NoopTask;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.HeapMemoryTaskStorage;
import io.druid.indexing.overlord.TaskLockbox;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.Intervals;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.LinearShardSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TaskActionPreconditionsTest
{
  private TaskLockbox lockbox;
  private Task task;
  private Set<DataSegment> segments;

  @Before
  public void setup()
  {
    lockbox = new TaskLockbox(new HeapMemoryTaskStorage(new TaskStorageConfig(null)));
    task = NoopTask.create();
    lockbox.add(task);

    segments = ImmutableSet.of(
        new DataSegment.Builder()
            .dataSource(task.getDataSource())
            .interval(Intervals.of("2017-01-01/2017-01-02"))
            .version(DateTimes.nowUtc().toString())
            .shardSpec(new LinearShardSpec(2))
            .build(),
        new DataSegment.Builder()
            .dataSource(task.getDataSource())
            .interval(Intervals.of("2017-01-02/2017-01-03"))
            .version(DateTimes.nowUtc().toString())
            .shardSpec(new LinearShardSpec(2))
            .build(),
        new DataSegment.Builder()
            .dataSource(task.getDataSource())
            .interval(Intervals.of("2017-01-03/2017-01-04"))
            .version(DateTimes.nowUtc().toString())
            .shardSpec(new LinearShardSpec(2))
            .build()
    );
  }

  @Test
  public void testCheckLockCoversSegments() throws Exception
  {
    final List<Interval> intervals = ImmutableList.of(
        Intervals.of("2017-01-01/2017-01-02"),
        Intervals.of("2017-01-02/2017-01-03"),
        Intervals.of("2017-01-03/2017-01-04")
    );

    final Map<Interval, TaskLock> locks = intervals.stream().collect(
        Collectors.toMap(
            Function.identity(),
            interval -> {
              final TaskLock lock = lockbox.tryLock(TaskLockType.EXCLUSIVE, task, interval).getTaskLock();
              Assert.assertNotNull(lock);
              return lock;
            }
        )
    );

    Assert.assertEquals(3, locks.size());
    Assert.assertTrue(TaskActionPreconditions.isLockCoversSegments(task, lockbox, segments));
  }

  @Test
  public void testCheckLargeLockCoversSegments() throws Exception
  {
    final List<Interval> intervals = ImmutableList.of(
        Intervals.of("2017-01-01/2017-01-04")
    );

    final Map<Interval, TaskLock> locks = intervals.stream().collect(
        Collectors.toMap(
            Function.identity(),
            interval -> {
              final TaskLock lock = lockbox.tryLock(TaskLockType.EXCLUSIVE, task, interval).getTaskLock();
              Assert.assertNotNull(lock);
              return lock;
            }
        )
    );

    Assert.assertEquals(1, locks.size());
    Assert.assertTrue(TaskActionPreconditions.isLockCoversSegments(task, lockbox, segments));
  }

  @Test
  public void testCheckLockCoversSegmentsWithOverlappedIntervals() throws Exception
  {
    final List<Interval> lockIntervals = ImmutableList.of(
        Intervals.of("2016-12-31/2017-01-01"),
        Intervals.of("2017-01-01/2017-01-02"),
        Intervals.of("2017-01-02/2017-01-03")
    );

    final Map<Interval, TaskLock> locks = lockIntervals.stream().collect(
        Collectors.toMap(
            Function.identity(),
            interval -> {
              final TaskLock lock = lockbox.tryLock(TaskLockType.EXCLUSIVE, task, interval).getTaskLock();
              Assert.assertNotNull(lock);
              return lock;
            }
        )
    );

    Assert.assertEquals(3, locks.size());
    Assert.assertFalse(TaskActionPreconditions.isLockCoversSegments(task, lockbox, segments));
  }
}
