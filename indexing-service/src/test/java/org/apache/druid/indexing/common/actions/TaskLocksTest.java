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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.SegmentLock;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TimeChunkLock;
import org.apache.druid.indexing.common.config.TaskStorageConfig;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.overlord.HeapMemoryTaskStorage;
import org.apache.druid.indexing.overlord.LockResult;
import org.apache.druid.indexing.overlord.SpecificSegmentLockRequest;
import org.apache.druid.indexing.overlord.TaskLockbox;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.TimeChunkLockRequest;
import org.apache.druid.indexing.test.TestIndexerMetadataStorageCoordinator;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.metadata.ReplaceTaskLock;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TaskLocksTest
{
  private TaskLockbox lockbox;
  private Task task;

  @Before
  public void setup()
  {
    final TaskStorage taskStorage = new HeapMemoryTaskStorage(new TaskStorageConfig(null));
    lockbox = new TaskLockbox(
        taskStorage,
        new TestIndexerMetadataStorageCoordinator()
    );
    task = NoopTask.create();
    taskStorage.insert(task, TaskStatus.running(task.getId()));
    lockbox.add(task);
  }

  private Set<DataSegment> createTimeChunkedSegments()
  {
    return ImmutableSet.of(
        new DataSegment.Builder()
            .dataSource(task.getDataSource())
            .interval(Intervals.of("2017-01-01/2017-01-02"))
            .version(DateTimes.nowUtc().toString())
            .shardSpec(new LinearShardSpec(2))
            .size(0)
            .build(),
        new DataSegment.Builder()
            .dataSource(task.getDataSource())
            .interval(Intervals.of("2017-01-02/2017-01-03"))
            .version(DateTimes.nowUtc().toString())
            .shardSpec(new LinearShardSpec(2))
            .size(0)
            .build(),
        new DataSegment.Builder()
            .dataSource(task.getDataSource())
            .interval(Intervals.of("2017-01-03/2017-01-04"))
            .version(DateTimes.nowUtc().toString())
            .shardSpec(new LinearShardSpec(2))
            .size(0)
            .build()
    );
  }

  private Set<DataSegment> createNumberedPartitionedSegments()
  {
    final String version = DateTimes.nowUtc().toString();
    return ImmutableSet.of(
        new DataSegment.Builder()
            .dataSource(task.getDataSource())
            .interval(Intervals.of("2017-01-01/2017-01-02"))
            .version(version)
            .shardSpec(new NumberedShardSpec(0, 0))
            .size(0)
            .build(),
        new DataSegment.Builder()
            .dataSource(task.getDataSource())
            .interval(Intervals.of("2017-01-01/2017-01-02"))
            .version(version)
            .shardSpec(new NumberedShardSpec(1, 0))
            .size(0)
            .build(),
        new DataSegment.Builder()
            .dataSource(task.getDataSource())
            .interval(Intervals.of("2017-01-01/2017-01-02"))
            .version(version)
            .shardSpec(new NumberedShardSpec(2, 0))
            .size(0)
            .build(),
        new DataSegment.Builder()
            .dataSource(task.getDataSource())
            .interval(Intervals.of("2017-01-01/2017-01-02"))
            .version(version)
            .shardSpec(new NumberedShardSpec(3, 0))
            .size(0)
            .build(),
        new DataSegment.Builder()
            .dataSource(task.getDataSource())
            .interval(Intervals.of("2017-01-01/2017-01-02"))
            .version(version)
            .shardSpec(new NumberedShardSpec(4, 0))
            .size(0)
            .build()
    );
  }

  private TaskLock tryTimeChunkLock(Task task, Interval interval, TaskLockType lockType)
  {
    final TaskLock taskLock = lockbox
        .tryLock(task, new TimeChunkLockRequest(lockType, task, interval, null))
        .getTaskLock();
    Assert.assertNotNull(taskLock);
    return taskLock;
  }

  private LockResult trySegmentLock(Task task, Interval interval, String version, int partitonId)
  {
    return lockbox.tryLock(
        task,
        new SpecificSegmentLockRequest(TaskLockType.EXCLUSIVE, task, interval, version, partitonId)
    );
  }

  @Test
  public void testCheckLockCoversSegments()
  {
    final Set<DataSegment> segments = createTimeChunkedSegments();
    final List<Interval> intervals = ImmutableList.of(
        Intervals.of("2017-01-01/2017-01-02"),
        Intervals.of("2017-01-02/2017-01-03"),
        Intervals.of("2017-01-03/2017-01-04")
    );

    final Map<Interval, TaskLock> locks = intervals.stream().collect(
        Collectors.toMap(
            Function.identity(),
            interval -> tryTimeChunkLock(task, interval, TaskLockType.EXCLUSIVE)
        )
    );

    Assert.assertEquals(3, locks.size());
    Assert.assertTrue(TaskLocks.isLockCoversSegments(task, lockbox, segments));
  }

  @Test
  public void testCheckSegmentLockCoversSegments()
  {
    final Set<DataSegment> segments = createNumberedPartitionedSegments();
    final Interval interval = Intervals.of("2017-01-01/2017-01-02");

    final String version = DateTimes.nowUtc().toString();
    final List<TaskLock> locks = IntStream
        .range(0, 5)
        .mapToObj(
            partitionId -> {
              final TaskLock lock = trySegmentLock(task, interval, version, partitionId).getTaskLock();
              Assert.assertNotNull(lock);
              return lock;
            }
        ).collect(Collectors.toList());

    Assert.assertEquals(5, locks.size());
    Assert.assertTrue(TaskLocks.isLockCoversSegments(task, lockbox, segments));
  }

  @Test
  public void testCheckLargeLockCoversSegments()
  {
    final Set<DataSegment> segments = createTimeChunkedSegments();
    final List<Interval> intervals = ImmutableList.of(
        Intervals.of("2017-01-01/2017-01-04")
    );

    final Map<Interval, TaskLock> locks = intervals.stream().collect(
        Collectors.toMap(
            Function.identity(),
            interval -> tryTimeChunkLock(task, interval, TaskLockType.EXCLUSIVE)
        )
    );

    Assert.assertEquals(1, locks.size());
    Assert.assertTrue(TaskLocks.isLockCoversSegments(task, lockbox, segments));
  }

  @Test
  public void testCheckLockCoversSegmentsWithOverlappedIntervals()
  {
    final Set<DataSegment> segments = createTimeChunkedSegments();
    final List<Interval> lockIntervals = ImmutableList.of(
        Intervals.of("2016-12-31/2017-01-01"),
        Intervals.of("2017-01-01/2017-01-02"),
        Intervals.of("2017-01-02/2017-01-03")
    );

    final Map<Interval, TaskLock> locks = lockIntervals.stream().collect(
        Collectors.toMap(
            Function.identity(),
            interval -> tryTimeChunkLock(task, interval, TaskLockType.EXCLUSIVE)
        )
    );

    Assert.assertEquals(3, locks.size());
    Assert.assertFalse(TaskLocks.isLockCoversSegments(task, lockbox, segments));
  }

  @Test
  public void testFindLocksForSegments()
  {
    final Set<DataSegment> segments = createTimeChunkedSegments();
    final List<Interval> intervals = ImmutableList.of(
        Intervals.of("2017-01-01/2017-01-02"),
        Intervals.of("2017-01-02/2017-01-03"),
        Intervals.of("2017-01-03/2017-01-04")
    );

    final Map<Interval, TaskLock> locks = intervals.stream().collect(
        Collectors.toMap(
            Function.identity(),
            interval -> tryTimeChunkLock(task, interval, TaskLockType.EXCLUSIVE)
        )
    );

    Assert.assertEquals(3, locks.size());
    Assert.assertEquals(
        ImmutableList.of(
            newTimeChunkLock(intervals.get(0), locks.get(intervals.get(0)).getVersion()),
            newTimeChunkLock(intervals.get(1), locks.get(intervals.get(1)).getVersion()),
            newTimeChunkLock(intervals.get(2), locks.get(intervals.get(2)).getVersion())
        ),
        TaskLocks.findLocksForSegments(task, lockbox, segments)
    );
  }

  @Test
  public void testFindSegmentLocksForSegments()
  {
    final Set<DataSegment> segments = createNumberedPartitionedSegments();
    final Interval interval = Intervals.of("2017-01-01/2017-01-02");

    final String version = DateTimes.nowUtc().toString();
    final List<TaskLock> locks = IntStream
        .range(0, 5)
        .mapToObj(
            partitionId -> {
              final TaskLock lock = trySegmentLock(task, interval, version, partitionId).getTaskLock();
              Assert.assertNotNull(lock);
              return lock;
            }
        ).collect(Collectors.toList());

    Assert.assertEquals(5, locks.size());
    Assert.assertEquals(
        ImmutableList.of(
            newSegmentLock(interval, locks.get(0).getVersion(), 0),
            newSegmentLock(interval, locks.get(0).getVersion(), 1),
            newSegmentLock(interval, locks.get(0).getVersion(), 2),
            newSegmentLock(interval, locks.get(0).getVersion(), 3),
            newSegmentLock(interval, locks.get(0).getVersion(), 4)
        ),
        TaskLocks.findLocksForSegments(task, lockbox, segments)
    );
  }

  @Test
  public void testRevokedLocksDoNotCoverSegments()
  {
    final Set<DataSegment> segments = createNumberedPartitionedSegments();
    final Interval interval = Intervals.of("2017-01-01/2017-01-02");

    final TaskLock lock = tryTimeChunkLock(task, interval, TaskLockType.EXCLUSIVE);
    Assert.assertTrue(TaskLocks.isLockCoversSegments(task, lockbox, segments));

    lockbox.revokeLock(task.getId(), lock);
    Assert.assertFalse(TaskLocks.isLockCoversSegments(task, lockbox, segments));
  }

  @Test
  public void testFindReplaceLocksCoveringSegments()
  {
    final Set<DataSegment> segments = createTimeChunkedSegments();

    final Map<DataSegment, TaskLock> lockResults = segments.stream().collect(
        Collectors.toMap(
            segment -> segment,
            segment -> tryTimeChunkLock(task, segment.getInterval(), TaskLockType.REPLACE)
        )
    );

    final Map<DataSegment, ReplaceTaskLock> observedLocks
        = TaskLocks.findReplaceLocksCoveringSegments(task.getDataSource(), lockbox, segments);
    Assert.assertEquals(segments.size(), observedLocks.size());
    for (DataSegment segment : segments) {
      TaskLock lockFromResult = lockResults.get(segment);
      Assert.assertEquals(
          new ReplaceTaskLock(task.getId(), lockFromResult.getInterval(), lockFromResult.getVersion()),
          observedLocks.get(segment)
      );
    }
  }

  @Test
  public void testLockTypeForAppendUsingConcurrentLocks()
  {
    Assert.assertEquals(
        TaskLockType.APPEND,
        TaskLocks.determineLockTypeForAppend(
            ImmutableMap.of(Tasks.USE_CONCURRENT_LOCKS, true)
        )
    );
  }

  @Test
  public void testLockTypeForAppendWithLockTypeInContext()
  {
    Assert.assertEquals(
        TaskLockType.REPLACE,
        TaskLocks.determineLockTypeForAppend(
            ImmutableMap.of(Tasks.TASK_LOCK_TYPE, "REPLACE")
        )
    );
    Assert.assertEquals(
        TaskLockType.APPEND,
        TaskLocks.determineLockTypeForAppend(
            ImmutableMap.of(Tasks.TASK_LOCK_TYPE, "APPEND")
        )
    );
    Assert.assertEquals(
        TaskLockType.SHARED,
        TaskLocks.determineLockTypeForAppend(
            ImmutableMap.of(Tasks.TASK_LOCK_TYPE, "SHARED")
        )
    );
    Assert.assertEquals(
        TaskLockType.EXCLUSIVE,
        TaskLocks.determineLockTypeForAppend(
            ImmutableMap.of(Tasks.TASK_LOCK_TYPE, "EXCLUSIVE", Tasks.USE_SHARED_LOCK, true)
        )
    );
  }

  @Test
  public void testLockTypeForAppendWithNoLockTypeInContext()
  {
    Assert.assertEquals(
        TaskLockType.EXCLUSIVE,
        TaskLocks.determineLockTypeForAppend(
            ImmutableMap.of()
        )
    );
    Assert.assertEquals(
        TaskLockType.EXCLUSIVE,
        TaskLocks.determineLockTypeForAppend(
            ImmutableMap.of(Tasks.USE_SHARED_LOCK, false)
        )
    );
    Assert.assertEquals(
        TaskLockType.SHARED,
        TaskLocks.determineLockTypeForAppend(
            ImmutableMap.of(Tasks.USE_SHARED_LOCK, true)
        )
    );
  }

  private TimeChunkLock newTimeChunkLock(Interval interval, String version)
  {
    return new TimeChunkLock(
        TaskLockType.EXCLUSIVE,
        task.getGroupId(),
        task.getDataSource(),
        interval,
        version,
        task.getPriority()
    );
  }

  private SegmentLock newSegmentLock(Interval interval, String version, int partitionId)
  {
    return new SegmentLock(
        TaskLockType.EXCLUSIVE,
        task.getGroupId(),
        task.getDataSource(),
        interval,
        version,
        partitionId,
        task.getPriority()
    );
  }
}
