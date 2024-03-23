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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.SegmentLock;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.HashBasedNumberedPartialShardSpec;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.apache.druid.timeline.partition.LinearPartialShardSpec;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.apache.druid.timeline.partition.NumberedPartialShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.PartialShardSpec;
import org.apache.druid.timeline.partition.ShardSpec;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@RunWith(Parameterized.class)
public class SegmentAllocateActionTest
{
  @Rule
  public TaskActionTestKit taskActionTestKit = new TaskActionTestKit();

  private static final String DATA_SOURCE = "none";
  private static final DateTime PARTY_TIME = DateTimes.of("1999");
  private static final DateTime THE_DISTANT_FUTURE = DateTimes.of("3000");

  private final boolean useBatch;
  private final LockGranularity lockGranularity;

  private SegmentAllocationQueue allocationQueue;

  @Parameterized.Parameters(name = "granularity = {0}, useBatch = {1}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{LockGranularity.SEGMENT, true},
        new Object[]{LockGranularity.SEGMENT, false},
        new Object[]{LockGranularity.TIME_CHUNK, true},
        new Object[]{LockGranularity.TIME_CHUNK, false}
    );
  }

  public SegmentAllocateActionTest(LockGranularity lockGranularity, boolean useBatch)
  {
    this.lockGranularity = lockGranularity;
    this.useBatch = useBatch;
  }

  @Before
  public void setUp()
  {
    ServiceEmitter emitter = EasyMock.createMock(ServiceEmitter.class);
    EmittingLogger.registerEmitter(emitter);
    EasyMock.replay(emitter);
    allocationQueue = taskActionTestKit.getTaskActionToolbox().getSegmentAllocationQueue();
    if (allocationQueue != null) {
      allocationQueue.start();
      allocationQueue.becomeLeader();
    }
  }

  @After
  public void tearDown()
  {
    if (allocationQueue != null) {
      allocationQueue.stop();
    }
  }

  @Test
  public void testManySegmentsSameInterval()
  {
    final Task task = NoopTask.create();

    taskActionTestKit.getTaskLockbox().add(task);

    final SegmentIdWithShardSpec id1 = allocate(
        task,
        PARTY_TIME,
        Granularities.NONE,
        Granularities.HOUR,
        "s1",
        null
    );
    final SegmentIdWithShardSpec id2 = allocate(
        task,
        PARTY_TIME,
        Granularities.NONE,
        Granularities.HOUR,
        "s1",
        id1.toString()
    );
    final SegmentIdWithShardSpec id3 = allocate(
        task,
        PARTY_TIME,
        Granularities.NONE,
        Granularities.HOUR,
        "s1",
        id2.toString()
    );

    if (lockGranularity == LockGranularity.TIME_CHUNK) {
      final TaskLock partyLock = Iterables.getOnlyElement(
          FluentIterable.from(taskActionTestKit.getTaskLockbox().findLocksForTask(task))
                        .filter(input -> input.getInterval().contains(PARTY_TIME))
      );

      assertSameIdentifier(
          id1,
          new SegmentIdWithShardSpec(
              DATA_SOURCE,
              Granularities.HOUR.bucket(PARTY_TIME),
              partyLock.getVersion(),
              new NumberedShardSpec(0, 0)
          )
      );
      assertSameIdentifier(
          id2,
          new SegmentIdWithShardSpec(
              DATA_SOURCE,
              Granularities.HOUR.bucket(PARTY_TIME),
              partyLock.getVersion(),
              new NumberedShardSpec(1, 0)
          )
      );
      assertSameIdentifier(
          id3,
          new SegmentIdWithShardSpec(
              DATA_SOURCE,
              Granularities.HOUR.bucket(PARTY_TIME),
              partyLock.getVersion(),
              new NumberedShardSpec(2, 0)
          )
      );
    } else {
      final List<TaskLock> partyTimeLocks = taskActionTestKit.getTaskLockbox()
                                                             .findLocksForTask(task)
                                                             .stream()
                                                             .filter(input -> input.getInterval().contains(PARTY_TIME))
                                                             .collect(Collectors.toList());

      Assert.assertEquals(3, partyTimeLocks.size());

      assertSameIdentifier(
          new SegmentIdWithShardSpec(
              DATA_SOURCE,
              Granularities.HOUR.bucket(PARTY_TIME),
              id1.getVersion(),
              new NumberedShardSpec(0, 0)
          ),
          id1
      );
      assertSameIdentifier(
          new SegmentIdWithShardSpec(
              DATA_SOURCE,
              Granularities.HOUR.bucket(PARTY_TIME),
              id1.getVersion(),
              new NumberedShardSpec(1, 0)
          ),
          id2
      );
      assertSameIdentifier(
          new SegmentIdWithShardSpec(
              DATA_SOURCE,
              Granularities.HOUR.bucket(PARTY_TIME),
              id1.getVersion(),
              new NumberedShardSpec(2, 0)
          ),
          id3
      );
    }
  }

  @Test
  public void testResumeSequence()
  {
    final Task task = NoopTask.create();

    taskActionTestKit.getTaskLockbox().add(task);

    final Map<Integer, SegmentIdWithShardSpec> allocatedPartyTimeIds = new HashMap<>();
    final Map<Integer, SegmentIdWithShardSpec> allocatedFutureIds = new HashMap<>();
    final SegmentIdWithShardSpec id1 = allocate(
        task,
        PARTY_TIME,
        Granularities.NONE,
        Granularities.HOUR,
        "s1",
        null
    );
    Assert.assertNotNull(id1);
    allocatedPartyTimeIds.put(id1.getShardSpec().getPartitionNum(), id1);
    final SegmentIdWithShardSpec id2 = allocate(
        task,
        THE_DISTANT_FUTURE,
        Granularities.NONE,
        Granularities.HOUR,
        "s1",
        id1.toString()
    );
    Assert.assertNotNull(id2);
    allocatedFutureIds.put(id2.getShardSpec().getPartitionNum(), id2);
    final SegmentIdWithShardSpec id3 = allocate(
        task,
        PARTY_TIME,
        Granularities.NONE,
        Granularities.HOUR,
        "s1",
        id2.toString()
    );
    Assert.assertNotNull(id3);
    allocatedPartyTimeIds.put(id3.getShardSpec().getPartitionNum(), id3);
    final SegmentIdWithShardSpec id4 = allocate(
        task,
        PARTY_TIME,
        Granularities.NONE,
        Granularities.HOUR,
        "s1",
        id1.toString()
    );
    Assert.assertNull(id4);
    final SegmentIdWithShardSpec id5 = allocate(
        task,
        THE_DISTANT_FUTURE,
        Granularities.NONE,
        Granularities.HOUR,
        "s1",
        id1.toString()
    );
    Assert.assertNotNull(id5);
    allocatedFutureIds.put(id5.getShardSpec().getPartitionNum(), id5);
    final SegmentIdWithShardSpec id6 = allocate(
        task,
        THE_DISTANT_FUTURE,
        Granularities.NONE,
        Granularities.MINUTE,
        "s1",
        id1.toString()
    );
    Assert.assertNull(id6);
    final SegmentIdWithShardSpec id7 = allocate(
        task,
        THE_DISTANT_FUTURE,
        Granularities.NONE,
        Granularities.DAY,
        "s1",
        id1.toString()
    );
    Assert.assertNotNull(id7);
    allocatedFutureIds.put(id7.getShardSpec().getPartitionNum(), id7);

    if (lockGranularity == LockGranularity.TIME_CHUNK) {
      final TaskLock partyLock = Iterables.getOnlyElement(
          FluentIterable.from(taskActionTestKit.getTaskLockbox().findLocksForTask(task))
                        .filter(input -> input.getInterval().contains(PARTY_TIME))
      );
      final TaskLock futureLock = Iterables.getOnlyElement(
          FluentIterable.from(taskActionTestKit.getTaskLockbox().findLocksForTask(task))
                        .filter(input -> input.getInterval().contains(THE_DISTANT_FUTURE))
      );

      assertSameIdentifier(
          id1,
          new SegmentIdWithShardSpec(
              DATA_SOURCE,
              Granularities.HOUR.bucket(PARTY_TIME),
              partyLock.getVersion(),
              new NumberedShardSpec(0, 0)
          )
      );
      assertSameIdentifier(
          id2,
          new SegmentIdWithShardSpec(
              DATA_SOURCE,
              Granularities.HOUR.bucket(THE_DISTANT_FUTURE),
              futureLock.getVersion(),
              new NumberedShardSpec(0, 0)
          )
      );
      assertSameIdentifier(
          id3,
          new SegmentIdWithShardSpec(
              DATA_SOURCE,
              Granularities.HOUR.bucket(PARTY_TIME),
              partyLock.getVersion(),
              new NumberedShardSpec(1, 0)
          )
      );
    } else {
      final List<TaskLock> partyLocks = taskActionTestKit.getTaskLockbox()
                                                         .findLocksForTask(task)
                                                         .stream()
                                                         .filter(input -> input.getInterval().contains(PARTY_TIME))
                                                         .collect(Collectors.toList());

      Assert.assertEquals(2, partyLocks.size());
      final Map<Integer, SegmentLock> partitionIdToLock = new HashMap<>();
      partyLocks.forEach(lock -> {
        Assert.assertEquals(LockGranularity.SEGMENT, lock.getGranularity());
        final SegmentLock segmentLock = (SegmentLock) lock;
        partitionIdToLock.put(segmentLock.getPartitionId(), segmentLock);
      });

      for (Entry<Integer, SegmentLock> entry : partitionIdToLock.entrySet()) {
        assertSameIdentifier(
            new SegmentIdWithShardSpec(
                DATA_SOURCE,
                Granularities.HOUR.bucket(PARTY_TIME),
                allocatedPartyTimeIds.get(entry.getKey()).getVersion(),
                new NumberedShardSpec(entry.getValue().getPartitionId(), 0)
            ),
            allocatedPartyTimeIds.get(entry.getKey())
        );
      }

      final List<TaskLock> futureLocks = taskActionTestKit
          .getTaskLockbox()
          .findLocksForTask(task)
          .stream()
          .filter(input -> input.getInterval().contains(THE_DISTANT_FUTURE))
          .collect(Collectors.toList());

      Assert.assertEquals(1, futureLocks.size());
      partitionIdToLock.clear();
      futureLocks.forEach(lock -> {
        Assert.assertEquals(LockGranularity.SEGMENT, lock.getGranularity());
        final SegmentLock segmentLock = (SegmentLock) lock;
        partitionIdToLock.put(segmentLock.getPartitionId(), segmentLock);
      });

      for (Entry<Integer, SegmentLock> entry : partitionIdToLock.entrySet()) {
        assertSameIdentifier(
            new SegmentIdWithShardSpec(
                DATA_SOURCE,
                Granularities.HOUR.bucket(THE_DISTANT_FUTURE),
                allocatedFutureIds.get(entry.getKey()).getVersion(),
                new NumberedShardSpec(entry.getValue().getPartitionId(), 0)
            ),
            allocatedFutureIds.get(entry.getKey())
        );
      }
    }

    Assert.assertNull(id4);
    assertSameIdentifier(id2, id5);
    Assert.assertNull(id6);
    assertSameIdentifier(id2, id7);
  }

  @Test
  public void testSegmentIsAllocatedForLatestUsedSegmentVersion() throws IOException
  {
    final Task task = NoopTask.create();
    taskActionTestKit.getTaskLockbox().add(task);

    final String sequenceName = "sequence_1";

    // Allocate segments when there are no committed segments
    final SegmentIdWithShardSpec pendingSegmentV01 =
        allocate(task, PARTY_TIME, Granularities.NONE, Granularities.HOUR, sequenceName, null);
    final SegmentIdWithShardSpec pendingSegmentV02 =
        allocate(task, PARTY_TIME, Granularities.NONE, Granularities.HOUR, sequenceName, null);

    assertSameIdentifier(pendingSegmentV01, pendingSegmentV02);

    // Commit a segment for version V1
    final DataSegment segmentV1
        = DataSegment.builder()
                     .dataSource(DATA_SOURCE)
                     .interval(Granularities.HOUR.bucket(PARTY_TIME))
                     .version(PARTY_TIME.plusDays(1).toString())
                     .shardSpec(new LinearShardSpec(0))
                     .size(100)
                     .build();
    taskActionTestKit.getMetadataStorageCoordinator().commitSegments(
        Collections.singleton(segmentV1)
    );

    // Verify that new allocations use version V1
    final SegmentIdWithShardSpec pendingSegmentV11 =
        allocate(task, PARTY_TIME, Granularities.NONE, Granularities.HOUR, sequenceName, null);
    final SegmentIdWithShardSpec pendingSegmentV12 =
        allocate(task, PARTY_TIME, Granularities.NONE, Granularities.HOUR, sequenceName, null);

    assertSameIdentifier(pendingSegmentV11, pendingSegmentV12);
    Assert.assertEquals(segmentV1.getVersion(), pendingSegmentV11.getVersion());

    Assert.assertNotEquals(pendingSegmentV01, pendingSegmentV11);

    // Commit a segment for version V2 to overshadow V1
    final DataSegment segmentV2
        = DataSegment.builder()
                     .dataSource(DATA_SOURCE)
                     .interval(Granularities.HOUR.bucket(PARTY_TIME))
                     .version(PARTY_TIME.plusDays(2).toString())
                     .shardSpec(new LinearShardSpec(0))
                     .size(100)
                     .build();
    taskActionTestKit.getMetadataStorageCoordinator().commitSegments(
        Collections.singleton(segmentV2)
    );
    Assert.assertTrue(segmentV2.getVersion().compareTo(segmentV1.getVersion()) > 0);

    // Verify that new segment allocations use version V2
    final SegmentIdWithShardSpec pendingSegmentV21 =
        allocate(task, PARTY_TIME, Granularities.NONE, Granularities.HOUR, sequenceName, null);
    final SegmentIdWithShardSpec pendingSegmentV22 =
        allocate(task, PARTY_TIME, Granularities.NONE, Granularities.HOUR, sequenceName, null);
    assertSameIdentifier(pendingSegmentV21, pendingSegmentV22);
    Assert.assertEquals(segmentV2.getVersion(), pendingSegmentV21.getVersion());

    Assert.assertNotEquals(pendingSegmentV21, pendingSegmentV01);
    Assert.assertNotEquals(pendingSegmentV21, pendingSegmentV11);
  }

  @Test
  public void testMultipleSequences()
  {
    final Task task = NoopTask.create();

    taskActionTestKit.getTaskLockbox().add(task);

    final SegmentIdWithShardSpec id1 = allocate(task, PARTY_TIME, Granularities.NONE, Granularities.HOUR, "s1", null);
    final SegmentIdWithShardSpec id2 = allocate(task, PARTY_TIME, Granularities.NONE, Granularities.HOUR, "s2", null);
    final SegmentIdWithShardSpec id3 = allocate(
        task,
        PARTY_TIME,
        Granularities.NONE,
        Granularities.HOUR,
        "s1",
        id1.toString()
    );
    final SegmentIdWithShardSpec id4 = allocate(
        task,
        THE_DISTANT_FUTURE,
        Granularities.NONE,
        Granularities.HOUR,
        "s1",
        id3.toString()
    );
    final SegmentIdWithShardSpec id5 = allocate(
        task,
        THE_DISTANT_FUTURE,
        Granularities.NONE,
        Granularities.HOUR,
        "s2",
        id2.toString()
    );
    final SegmentIdWithShardSpec id6 = allocate(
        task,
        PARTY_TIME,
        Granularities.NONE,
        Granularities.HOUR,
        "s1",
        null
    );

    if (lockGranularity == LockGranularity.TIME_CHUNK) {
      final TaskLock partyLock = Iterables.getOnlyElement(
          FluentIterable.from(taskActionTestKit.getTaskLockbox().findLocksForTask(task))
                        .filter(input -> input.getInterval().contains(PARTY_TIME))
      );
      final TaskLock futureLock = Iterables.getOnlyElement(
          FluentIterable.from(taskActionTestKit.getTaskLockbox().findLocksForTask(task))
                        .filter(input -> input.getInterval().contains(THE_DISTANT_FUTURE))
      );

      assertSameIdentifier(
          id1,
          new SegmentIdWithShardSpec(
              DATA_SOURCE,
              Granularities.HOUR.bucket(PARTY_TIME),
              partyLock.getVersion(),
              new NumberedShardSpec(0, 0)
          )
      );
      assertSameIdentifier(
          id2,
          new SegmentIdWithShardSpec(
              DATA_SOURCE,
              Granularities.HOUR.bucket(PARTY_TIME),
              partyLock.getVersion(),
              new NumberedShardSpec(1, 0)
          )
      );
      assertSameIdentifier(
          id3,
          new SegmentIdWithShardSpec(
              DATA_SOURCE,
              Granularities.HOUR.bucket(PARTY_TIME),
              partyLock.getVersion(),
              new NumberedShardSpec(2, 0)
          )
      );
      assertSameIdentifier(
          id4,
          new SegmentIdWithShardSpec(
              DATA_SOURCE,
              Granularities.HOUR.bucket(THE_DISTANT_FUTURE),
              futureLock.getVersion(),
              new NumberedShardSpec(0, 0)
          )
      );
      assertSameIdentifier(
          id5,
          new SegmentIdWithShardSpec(
              DATA_SOURCE,
              Granularities.HOUR.bucket(THE_DISTANT_FUTURE),
              futureLock.getVersion(),
              new NumberedShardSpec(1, 0)
          )
      );
    } else {
      final List<TaskLock> partyLocks = taskActionTestKit.getTaskLockbox()
                                                         .findLocksForTask(task)
                                                         .stream()
                                                         .filter(input -> input.getInterval().contains(PARTY_TIME))
                                                         .collect(Collectors.toList());

      Assert.assertEquals(3, partyLocks.size());

      assertSameIdentifier(
          new SegmentIdWithShardSpec(
              DATA_SOURCE,
              Granularities.HOUR.bucket(PARTY_TIME),
              partyLocks.get(0).getVersion(),
              new NumberedShardSpec(0, 0)
          ),
          id1
      );
      assertSameIdentifier(
          new SegmentIdWithShardSpec(
              DATA_SOURCE,
              Granularities.HOUR.bucket(PARTY_TIME),
              partyLocks.get(1).getVersion(),
              new NumberedShardSpec(1, 0)
          ),
          id2
      );
      assertSameIdentifier(
          new SegmentIdWithShardSpec(
              DATA_SOURCE,
              Granularities.HOUR.bucket(PARTY_TIME),
              partyLocks.get(2).getVersion(),
              new NumberedShardSpec(2, 0)
          ),
          id3
      );

      final List<TaskLock> futureLocks = taskActionTestKit
          .getTaskLockbox()
          .findLocksForTask(task)
          .stream()
          .filter(input -> input.getInterval().contains(THE_DISTANT_FUTURE))
          .collect(Collectors.toList());

      Assert.assertEquals(2, futureLocks.size());

      assertSameIdentifier(
          new SegmentIdWithShardSpec(
              DATA_SOURCE,
              Granularities.HOUR.bucket(THE_DISTANT_FUTURE),
              futureLocks.get(0).getVersion(),
              new NumberedShardSpec(0, 0)
          ),
          id4
      );
      assertSameIdentifier(
          new SegmentIdWithShardSpec(
              DATA_SOURCE,
              Granularities.HOUR.bucket(THE_DISTANT_FUTURE),
              futureLocks.get(1).getVersion(),
              new NumberedShardSpec(1, 0)
          ),
          id5
      );
    }

    assertSameIdentifier(id1, id6);
  }

  @Test
  public void testAddToExistingLinearShardSpecsSameGranularity() throws Exception
  {
    final Task task = NoopTask.create();

    taskActionTestKit.getMetadataStorageCoordinator().commitSegments(
        ImmutableSet.of(
            DataSegment.builder()
                       .dataSource(DATA_SOURCE)
                       .interval(Granularities.HOUR.bucket(PARTY_TIME))
                       .version(PARTY_TIME.toString())
                       .shardSpec(new LinearShardSpec(0))
                       .size(0)
                       .build(),
            DataSegment.builder()
                       .dataSource(DATA_SOURCE)
                       .interval(Granularities.HOUR.bucket(PARTY_TIME))
                       .version(PARTY_TIME.toString())
                       .shardSpec(new LinearShardSpec(1))
                       .size(0)
                       .build()
        )
    );

    taskActionTestKit.getTaskLockbox().add(task);

    final SegmentIdWithShardSpec id1 = allocate(
        task,
        PARTY_TIME,
        Granularities.NONE,
        Granularities.HOUR,
        "s1",
        null,
        LinearPartialShardSpec.instance()
    );
    final SegmentIdWithShardSpec id2 = allocate(
        task,
        PARTY_TIME,
        Granularities.NONE,
        Granularities.HOUR,
        "s1",
        id1.toString(),
        LinearPartialShardSpec.instance()
    );

    assertSameIdentifier(
        new SegmentIdWithShardSpec(
            DATA_SOURCE,
            Granularities.HOUR.bucket(PARTY_TIME),
            PARTY_TIME.toString(),
            new LinearShardSpec(2)
        ),
        id1
    );
    assertSameIdentifier(
        new SegmentIdWithShardSpec(
            DATA_SOURCE,
            Granularities.HOUR.bucket(PARTY_TIME),
            PARTY_TIME.toString(),
            new LinearShardSpec(3)
        ),
        id2
    );
  }

  @Test
  public void testAddToExistingNumberedShardSpecsSameGranularity() throws Exception
  {
    final Task task = NoopTask.create();

    taskActionTestKit.getMetadataStorageCoordinator().commitSegments(
        ImmutableSet.of(
            DataSegment.builder()
                       .dataSource(DATA_SOURCE)
                       .interval(Granularities.HOUR.bucket(PARTY_TIME))
                       .version(PARTY_TIME.toString())
                       .shardSpec(new NumberedShardSpec(0, 2))
                       .size(0)
                       .build(),
            DataSegment.builder()
                       .dataSource(DATA_SOURCE)
                       .interval(Granularities.HOUR.bucket(PARTY_TIME))
                       .version(PARTY_TIME.toString())
                       .shardSpec(new NumberedShardSpec(1, 2))
                       .size(0)
                       .build()
        )
    );

    taskActionTestKit.getTaskLockbox().add(task);

    final SegmentIdWithShardSpec id1 = allocate(
        task,
        PARTY_TIME,
        Granularities.NONE,
        Granularities.HOUR,
        "s1",
        null
    );
    final SegmentIdWithShardSpec id2 = allocate(
        task,
        PARTY_TIME,
        Granularities.NONE,
        Granularities.HOUR,
        "s1",
        id1.toString()
    );

    assertSameIdentifier(
        new SegmentIdWithShardSpec(
            DATA_SOURCE,
            Granularities.HOUR.bucket(PARTY_TIME),
            PARTY_TIME.toString(),
            new NumberedShardSpec(2, 2)
        ),
        id1
    );
    assertSameIdentifier(
        new SegmentIdWithShardSpec(
            DATA_SOURCE,
            Granularities.HOUR.bucket(PARTY_TIME),
            PARTY_TIME.toString(),
            new NumberedShardSpec(3, 2)
        ),
        id2
    );
  }

  @Test
  public void testAddToExistingNumberedShardSpecsCoarserPreferredGranularity() throws Exception
  {
    final Task task = NoopTask.create();

    taskActionTestKit.getMetadataStorageCoordinator().commitSegments(
        ImmutableSet.of(
            DataSegment.builder()
                       .dataSource(DATA_SOURCE)
                       .interval(Granularities.HOUR.bucket(PARTY_TIME))
                       .version(PARTY_TIME.toString())
                       .shardSpec(new NumberedShardSpec(0, 2))
                       .size(0)
                       .build(),
            DataSegment.builder()
                       .dataSource(DATA_SOURCE)
                       .interval(Granularities.HOUR.bucket(PARTY_TIME))
                       .version(PARTY_TIME.toString())
                       .shardSpec(new NumberedShardSpec(1, 2))
                       .size(0)
                       .build()
        )
    );

    taskActionTestKit.getTaskLockbox().add(task);

    final SegmentIdWithShardSpec id1 = allocate(task, PARTY_TIME, Granularities.NONE, Granularities.DAY, "s1", null);

    assertSameIdentifier(
        new SegmentIdWithShardSpec(
            DATA_SOURCE,
            Granularities.HOUR.bucket(PARTY_TIME),
            PARTY_TIME.toString(),
            new NumberedShardSpec(2, 2)
        ),
        id1
    );
  }

  @Test
  public void testAddToExistingNumberedShardSpecsFinerPreferredGranularity() throws Exception
  {
    final Task task = NoopTask.create();

    taskActionTestKit.getMetadataStorageCoordinator().commitSegments(
        ImmutableSet.of(
            DataSegment.builder()
                       .dataSource(DATA_SOURCE)
                       .interval(Granularities.HOUR.bucket(PARTY_TIME))
                       .version(PARTY_TIME.toString())
                       .shardSpec(new NumberedShardSpec(0, 2))
                       .size(0)
                       .build(),
            DataSegment.builder()
                       .dataSource(DATA_SOURCE)
                       .interval(Granularities.HOUR.bucket(PARTY_TIME))
                       .version(PARTY_TIME.toString())
                       .shardSpec(new NumberedShardSpec(1, 2))
                       .size(0)
                       .build()
        )
    );

    taskActionTestKit.getTaskLockbox().add(task);

    final SegmentIdWithShardSpec id1 = allocate(task, PARTY_TIME, Granularities.NONE, Granularities.MINUTE, "s1", null);

    assertSameIdentifier(
        new SegmentIdWithShardSpec(
            DATA_SOURCE,
            Granularities.HOUR.bucket(PARTY_TIME),
            PARTY_TIME.toString(),
            new NumberedShardSpec(2, 2)
        ),
        id1
    );
  }

  @Test
  public void testCannotAddToExistingNumberedShardSpecsWithCoarserQueryGranularity() throws Exception
  {
    final Task task = NoopTask.create();

    taskActionTestKit.getMetadataStorageCoordinator().commitSegments(
        ImmutableSet.of(
            DataSegment.builder()
                       .dataSource(DATA_SOURCE)
                       .interval(Granularities.HOUR.bucket(PARTY_TIME))
                       .version(PARTY_TIME.toString())
                       .shardSpec(new NumberedShardSpec(0, 2))
                       .size(0)
                       .build(),
            DataSegment.builder()
                       .dataSource(DATA_SOURCE)
                       .interval(Granularities.HOUR.bucket(PARTY_TIME))
                       .version(PARTY_TIME.toString())
                       .shardSpec(new NumberedShardSpec(1, 2))
                       .size(0)
                       .build()
        )
    );

    taskActionTestKit.getTaskLockbox().add(task);

    final SegmentIdWithShardSpec id1 = allocate(task, PARTY_TIME, Granularities.DAY, Granularities.DAY, "s1", null);

    Assert.assertNull(id1);
  }

  @Test
  public void testCannotDoAnythingWithSillyQueryGranularity()
  {
    final Task task = NoopTask.create();
    taskActionTestKit.getTaskLockbox().add(task);

    final SegmentIdWithShardSpec id1 = allocate(task, PARTY_TIME, Granularities.DAY, Granularities.HOUR, "s1", null);

    Assert.assertNull(id1);
  }

  @Test
  public void testWithPartialShardSpecAndOvershadowingSegments() throws IOException
  {
    final Task task = NoopTask.create();
    taskActionTestKit.getTaskLockbox().add(task);

    final ObjectMapper objectMapper = new DefaultObjectMapper();

    taskActionTestKit.getMetadataStorageCoordinator().commitSegments(
        ImmutableSet.of(
            DataSegment.builder()
                       .dataSource(DATA_SOURCE)
                       .interval(Granularities.HOUR.bucket(PARTY_TIME))
                       .version(PARTY_TIME.toString())
                       .shardSpec(
                           new HashBasedNumberedShardSpec(0, 2, 0, 2, ImmutableList.of("dim1"), null, objectMapper)
                       )
                       .size(0)
                       .build(),
            DataSegment.builder()
                       .dataSource(DATA_SOURCE)
                       .interval(Granularities.HOUR.bucket(PARTY_TIME))
                       .version(PARTY_TIME.toString())
                       .shardSpec(
                           new HashBasedNumberedShardSpec(1, 2, 1, 2, ImmutableList.of("dim1"), null, objectMapper)
                       )
                       .size(0)
                       .build()
        )
    );

    final SegmentAllocateAction action = new SegmentAllocateAction(
        DATA_SOURCE,
        PARTY_TIME,
        Granularities.MINUTE,
        Granularities.HOUR,
        "seq",
        null,
        true,
        new HashBasedNumberedPartialShardSpec(ImmutableList.of("dim1"), 1, 2, null),
        lockGranularity,
        null
    );
    final SegmentIdWithShardSpec segmentIdentifier = action.perform(task, taskActionTestKit.getTaskActionToolbox());
    Assert.assertNotNull(segmentIdentifier);

    final ShardSpec shardSpec = segmentIdentifier.getShardSpec();
    Assert.assertEquals(2, shardSpec.getPartitionNum());

    Assert.assertTrue(shardSpec instanceof HashBasedNumberedShardSpec);
    final HashBasedNumberedShardSpec hashBasedNumberedShardSpec = (HashBasedNumberedShardSpec) shardSpec;
    Assert.assertEquals(2, hashBasedNumberedShardSpec.getNumCorePartitions());
    Assert.assertEquals(ImmutableList.of("dim1"), hashBasedNumberedShardSpec.getPartitionDimensions());
  }

  @Test
  public void testSameIntervalWithSegmentGranularity()
  {
    final Task task = NoopTask.create();
    taskActionTestKit.getTaskLockbox().add(task);
    Granularity segmentGranularity = new PeriodGranularity(
        Period.hours(1),
        null,
        DateTimes.inferTzFromString("Asia/Shanghai")
    );

    final SegmentIdWithShardSpec id1 = allocate(
        task,
        PARTY_TIME,
        Granularities.MINUTE,
        segmentGranularity,
        "s1",
        null
    );
    final SegmentIdWithShardSpec id2 = allocate(
        task,
        PARTY_TIME,
        Granularities.MINUTE,
        segmentGranularity,
        "s2",
        null
    );
    Assert.assertNotNull(id1);
    Assert.assertNotNull(id2);
  }

  @Test
  public void testAllocateAllGranularity()
  {
    final Task task = NoopTask.create();
    taskActionTestKit.getTaskLockbox().add(task);

    final SegmentIdWithShardSpec id1 = allocate(
        task,
        PARTY_TIME,
        Granularities.MINUTE,
        Granularities.ALL,
        "s1",
        null
    );
    final SegmentIdWithShardSpec id2 = allocate(
        task,
        PARTY_TIME,
        Granularities.MINUTE,
        Granularities.ALL,
        "s2",
        null
    );

    Assert.assertNotNull(id1);
    Assert.assertNotNull(id2);
    Assert.assertEquals(Intervals.ETERNITY, id1.getInterval());
    Assert.assertEquals(Intervals.ETERNITY, id2.getInterval());
  }

  @Test
  public void testAllocateWeekOnlyWhenWeekIsPreferred()
  {
    final Task task = NoopTask.create();
    taskActionTestKit.getTaskLockbox().add(task);

    final SegmentIdWithShardSpec id1 = allocate(
        task,
        DateTimes.of("2023-12-16"),
        Granularities.MINUTE,
        Granularities.HOUR,
        "s1",
        null
    );

    final SegmentIdWithShardSpec id2 = allocate(
        task,
        DateTimes.of("2023-12-18"),
        Granularities.MINUTE,
        Granularities.WEEK,
        "s2",
        null
    );

    Assert.assertNotNull(id1);
    Assert.assertNotNull(id2);
    Assert.assertEquals(Duration.ofHours(1).toMillis(), id1.getInterval().toDurationMillis());
    Assert.assertEquals(Duration.ofDays(7).toMillis(), id2.getInterval().toDurationMillis());
  }

  @Test
  public void testAllocateDayWhenMonthNotPossible()
  {
    final Task task = NoopTask.create();
    taskActionTestKit.getTaskLockbox().add(task);

    final SegmentIdWithShardSpec id1 = allocate(
        task,
        DateTimes.of("2023-12-16"),
        Granularities.MINUTE,
        Granularities.HOUR,
        "s1",
        null
    );

    final SegmentIdWithShardSpec id2 = allocate(
        task,
        DateTimes.of("2023-12-18"),
        Granularities.MINUTE,
        Granularities.MONTH,
        "s2",
        null
    );

    Assert.assertNotNull(id1);
    Assert.assertNotNull(id2);
    Assert.assertEquals(Duration.ofHours(1).toMillis(), id1.getInterval().toDurationMillis());
    Assert.assertEquals(Duration.ofDays(1).toMillis(), id2.getInterval().toDurationMillis());
  }

  private SegmentIdWithShardSpec allocate(
      final Task task,
      final DateTime timestamp,
      final Granularity queryGranularity,
      final Granularity preferredSegmentGranularity,
      final String sequenceName,
      final String sequencePreviousId
  )
  {
    return allocate(
        task,
        timestamp,
        queryGranularity,
        preferredSegmentGranularity,
        sequenceName,
        sequencePreviousId,
        NumberedPartialShardSpec.instance()
    );
  }

  private SegmentIdWithShardSpec allocate(
      final Task task,
      final DateTime timestamp,
      final Granularity queryGranularity,
      final Granularity preferredSegmentGranularity,
      final String sequenceName,
      final String sequencePreviousId,
      final PartialShardSpec partialShardSpec
  )
  {
    final SegmentAllocateAction action = new SegmentAllocateAction(
        DATA_SOURCE,
        timestamp,
        queryGranularity,
        preferredSegmentGranularity,
        sequenceName,
        sequencePreviousId,
        false,
        partialShardSpec,
        lockGranularity,
        null
    );

    try {
      if (useBatch) {
        return action.performAsync(task, taskActionTestKit.getTaskActionToolbox())
                     .get(5, TimeUnit.SECONDS);
      } else {
        return action.perform(task, taskActionTestKit.getTaskActionToolbox());
      }
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void assertSameIdentifier(final SegmentIdWithShardSpec expected, final SegmentIdWithShardSpec actual)
  {
    Assert.assertEquals(expected, actual);
    Assert.assertEquals(expected.getShardSpec(), actual.getShardSpec());
  }
}
