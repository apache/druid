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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import io.druid.granularity.DurationGranularity;
import io.druid.granularity.QueryGranularity;
import io.druid.granularity.QueryGranularities;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.task.NoopTask;
import io.druid.indexing.common.task.Task;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.Granularity;
import io.druid.java.util.common.ISE;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.LinearShardSpec;
import io.druid.timeline.partition.NumberedShardSpec;
import io.druid.timeline.partition.SingleDimensionShardSpec;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class SegmentAllocateActionTest
{
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Rule
  public TaskActionTestKit taskActionTestKit = new TaskActionTestKit();

  private static final String DATA_SOURCE = "none";
  private static final DateTime PARTY_TIME = new DateTime("1999");
  private static final DateTime THE_DISTANT_FUTURE = new DateTime("3000");

  @Test
  public void testGranularitiesFinerThanDay() throws Exception
  {
    Assert.assertEquals(
        ImmutableList.of(
            Granularity.DAY,
            Granularity.SIX_HOUR,
            Granularity.HOUR,
            Granularity.FIFTEEN_MINUTE,
            Granularity.TEN_MINUTE,
            Granularity.FIVE_MINUTE,
            Granularity.MINUTE,
            Granularity.SECOND
        ),
        SegmentAllocateAction.granularitiesFinerThan(Granularity.DAY)
    );
  }

  @Test
  public void testGranularitiesFinerThanHour() throws Exception
  {
    Assert.assertEquals(
        ImmutableList.of(
            Granularity.HOUR,
            Granularity.FIFTEEN_MINUTE,
            Granularity.TEN_MINUTE,
            Granularity.FIVE_MINUTE,
            Granularity.MINUTE,
            Granularity.SECOND
        ),
        SegmentAllocateAction.granularitiesFinerThan(Granularity.HOUR)
    );
  }

  @Test
  public void testManySegmentsSameInterval() throws Exception
  {
    final Task task = new NoopTask(null, 0, 0, null, null, null);

    taskActionTestKit.getTaskLockbox().add(task);

    final SegmentIdentifier id1 = allocate(
        task,
        PARTY_TIME,
        QueryGranularities.NONE,
        Granularity.HOUR,
        "s1",
        null
    );
    final SegmentIdentifier id2 = allocate(
        task,
        PARTY_TIME,
        QueryGranularities.NONE,
        Granularity.HOUR,
        "s1",
        id1.getIdentifierAsString()
    );
    final SegmentIdentifier id3 = allocate(
        task,
        PARTY_TIME,
        QueryGranularities.NONE,
        Granularity.HOUR,
        "s1",
        id2.getIdentifierAsString()
    );

    final TaskLock partyLock = Iterables.getOnlyElement(
        FluentIterable.from(taskActionTestKit.getTaskLockbox().findLocksForTask(task))
                      .filter(
                          new Predicate<TaskLock>()
                          {
                            @Override
                            public boolean apply(TaskLock input)
                            {
                              return input.getInterval().contains(PARTY_TIME);
                            }
                          }
                      )
    );

    assertSameIdentifier(
        id1,
        new SegmentIdentifier(
            DATA_SOURCE,
            Granularity.HOUR.bucket(PARTY_TIME),
            partyLock.getVersion(),
            new NumberedShardSpec(0, 0)
        )
    );
    assertSameIdentifier(
        id2,
        new SegmentIdentifier(
            DATA_SOURCE,
            Granularity.HOUR.bucket(PARTY_TIME),
            partyLock.getVersion(),
            new NumberedShardSpec(1, 0)
        )
    );
    assertSameIdentifier(
        id3,
        new SegmentIdentifier(
            DATA_SOURCE,
            Granularity.HOUR.bucket(PARTY_TIME),
            partyLock.getVersion(),
            new NumberedShardSpec(2, 0)
        )
    );
  }

  @Test
  public void testResumeSequence() throws Exception
  {
    final Task task = new NoopTask(null, 0, 0, null, null, null);

    taskActionTestKit.getTaskLockbox().add(task);

    final SegmentIdentifier id1 = allocate(
        task,
        PARTY_TIME,
        QueryGranularities.NONE,
        Granularity.HOUR,
        "s1",
        null
    );
    final SegmentIdentifier id2 = allocate(
        task,
        THE_DISTANT_FUTURE,
        QueryGranularities.NONE,
        Granularity.HOUR,
        "s1",
        id1.getIdentifierAsString()
    );
    final SegmentIdentifier id3 = allocate(
        task,
        PARTY_TIME,
        QueryGranularities.NONE,
        Granularity.HOUR,
        "s1",
        id2.getIdentifierAsString()
    );
    final SegmentIdentifier id4 = allocate(
        task,
        PARTY_TIME,
        QueryGranularities.NONE,
        Granularity.HOUR,
        "s1",
        id1.getIdentifierAsString()
    );
    final SegmentIdentifier id5 = allocate(
        task,
        THE_DISTANT_FUTURE,
        QueryGranularities.NONE,
        Granularity.HOUR,
        "s1",
        id1.getIdentifierAsString()
    );
    final SegmentIdentifier id6 = allocate(
        task,
        THE_DISTANT_FUTURE,
        QueryGranularities.NONE,
        Granularity.MINUTE,
        "s1",
        id1.getIdentifierAsString()
    );
    final SegmentIdentifier id7 = allocate(
        task,
        THE_DISTANT_FUTURE,
        QueryGranularities.NONE,
        Granularity.DAY,
        "s1",
        id1.getIdentifierAsString()
    );

    final TaskLock partyLock = Iterables.getOnlyElement(
        FluentIterable.from(taskActionTestKit.getTaskLockbox().findLocksForTask(task))
                      .filter(
                          new Predicate<TaskLock>()
                          {
                            @Override
                            public boolean apply(TaskLock input)
                            {
                              return input.getInterval().contains(PARTY_TIME);
                            }
                          }
                      )
    );
    final TaskLock futureLock = Iterables.getOnlyElement(
        FluentIterable.from(taskActionTestKit.getTaskLockbox().findLocksForTask(task))
                      .filter(
                          new Predicate<TaskLock>()
                          {
                            @Override
                            public boolean apply(TaskLock input)
                            {
                              return input.getInterval().contains(THE_DISTANT_FUTURE);
                            }
                          }
                      )
    );

    assertSameIdentifier(
        id1,
        new SegmentIdentifier(
            DATA_SOURCE,
            Granularity.HOUR.bucket(PARTY_TIME),
            partyLock.getVersion(),
            new NumberedShardSpec(0, 0)
        )
    );
    assertSameIdentifier(
        id2,
        new SegmentIdentifier(
            DATA_SOURCE,
            Granularity.HOUR.bucket(THE_DISTANT_FUTURE),
            futureLock.getVersion(),
            new NumberedShardSpec(0, 0)
        )
    );
    assertSameIdentifier(
        id3,
        new SegmentIdentifier(
            DATA_SOURCE,
            Granularity.HOUR.bucket(PARTY_TIME),
            partyLock.getVersion(),
            new NumberedShardSpec(1, 0)
        )
    );
    Assert.assertNull(id4);
    assertSameIdentifier(id5, id2);
    Assert.assertNull(id6);
    assertSameIdentifier(id7, id2);
  }

  @Test
  public void testMultipleSequences() throws Exception
  {
    final Task task = new NoopTask(null, 0, 0, null, null, null);

    taskActionTestKit.getTaskLockbox().add(task);

    final SegmentIdentifier id1 = allocate(task, PARTY_TIME, QueryGranularities.NONE, Granularity.HOUR, "s1", null);
    final SegmentIdentifier id2 = allocate(task, PARTY_TIME, QueryGranularities.NONE, Granularity.HOUR, "s2", null);
    final SegmentIdentifier id3 = allocate(
        task,
        PARTY_TIME,
        QueryGranularities.NONE,
        Granularity.HOUR,
        "s1",
        id1.getIdentifierAsString()
    );
    final SegmentIdentifier id4 = allocate(
        task,
        THE_DISTANT_FUTURE,
        QueryGranularities.NONE,
        Granularity.HOUR,
        "s1",
        id3.getIdentifierAsString()
    );
    final SegmentIdentifier id5 = allocate(
        task,
        THE_DISTANT_FUTURE,
        QueryGranularities.NONE,
        Granularity.HOUR,
        "s2",
        id2.getIdentifierAsString()
    );
    final SegmentIdentifier id6 = allocate(task, PARTY_TIME, QueryGranularities.NONE, Granularity.HOUR, "s1", null);

    final TaskLock partyLock = Iterables.getOnlyElement(
        FluentIterable.from(taskActionTestKit.getTaskLockbox().findLocksForTask(task))
                      .filter(
                          new Predicate<TaskLock>()
                          {
                            @Override
                            public boolean apply(TaskLock input)
                            {
                              return input.getInterval().contains(PARTY_TIME);
                            }
                          }
                      )
    );
    final TaskLock futureLock = Iterables.getOnlyElement(
        FluentIterable.from(taskActionTestKit.getTaskLockbox().findLocksForTask(task))
                      .filter(
                          new Predicate<TaskLock>()
                          {
                            @Override
                            public boolean apply(TaskLock input)
                            {
                              return input.getInterval().contains(THE_DISTANT_FUTURE);
                            }
                          }
                      )
    );

    assertSameIdentifier(
        id1,
        new SegmentIdentifier(
            DATA_SOURCE,
            Granularity.HOUR.bucket(PARTY_TIME),
            partyLock.getVersion(),
            new NumberedShardSpec(0, 0)
        )
    );
    assertSameIdentifier(
        id2,
        new SegmentIdentifier(
            DATA_SOURCE,
            Granularity.HOUR.bucket(PARTY_TIME),
            partyLock.getVersion(),
            new NumberedShardSpec(1, 0)
        )
    );
    assertSameIdentifier(
        id3,
        new SegmentIdentifier(
            DATA_SOURCE,
            Granularity.HOUR.bucket(PARTY_TIME),
            partyLock.getVersion(),
            new NumberedShardSpec(2, 0)
        )
    );
    assertSameIdentifier(
        id4,
        new SegmentIdentifier(
            DATA_SOURCE,
            Granularity.HOUR.bucket(THE_DISTANT_FUTURE),
            futureLock.getVersion(),
            new NumberedShardSpec(0, 0)
        )
    );
    assertSameIdentifier(
        id5,
        new SegmentIdentifier(
            DATA_SOURCE,
            Granularity.HOUR.bucket(THE_DISTANT_FUTURE),
            futureLock.getVersion(),
            new NumberedShardSpec(1, 0)
        )
    );
    assertSameIdentifier(
        id6,
        id1
    );
  }

  @Test
  public void testAddToExistingLinearShardSpecsSameGranularity() throws Exception
  {
    final Task task = new NoopTask(null, 0, 0, null, null, null);

    taskActionTestKit.getMetadataStorageCoordinator().announceHistoricalSegments(
        ImmutableSet.of(
            DataSegment.builder()
                       .dataSource(DATA_SOURCE)
                       .interval(Granularity.HOUR.bucket(PARTY_TIME))
                       .version(PARTY_TIME.toString())
                       .shardSpec(new LinearShardSpec(0))
                       .build(),
            DataSegment.builder()
                       .dataSource(DATA_SOURCE)
                       .interval(Granularity.HOUR.bucket(PARTY_TIME))
                       .version(PARTY_TIME.toString())
                       .shardSpec(new LinearShardSpec(1))
                       .build()
        )
    );

    taskActionTestKit.getTaskLockbox().add(task);

    final SegmentIdentifier id1 = allocate(
        task,
        PARTY_TIME,
        QueryGranularities.NONE,
        Granularity.HOUR,
        "s1",
        null
    );
    final SegmentIdentifier id2 = allocate(
        task,
        PARTY_TIME,
        QueryGranularities.NONE,
        Granularity.HOUR,
        "s1",
        id1.getIdentifierAsString()
    );

    assertSameIdentifier(
        id1,
        new SegmentIdentifier(
            DATA_SOURCE,
            Granularity.HOUR.bucket(PARTY_TIME),
            PARTY_TIME.toString(),
            new LinearShardSpec(2)
        )
    );
    assertSameIdentifier(
        id2,
        new SegmentIdentifier(
            DATA_SOURCE,
            Granularity.HOUR.bucket(PARTY_TIME),
            PARTY_TIME.toString(),
            new LinearShardSpec(3)
        )
    );
  }

  @Test
  public void testAddToExistingNumberedShardSpecsSameGranularity() throws Exception
  {
    final Task task = new NoopTask(null, 0, 0, null, null, null);

    taskActionTestKit.getMetadataStorageCoordinator().announceHistoricalSegments(
        ImmutableSet.of(
            DataSegment.builder()
                       .dataSource(DATA_SOURCE)
                       .interval(Granularity.HOUR.bucket(PARTY_TIME))
                       .version(PARTY_TIME.toString())
                       .shardSpec(new NumberedShardSpec(0, 2))
                       .build(),
            DataSegment.builder()
                       .dataSource(DATA_SOURCE)
                       .interval(Granularity.HOUR.bucket(PARTY_TIME))
                       .version(PARTY_TIME.toString())
                       .shardSpec(new NumberedShardSpec(1, 2))
                       .build()
        )
    );

    taskActionTestKit.getTaskLockbox().add(task);

    final SegmentIdentifier id1 = allocate(
        task,
        PARTY_TIME,
        QueryGranularities.NONE,
        Granularity.HOUR,
        "s1",
        null
    );
    final SegmentIdentifier id2 = allocate(
        task,
        PARTY_TIME,
        QueryGranularities.NONE,
        Granularity.HOUR,
        "s1",
        id1.getIdentifierAsString()
    );

    assertSameIdentifier(
        id1,
        new SegmentIdentifier(
            DATA_SOURCE,
            Granularity.HOUR.bucket(PARTY_TIME),
            PARTY_TIME.toString(),
            new NumberedShardSpec(2, 2)
        )
    );
    assertSameIdentifier(
        id2,
        new SegmentIdentifier(
            DATA_SOURCE,
            Granularity.HOUR.bucket(PARTY_TIME),
            PARTY_TIME.toString(),
            new NumberedShardSpec(3, 2)
        )
    );
  }

  @Test
  public void testAddToExistingNumberedShardSpecsCoarserPreferredGranularity() throws Exception
  {
    final Task task = new NoopTask(null, 0, 0, null, null, null);

    taskActionTestKit.getMetadataStorageCoordinator().announceHistoricalSegments(
        ImmutableSet.of(
            DataSegment.builder()
                       .dataSource(DATA_SOURCE)
                       .interval(Granularity.HOUR.bucket(PARTY_TIME))
                       .version(PARTY_TIME.toString())
                       .shardSpec(new NumberedShardSpec(0, 2))
                       .build(),
            DataSegment.builder()
                       .dataSource(DATA_SOURCE)
                       .interval(Granularity.HOUR.bucket(PARTY_TIME))
                       .version(PARTY_TIME.toString())
                       .shardSpec(new NumberedShardSpec(1, 2))
                       .build()
        )
    );

    taskActionTestKit.getTaskLockbox().add(task);

    final SegmentIdentifier id1 = allocate(task, PARTY_TIME, QueryGranularities.NONE, Granularity.DAY, "s1", null);

    assertSameIdentifier(
        id1,
        new SegmentIdentifier(
            DATA_SOURCE,
            Granularity.HOUR.bucket(PARTY_TIME),
            PARTY_TIME.toString(),
            new NumberedShardSpec(2, 2)
        )
    );
  }

  @Test
  public void testAddToExistingNumberedShardSpecsFinerPreferredGranularity() throws Exception
  {
    final Task task = new NoopTask(null, 0, 0, null, null, null);

    taskActionTestKit.getMetadataStorageCoordinator().announceHistoricalSegments(
        ImmutableSet.of(
            DataSegment.builder()
                       .dataSource(DATA_SOURCE)
                       .interval(Granularity.HOUR.bucket(PARTY_TIME))
                       .version(PARTY_TIME.toString())
                       .shardSpec(new NumberedShardSpec(0, 2))
                       .build(),
            DataSegment.builder()
                       .dataSource(DATA_SOURCE)
                       .interval(Granularity.HOUR.bucket(PARTY_TIME))
                       .version(PARTY_TIME.toString())
                       .shardSpec(new NumberedShardSpec(1, 2))
                       .build()
        )
    );

    taskActionTestKit.getTaskLockbox().add(task);

    final SegmentIdentifier id1 = allocate(task, PARTY_TIME, QueryGranularities.NONE, Granularity.MINUTE, "s1", null);

    assertSameIdentifier(
        id1,
        new SegmentIdentifier(
            DATA_SOURCE,
            Granularity.HOUR.bucket(PARTY_TIME),
            PARTY_TIME.toString(),
            new NumberedShardSpec(2, 2)
        )
    );
  }

  @Test
  public void testCannotAddToExistingNumberedShardSpecsWithCoarserQueryGranularity() throws Exception
  {
    final Task task = new NoopTask(null, 0, 0, null, null, null);

    taskActionTestKit.getMetadataStorageCoordinator().announceHistoricalSegments(
        ImmutableSet.of(
            DataSegment.builder()
                       .dataSource(DATA_SOURCE)
                       .interval(Granularity.HOUR.bucket(PARTY_TIME))
                       .version(PARTY_TIME.toString())
                       .shardSpec(new NumberedShardSpec(0, 2))
                       .build(),
            DataSegment.builder()
                       .dataSource(DATA_SOURCE)
                       .interval(Granularity.HOUR.bucket(PARTY_TIME))
                       .version(PARTY_TIME.toString())
                       .shardSpec(new NumberedShardSpec(1, 2))
                       .build()
        )
    );

    taskActionTestKit.getTaskLockbox().add(task);

    final SegmentIdentifier id1 = allocate(task, PARTY_TIME, QueryGranularities.DAY, Granularity.DAY, "s1", null);

    Assert.assertNull(id1);
  }

  @Test
  public void testCannotDoAnythingWithSillyQueryGranularity() throws Exception
  {
    final Task task = new NoopTask(null, 0, 0, null, null, null);
    taskActionTestKit.getTaskLockbox().add(task);

    final SegmentIdentifier id1 = allocate(task, PARTY_TIME, QueryGranularities.DAY, Granularity.HOUR, "s1", null);

    Assert.assertNull(id1);
  }

  @Test
  public void testCannotAddToExistingSingleDimensionShardSpecs() throws Exception
  {
    final Task task = new NoopTask(null, 0, 0, null, null, null);

    taskActionTestKit.getMetadataStorageCoordinator().announceHistoricalSegments(
        ImmutableSet.of(
            DataSegment.builder()
                       .dataSource(DATA_SOURCE)
                       .interval(Granularity.HOUR.bucket(PARTY_TIME))
                       .version(PARTY_TIME.toString())
                       .shardSpec(new SingleDimensionShardSpec("foo", null, "bar", 0))
                       .build(),
            DataSegment.builder()
                       .dataSource(DATA_SOURCE)
                       .interval(Granularity.HOUR.bucket(PARTY_TIME))
                       .version(PARTY_TIME.toString())
                       .shardSpec(new SingleDimensionShardSpec("foo", "bar", null, 1))
                       .build()
        )
    );

    taskActionTestKit.getTaskLockbox().add(task);

    final SegmentIdentifier id1 = allocate(task, PARTY_TIME, QueryGranularities.NONE, Granularity.HOUR, "s1", null);

    Assert.assertNull(id1);
  }

  @Test
  public void testSerde() throws Exception
  {
    final SegmentAllocateAction action = new SegmentAllocateAction(
        DATA_SOURCE,
        PARTY_TIME,
        QueryGranularities.MINUTE,
        Granularity.HOUR,
        "s1",
        "prev"
    );

    final ObjectMapper objectMapper = new DefaultObjectMapper();
    final SegmentAllocateAction action2 = (SegmentAllocateAction) objectMapper.readValue(
        objectMapper.writeValueAsBytes(action),
        TaskAction.class
    );

    Assert.assertEquals(DATA_SOURCE, action2.getDataSource());
    Assert.assertEquals(PARTY_TIME, action2.getTimestamp());
    Assert.assertEquals(new DurationGranularity(60000, 0), action2.getQueryGranularity());
    Assert.assertSame(Granularity.HOUR, action2.getPreferredSegmentGranularity());
    Assert.assertEquals("s1", action2.getSequenceName());
    Assert.assertEquals("prev", action2.getPreviousSegmentId());
  }

  private SegmentIdentifier allocate(
      final Task task,
      final DateTime timestamp,
      final QueryGranularity queryGranularity,
      final Granularity preferredSegmentGranularity,
      final String sequenceName,
      final String sequencePreviousId
  ) throws Exception
  {
    final SegmentAllocateAction action = new SegmentAllocateAction(
        DATA_SOURCE,
        timestamp,
        queryGranularity,
        preferredSegmentGranularity,
        sequenceName,
        sequencePreviousId
    );
    return action.perform(task, taskActionTestKit.getTaskActionToolbox());
  }

  private void assertSameIdentifier(final SegmentIdentifier one, final SegmentIdentifier other)
  {
    Assert.assertEquals(one, other);
    Assert.assertEquals(one.getShardSpec().getPartitionNum(), other.getShardSpec().getPartitionNum());

    if (one.getShardSpec().getClass() == NumberedShardSpec.class
        && other.getShardSpec().getClass() == NumberedShardSpec.class) {
      Assert.assertEquals(
          ((NumberedShardSpec) one.getShardSpec()).getPartitions(),
          ((NumberedShardSpec) other.getShardSpec()).getPartitions()
      );
    } else if (one.getShardSpec().getClass() == LinearShardSpec.class
               && other.getShardSpec().getClass() == LinearShardSpec.class) {
      // do nothing
    } else {
      throw new ISE(
          "Unexpected shardSpecs [%s] and [%s]",
          one.getShardSpec().getClass(),
          other.getShardSpec().getClass()
      );
    }
  }
}
