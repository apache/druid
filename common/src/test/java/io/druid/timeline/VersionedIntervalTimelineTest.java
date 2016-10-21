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

package io.druid.timeline;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

import io.druid.common.utils.JodaUtils;
import io.druid.java.util.common.Pair;
import io.druid.timeline.partition.ImmutablePartitionHolder;
import io.druid.timeline.partition.IntegerPartitionChunk;
import io.druid.timeline.partition.PartitionChunk;
import io.druid.timeline.partition.PartitionHolder;
import io.druid.timeline.partition.SingleElementPartitionChunk;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Hours;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 */
public class VersionedIntervalTimelineTest
{
  VersionedIntervalTimeline<String, Integer> timeline;

  @Before
  public void setUp() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-04-01/2011-04-03", "1", 2);
    add("2011-04-03/2011-04-06", "1", 3);
    add("2011-04-01/2011-04-09", "2", 1);
    add("2011-04-06/2011-04-09", "3", 4);
    add("2011-04-01/2011-04-02", "3", 5);

    add("2011-05-01/2011-05-02", "1", 6);
    add("2011-05-01/2011-05-05", "2", 7);
    add("2011-05-03/2011-05-04", "3", 8);
    add("2011-05-01/2011-05-10", "4", 9);

    add("2011-10-01/2011-10-02", "1", 1);
    add("2011-10-02/2011-10-03", "3", IntegerPartitionChunk.make(null, 10, 0, 20));
    add("2011-10-02/2011-10-03", "3", IntegerPartitionChunk.make(10, null, 1, 21));
    add("2011-10-03/2011-10-04", "3", 3);
    add("2011-10-04/2011-10-05", "4", 4);
    add("2011-10-05/2011-10-06", "5", 5);
  }

  @Test
  public void testApril() throws Exception
  {
    assertValues(
        Arrays.asList(
            createExpected("2011-04-01/2011-04-02", "3", 5),
            createExpected("2011-04-02/2011-04-06", "2", 1),
            createExpected("2011-04-06/2011-04-09", "3", 4)
        ),
        timeline.lookup(new Interval("2011-04-01/2011-04-09"))
    );
  }

  @Test
  public void testApril2() throws Exception
  {
    Assert.assertEquals(
        makeSingle(1),
        timeline.remove(new Interval("2011-04-01/2011-04-09"), "2", makeSingle(1))
    );
    assertValues(
        Arrays.asList(
            createExpected("2011-04-01/2011-04-02", "3", 5),
            createExpected("2011-04-02/2011-04-03", "1", 2),
            createExpected("2011-04-03/2011-04-06", "1", 3),
            createExpected("2011-04-06/2011-04-09", "3", 4)
        ),
        timeline.lookup(new Interval("2011-04-01/2011-04-09"))
    );
  }

  @Test
  public void testApril3() throws Exception
  {
    Assert.assertEquals(
        makeSingle(1),
        timeline.remove(new Interval("2011-04-01/2011-04-09"), "2", makeSingle(1))
    );
    Assert.assertEquals(
        makeSingle(2),
        timeline.remove(new Interval("2011-04-01/2011-04-03"), "1", makeSingle(2))
    );
    assertValues(
        Arrays.asList(
            createExpected("2011-04-01/2011-04-02", "3", 5),
            createExpected("2011-04-03/2011-04-06", "1", 3),
            createExpected("2011-04-06/2011-04-09", "3", 4)
        ),
        timeline.lookup(new Interval("2011-04-01/2011-04-09"))
    );
  }

  @Test
  public void testApril4() throws Exception
  {
    Assert.assertEquals(
        makeSingle(1),
        timeline.remove(new Interval("2011-04-01/2011-04-09"), "2", makeSingle(1))
    );
    assertValues(
        Arrays.asList(
            createExpected("2011-04-01/2011-04-02", "3", 5),
            createExpected("2011-04-02/2011-04-03", "1", 2),
            createExpected("2011-04-03/2011-04-05", "1", 3)
        ),
        timeline.lookup(new Interval("2011-04-01/2011-04-05"))
    );

    assertValues(
        Arrays.asList(
            createExpected("2011-04-02T18/2011-04-03", "1", 2),
            createExpected("2011-04-03/2011-04-04T01", "1", 3)
        ),
        timeline.lookup(new Interval("2011-04-02T18/2011-04-04T01"))
    );
  }

  @Test
  public void testMay() throws Exception
  {
    assertValues(
        Arrays.asList(
            createExpected("2011-05-01/2011-05-09", "4", 9)
        ),
        timeline.lookup(new Interval("2011-05-01/2011-05-09"))
    );
  }

  @Test
  public void testMay2() throws Exception
  {
    Assert.assertNotNull(timeline.remove(new Interval("2011-05-01/2011-05-10"), "4", makeSingle(1)));
    assertValues(
        Arrays.asList(
            createExpected("2011-05-01/2011-05-03", "2", 7),
            createExpected("2011-05-03/2011-05-04", "3", 8),
            createExpected("2011-05-04/2011-05-05", "2", 7)
        ),
        timeline.lookup(new Interval("2011-05-01/2011-05-09"))
    );
  }

  @Test
  public void testMay3() throws Exception
  {
    Assert.assertEquals(
        makeSingle(9),
        timeline.remove(new Interval("2011-05-01/2011-05-10"), "4", makeSingle(9))
    );
    Assert.assertEquals(
        makeSingle(7),
        timeline.remove(new Interval("2011-05-01/2011-05-05"), "2", makeSingle(7))
    );
    assertValues(
        Arrays.asList(
            createExpected("2011-05-01/2011-05-02", "1", 6),
            createExpected("2011-05-03/2011-05-04", "3", 8)
        ),
        timeline.lookup(new Interval("2011-05-01/2011-05-09"))
    );
  }

  @Test
  public void testInsertInWrongOrder() throws Exception
  {
    DateTime overallStart = new DateTime().minus(Hours.TWO);

    Assert.assertTrue(
        "These timestamps have to be at the end AND include now for this test to work.",
        overallStart.isAfter(timeline.incompletePartitionsTimeline.lastEntry().getKey().getEnd())
    );

    final Interval oneHourInterval1 = new Interval(overallStart.plus(Hours.THREE), overallStart.plus(Hours.FOUR));
    final Interval oneHourInterval2 = new Interval(overallStart.plus(Hours.FOUR), overallStart.plus(Hours.FIVE));

    add(oneHourInterval1, "1", 1);
    add(oneHourInterval2, "1", 1);
    add(new Interval(overallStart, overallStart.plus(Days.ONE)), "2", 2);

    assertValues(
        Arrays.asList(
            createExpected(oneHourInterval1.toString(), "2", 2)
        ),
        timeline.lookup(oneHourInterval1)
    );
  }

  @Test
  public void testRemove() throws Exception
  {
    for (TimelineObjectHolder<String, Integer> holder : timeline.findOvershadowed()) {
      for (PartitionChunk<Integer> chunk : holder.getObject()) {
        timeline.remove(holder.getInterval(), holder.getVersion(), chunk);
      }
    }

    Assert.assertTrue(timeline.findOvershadowed().isEmpty());
  }

  @Test
  public void testFindEntry() throws Exception
  {
    Assert.assertEquals(
        new ImmutablePartitionHolder<Integer>(new PartitionHolder<Integer>(makeSingle(1))),
        timeline.findEntry(new Interval("2011-10-01/2011-10-02"), "1")
    );

    Assert.assertEquals(
        new ImmutablePartitionHolder<Integer>(new PartitionHolder<Integer>(makeSingle(1))),
        timeline.findEntry(new Interval("2011-10-01/2011-10-01T10"), "1")
    );

    Assert.assertEquals(
        new ImmutablePartitionHolder<Integer>(new PartitionHolder<Integer>(makeSingle(1))),
        timeline.findEntry(new Interval("2011-10-01T02/2011-10-02"), "1")
    );

    Assert.assertEquals(
        new ImmutablePartitionHolder<Integer>(new PartitionHolder<Integer>(makeSingle(1))),
        timeline.findEntry(new Interval("2011-10-01T04/2011-10-01T17"), "1")
    );

    Assert.assertEquals(
        null,
        timeline.findEntry(new Interval("2011-10-01T04/2011-10-01T17"), "2")
    );

    Assert.assertEquals(
        null,
        timeline.findEntry(new Interval("2011-10-01T04/2011-10-02T17"), "1")
    );
  }

  @Test
  public void testFindEntryWithOverlap() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-01-01/2011-01-10", "1", 1);
    add("2011-01-02/2011-01-05", "2", 1);

    Assert.assertEquals(
        new ImmutablePartitionHolder<Integer>(new PartitionHolder<Integer>(makeSingle(1))),
        timeline.findEntry(new Interval("2011-01-02T02/2011-01-04"), "1")
    );
  }

  @Test
  public void testPartitioning() throws Exception
  {
    assertValues(
        ImmutableList.of(
            createExpected("2011-10-01/2011-10-02", "1", 1),
            createExpected(
                "2011-10-02/2011-10-03", "3",
                Arrays.<PartitionChunk<Integer>>asList(
                    IntegerPartitionChunk.make(null, 10, 0, 20),
                    IntegerPartitionChunk.make(10, null, 1, 21)
                )
            ),
            createExpected("2011-10-03/2011-10-04", "3", 3),
            createExpected("2011-10-04/2011-10-05", "4", 4),
            createExpected("2011-10-05/2011-10-06", "5", 5)
        ),
        timeline.lookup(new Interval("2011-10-01/2011-10-06"))
    );
  }

  @Test
  public void testPartialPartitionNotReturned() throws Exception
  {
    testRemove();

    add("2011-10-06/2011-10-07", "6", IntegerPartitionChunk.make(null, 10, 0, 60));
    assertValues(
        ImmutableList.of(createExpected("2011-10-05/2011-10-06", "5", 5)),
        timeline.lookup(new Interval("2011-10-05/2011-10-07"))
    );
    Assert.assertTrue("Expected no overshadowed entries", timeline.findOvershadowed().isEmpty());

    add("2011-10-06/2011-10-07", "6", IntegerPartitionChunk.make(10, 20, 1, 61));
    assertValues(
        ImmutableList.of(createExpected("2011-10-05/2011-10-06", "5", 5)),
        timeline.lookup(new Interval("2011-10-05/2011-10-07"))
    );
    Assert.assertTrue("Expected no overshadowed entries", timeline.findOvershadowed().isEmpty());

    add("2011-10-06/2011-10-07", "6", IntegerPartitionChunk.make(20, null, 2, 62));
    assertValues(
        ImmutableList.of(
            createExpected("2011-10-05/2011-10-06", "5", 5),
            createExpected(
                "2011-10-06/2011-10-07", "6",
                Arrays.<PartitionChunk<Integer>>asList(
                    IntegerPartitionChunk.make(null, 10, 0, 60),
                    IntegerPartitionChunk.make(10, 20, 1, 61),
                    IntegerPartitionChunk.make(20, null, 2, 62)
                )
            )
        ),
        timeline.lookup(new Interval("2011-10-05/2011-10-07"))
    );
    Assert.assertTrue("Expected no overshadowed entries", timeline.findOvershadowed().isEmpty());
  }

  @Test
  public void testIncompletePartitionDoesNotOvershadow() throws Exception
  {
    testRemove();

    add("2011-10-05/2011-10-07", "6", IntegerPartitionChunk.make(null, 10, 0, 60));
    Assert.assertTrue("Expected no overshadowed entries", timeline.findOvershadowed().isEmpty());

    add("2011-10-05/2011-10-07", "6", IntegerPartitionChunk.make(10, 20, 1, 61));
    Assert.assertTrue("Expected no overshadowed entries", timeline.findOvershadowed().isEmpty());

    add("2011-10-05/2011-10-07", "6", IntegerPartitionChunk.make(20, null, 2, 62));
    assertValues(
        ImmutableSet.of(
            createExpected("2011-10-05/2011-10-06", "5", 5)
        ),
        timeline.findOvershadowed()
    );
  }

  @Test
  public void testRemovePartitionMakesIncomplete() throws Exception
  {
    testIncompletePartitionDoesNotOvershadow();

    final IntegerPartitionChunk<Integer> chunk = IntegerPartitionChunk.make(null, 10, 0, 60);
    Assert.assertEquals(chunk, timeline.remove(new Interval("2011-10-05/2011-10-07"), "6", chunk));
    assertValues(
        ImmutableList.of(createExpected("2011-10-05/2011-10-06", "5", 5)),
        timeline.lookup(new Interval("2011-10-05/2011-10-07"))
    );
    Assert.assertTrue("Expected no overshadowed entries", timeline.findOvershadowed().isEmpty());
  }

  @Test
  public void testInsertAndRemoveSameThingsion() throws Exception
  {
    add("2011-05-01/2011-05-10", "5", 10);
    assertValues(
        Arrays.asList(
            createExpected("2011-05-01/2011-05-09", "5", 10)
        ),
        timeline.lookup(new Interval("2011-05-01/2011-05-09"))
    );

    Assert.assertEquals(
        makeSingle(10),
        timeline.remove(new Interval("2011-05-01/2011-05-10"), "5", makeSingle(10))
    );
    assertValues(
        Arrays.asList(
            createExpected("2011-05-01/2011-05-09", "4", 9)
        ),
        timeline.lookup(new Interval("2011-05-01/2011-05-09"))
    );

    add("2011-05-01/2011-05-10", "5", 10);
    assertValues(
        Arrays.asList(
            createExpected("2011-05-01/2011-05-09", "5", 10)
        ),
        timeline.lookup(new Interval("2011-05-01/2011-05-09"))
    );

    Assert.assertEquals(
        makeSingle(9),
        timeline.remove(new Interval("2011-05-01/2011-05-10"), "4", makeSingle(9))
    );
    assertValues(
        Arrays.asList(
            createExpected("2011-05-01/2011-05-09", "5", 10)
        ),
        timeline.lookup(new Interval("2011-05-01/2011-05-09"))
    );
  }

  //   1|----|
  //      1|----|
  @Test
  public void testOverlapSameVersionThrowException() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-01-01/2011-01-10", "1", 1);

    boolean exceptionCaught = false;
    try {
      add("2011-01-05/2011-01-15", "1", 3);
    }
    catch (UnsupportedOperationException e) {
      exceptionCaught = true;
    }

    //Assert.assertTrue("Exception wasn't thrown.", exceptionCaught);
  }

  //   1|----|
  //   1|----|
  @Test
  public void testOverlapSameVersionIsOkay() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-01-01/2011-01-10", "1", 1);
    add("2011-01-01/2011-01-10", "2", 2);
    add("2011-01-01/2011-01-10", "2", 3);
    add("2011-01-01/2011-01-10", "1", 4);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-10", "2", 2)
        ),
        timeline.lookup(new Interval("2011-01-01/2011-01-10"))
    );
  }

  // 1|----|----|
  //   2|----|
  @Test
  public void testOverlapSecondBetween() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-01-01/2011-01-10", "1", 1);
    add("2011-01-10/2011-01-20", "1", 2);
    add("2011-01-05/2011-01-15", "2", 3);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-05", "1", 1),
            createExpected("2011-01-05/2011-01-15", "2", 3),
            createExpected("2011-01-15/2011-01-20", "1", 2)
        ),
        timeline.lookup(new Interval("2011-01-01/2011-01-20"))
    );
  }

  //   2|----|
  // 1|----|----|
  @Test
  public void testOverlapFirstBetween() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-01-05/2011-01-15", "2", 3);
    add("2011-01-01/2011-01-10", "1", 1);
    add("2011-01-10/2011-01-20", "1", 2);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-05", "1", 1),
            createExpected("2011-01-05/2011-01-15", "2", 3),
            createExpected("2011-01-15/2011-01-20", "1", 2)
        ),
        timeline.lookup(new Interval("2011-01-01/2011-01-20"))
    );
  }

  //   1|----|
  //      2|----|
  @Test
  public void testOverlapFirstBefore() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-01-01/2011-01-10", "1", 1);
    add("2011-01-05/2011-01-15", "2", 3);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-05", "1", 1),
            createExpected("2011-01-05/2011-01-15", "2", 3)
        ),
        timeline.lookup(new Interval("2011-01-01/2011-01-15"))
    );
  }

  //      2|----|
  //   1|----|
  @Test
  public void testOverlapFirstAfter() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-01-05/2011-01-15", "2", 3);
    add("2011-01-01/2011-01-10", "1", 1);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-05", "1", 1),
            createExpected("2011-01-05/2011-01-15", "2", 3)
        ),
        timeline.lookup(new Interval("2011-01-01/2011-01-15"))
    );
  }

  //   1|----|
  // 2|----|
  @Test
  public void testOverlapSecondBefore() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-01-05/2011-01-15", "1", 3);
    add("2011-01-01/2011-01-10", "2", 1);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-10", "2", 1),
            createExpected("2011-01-10/2011-01-15", "1", 3)
        ),
        timeline.lookup(new Interval("2011-01-01/2011-01-15"))
    );
  }

  // 2|----|
  //   1|----|
  @Test
  public void testOverlapSecondAfter() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-01-01/2011-01-10", "2", 3);
    add("2011-01-05/2011-01-15", "1", 1);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-10", "2", 1),
            createExpected("2011-01-10/2011-01-15", "1", 3)
        ),
        timeline.lookup(new Interval("2011-01-01/2011-01-15"))
    );
  }

  //   1|----------|
  //      2|----|
  @Test
  public void testOverlapFirstLarger() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-01-01/2011-01-20", "1", 2);
    add("2011-01-05/2011-01-15", "2", 3);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-05", "1", 2),
            createExpected("2011-01-05/2011-01-15", "2", 3),
            createExpected("2011-01-15/2011-01-20", "1", 2)
        ),
        timeline.lookup(new Interval("2011-01-01/2011-01-20"))
    );
  }

  //      2|----|
  //   1|----------|
  @Test
  public void testOverlapSecondLarger() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-01-05/2011-01-15", "2", 3);
    add("2011-01-01/2011-01-20", "1", 2);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-05", "1", 2),
            createExpected("2011-01-05/2011-01-15", "2", 3),
            createExpected("2011-01-15/2011-01-20", "1", 2)
        ),
        timeline.lookup(new Interval("2011-01-01/2011-01-20"))
    );
  }

  //   1|----|-----|
  //   2|-------|
  @Test
  public void testOverlapSecondPartialAlign() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-01-01/2011-01-10", "1", 1);
    add("2011-01-10/2011-01-20", "1", 2);
    add("2011-01-01/2011-01-15", "2", 3);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-15", "2", 3),
            createExpected("2011-01-15/2011-01-20", "1", 2)
        ),
        timeline.lookup(new Interval("2011-01-01/2011-01-20"))
    );
  }

  //   2|-------|
  //   1|----|-----|
  @Test
  public void testOverlapFirstPartialAlign() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-01-01/2011-01-15", "2", 3);
    add("2011-01-01/2011-01-10", "1", 1);
    add("2011-01-10/2011-01-20", "1", 2);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-15", "2", 3),
            createExpected("2011-01-15/2011-01-20", "1", 2)
        ),
        timeline.lookup(new Interval("2011-01-01/2011-01-20"))
    );
  }

  //   1|-------|
  //       2|------------|
  //     3|---|
  @Test
  public void testOverlapAscending() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-01-01/2011-01-10", "1", 1);
    add("2011-01-05/2011-01-20", "2", 2);
    add("2011-01-03/2011-01-06", "3", 3);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-03", "1", 1),
            createExpected("2011-01-03/2011-01-06", "3", 3),
            createExpected("2011-01-06/2011-01-20", "2", 2)
        ),
        timeline.lookup(new Interval("2011-01-01/2011-01-20"))
    );
  }

  //     3|---|
  //       2|------------|
  //   1|-------|
  @Test
  public void testOverlapDescending() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-01-03/2011-01-06", "3", 3);
    add("2011-01-05/2011-01-20", "2", 2);
    add("2011-01-01/2011-01-10", "1", 1);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-03", "1", 1),
            createExpected("2011-01-03/2011-01-06", "3", 3),
            createExpected("2011-01-06/2011-01-20", "2", 2)
        ),
        timeline.lookup(new Interval("2011-01-01/2011-01-20"))
    );
  }

  //       2|------------|
  //     3|---|
  //   1|-------|
  @Test
  public void testOverlapMixed() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-01-05/2011-01-20", "2", 2);
    add("2011-01-03/2011-01-06", "3", 3);
    add("2011-01-01/2011-01-10", "1", 1);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-03", "1", 1),
            createExpected("2011-01-03/2011-01-06", "3", 3),
            createExpected("2011-01-06/2011-01-20", "2", 2)
        ),
        timeline.lookup(new Interval("2011-01-01/2011-01-20"))
    );
  }

  //    1|-------------|
  //      2|--------|
  //      3|-----|
  @Test
  public void testOverlapContainedAscending() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-01-01/2011-01-20", "1", 1);
    add("2011-01-02/2011-01-10", "2", 2);
    add("2011-01-02/2011-01-06", "3", 3);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-02", "1", 1),
            createExpected("2011-01-02/2011-01-06", "3", 3),
            createExpected("2011-01-06/2011-01-10", "2", 2),
            createExpected("2011-01-10/2011-01-20", "1", 1)
        ),
        timeline.lookup(new Interval("2011-01-01/2011-01-20"))
    );
  }

  //      3|-----|
  //      2|--------|
  //    1|-------------|
  @Test
  public void testOverlapContainedDescending() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-01-02/2011-01-06", "3", 3);
    add("2011-01-02/2011-01-10", "2", 2);
    add("2011-01-01/2011-01-20", "1", 1);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-02", "1", 1),
            createExpected("2011-01-02/2011-01-06", "3", 3),
            createExpected("2011-01-06/2011-01-10", "2", 2),
            createExpected("2011-01-10/2011-01-20", "1", 1)
        ),
        timeline.lookup(new Interval("2011-01-01/2011-01-20"))
    );
  }

  //      2|--------|
  //      3|-----|
  //    1|-------------|
  @Test
  public void testOverlapContainedmixed() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-01-02/2011-01-10", "2", 2);
    add("2011-01-02/2011-01-06", "3", 3);
    add("2011-01-01/2011-01-20", "1", 1);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-02", "1", 1),
            createExpected("2011-01-02/2011-01-06", "3", 3),
            createExpected("2011-01-06/2011-01-10", "2", 2),
            createExpected("2011-01-10/2011-01-20", "1", 1)
        ),
        timeline.lookup(new Interval("2011-01-01/2011-01-20"))
    );
  }

  //      1|------|------|----|
  //                 2|-----|
  @Test
  public void testOverlapSecondContained() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-01-01/2011-01-07", "1", 1);
    add("2011-01-07/2011-01-15", "1", 2);
    add("2011-01-15/2011-01-20", "1", 3);
    add("2011-01-10/2011-01-13", "2", 4);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-07", "1", 1),
            createExpected("2011-01-07/2011-01-10", "1", 2),
            createExpected("2011-01-10/2011-01-13", "2", 4),
            createExpected("2011-01-13/2011-01-15", "1", 2),
            createExpected("2011-01-15/2011-01-20", "1", 3)
        ),
        timeline.lookup(new Interval("2011-01-01/2011-01-20"))
    );
  }

  //                 2|-----|
  //      1|------|------|----|
  @Test
  public void testOverlapFirstContained() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-01-10/2011-01-13", "2", 4);
    add("2011-01-01/2011-01-07", "1", 1);
    add("2011-01-07/2011-01-15", "1", 2);
    add("2011-01-15/2011-01-20", "1", 3);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-07", "1", 1),
            createExpected("2011-01-07/2011-01-10", "1", 2),
            createExpected("2011-01-10/2011-01-13", "2", 4),
            createExpected("2011-01-13/2011-01-15", "1", 2),
            createExpected("2011-01-15/2011-01-20", "1", 3)
        ),
        timeline.lookup(new Interval("2011-01-01/2011-01-20"))
    );
  }

  //  1|----|----|
  //  2|---------|
  @Test
  public void testOverlapSecondContainsFirst() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-01-01/2011-01-20", "1", 1);
    add("2011-01-01/2011-01-10", "2", 2);
    add("2011-01-10/2011-01-20", "2", 3);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-10", "2", 2),
            createExpected("2011-01-10/2011-01-20", "2", 3)
        ),
        timeline.lookup(new Interval("2011-01-01/2011-01-20"))
    );
  }

  //  2|---------|
  //  1|----|----|
  @Test
  public void testOverlapFirstContainsSecond() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-01-10/2011-01-20", "2", 3);
    add("2011-01-01/2011-01-20", "1", 1);
    add("2011-01-01/2011-01-10", "2", 2);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-10", "2", 2),
            createExpected("2011-01-10/2011-01-20", "2", 3)
        ),
        timeline.lookup(new Interval("2011-01-01/2011-01-20"))
    );
  }

  //  1|----|
  //     2|----|
  //        3|----|
  @Test
  public void testOverlapLayeredAscending() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-01-01/2011-01-10", "1", 1);
    add("2011-01-05/2011-01-15", "2", 2);
    add("2011-01-15/2011-01-25", "3", 3);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-05", "1", 1),
            createExpected("2011-01-05/2011-01-15", "2", 2),
            createExpected("2011-01-15/2011-01-25", "3", 3)
        ),
        timeline.lookup(new Interval("2011-01-01/2011-01-25"))
    );
  }

  //        3|----|
  //     2|----|
  //  1|----|
  @Test
  public void testOverlapLayeredDescending() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-01-15/2011-01-25", "3", 3);
    add("2011-01-05/2011-01-15", "2", 2);
    add("2011-01-01/2011-01-10", "1", 1);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-05", "1", 1),
            createExpected("2011-01-05/2011-01-15", "2", 2),
            createExpected("2011-01-15/2011-01-25", "3", 3)
        ),
        timeline.lookup(new Interval("2011-01-01/2011-01-25"))
    );
  }

  //  2|----|     |----|
  // 1|-------------|
  @Test
  public void testOverlapV1Large() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-01-01/2011-01-15", "1", 1);
    add("2011-01-03/2011-01-05", "2", 2);
    add("2011-01-13/2011-01-20", "2", 3);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-03", "1", 1),
            createExpected("2011-01-03/2011-01-05", "2", 2),
            createExpected("2011-01-05/2011-01-13", "1", 1),
            createExpected("2011-01-13/2011-01-20", "2", 2)
        ),
        timeline.lookup(new Interval("2011-01-01/2011-01-20"))
    );
  }

  // 2|-------------|
  //  1|----|     |----|
  @Test
  public void testOverlapV2Large() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-01-01/2011-01-15", "2", 1);
    add("2011-01-03/2011-01-05", "1", 2);
    add("2011-01-13/2011-01-20", "1", 3);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-15", "2", 2),
            createExpected("2011-01-15/2011-01-20", "1", 1)
        ),
        timeline.lookup(new Interval("2011-01-01/2011-01-20"))
    );
  }

  //    1|-------------|
  //  2|----|   |----|
  @Test
  public void testOverlapV1LargeIsAfter() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-01-03/2011-01-20", "1", 1);
    add("2011-01-01/2011-01-05", "2", 2);
    add("2011-01-13/2011-01-17", "2", 3);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-05", "2", 2),
            createExpected("2011-01-05/2011-01-13", "1", 1),
            createExpected("2011-01-13/2011-01-17", "2", 3),
            createExpected("2011-01-17/2011-01-20", "1", 1)
        ),
        timeline.lookup(new Interval("2011-01-01/2011-01-20"))
    );
  }

  //  2|----|   |----|
  //    1|-------------|
  @Test
  public void testOverlapV1SecondLargeIsAfter() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-01-13/2011-01-17", "2", 3);
    add("2011-01-01/2011-01-05", "2", 2);
    add("2011-01-03/2011-01-20", "1", 1);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-05", "2", 2),
            createExpected("2011-01-05/2011-01-13", "1", 1),
            createExpected("2011-01-13/2011-01-17", "2", 3),
            createExpected("2011-01-17/2011-01-20", "1", 1)
        ),
        timeline.lookup(new Interval("2011-01-01/2011-01-20"))
    );
  }

  //    1|-----------|
  //  2|----|     |----|
  @Test
  public void testOverlapV1FirstBetween() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-01-03/2011-01-17", "1", 1);
    add("2011-01-01/2011-01-05", "2", 2);
    add("2011-01-15/2011-01-20", "2", 3);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-05", "2", 2),
            createExpected("2011-01-05/2011-01-15", "1", 1),
            createExpected("2011-01-15/2011-01-20", "2", 3)
        ),
        timeline.lookup(new Interval("2011-01-01/2011-01-20"))
    );
  }

  //  2|----|     |----|
  //    1|-----------|
  @Test
  public void testOverlapV1SecondBetween() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-01-01/2011-01-05", "2", 2);
    add("2011-01-15/2011-01-20", "2", 3);
    add("2011-01-03/2011-01-17", "1", 1);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-05", "2", 2),
            createExpected("2011-01-05/2011-01-15", "1", 1),
            createExpected("2011-01-15/2011-01-20", "2", 3)
        ),
        timeline.lookup(new Interval("2011-01-01/2011-01-20"))
    );
  }

  //               4|---|
  //           3|---|
  //       2|---|
  // 1|-------------|
  @Test
  public void testOverlapLargeUnderlyingWithSmallDayAlignedOverlays() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-01-01/2011-01-05", "1", 1);
    add("2011-01-03/2011-01-04", "2", 2);
    add("2011-01-04/2011-01-05", "3", 3);
    add("2011-01-05/2011-01-06", "4", 4);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-03", "1", 1),
            createExpected("2011-01-03/2011-01-04", "2", 2),
            createExpected("2011-01-04/2011-01-05", "3", 3),
            createExpected("2011-01-05/2011-01-06", "4", 4)
        ),
        timeline.lookup(new Interval("0000-01-01/3000-01-01"))
    );
  }

  //     |----3---||---1---|
  //   |---2---|
  @Test
  public void testOverlapCausesNullEntries() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-01-01T12/2011-01-02", "3", 3);
    add("2011-01-02/3011-01-03", "1", 1);
    add("2011-01-01/2011-01-02", "2", 2);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-01T12", "2", 2),
            createExpected("2011-01-01T12/2011-01-02", "3", 3),
            createExpected("2011-01-02/3011-01-03", "1", 1)
        ),
        timeline.lookup(new Interval("2011-01-01/3011-01-03"))
    );
  }

  //    1|----|    |----|
  //   2|------|  |------|
  //  3|------------------|
  @Test
  public void testOverlapOvershadowedThirdContains() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-01-03/2011-01-06", "1", 1);
    add("2011-01-09/2011-01-12", "1", 2);
    add("2011-01-02/2011-01-08", "2", 3);
    add("2011-01-10/2011-01-16", "2", 4);
    add("2011-01-01/2011-01-20", "3", 5);

    assertValues(
        Sets.newHashSet(
            createExpected("2011-01-02/2011-01-08", "2", 3),
            createExpected("2011-01-10/2011-01-16", "2", 4),
            createExpected("2011-01-03/2011-01-06", "1", 1),
            createExpected("2011-01-09/2011-01-12", "1", 2)
        ),
        timeline.findOvershadowed()
    );
  }

  // 2|------|------|
  // 1|-------------|
  // 3|-------------|
  @Test
  public void testOverlapOvershadowedAligned() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-01-01/2011-01-05", "2", 1);
    add("2011-01-05/2011-01-10", "2", 2);
    add("2011-01-01/2011-01-10", "1", 3);
    add("2011-01-01/2011-01-10", "3", 4);

    assertValues(
        Sets.newHashSet(
            createExpected("2011-01-01/2011-01-05", "2", 1),
            createExpected("2011-01-05/2011-01-10", "2", 2),
            createExpected("2011-01-01/2011-01-10", "1", 3)
        ),
        timeline.findOvershadowed()
    );
  }

  // 2|----|      |-----|
  //     1|---------|
  // 3|-----------|
  @Test
  public void testOverlapOvershadowedSomeComplexOverlapsCantThinkOfBetterName() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-01-01/2011-01-05", "2", 1);
    add("2011-01-10/2011-01-15", "2", 2);
    add("2011-01-03/2011-01-12", "1", 3);
    add("2011-01-01/2011-01-10", "3", 4);

    assertValues(
        Sets.newHashSet(
            createExpected("2011-01-03/2011-01-12", "1", 3),
            createExpected("2011-01-01/2011-01-05", "2", 1)
        ),
        timeline.findOvershadowed()
    );
  }

  @Test
  public void testOverlapAndRemove() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-01-01/2011-01-20", "1", 1);
    add("2011-01-10/2011-01-15", "2", 2);

    timeline.remove(new Interval("2011-01-10/2011-01-15"), "2", makeSingle(2));

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-20", "1", 1)
        ),
        timeline.lookup(new Interval("2011-01-01/2011-01-20"))
    );
  }

  @Test
  public void testOverlapAndRemove2() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-01-01/2011-01-20", "1", 1);
    add("2011-01-10/2011-01-20", "2", 2);
    add("2011-01-20/2011-01-30", "3", 4);

    timeline.remove(new Interval("2011-01-10/2011-01-20"), "2", makeSingle(2));

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-20", "1", 1),
            createExpected("2011-01-20/2011-01-30", "3", 4)

        ),
        timeline.lookup(new Interval("2011-01-01/2011-01-30"))
    );
  }

  @Test
  public void testOverlapAndRemove3() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-01-01/2011-01-20", "1", 1);
    add("2011-01-02/2011-01-03", "2", 2);
    add("2011-01-10/2011-01-14", "2", 3);

    timeline.remove(new Interval("2011-01-02/2011-01-03"), "2", makeSingle(2));
    timeline.remove(new Interval("2011-01-10/2011-01-14"), "2", makeSingle(3));

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-20", "1", 1)

        ),
        timeline.lookup(new Interval("2011-01-01/2011-01-20"))
    );
  }

  @Test
  public void testOverlapAndRemove4() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-01-01/2011-01-20", "1", 1);
    add("2011-01-10/2011-01-15", "2", 2);
    add("2011-01-15/2011-01-20", "2", 3);

    timeline.remove(new Interval("2011-01-15/2011-01-20"), "2", makeSingle(3));

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-10", "1", 1),
            createExpected("2011-01-10/2011-01-15", "2", 2),
            createExpected("2011-01-15/2011-01-20", "1", 1)
        ),
        timeline.lookup(new Interval("2011-01-01/2011-01-20"))
    );
  }

  @Test
  public void testOverlapAndRemove5() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-01-01/2011-01-20", "1", 1);
    add("2011-01-10/2011-01-15", "2", 2);
    timeline.remove(new Interval("2011-01-10/2011-01-15"), "2", makeSingle(2));
    add("2011-01-01/2011-01-20", "1", 1);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-20", "1", 1)
        ),
        timeline.lookup(new Interval("2011-01-01/2011-01-20"))
    );
  }

  @Test
  public void testRemoveSomethingDontHave() throws Exception
  {
    Assert.assertNull(
        "Don't have it, should be null",
        timeline.remove(new Interval("1970-01-01/2025-04-20"), "1", makeSingle(1))
    );
    Assert.assertNull(
        "Don't have it, should be null",
        timeline.remove(new Interval("2011-04-01/2011-04-09"), "version does not exist", makeSingle(1))
    );
  }

  @Test
  public void testRemoveNothingBacking() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-01-01/2011-01-05", "1", 1);
    add("2011-01-05/2011-01-10", "2", 2);
    add("2011-01-10/2011-01-15", "3", 3);
    add("2011-01-15/2011-01-20", "4", 4);

    timeline.remove(new Interval("2011-01-15/2011-01-20"), "4", makeSingle(4));

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-05", "1", 1),
            createExpected("2011-01-05/2011-01-10", "2", 2),
            createExpected("2011-01-10/2011-01-15", "3", 3)
        ),
        timeline.lookup(new Interval(new DateTime(0), new DateTime(JodaUtils.MAX_INSTANT)))
    );
  }

  @Test
  public void testOvershadowingHigherVersionWins1() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-04-01/2011-04-03", "1", 2);
    add("2011-04-03/2011-04-06", "1", 3);
    add("2011-04-06/2011-04-09", "1", 4);
    add("2011-04-01/2011-04-09", "2", 1);

    assertValues(
        ImmutableSet.of(
            createExpected("2011-04-01/2011-04-03", "1", 2),
            createExpected("2011-04-03/2011-04-06", "1", 3),
            createExpected("2011-04-06/2011-04-09", "1", 4)
        ),
        timeline.findOvershadowed()
    );
  }

  @Test
  public void testOvershadowingHigherVersionWins2() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-04-01/2011-04-09", "1", 1);
    add("2011-04-01/2011-04-03", "2", 2);
    add("2011-04-03/2011-04-06", "2", 3);
    add("2011-04-06/2011-04-09", "2", 4);

    assertValues(
        ImmutableSet.of(
            createExpected("2011-04-01/2011-04-09", "1", 1)
        ),
        timeline.findOvershadowed()
    );
  }

  @Test
  public void testOvershadowingHigherVersionWins3() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-04-01/2011-04-03", "1", 2);
    add("2011-04-03/2011-04-06", "1", 3);
    add("2011-04-09/2011-04-12", "1", 4);
    add("2011-04-01/2011-04-12", "2", 1);

    assertValues(
        Sets.newHashSet(
            createExpected("2011-04-01/2011-04-03", "1", 2),
            createExpected("2011-04-03/2011-04-06", "1", 3),
            createExpected("2011-04-09/2011-04-12", "1", 4)
        ),
        timeline.findOvershadowed()
    );
  }

  @Test
  public void testOvershadowingHigherVersionWins4() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-04-03/2011-04-06", "1", 3);
    add("2011-04-06/2011-04-09", "1", 4);
    add("2011-04-01/2011-04-09", "2", 1);

    assertValues(
        ImmutableSet.of(
            createExpected("2011-04-03/2011-04-06", "1", 3),
            createExpected("2011-04-06/2011-04-09", "1", 4)
        ),
        timeline.findOvershadowed()
    );
  }

  @Test
  public void testOvershadowingHigherVersionNeverOvershadowedByLower1() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-04-01/2011-04-09", "1", 1);
    add("2011-04-03/2011-04-06", "2", 3);
    add("2011-04-06/2011-04-09", "2", 4);

    assertValues(
        ImmutableSet.<Pair<Interval, Pair<String, PartitionHolder<Integer>>>>of(),
        timeline.findOvershadowed()
    );
  }

  @Test
  public void testOvershadowingHigherVersionNeverOvershadowedByLower2() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-04-01/2011-04-09", "1", 1);
    add("2011-04-01/2011-04-03", "2", 2);
    add("2011-04-06/2011-04-09", "2", 4);

    assertValues(
        ImmutableSet.<Pair<Interval, Pair<String, PartitionHolder<Integer>>>>of(),
        timeline.findOvershadowed()
    );
  }

  @Test
  public void testOvershadowingHigherVersionNeverOvershadowedByLower3() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-04-01/2011-04-09", "1", 1);
    add("2011-04-01/2011-04-03", "2", 2);
    add("2011-04-03/2011-04-06", "2", 3);

    assertValues(
        ImmutableSet.<Pair<Interval, Pair<String, PartitionHolder<Integer>>>>of(),
        timeline.findOvershadowed()
    );
  }

  @Test
  public void testOvershadowingHigherVersionNeverOvershadowedByLower4() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-04-01/2011-04-09", "2", 1);
    add("2011-04-01/2011-04-03", "3", 2);
    add("2011-04-03/2011-04-06", "4", 3);
    add("2011-04-03/2011-04-06", "1", 3);

    assertValues(
        ImmutableSet.of(
            createExpected("2011-04-03/2011-04-06", "1", 3)
        ),
        timeline.findOvershadowed()
    );
  }

  @Test
  public void testOvershadowingHigherVersionNeverOvershadowedByLower5() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-04-01/2011-04-12", "2", 1);
    add("2011-04-01/2011-04-03", "3", 2);
    add("2011-04-06/2011-04-09", "4", 3);
    add("2011-04-09/2011-04-12", "1", 3);
    add("2011-04-03/2011-04-06", "1", 3);

    assertValues(
        Sets.newHashSet(
            createExpected("2011-04-03/2011-04-06", "1", 3),
            createExpected("2011-04-09/2011-04-12", "1", 3)
        ),
        timeline.findOvershadowed()
    );
  }

  @Test
  public void testOvershadowingSameIntervalHighVersionWins() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-04-01/2011-04-09", "1", 1);
    add("2011-04-01/2011-04-09", "9", 2);
    add("2011-04-01/2011-04-09", "2", 3);

    assertValues(
        Sets.newHashSet(
            createExpected("2011-04-01/2011-04-09", "2", 3),
            createExpected("2011-04-01/2011-04-09", "1", 1)
        ),
        timeline.findOvershadowed()
    );
  }

  @Test
  public void testOvershadowingSameIntervalSameVersionAllKept() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-04-01/2011-04-09", "1", 1);
    add("2011-04-01/2011-04-09", "9", 2);
    add("2011-04-01/2011-04-09", "2", 3);
    add("2011-04-01/2011-04-09", "9", 4);

    assertValues(
        Sets.newHashSet(
            createExpected("2011-04-01/2011-04-09", "2", 3),
            createExpected("2011-04-01/2011-04-09", "1", 1)
        ),
        timeline.findOvershadowed()
    );
  }

  @Test
  public void testNotFoundReturnsEmpty() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-04-01/2011-04-09", "1", 1);

    Assert.assertTrue(timeline.lookup(Interval.parse("1970/1980")).isEmpty());
  }

  // https://github.com/druid-io/druid/issues/3010
  @Test
  public void testRemoveIncompleteKeepsComplete() throws Exception
  {
    timeline = makeStringIntegerTimeline();

    add("2011-04-01/2011-04-02", "1", IntegerPartitionChunk.make(null, 1, 0, 77));
    add("2011-04-01/2011-04-02", "1", IntegerPartitionChunk.make(1, null, 1, 88));
    add("2011-04-01/2011-04-02", "2", IntegerPartitionChunk.make(null, 1, 0, 99));

    assertValues(
        ImmutableList.of(
            createExpected("2011-04-01/2011-04-02", "1",
                           Arrays.<PartitionChunk<Integer>>asList(
                               IntegerPartitionChunk.make(null, 1, 0, 77),
                               IntegerPartitionChunk.make(1, null, 1, 88)
                           )
            )
        ),
        timeline.lookup(new Interval("2011-04-01/2011-04-02"))
    );

    add("2011-04-01/2011-04-02", "3", IntegerPartitionChunk.make(null, 1, 0, 110));

    assertValues(
        ImmutableList.of(
            createExpected("2011-04-01/2011-04-02", "1",
                           Arrays.<PartitionChunk<Integer>>asList(
                               IntegerPartitionChunk.make(null, 1, 0, 77),
                               IntegerPartitionChunk.make(1, null, 1, 88)
                           )
            )
        ),
        timeline.lookup(new Interval("2011-04-01/2011-04-02"))
    );
    assertValues(
        Sets.newHashSet(
            createExpected("2011-04-01/2011-04-02", "2",
                           Arrays.<PartitionChunk<Integer>>asList(
                               IntegerPartitionChunk.make(null, 1, 0, 99)
                           )
            )
        ),
        timeline.findOvershadowed()
    );

    testRemove();

    assertValues(
        ImmutableList.of(
            createExpected("2011-04-01/2011-04-02", "1",
                           Arrays.<PartitionChunk<Integer>>asList(
                               IntegerPartitionChunk.make(null, 1, 0, 77),
                               IntegerPartitionChunk.make(1, null, 1, 88)
                           )
            )
        ),
        timeline.lookup(new Interval("2011-04-01/2011-04-02"))
    );
  }

  private Pair<Interval, Pair<String, PartitionHolder<Integer>>> createExpected(
      String intervalString,
      String version,
      Integer value
  )
  {
    return createExpected(
        intervalString,
        version,
        Arrays.<PartitionChunk<Integer>>asList(makeSingle(value))
    );
  }

  private Pair<Interval, Pair<String, PartitionHolder<Integer>>> createExpected(
      String intervalString,
      String version,
      List<PartitionChunk<Integer>> values
  )
  {
    return Pair.of(
        new Interval(intervalString),
        Pair.of(version, new PartitionHolder<Integer>(values))
    );
  }

  private SingleElementPartitionChunk<Integer> makeSingle(Integer value)
  {
    return new SingleElementPartitionChunk<Integer>(value);
  }

  private void add(String interval, String version, Integer value)
  {
    add(new Interval(interval), version, value);
  }

  private void add(Interval interval, String version, Integer value)
  {
    add(new Interval(interval), version, makeSingle(value));
  }

  private void add(String interval, String version, PartitionChunk<Integer> value)
  {
    add(new Interval(interval), version, value);
  }

  private void add(Interval interval, String version, PartitionChunk<Integer> value)
  {
    timeline.add(interval, version, value);
  }

  private void assertValues(
      List<Pair<Interval, Pair<String, PartitionHolder<Integer>>>> expected,
      List<TimelineObjectHolder<String, Integer>> actual
  )
  {
    Assert.assertEquals("Sizes did not match.", expected.size(), actual.size());

    Iterator<Pair<Interval, Pair<String, PartitionHolder<Integer>>>> expectedIter = expected.iterator();
    Iterator<TimelineObjectHolder<String, Integer>> actualIter = actual.iterator();

    while (expectedIter.hasNext()) {
      Pair<Interval, Pair<String, PartitionHolder<Integer>>> pair = expectedIter.next();
      TimelineObjectHolder<String, Integer> holder = actualIter.next();

      Assert.assertEquals(pair.lhs, holder.getInterval());
      Assert.assertEquals(pair.rhs.lhs, holder.getVersion());
      Assert.assertEquals(pair.rhs.rhs, holder.getObject());
    }
  }

  private void assertValues(
      Set<Pair<Interval, Pair<String, PartitionHolder<Integer>>>> expected,
      Set<TimelineObjectHolder<String, Integer>> actual
  )
  {
    Assert.assertEquals("Sizes did not match.", expected.size(), actual.size());

    Set<Pair<Interval, Pair<String, PartitionHolder<Integer>>>> actualSet =
        Sets.newHashSet(
            Iterables.transform(
                actual,
                new Function<TimelineObjectHolder<String, Integer>, Pair<Interval, Pair<String, PartitionHolder<Integer>>>>()
                {
                  @Override
                  public Pair<Interval, Pair<String, PartitionHolder<Integer>>> apply(
                      TimelineObjectHolder<String, Integer> input
                  )
                  {
                    return new Pair<>(input.getInterval(), new Pair<>(input.getVersion(), input.getObject()));
                  }
                }
            )
        );

    Assert.assertEquals(expected, actualSet);
  }

  private VersionedIntervalTimeline<String, Integer> makeStringIntegerTimeline()
  {
    return new VersionedIntervalTimeline<String, Integer>(Ordering.<String>natural());
  }

}
