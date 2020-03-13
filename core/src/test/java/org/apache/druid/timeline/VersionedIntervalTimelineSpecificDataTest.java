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

package org.apache.druid.timeline;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.timeline.partition.IntegerPartitionChunk;
import org.apache.druid.timeline.partition.OvershadowableInteger;
import org.apache.druid.timeline.partition.PartitionHolder;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Hours;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

/**
 * This test class is separated from {@link VersionedIntervalTimelineTest} because it populates specific data for tests
 * in {@link #setUp()}.
 */
public class VersionedIntervalTimelineSpecificDataTest extends VersionedIntervalTimelineTestBase
{
  @Before
  public void setUp()
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
    add("2011-10-02/2011-10-03", "3", IntegerPartitionChunk.make(null, 10, 0, new OvershadowableInteger("3", 0, 20)));
    add("2011-10-02/2011-10-03", "3", IntegerPartitionChunk.make(10, null, 1, new OvershadowableInteger("3", 1, 21)));
    add("2011-10-03/2011-10-04", "3", 3);
    add("2011-10-04/2011-10-05", "4", 4);
    add("2011-10-05/2011-10-06", "5", 5);
  }

  @Test
  public void testApril()
  {
    assertValues(
        Arrays.asList(
            createExpected("2011-04-01/2011-04-02", "3", 5),
            createExpected("2011-04-02/2011-04-06", "2", 1),
            createExpected("2011-04-06/2011-04-09", "3", 4)
        ),
        timeline.lookup(Intervals.of("2011-04-01/2011-04-09"))
    );
  }

  @Test
  public void testApril2()
  {
    Assert.assertEquals(
        makeSingle("2", 1),
        timeline.remove(Intervals.of("2011-04-01/2011-04-09"), "2", makeSingle("2", 1))
    );
    assertValues(
        Arrays.asList(
            createExpected("2011-04-01/2011-04-02", "3", 5),
            createExpected("2011-04-02/2011-04-03", "1", 2),
            createExpected("2011-04-03/2011-04-06", "1", 3),
            createExpected("2011-04-06/2011-04-09", "3", 4)
        ),
        timeline.lookup(Intervals.of("2011-04-01/2011-04-09"))
    );
  }

  @Test
  public void testApril3()
  {
    Assert.assertEquals(
        makeSingle("2", 1),
        timeline.remove(Intervals.of("2011-04-01/2011-04-09"), "2", makeSingle("2", 1))
    );
    Assert.assertEquals(
        makeSingle("1", 2),
        timeline.remove(Intervals.of("2011-04-01/2011-04-03"), "1", makeSingle("1", 2))
    );
    assertValues(
        Arrays.asList(
            createExpected("2011-04-01/2011-04-02", "3", 5),
            createExpected("2011-04-03/2011-04-06", "1", 3),
            createExpected("2011-04-06/2011-04-09", "3", 4)
        ),
        timeline.lookup(Intervals.of("2011-04-01/2011-04-09"))
    );
  }

  @Test
  public void testApril4()
  {
    Assert.assertEquals(
        makeSingle("2", 1),
        timeline.remove(Intervals.of("2011-04-01/2011-04-09"), "2", makeSingle("2", 1))
    );
    assertValues(
        Arrays.asList(
            createExpected("2011-04-01/2011-04-02", "3", 5),
            createExpected("2011-04-02/2011-04-03", "1", 2),
            createExpected("2011-04-03/2011-04-05", "1", 3)
        ),
        timeline.lookup(Intervals.of("2011-04-01/2011-04-05"))
    );

    assertValues(
        Arrays.asList(
            createExpected("2011-04-02T18/2011-04-03", "1", 2),
            createExpected("2011-04-03/2011-04-04T01", "1", 3)
        ),
        timeline.lookup(Intervals.of("2011-04-02T18/2011-04-04T01"))
    );
  }

  @Test
  public void testMay()
  {
    assertValues(
        Collections.singletonList(
            createExpected("2011-05-01/2011-05-09", "4", 9)
        ),
        timeline.lookup(Intervals.of("2011-05-01/2011-05-09"))
    );
  }

  @Test
  public void testMay2()
  {
    Assert.assertNotNull(timeline.remove(Intervals.of("2011-05-01/2011-05-10"), "4", makeSingle("4", 9)));
    assertValues(
        Arrays.asList(
            createExpected("2011-05-01/2011-05-03", "2", 7),
            createExpected("2011-05-03/2011-05-04", "3", 8),
            createExpected("2011-05-04/2011-05-05", "2", 7)
        ),
        timeline.lookup(Intervals.of("2011-05-01/2011-05-09"))
    );
  }

  @Test
  public void testMay3()
  {
    Assert.assertEquals(
        makeSingle("4", 9),
        timeline.remove(Intervals.of("2011-05-01/2011-05-10"), "4", makeSingle("4", 9))
    );
    Assert.assertEquals(
        makeSingle("2", 7),
        timeline.remove(Intervals.of("2011-05-01/2011-05-05"), "2", makeSingle("2", 7))
    );
    assertValues(
        Arrays.asList(
            createExpected("2011-05-01/2011-05-02", "1", 6),
            createExpected("2011-05-03/2011-05-04", "3", 8)
        ),
        timeline.lookup(Intervals.of("2011-05-01/2011-05-09"))
    );
  }

  @Test
  public void testInsertInWrongOrder()
  {
    DateTime overallStart = DateTimes.nowUtc().minus(Hours.TWO);

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
        Collections.singletonList(
            createExpected(oneHourInterval1.toString(), "2", 2)
        ),
        timeline.lookup(oneHourInterval1)
    );
  }

  @Test
  public void testRemove()
  {
    checkRemove();
  }

  @Test
  public void testFindEntry()
  {
    Assert.assertEquals(
        new PartitionHolder<>(makeSingle("1", 1)).asImmutable(),
        timeline.findEntry(Intervals.of("2011-10-01/2011-10-02"), "1")
    );

    Assert.assertEquals(
        new PartitionHolder<>(makeSingle("1", 1)).asImmutable(),
        timeline.findEntry(Intervals.of("2011-10-01/2011-10-01T10"), "1")
    );

    Assert.assertEquals(
        new PartitionHolder<>(makeSingle("1", 1)).asImmutable(),
        timeline.findEntry(Intervals.of("2011-10-01T02/2011-10-02"), "1")
    );

    Assert.assertEquals(
        new PartitionHolder<>(makeSingle("1", 1)).asImmutable(),
        timeline.findEntry(Intervals.of("2011-10-01T04/2011-10-01T17"), "1")
    );

    Assert.assertEquals(
        null,
        timeline.findEntry(Intervals.of("2011-10-01T04/2011-10-01T17"), "2")
    );

    Assert.assertEquals(
        null,
        timeline.findEntry(Intervals.of("2011-10-01T04/2011-10-02T17"), "1")
    );
  }

  @Test
  public void testPartitioning()
  {
    assertValues(
        ImmutableList.of(
            createExpected("2011-10-01/2011-10-02", "1", 1),
            createExpected(
                "2011-10-02/2011-10-03",
                "3",
                Arrays.asList(
                    IntegerPartitionChunk.make(null, 10, 0, new OvershadowableInteger("3", 0, 20)),
                    IntegerPartitionChunk.make(10, null, 1, new OvershadowableInteger("3", 1, 21))
                )
            ),
            createExpected("2011-10-03/2011-10-04", "3", 3),
            createExpected("2011-10-04/2011-10-05", "4", 4),
            createExpected("2011-10-05/2011-10-06", "5", 5)
        ),
        timeline.lookup(Intervals.of("2011-10-01/2011-10-06"))
    );
  }

  @Test
  public void testPartialPartitionNotReturned()
  {
    testRemove();

    add("2011-10-06/2011-10-07", "6", IntegerPartitionChunk.make(null, 10, 0, new OvershadowableInteger("6", 0, 60)));
    assertValues(
        ImmutableList.of(createExpected("2011-10-05/2011-10-06", "5", 5)),
        timeline.lookup(Intervals.of("2011-10-05/2011-10-07"))
    );
    Assert.assertTrue("Expected no overshadowed entries", timeline.findFullyOvershadowed().isEmpty());

    add("2011-10-06/2011-10-07", "6", IntegerPartitionChunk.make(10, 20, 1, new OvershadowableInteger("6", 1, 61)));
    assertValues(
        ImmutableList.of(createExpected("2011-10-05/2011-10-06", "5", 5)),
        timeline.lookup(Intervals.of("2011-10-05/2011-10-07"))
    );
    Assert.assertTrue("Expected no overshadowed entries", timeline.findFullyOvershadowed().isEmpty());

    add("2011-10-06/2011-10-07", "6", IntegerPartitionChunk.make(20, null, 2, new OvershadowableInteger("6", 2, 62)));
    assertValues(
        ImmutableList.of(
            createExpected("2011-10-05/2011-10-06", "5", 5),
            createExpected(
                "2011-10-06/2011-10-07",
                "6",
                Arrays.asList(
                    IntegerPartitionChunk.make(null, 10, 0, new OvershadowableInteger("6", 0, 60)),
                    IntegerPartitionChunk.make(10, 20, 1, new OvershadowableInteger("6", 1, 61)),
                    IntegerPartitionChunk.make(20, null, 2, new OvershadowableInteger("6", 2, 62))
                )
            )
        ),
        timeline.lookup(Intervals.of("2011-10-05/2011-10-07"))
    );
    Assert.assertTrue("Expected no overshadowed entries", timeline.findFullyOvershadowed().isEmpty());
  }

  @Test
  public void testIncompletePartitionDoesNotOvershadow()
  {
    testRemove();

    add("2011-10-05/2011-10-07", "6", IntegerPartitionChunk.make(null, 10, 0, new OvershadowableInteger("6", 0, 60)));
    Assert.assertTrue("Expected no overshadowed entries", timeline.findFullyOvershadowed().isEmpty());

    add("2011-10-05/2011-10-07", "6", IntegerPartitionChunk.make(10, 20, 1, new OvershadowableInteger("6", 1, 61)));
    Assert.assertTrue("Expected no overshadowed entries", timeline.findFullyOvershadowed().isEmpty());

    add("2011-10-05/2011-10-07", "6", IntegerPartitionChunk.make(20, null, 2, new OvershadowableInteger("6", 2, 62)));
    assertValues(
        ImmutableSet.of(
            createExpected("2011-10-05/2011-10-06", "5", 5)
        ),
        timeline.findFullyOvershadowed()
    );
  }

  @Test
  public void testRemovePartitionMakesIncomplete()
  {
    testIncompletePartitionDoesNotOvershadow();

    final IntegerPartitionChunk<OvershadowableInteger> chunk = IntegerPartitionChunk.make(
        null,
        10,
        0,
        new OvershadowableInteger("6", 0, 60)
    );
    Assert.assertEquals(chunk, timeline.remove(Intervals.of("2011-10-05/2011-10-07"), "6", chunk));
    assertValues(
        ImmutableList.of(createExpected("2011-10-05/2011-10-06", "5", 5)),
        timeline.lookup(Intervals.of("2011-10-05/2011-10-07"))
    );
    Assert.assertTrue("Expected no overshadowed entries", timeline.findFullyOvershadowed().isEmpty());
  }

  @Test
  public void testInsertAndRemoveSameThingsion()
  {
    add("2011-05-01/2011-05-10", "5", 10);
    assertValues(
        Collections.singletonList(
            createExpected("2011-05-01/2011-05-09", "5", 10)
        ),
        timeline.lookup(Intervals.of("2011-05-01/2011-05-09"))
    );

    Assert.assertEquals(
        makeSingle("5", 10),
        timeline.remove(Intervals.of("2011-05-01/2011-05-10"), "5", makeSingle("5", 10))
    );
    assertValues(
        Collections.singletonList(
            createExpected("2011-05-01/2011-05-09", "4", 9)
        ),
        timeline.lookup(Intervals.of("2011-05-01/2011-05-09"))
    );

    add("2011-05-01/2011-05-10", "5", 10);
    assertValues(
        Collections.singletonList(
            createExpected("2011-05-01/2011-05-09", "5", 10)
        ),
        timeline.lookup(Intervals.of("2011-05-01/2011-05-09"))
    );

    Assert.assertEquals(
        makeSingle("4", 9),
        timeline.remove(Intervals.of("2011-05-01/2011-05-10"), "4", makeSingle("4", 9))
    );
    assertValues(
        Collections.singletonList(
            createExpected("2011-05-01/2011-05-09", "5", 10)
        ),
        timeline.lookup(Intervals.of("2011-05-01/2011-05-09"))
    );
  }

  @Test
  public void testRemoveSomethingDontHave()
  {
    Assert.assertNull(
        "Don't have it, should be null",
        timeline.remove(Intervals.of("1970-01-01/2025-04-20"), "1", makeSingle("1", 1))
    );
    Assert.assertNull(
        "Don't have it, should be null",
        timeline.remove(
            Intervals.of("2011-04-01/2011-04-09"),
            "version does not exist",
            makeSingle("version does not exist", 1)
        )
    );
  }
}
