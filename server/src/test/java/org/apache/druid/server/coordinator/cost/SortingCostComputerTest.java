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

package org.apache.druid.server.coordinator.cost;

import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.CostBalancerStrategy;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class SortingCostComputerTest
{
  private static final Logger log = new Logger(SortingCostComputerTest.class);
  private static final Random RANDOM = new Random(894);
  private static final String DATA_SOURCE = "dataSource";
  private static final DateTime REFERENCE_TIME = DateTimes.of("2014-01-01T00:00:00");
  private static final double EPSILON = 0.00001;


  @Test
  public void notInCalculationIntervalCostTest()
  {
    Set<DataSegment> segmentSet = ImmutableSet.of(
        createSegment(DATA_SOURCE, shifted1HInterval(REFERENCE_TIME, 0), 100)
    );
    SortingCostComputer computer = new SortingCostComputer(segmentSet);
    Assert.assertEquals(
        0,
        computer.cost(createSegment(DATA_SOURCE, shifted1HInterval(REFERENCE_TIME, (int) TimeUnit.DAYS.toHours(50)), 100)),
        EPSILON
    );
  }

  @Test
  public void calculationIntervalTest()
  {
    DataSegment segmentA = createSegment(DATA_SOURCE, shifted1HInterval(REFERENCE_TIME, 0), 100);
    DataSegment segmentB = createSegment(
        DATA_SOURCE,
        shifted1HInterval(REFERENCE_TIME, (int) TimeUnit.DAYS.toHours(500)),
        100
    );

    SortingCostComputer.Bucket.Builder prototype = SortingCostComputer.Bucket.builder(
        new Interval(REFERENCE_TIME.minusHours(5), REFERENCE_TIME.plusHours(5))
    );
    prototype.addSegment(segmentA);
    SortingCostComputer.Bucket bucket = prototype.build();

    Assert.assertTrue(bucket.inCalculationInterval(segmentA));
    Assert.assertFalse(bucket.inCalculationInterval(segmentB));
  }

  @Test
  public void sameSegmentCostTest()
  {
    DataSegment segmentA = createSegment(DATA_SOURCE, shifted1HInterval(REFERENCE_TIME, 0), 100);
    DataSegment segmentB = createSegment(DATA_SOURCE, shifted1HInterval(REFERENCE_TIME, 0), 100);

    SegmentsCostCache.Bucket.Builder prototype = SegmentsCostCache.Bucket.builder(new Interval(
        REFERENCE_TIME.minusHours(5),
        REFERENCE_TIME.plusHours(5)
    ));

    prototype.addSegment(segmentA);
    SegmentsCostCache.Bucket bucket = prototype.build();

    double segmentCost = bucket.cost(segmentB);
    Assert.assertEquals(8.26147353873985E-4, segmentCost, EPSILON);
  }

  @Test
  public void perfComparisonTest()
  {
    final int n = 100000;

    Set<DataSegment> dataSegments = new HashSet<>();
    for (int i = 0; i < n; i++) {
      dataSegments.add(createSegment(DATA_SOURCE, shiftedRandomInterval(REFERENCE_TIME, 24 * RANDOM.nextInt(60)), 100));
    }

    DataSegment referenceSegment = createSegment("ANOTHER_DATA_SOURCE", shiftedRandomInterval(REFERENCE_TIME, 5), 100);

    long start;
    long end;

    start = System.currentTimeMillis();
    SortingCostComputer computer = new SortingCostComputer(dataSegments);
    end = System.currentTimeMillis();
    log.info("Precomputation time for " + n + " segments: " + (end - start) + " ms");

    start = System.currentTimeMillis();
    for (int i = 0; i < 1000; i++) {
      getExpectedCost(dataSegments, referenceSegment);
    }
    end = System.currentTimeMillis();
    log.info("Avg cost time: " + (end - start) + " us");

    start = System.currentTimeMillis();
    for (int i = 0; i < 1000; i++) {
      computer.cost(referenceSegment);
    }
    end = System.currentTimeMillis();
    log.info("Avg new cache cost time: " + (end - start) + " us");

    double expectedCost = getExpectedCost(dataSegments, referenceSegment);
    double cost = computer.cost(referenceSegment);

    Assert.assertEquals(1, expectedCost / cost, EPSILON);
  }

  @Test
  public void overallCorrectnessTest()
  {
    DataSegment referenceSegment = createSegment("ANOTHER_DATA_SOURCE", shifted1HInterval(REFERENCE_TIME, 5), 100);

    double expectedCost;
    double cost;
    Set<DataSegment> dataSegments = new HashSet<>();
    SortingCostComputer computer;

    // Same as reference interval
    for (int i = 0; i < 100; i++) {
      dataSegments.add(createSegment(DATA_SOURCE, shiftedXHInterval(REFERENCE_TIME, RANDOM.nextInt(20), 10), 100));
    }
    expectedCost = getExpectedCost(dataSegments, referenceSegment);
    computer = new SortingCostComputer(dataSegments);
    cost = computer.cost(referenceSegment);
    Assert.assertEquals(1, expectedCost / cost, EPSILON);


    // Overlapping intervals of larger size that enclose the reference interval
    for (int i = 0; i < 10; i++) {
      dataSegments.add(createSegment(DATA_SOURCE, shiftedXHInterval(REFERENCE_TIME, RANDOM.nextInt(40) - 70, 100), 100));
    }
    expectedCost = getExpectedCost(dataSegments, referenceSegment);
    computer = new SortingCostComputer(dataSegments);
    cost = computer.cost(referenceSegment);
    Assert.assertEquals(1, expectedCost / cost, EPSILON);

    // intervals of small size that are enclosed within the reference interval
    for (int i = 0; i < 10; i++) {
      dataSegments.add(createSegment(DATA_SOURCE, shiftedXHInterval(REFERENCE_TIME, RANDOM.nextInt(40) - 20, 1), 100));
    }
    expectedCost = getExpectedCost(dataSegments, referenceSegment);
    computer = new SortingCostComputer(dataSegments);
    cost = computer.cost(referenceSegment);
    Assert.assertEquals(1, expectedCost / cost, EPSILON);

    // intervals not intersecting, lying to its left
    for (int i = 0; i < 10; i++) {
      dataSegments.add(createSegment(DATA_SOURCE, shiftedRandomInterval(REFERENCE_TIME, -90), 100));
    }
    expectedCost = getExpectedCost(dataSegments, referenceSegment);
    computer = new SortingCostComputer(dataSegments);
    cost = computer.cost(referenceSegment);
    Assert.assertEquals(1, expectedCost / cost, EPSILON);

    // intervals not intersecting, lying to its right
    for (int i = 0; i < 10; i++) {
      dataSegments.add(createSegment(DATA_SOURCE, shiftedRandomInterval(REFERENCE_TIME, 60), 100));
    }
    expectedCost = getExpectedCost(dataSegments, referenceSegment);
    computer = new SortingCostComputer(dataSegments);
    cost = computer.cost(referenceSegment);
    Assert.assertEquals(1, expectedCost / cost, EPSILON);
  }

  @Test
  public void testLargeIntervals()
  {
    List<Interval> intervals = new ArrayList<>();
    // add ALL granularity buckets
    for (int i = 0; i < 50; i++) {
      intervals.add(new Interval(JodaUtils.MIN_INSTANT, JodaUtils.MAX_INSTANT));
    }
    // add random large intervals
    for (int i = 0; i < 150; i++) {
      int year = RANDOM.nextInt(6) - 10;
      intervals.add(new Interval(REFERENCE_TIME.plusYears(year),
                                 REFERENCE_TIME.plusYears(year + 1)));
    }
    // add random medium intervals
    for (int i = 0; i < 300; i++) {
      int week = RANDOM.nextInt(16) - 30;
      intervals.add(new Interval(REFERENCE_TIME.plusWeeks(week),
                                 REFERENCE_TIME.plusWeeks(week + 1)));
    }
    // add random small intervals
    for (int i = 0; i < 500; i++) {
      int hour = RANDOM.nextInt(16) - 30;
      intervals.add(new Interval(REFERENCE_TIME.plusHours(hour),
                                 REFERENCE_TIME.plusHours(hour + 1)));
    }

    Set<DataSegment> segments = intervals.stream()
                                          .map(interval -> createSegment(DATA_SOURCE, interval, 100))
                                          .collect(Collectors.toSet());
    SortingCostComputer computer = new SortingCostComputer(segments);

    List<DataSegment> referenceSegments = intervals.stream()
                                                   .map(interval -> createSegment("ANOTHER_DATA_SOURCE", interval, 100))
                                                   .collect(Collectors.toList());

    double expectedCost = 0.0;
    double cost = 0.0;
    for (DataSegment referenceSegment : referenceSegments) {
      expectedCost += getExpectedCost(segments, referenceSegment);
      cost += computer.cost(referenceSegment);
    }
    Assert.assertEquals(expectedCost + ", " + cost, 1, expectedCost / cost, 0.01);
  }

  // ( ) [ ]
  @Test
  public void toTheLeftTest()
  {
    DataSegment origin = createSegment(DATA_SOURCE, shiftedXHInterval(REFERENCE_TIME, 2, 2), 100);
    SortingCostComputer computer = new SortingCostComputer(ImmutableSet.of(origin));

    DataSegment segment = createSegment("blah", shiftedXHInterval(REFERENCE_TIME, -3, 2), 100);

    double cost = computer.cost(segment);
    double expectedCost = CostBalancerStrategy.computeJointSegmentsCost(origin, segment);
    Assert.assertEquals(expectedCost, cost, EPSILON);
  }

  // ( [ ) ]
  @Test
  public void leftOverlapTest()
  {
    DataSegment origin = createSegment(DATA_SOURCE, shiftedXHInterval(REFERENCE_TIME, 2, 2), 100);
    SortingCostComputer computer = new SortingCostComputer(ImmutableSet.of(origin));

    DataSegment segment = createSegment("blah", shiftedXHInterval(REFERENCE_TIME, 1, 2), 100);

    double cost = computer.cost(segment);
    double expectedCost = CostBalancerStrategy.computeJointSegmentsCost(origin, segment);
    Assert.assertEquals(expectedCost, cost, EPSILON);
  }

  // ( [ ] )
  @Test
  public void enclosedTest()
  {
    DataSegment origin = createSegment(DATA_SOURCE, shiftedXHInterval(REFERENCE_TIME, 2, 4), 100);
    SortingCostComputer computer = new SortingCostComputer(ImmutableSet.of(origin));

    DataSegment segment = createSegment("blah", shiftedXHInterval(REFERENCE_TIME, 3, 2), 100);

    double cost = computer.cost(segment);
    double expectedCost = CostBalancerStrategy.computeJointSegmentsCost(origin, segment);
    Assert.assertEquals(expectedCost, cost, EPSILON);
  }

  // [ ( ) ]
  @Test
  public void enclosesTest()
  {
    DataSegment origin = createSegment(DATA_SOURCE, shiftedXHInterval(REFERENCE_TIME, 2, 2), 100);
    SortingCostComputer computer = new SortingCostComputer(ImmutableSet.of(origin));

    DataSegment segment = createSegment("blah", shiftedXHInterval(REFERENCE_TIME, 1, 4), 100);

    double cost = computer.cost(segment);
    double expectedCost = CostBalancerStrategy.computeJointSegmentsCost(origin, segment);
    Assert.assertEquals(expectedCost, cost, EPSILON);
  }

  // [ ( ] )
  @Test
  public void rightOverlapTest()
  {
    DataSegment origin = createSegment(DATA_SOURCE, shiftedXHInterval(REFERENCE_TIME, 2, 2), 100);
    SortingCostComputer computer = new SortingCostComputer(ImmutableSet.of(origin));

    DataSegment segment = createSegment("blah", shiftedXHInterval(REFERENCE_TIME, 3, 2), 100);

    double cost = computer.cost(segment);
    double expectedCost = CostBalancerStrategy.computeJointSegmentsCost(origin, segment);
    Assert.assertEquals(expectedCost, cost, EPSILON);
  }

  // [ ] ( )
  @Test
  public void toTheRightTest()
  {
    DataSegment origin = createSegment(DATA_SOURCE, shiftedXHInterval(REFERENCE_TIME, 2, 2), 100);
    SortingCostComputer computer = new SortingCostComputer(ImmutableSet.of(origin));

    DataSegment segment = createSegment("blah", shiftedXHInterval(REFERENCE_TIME, 5, 2), 100);

    double cost = computer.cost(segment);
    double expectedCost = CostBalancerStrategy.computeJointSegmentsCost(origin, segment);
    Assert.assertEquals(expectedCost, cost, EPSILON);
  }

  private static double getExpectedCost(Collection<DataSegment> segments, DataSegment referenceSegment)
  {
    double cost = 0;
    for (DataSegment segment : segments) {
      cost += CostBalancerStrategy.computeJointSegmentsCost(segment, referenceSegment);
    }
    return cost;
  }

  private static Interval shiftedXHInterval(DateTime REFERENCE_TIME, int shiftInHours, int duration)
  {
    return new Interval(
        REFERENCE_TIME.plusHours(shiftInHours),
        REFERENCE_TIME.plusHours(shiftInHours + duration)
    );
  }

  private static Interval shifted1HInterval(DateTime REFERENCE_TIME, int shiftInHours)
  {
    return shiftedXHInterval(REFERENCE_TIME, shiftInHours, 1);
  }

  private static Interval shiftedRandomInterval(DateTime REFERENCE_TIME, int shiftInHours)
  {
    return new Interval(
        REFERENCE_TIME.plusHours(shiftInHours),
        REFERENCE_TIME.plusHours(shiftInHours + RANDOM.nextInt(100))
    );
  }

  private static DataSegment createSegment(String dataSource, Interval interval, long size)
  {
    return new DataSegment(
        dataSource,
        interval,
        UUID.randomUUID().toString(),
        new ConcurrentHashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        null,
        0,
        size
    );
  }
}
