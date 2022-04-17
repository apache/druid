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

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.server.coordinator.CostBalancerStrategy;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.druid.server.coordinator.cost.SegmentsCostCacheV3.NORMALIZATION_FACTOR;

public class SegmentsCostCacheV3Test
{

  private static final Random random = new Random(23894);
  private static final String DATA_SOURCE = "dataSource";
  private static final DateTime REFERENCE_TIME = DateTimes.of("2014-01-01T00:00:00");
  private static final double EPSILON = 0.0000001;

  @Test
  public void notInCalculationIntervalCostTest()
  {
    SegmentsCostCacheV3.Builder cacheBuilder = SegmentsCostCacheV3.builder();
    cacheBuilder.addSegment(
        createSegment(DATA_SOURCE, shifted1HInterval(REFERENCE_TIME, 0), 100)
    );
    SegmentsCostCacheV3 cache = cacheBuilder.build();
    Assert.assertEquals(
        0,
        cache.cost(createSegment(DATA_SOURCE, shifted1HInterval(REFERENCE_TIME, (int) TimeUnit.DAYS.toHours(50)), 100)),
        EPSILON
    );
  }

  @Test
  public void twoSegmentsCostTest()
  {
    DataSegment segmentA = createSegment(DATA_SOURCE, shifted1HInterval(REFERENCE_TIME, 0), 100);
    DataSegment segmentB = createSegment(DATA_SOURCE, shifted1HInterval(REFERENCE_TIME, -2), 100);

    SegmentsCostCacheV3.Bucket.Builder prototype = SegmentsCostCacheV3.Bucket.builder(new Interval(
        REFERENCE_TIME.minusHours(5),
        REFERENCE_TIME.plusHours(5)
    ));

    prototype.addSegment(segmentA);
    SegmentsCostCacheV3.Bucket bucket = prototype.build();

    double segmentCost = bucket.cost(segmentB);
    Assert.assertEquals(7.8735899489011E-4, segmentCost, EPSILON);
  }

  @Test
  public void calculationIntervalTest()
  {
    DataSegment segmentA = createSegment(DATA_SOURCE, shifted1HInterval(REFERENCE_TIME, 0), 100);
    DataSegment segmentB = createSegment(
        DATA_SOURCE,
        shifted1HInterval(REFERENCE_TIME, (int) TimeUnit.DAYS.toHours(50)),
        100
    );

    SegmentsCostCacheV3.Bucket.Builder prototype = SegmentsCostCacheV3.Bucket.builder(
        new Interval(REFERENCE_TIME.minusHours(5), REFERENCE_TIME.plusHours(5))
    );
    prototype.addSegment(segmentA);
    SegmentsCostCacheV3.Bucket bucket = prototype.build();

    Assert.assertTrue(bucket.inCalculationInterval(segmentA));
    Assert.assertFalse(bucket.inCalculationInterval(segmentB));
  }

  @Test
  public void sameSegmentCostTest()
  {
    DataSegment segmentA = createSegment(DATA_SOURCE, shifted1HInterval(REFERENCE_TIME, 0), 100);
    DataSegment segmentB = createSegment(DATA_SOURCE, shifted1HInterval(REFERENCE_TIME, 0), 100);

    SegmentsCostCacheV3.Bucket.Builder prototype = SegmentsCostCacheV3.Bucket.builder(new Interval(
        REFERENCE_TIME.minusHours(5),
        REFERENCE_TIME.plusHours(5)
    ));

    prototype.addSegment(segmentA);
    SegmentsCostCacheV3.Bucket bucket = prototype.build();

    double segmentCost = bucket.cost(segmentB);
    Assert.assertEquals(8.26147353873985E-4, segmentCost, EPSILON);
  }

  @Test
  public void multipleSegmentsCostTest()
  {
    DataSegment segmentA = createSegment(DATA_SOURCE, shifted1HInterval(REFERENCE_TIME, -2), 100);
    DataSegment segmentB = createSegment(DATA_SOURCE, shifted1HInterval(REFERENCE_TIME, 0), 100);
    DataSegment segmentC = createSegment(DATA_SOURCE, shifted1HInterval(REFERENCE_TIME, 2), 100);

    SegmentsCostCacheV3.Bucket.Builder prototype = SegmentsCostCacheV3.Bucket.builder(new Interval(
        REFERENCE_TIME.minusHours(5),
        REFERENCE_TIME.plusHours(5)
    ));

    prototype.addSegment(segmentA);
    prototype.addSegment(segmentC);
    SegmentsCostCacheV3.Bucket bucket = prototype.build();

    double segmentCost = bucket.cost(segmentB);

    Assert.assertEquals(0.001574717989780039, segmentCost, EPSILON);
  }

  @Test
  public void perfComparisonTest()
  {
    final int N = 100000;

    List<DataSegment> dataSegments = new ArrayList<>(1000);
    for (int i = 0; i < N; ++i) {
      dataSegments.add(createSegment(DATA_SOURCE, shiftedRandomInterval(REFERENCE_TIME, 24 * random.nextInt(60)), 100));
    }

    DataSegment referenceSegment = createSegment("ANOTHER_DATA_SOURCE", shiftedRandomInterval(REFERENCE_TIME, 5), 100);

    SegmentsCostCacheV3.Builder prototype = new SegmentsCostCacheV3.Builder();

    long start;
    long end;

    start = System.currentTimeMillis();
    dataSegments.forEach(prototype::addSegment);
    SegmentsCostCacheV3 cache = prototype.build();
    end = System.currentTimeMillis();
    System.out.println("Insertion time for " + N + " segments: " + (end - start) + " ms");

    start = System.currentTimeMillis();
    for (int i = 0; i < 1000; i++) {
      getExpectedCost(dataSegments, referenceSegment);
    }
    end = System.currentTimeMillis();
    System.out.println("Avg cost time: " + (end - start) + " us");

    start = System.currentTimeMillis();
    for (int i = 0; i < 1000; i++) {
      cache.cost(referenceSegment);
    }
    end = System.currentTimeMillis();
    System.out.println("Avg new cache cost time: " + (end - start) + " us");

    double expectedCost = getExpectedCost(dataSegments, referenceSegment);
    double cost = cache.cost(referenceSegment);

    Assert.assertEquals(1, expectedCost / cost, EPSILON);
  }

  @Test
  public void bucketCorrectnessTest()
  {
    List<DataSegment> dataSegments = new ArrayList<>();

    // Same as reference interval
    for (int i = 0; i < 100; i++) {
      dataSegments.add(createSegment(DATA_SOURCE, shiftedXHInterval(REFERENCE_TIME, random.nextInt(20), 10), 100));
    }

    // Overlapping intervals of larger size that enclose the reference interval
    for (int i = 0; i < 10; i++) {
      dataSegments.add(createSegment(DATA_SOURCE, shiftedXHInterval(REFERENCE_TIME, random.nextInt(40) - 70, 100), 100));
    }

    // intervals of small size that are enclosed within the reference interval
    for (int i = 0; i < 10; i++) {
      dataSegments.add(createSegment(DATA_SOURCE, shiftedXHInterval(REFERENCE_TIME, random.nextInt(40) - 20, 1), 100));
    }

    // intervals not intersecting, lying to its left
    for (int i = 0; i < 10; i++) {
      dataSegments.add(createSegment(DATA_SOURCE, shiftedRandomInterval(REFERENCE_TIME, -90), 100));
    }

    // intervals not intersecting, lying to its right
    for (int i = 0; i < 10; i++) {
      dataSegments.add(createSegment(DATA_SOURCE, shiftedRandomInterval(REFERENCE_TIME, 60), 100));
    }

    DataSegment referenceSegment = createSegment("ANOTHER_DATA_SOURCE", shifted1HInterval(REFERENCE_TIME, 5), 100);

    SegmentsCostCacheV3.Bucket.Builder prototype = SegmentsCostCacheV3.Bucket.builder(new Interval(
        REFERENCE_TIME.minusHours(90), REFERENCE_TIME.plusHours(90)
    ));

    dataSegments.forEach(prototype::addSegment);
    SegmentsCostCacheV3.Bucket bucket = prototype.build();

    double expectedCost = getExpectedCost(dataSegments, referenceSegment);

    double cost = bucket.cost(referenceSegment);

    Assert.assertEquals(NORMALIZATION_FACTOR, expectedCost / cost, EPSILON);
  }

  @Test
  public void overallCorrectnessTest()
  {
    List<DataSegment> dataSegments = new ArrayList<>();

    // Same as reference interval
    for (int i = 0; i < 100; i++) {
      dataSegments.add(createSegment(DATA_SOURCE, shiftedXHInterval(REFERENCE_TIME, random.nextInt(20), 10), 100));
    }

    // Overlapping intervals of larger size that enclose the reference interval
    for (int i = 0; i < 10; i++) {
      dataSegments.add(createSegment(DATA_SOURCE, shiftedXHInterval(REFERENCE_TIME, random.nextInt(40) - 70, 100), 100));
    }

    // intervals of small size that are enclosed within the reference interval
    for (int i = 0; i < 10; i++) {
      dataSegments.add(createSegment(DATA_SOURCE, shiftedXHInterval(REFERENCE_TIME, random.nextInt(40) - 20, 1), 100));
    }

    // intervals not intersecting, lying to its left
    for (int i = 0; i < 10; i++) {
      dataSegments.add(createSegment(DATA_SOURCE, shiftedRandomInterval(REFERENCE_TIME, -90), 100));
    }

    // intervals not intersecting, lying to its right
    for (int i = 0; i < 10; i++) {
      dataSegments.add(createSegment(DATA_SOURCE, shiftedRandomInterval(REFERENCE_TIME, 60), 100));
    }

    DataSegment referenceSegment = createSegment("ANOTHER_DATA_SOURCE", shifted1HInterval(REFERENCE_TIME, 5), 100);

    SegmentsCostCacheV3.Builder prototype = new SegmentsCostCacheV3.Builder();

    double expectedCost = getExpectedCost(dataSegments, referenceSegment);

    dataSegments.forEach(prototype::addSegment);
    SegmentsCostCacheV3 cache = prototype.build();
    double cost = cache.cost(referenceSegment);

    Assert.assertEquals(1, expectedCost / cost, EPSILON);
  }

  @Test
  public void testLargeIntervals()
  {
    List<Interval> intervals = new ArrayList<>();
    // add ALL granularity buckets
    for (int i = 0; i < 5; i++) {
      intervals.add(new Interval(JodaUtils.MIN_INSTANT, JodaUtils.MAX_INSTANT));
    }
    // add random large intervals
    for (int i = 0; i < 15; i++) {
      intervals.add(new Interval(REFERENCE_TIME.minusYears(random.nextInt(30)),
                                 REFERENCE_TIME.plusYears(random.nextInt(30))));
    }
    // add random medium intervals
    for (int i = 0; i < 30; i++) {
      intervals.add(new Interval(REFERENCE_TIME.minusWeeks(random.nextInt(30)),
                                 REFERENCE_TIME.plusWeeks(random.nextInt(30))));
    }
    // add random small intervals
    for (int i = 0; i < 50; i++) {
      intervals.add(new Interval(REFERENCE_TIME.minusHours(random.nextInt(30)),
                                 REFERENCE_TIME.plusHours(random.nextInt(30))));
    }

    List<DataSegment> segments = intervals.stream()
                                          .map(interval -> createSegment(DATA_SOURCE, interval, 100))
                                          .collect(Collectors.toList());
    List<DataSegment> referenceSegments = intervals.stream()
                                                   .map(interval -> createSegment("ANOTHER_DATA_SOURCE", interval, 100))
                                                   .collect(Collectors.toList());

    for (DataSegment segment : segments) {
      for (DataSegment referenceSegment : referenceSegments) {
        SegmentsCostCacheV3.Builder builder = SegmentsCostCacheV3.builder();
        builder.addSegment(segment);
        SegmentsCostCacheV3 cache = builder.build();

        double expectedCost = CostBalancerStrategy.computeJointSegmentsCost(segment, referenceSegment);
        double cost = cache.cost(referenceSegment);
        Assert.assertEquals(1, expectedCost / cost, 0.0001);
      }
    }

    SegmentsCostCacheV3.Builder builder = SegmentsCostCacheV3.builder();
    segments.forEach(builder::addSegment);
    SegmentsCostCacheV3 cache = builder.build();
    for (DataSegment referenceSegment : referenceSegments) {
      double expectedCost = getExpectedCost(segments, referenceSegment);
      double cost = cache.cost(referenceSegment);
      Assert.assertEquals(1 , expectedCost / cost, 0.01);
    }
  }

  // ( ) [ ]
  @Test
  public void leftOfBucketTest()
  {
    DataSegment origin = createSegment(DATA_SOURCE, shiftedXHInterval(REFERENCE_TIME, 0, 2), 100);
    SegmentsCostCacheV3.Builder builder = SegmentsCostCacheV3.builder();
    builder.addSegment(origin);
    SegmentsCostCacheV3 cache = builder.build();

    DataSegment segment = createSegment("blah", shiftedXHInterval(REFERENCE_TIME, -3, 2), 100);

    double cost = cache.cost(segment);
    double expectedCost = CostBalancerStrategy.computeJointSegmentsCost(origin, segment);
    Assert.assertEquals(cost, expectedCost, EPSILON);
  }

  // ( [ ) ]
  @Test
  public void leftOverlapWithBucketTest()
  {
    DataSegment origin = createSegment(DATA_SOURCE, shiftedXHInterval(REFERENCE_TIME, 0, 2), 100);
    SegmentsCostCacheV3.Builder builder = SegmentsCostCacheV3.builder();
    builder.addSegment(origin);
    SegmentsCostCacheV3 cache = builder.build();

    DataSegment segment = createSegment("blah", shiftedXHInterval(REFERENCE_TIME, -1, 2), 100);

    double cost = cache.cost(segment);
    double expectedCost = CostBalancerStrategy.computeJointSegmentsCost(origin, segment);
    Assert.assertEquals(cost, expectedCost, EPSILON);
  }

  // ( [ ] )
  @Test
  public void enclosedByBucketTest()
  {
    DataSegment origin = createSegment(DATA_SOURCE, shiftedXHInterval(REFERENCE_TIME, 0, 4), 100);
    SegmentsCostCacheV3.Builder builder = SegmentsCostCacheV3.builder();
    builder.addSegment(origin);
    SegmentsCostCacheV3 cache = builder.build();

    DataSegment segment = createSegment("blah", shiftedXHInterval(REFERENCE_TIME, 1, 2), 100);

    double cost = cache.cost(segment);
    double expectedCost = CostBalancerStrategy.computeJointSegmentsCost(origin, segment);
    Assert.assertEquals(cost, expectedCost, EPSILON);
  }

  // [ ( ) ]
  @Test
  public void enclosesBucketTest()
  {
    DataSegment origin = createSegment(DATA_SOURCE, shiftedXHInterval(REFERENCE_TIME, 0, 2), 100);
    SegmentsCostCacheV3.Builder builder = SegmentsCostCacheV3.builder();
    builder.addSegment(origin);
    SegmentsCostCacheV3 cache = builder.build();

    DataSegment segment = createSegment("blah", shiftedXHInterval(REFERENCE_TIME, -1, 4), 100);

    double cost = cache.cost(segment);
    double expectedCost = CostBalancerStrategy.computeJointSegmentsCost(origin, segment);
    Assert.assertEquals(cost, expectedCost, EPSILON);
  }

  // [ ( ] )
  @Test
  public void rightOverlapWithBucketTest()
  {
    DataSegment origin = createSegment(DATA_SOURCE, shiftedXHInterval(REFERENCE_TIME, 0, 2), 100);
    SegmentsCostCacheV3.Builder builder = SegmentsCostCacheV3.builder();
    builder.addSegment(origin);
    SegmentsCostCacheV3 cache = builder.build();

    DataSegment segment = createSegment("blah", shiftedXHInterval(REFERENCE_TIME, 1, 2), 100);

    double cost = cache.cost(segment);
    double expectedCost = CostBalancerStrategy.computeJointSegmentsCost(origin, segment);
    Assert.assertEquals(cost, expectedCost, EPSILON);
  }

  // [ ] ( )
  @Test
  public void rightOfBucketTest()
  {
    DataSegment origin = createSegment(DATA_SOURCE, shiftedXHInterval(REFERENCE_TIME, 0, 2), 100);
    SegmentsCostCacheV3.Builder builder = SegmentsCostCacheV3.builder();
    builder.addSegment(origin);
    SegmentsCostCacheV3 cache = builder.build();

    DataSegment segment = createSegment("blah", shiftedXHInterval(REFERENCE_TIME, 3, 2), 100);

    double cost = cache.cost(segment);
    double expectedCost = CostBalancerStrategy.computeJointSegmentsCost(origin, segment);
    Assert.assertEquals(cost, expectedCost, EPSILON);
  }

  private static double getExpectedCost(List<DataSegment> segments, DataSegment referenceSegment)
  {
    double cost = 0;
    for (DataSegment segment : segments) {
      cost += CostBalancerStrategy.computeJointSegmentsCost(segment, referenceSegment);
    }
    return cost;
  }

  private static Interval shiftedXHInterval(DateTime REFERENCE_TIME, int shiftInHours, int X)
  {
    return new Interval(
        REFERENCE_TIME.plusHours(shiftInHours),
        REFERENCE_TIME.plusHours(shiftInHours + X)
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
        REFERENCE_TIME.plusHours(shiftInHours + random.nextInt(100))
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
