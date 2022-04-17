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

import static org.apache.druid.server.coordinator.cost.SegmentsCostCache.NORMALIZATION_FACTOR;

public class SegmentsCostCacheTest
{

  private static final Random RANDOM = new Random(23894);
  private static final String DATA_SOURCE = "dataSource";
  private static DateTime REFERENCE_TIME = DateTimes.of("2014-01-01T00:00:00");
  private static final double EPSILON = 0.0000001;

  @Test
  public void notInCalculationIntervalCostTest()
  {
    SegmentsCostCache.Builder cacheBuilder = SegmentsCostCache.builder();
    cacheBuilder.addSegment(
        createSegment(DATA_SOURCE, shifted1HInterval(REFERENCE_TIME, 0), 100)
    );
    SegmentsCostCache cache = cacheBuilder.build();
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

    SegmentsCostCache.Bucket.Builder prototype = SegmentsCostCache.Bucket.builder(new Interval(
        REFERENCE_TIME.minusHours(5),
        REFERENCE_TIME.plusHours(5)
    ));

    prototype.addSegment(segmentA);
    SegmentsCostCache.Bucket bucket = prototype.build();

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

    SegmentsCostCache.Bucket.Builder prototype = SegmentsCostCache.Bucket.builder(
        new Interval(REFERENCE_TIME.minusHours(5), REFERENCE_TIME.plusHours(5))
    );
    prototype.addSegment(segmentA);
    SegmentsCostCache.Bucket bucket = prototype.build();

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
  public void multipleSegmentsCostTest()
  {
    DataSegment segmentA = createSegment(DATA_SOURCE, shifted1HInterval(REFERENCE_TIME, -2), 100);
    DataSegment segmentB = createSegment(DATA_SOURCE, shifted1HInterval(REFERENCE_TIME, 0), 100);
    DataSegment segmentC = createSegment(DATA_SOURCE, shifted1HInterval(REFERENCE_TIME, 2), 100);

    SegmentsCostCache.Bucket.Builder prototype = SegmentsCostCache.Bucket.builder(new Interval(
        REFERENCE_TIME.minusHours(5),
        REFERENCE_TIME.plusHours(5)
    ));

    prototype.addSegment(segmentA);
    prototype.addSegment(segmentC);
    SegmentsCostCache.Bucket bucket = prototype.build();

    double segmentCost = bucket.cost(segmentB);

    Assert.assertEquals(0.001574717989780039, segmentCost, EPSILON);
  }

  @Test
  public void perfComparisonTest()
  {
    final int n = 100000;

    List<DataSegment> dataSegments = new ArrayList<>(1000);
    for (int i = 0; i < n; ++i) {
      dataSegments.add(createSegment(DATA_SOURCE, shiftedRandomInterval(REFERENCE_TIME, 24 * RANDOM.nextInt(60)), 100));
    }

    DataSegment referenceSegment = createSegment("ANOTHER_DATA_SOURCE", shiftedRandomInterval(REFERENCE_TIME, 5), 100);

    SegmentsCostCache.Builder prototype = new SegmentsCostCache.Builder();

    long start;
    long end;

    start = System.currentTimeMillis();

    dataSegments.forEach(prototype::addSegment);
    SegmentsCostCache cache = prototype.build();

    end = System.currentTimeMillis();
    System.out.println("Insertion time for " + n + " segments: " + (end - start) + " ms");

    start = System.currentTimeMillis();

    double origCost = 0;
    for (DataSegment segment : dataSegments) {
      origCost += CostBalancerStrategy.computeJointSegmentsCost(segment, referenceSegment);
    }

    end = System.currentTimeMillis();
    System.out.println("Avg cost time: " + ((end - start) * 1000) + " us");

    start = System.currentTimeMillis();

    for (int i = 0; i < 1000; i++) {
      cache.cost(referenceSegment);
    }

    end = System.currentTimeMillis();
    System.out.println("Avg cache cost time: " + (end - start) + " us");

    double cost = cache.cost(referenceSegment);

    Assert.assertEquals(1, origCost / cost, EPSILON);
  }

  @Test
  public void bucketCorrectnessTest()
  {
    List<DataSegment> dataSegments = new ArrayList<>();

    // Same as reference interval
    for (int i = 0; i < 100; i++) {
      dataSegments.add(createSegment(DATA_SOURCE, shiftedXHInterval(REFERENCE_TIME, RANDOM.nextInt(20), 10), 100));
    }

    // Overlapping intervals of larger size that enclose the reference interval
    for (int i = 0; i < 10; i++) {
      dataSegments.add(createSegment(DATA_SOURCE, shiftedXHInterval(REFERENCE_TIME, RANDOM.nextInt(40) - 70, 100), 100));
    }

    // intervals of small size that are enclosed within the reference interval
    for (int i = 0; i < 10; i++) {
      dataSegments.add(createSegment(DATA_SOURCE, shiftedXHInterval(REFERENCE_TIME, RANDOM.nextInt(40) - 20, 1), 100));
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

    SegmentsCostCache.Bucket.Builder prototype = SegmentsCostCache.Bucket.builder(new Interval(
        REFERENCE_TIME.minusHours(90), REFERENCE_TIME.plusHours(90)
    ));

    dataSegments.forEach(prototype::addSegment);
    SegmentsCostCache.Bucket bucket = prototype.build();

    double origCost = 0;
    for (DataSegment segment : dataSegments) {
      origCost += CostBalancerStrategy.computeJointSegmentsCost(segment, referenceSegment);
    }

    double cost = bucket.cost(referenceSegment);

    Assert.assertEquals(NORMALIZATION_FACTOR, origCost / cost, EPSILON);
  }

  @Test
  public void overallCorrectnessTest()
  {
    List<DataSegment> dataSegments = new ArrayList<>();

    // Same as reference interval
    for (int i = 0; i < 100; i++) {
      dataSegments.add(createSegment(DATA_SOURCE, shiftedXHInterval(REFERENCE_TIME, RANDOM.nextInt(20), 10), 100));
    }

    // Overlapping intervals of larger size that enclose the reference interval
    for (int i = 0; i < 10; i++) {
      dataSegments.add(createSegment(DATA_SOURCE, shiftedXHInterval(REFERENCE_TIME, RANDOM.nextInt(40) - 70, 100), 100));
    }

    // intervals of small size that are enclosed within the reference interval
    for (int i = 0; i < 10; i++) {
      dataSegments.add(createSegment(DATA_SOURCE, shiftedXHInterval(REFERENCE_TIME, RANDOM.nextInt(40) - 20, 1), 100));
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

    SegmentsCostCache.Builder prototype = new SegmentsCostCache.Builder();

    dataSegments.forEach(prototype::addSegment);
    SegmentsCostCache cache = prototype.build();

    double origCost = 0;
    for (DataSegment segment : dataSegments) {
      origCost += CostBalancerStrategy.computeJointSegmentsCost(segment, referenceSegment);
    }

    double cost = cache.cost(referenceSegment);

    Assert.assertEquals(1, origCost / cost, EPSILON);
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
        REFERENCE_TIME.plusHours(shiftInHours + RANDOM.nextInt(1000))
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
