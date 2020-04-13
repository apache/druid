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

package org.apache.druid.server.coordinator;

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.cost.SegmentsCostCache;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@Fork(1)
public class CachingCostBalancerStrategyBenchmark
{
  private static final Logger log = new Logger(CachingCostBalancerStrategyBenchmark.class);

  private static final int NUMBER_OF_SEGMENTS = 100000;
  private static final int NUMBER_OF_QUERIES = 500;

  private static final long DAYS_IN_MONTH = 30;

  private final DateTime referenceTime = DateTimes.of("2014-01-01T00:00:00");
  private final Set<DataSegment> segments = new HashSet<>();
  private final Set<DataSegment> segmentQueries = new HashSet<>();

  private SegmentsCostCache segmentsCostCache;

  @Setup
  public void createSegments()
  {
    Random random = ThreadLocalRandom.current();
    SegmentsCostCache.Builder prototype = SegmentsCostCache.builder();
    for (int i = 0; i < NUMBER_OF_SEGMENTS; ++i) {
      DataSegment segment = createSegment(random.nextInt((int) TimeUnit.DAYS.toHours(DAYS_IN_MONTH)));
      segments.add(segment);
    }
    segmentsCostCache = prototype.build();
    for (int i = 0; i < NUMBER_OF_QUERIES; ++i) {
      DataSegment segment = createSegment(random.nextInt((int) TimeUnit.DAYS.toHours(DAYS_IN_MONTH)));
      segmentQueries.add(segment);
    }
    for (DataSegment segment : segments) {
      prototype.addSegment(segment);
    }

    log.info("GENERATING SEGMENTS : %d / %d", NUMBER_OF_SEGMENTS, NUMBER_OF_QUERIES);
  }

  @Benchmark
  public double measureCostStrategy()
  {
    double cost = 0.0;
    for (DataSegment segment : segmentQueries) {
      cost += CostBalancerStrategy.computeJointSegmentsCost(segment, segments);
    }
    return cost;
  }

  @Benchmark
  public double measureCachingCostStrategy()
  {
    double cost = 0.0;
    for (DataSegment segment : segmentQueries) {
      cost += segmentsCostCache.cost(segment);
    }
    return cost;
  }

  private DataSegment createSegment(int shift)
  {
    return new DataSegment(
        "dataSource",
        new Interval(referenceTime.plusHours(shift), referenceTime.plusHours(shift).plusHours(1)),
        "version",
        Collections.emptyMap(),
        Collections.emptyList(),
        Collections.emptyList(),
        null,
        0,
        100
    );
  }

}
