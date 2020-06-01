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
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class CostBalancerStrategyBenchmark
{
  private static final DateTime T0 = DateTimes.of("2016-01-01T01:00:00Z");

  private List<DataSegment> segments;
  private DataSegment segment;

  int x1 = 2;
  int y0 = 3;
  int y1 = 4;

  int n = 10000;

  @Setup
  public void setupDummyCluster()
  {
    segment = createSegment(T0);

    Random r = ThreadLocalRandom.current();
    segments = new ArrayList<>(n);
    for (int i = 0; i < n; ++i) {
      final DateTime t = T0.minusHours(r.nextInt(365 * 24) - 365 * 12);
      segments.add(createSegment(t));
    }
  }

  DataSegment createSegment(DateTime t)
  {
    return new DataSegment(
        "test",
        new Interval(t, t.plusHours(1)),
        "v1",
        null,
        null,
        null,
        null,
        0,
        0
    );
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Fork(1)
  public double measureCostStrategySingle()
  {
    double totalCost = 0;
    for (DataSegment s : segments) {
      totalCost += CostBalancerStrategy.computeJointSegmentsCost(segment, s);
    }
    return totalCost;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Fork(1)
  public double measureIntervalPenalty()
  {
    return CostBalancerStrategy.intervalCost(x1, y0, y1);
  }
}
