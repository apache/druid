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

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.balancer.CostBalancerStrategy;
import org.apache.druid.server.coordinator.loading.TestLoadQueuePeon;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks {@link CostBalancerStrategy#computePlacementCost}, the per-server placement-cost computation the balancer
 * invokes for every candidate server when placing a segment. The cost of a single call scales with the number of
 * interval buckets the server's segments occupy, so {@code historyDays} (the span of daily intervals held by the
 * server) is the primary parameter to vary.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
public class ComputePlacementCostBenchmark
{
  private static final long DAY_MILLIS = TimeUnit.DAYS.toMillis(1);
  private static final long T0 = DateTimes.of("2026-01-01T00:00:00Z").getMillis();
  private static final String DATASOURCE = "ds0";

  /** Span of contiguous daily intervals held by the server. */
  @Param({"180", "730", "3650"})
  private int historyDays;

  private ListeningExecutorService exec;
  private ExposedCostBalancerStrategy strategy;
  private ServerHolder server;
  private DataSegment proposalSegment;

  @Setup(Level.Trial)
  public void setup()
  {
    exec = MoreExecutors.newDirectExecutorService();
    strategy = new ExposedCostBalancerStrategy(exec);

    final List<DataSegment> segments = new ArrayList<>(historyDays);
    for (int day = 0; day < historyDays; day++) {
      final long start = T0 - (long) (day + 1) * DAY_MILLIS;
      segments.add(createSegment(Intervals.utc(start, start + DAY_MILLIS)));
    }

    final ImmutableDruidServer immutableServer = new ImmutableDruidServer(
        new DruidServerMetadata("server", "host", null, 1L << 40, null, ServerType.HISTORICAL, "_default_tier", 0),
        0L,
        ImmutableMap.of(DATASOURCE, new ImmutableDruidDataSource(DATASOURCE, Collections.emptyMap(), segments)),
        segments.size()
    );
    server = new ServerHolder(immutableServer, new TestLoadQueuePeon());

    proposalSegment = createSegment(Intervals.utc(T0 - DAY_MILLIS, T0));
  }

  @TearDown(Level.Trial)
  public void tearDown()
  {
    exec.shutdownNow();
  }

  @Benchmark
  public double computePlacementCost()
  {
    return strategy.computePlacementCost(proposalSegment, server);
  }

  private static DataSegment createSegment(Interval interval)
  {
    return new DataSegment(
        DATASOURCE,
        interval,
        "v1",
        Collections.emptyMap(),
        Collections.emptyList(),
        Collections.emptyList(),
        null,
        0,
        1
    );
  }

  /**
   * Exposes the protected {@link CostBalancerStrategy#computePlacementCost} so the benchmark exercises the production
   * implementation directly.
   */
  private static class ExposedCostBalancerStrategy extends CostBalancerStrategy
  {
    ExposedCostBalancerStrategy(ListeningExecutorService exec)
    {
      super(exec);
    }

    @Override
    public double computePlacementCost(DataSegment proposalSegment, ServerHolder server)
    {
      return super.computePlacementCost(proposalSegment, server);
    }
  }
}
