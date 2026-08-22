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

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.client.DruidServer;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.balancer.CostBalancerStrategy;
import org.apache.druid.server.coordinator.loading.TestLoadQueuePeon;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@Fork(1)
public class CostBalancerStrategyBenchmark
{
  private static final DateTime T0 = DateTimes.of("2016-01-01T01:00:00Z");
  private static final Interval INTERVAL1 = Intervals.of("2015-01-01T01:00:00Z/2015-01-01T02:00:00Z");
  private static final Interval INTERVAL2 = Intervals.of("2015-02-01T01:00:00Z/2015-02-01T02:00:00Z");
  private static final int NUMBER_OF_SEGMENTS = 10000;
  private static final int NUMBER_OF_SERVERS = 6;
  private static final int X1 = 2;
  private static final int Y0 = 3;
  private static final int Y1 = 4;

  private List<DataSegment> segments;
  private DataSegment segment;

  @Setup(Level.Trial)
  public void setup()
  {
    segment = createSegment(T0);

    Random r = ThreadLocalRandom.current();
    segments = new ArrayList<>(NUMBER_OF_SEGMENTS);
    for (int i = 0; i < NUMBER_OF_SEGMENTS; ++i) {
      final DateTime t = T0.minusHours(r.nextInt(365 * 24) - 365 * 12);
      segments.add(createSegment(t));
    }
  }

  @State(Scope.Benchmark)
  public static class PlacementState
  {
    @Param({"1", "4"})
    private int numThreads;

    private ListeningExecutorService exec;
    private CostBalancerStrategy strategy;
    private List<ServerHolder> serverHolderList;
    private DataSegment segmentToLoad;

    @Setup(Level.Trial)
    public void setup()
    {
      exec = MoreExecutors.listeningDecorator(Execs.multiThreaded(numThreads, "CostBalancerStrategyBenchmark-%d"));
      strategy = new CostBalancerStrategy(exec);
      serverHolderList = initServers();
      segmentToLoad = DataSegment.builder(SegmentId.of("testds", INTERVAL1, "1000", 0))
                                 .size(100L)
                                 .build();
    }

    @TearDown(Level.Trial)
    public void tearDown()
    {
      exec.shutdownNow();
    }

    private List<ServerHolder> initServers()
    {
      final List<DruidServer> servers = new ArrayList<>();
      for (int i = 0; i < NUMBER_OF_SERVERS; ++i) {
        servers.add(
            new DruidServer(
                "server_" + i,
                "localhost",
                null,
                10_000_000L,
                null,
                ServerType.HISTORICAL,
                "hot",
                1
            )
        );
      }

      final List<DataSegment> serverSegments =
          CreateDataSegments.ofDatasource("wikipedia")
                            .forIntervals(200, Granularities.DAY)
                            .withNumPartitions(100)
                            .eachOfSizeInMb(200);
      final Random random = new Random(100);
      serverSegments.forEach(segment -> servers.get(random.nextInt(servers.size())).addDataSegment(segment));

      return servers.stream()
                    .map(DruidServer::toImmutableDruidServer)
                    .map(server -> new ServerHolder(server, new TestLoadQueuePeon()))
                    .collect(Collectors.toList());
    }
  }

  private static DataSegment createSegment(DateTime t)
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
  public double measureCostStrategySingle()
  {
    double totalCost = 0;
    for (DataSegment s : segments) {
      totalCost += CostBalancerStrategy.computeJointSegmentsCost(segment, s);
    }
    return totalCost;
  }

  @Benchmark
  public double measureIntervalPenalty()
  {
    return CostBalancerStrategy.intervalCost(X1, Y0, Y1);
  }

  @Benchmark
  public ServerHolder measureFindServersToLoadSegment(PlacementState state)
  {
    final Iterator<ServerHolder> candidates = state.strategy.findServersToLoadSegment(
        state.segmentToLoad,
        state.serverHolderList
    );
    return candidates.hasNext() ? candidates.next() : null;
  }

  @Benchmark
  @OperationsPerInvocation(1000)
  public long measureJodaGap()
  {
    long diff = 0;
    for (int i = 0; i < 1000; i++) {
      diff += INTERVAL1.gap(INTERVAL2).toDurationMillis();
    }
    return diff;
  }
}
