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
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class BalancerStrategyBenchmark
{
  private static final Random RANDOM = new Random(0);
  private static final Interval TEST_SEGMENT_INTERVAL = Intervals.of("2012-03-15T00:00:00.000/2012-03-16T00:00:00.000");
  private static final int NUMBER_OF_SERVERS = 20;

  @Param({"default", "50percentOfSegmentsToConsiderPerMove", "useBatchedSegmentSampler"})
  private String mode;
  
  @Param({"10000", "100000", "1000000"})
  private int numberOfSegments;
  
  @Param({"10", "100", "1000"})
  private int maxSegmentsToMove;
  
  private final List<ServerHolder> serverHolders = new ArrayList<>();
  private boolean useBatchedSegmentSampler;
  private int reservoirSize = 1;
  private double percentOfSegmentsToConsider = 100;
  private final BalancerStrategy balancerStrategy = new CostBalancerStrategy(
      MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1))
  );

  @Setup(Level.Trial)
  public void setup()
  {
    switch (mode) {
      case "50percentOfSegmentsToConsiderPerMove":
        percentOfSegmentsToConsider = 50;
        useBatchedSegmentSampler = false;
        break;
      case "useBatchedSegmentSampler":
        reservoirSize = maxSegmentsToMove;
        useBatchedSegmentSampler = true;
        break;
      default:
    }
    
    List<List<DataSegment>> segmentList = new ArrayList<>(NUMBER_OF_SERVERS);
    IntStream.range(0, NUMBER_OF_SERVERS).forEach(i -> segmentList.add(new ArrayList<>()));
    for (int i = 0; i < numberOfSegments; i++) {
      segmentList.get(RANDOM.nextInt(NUMBER_OF_SERVERS)).add(
          new DataSegment(
              "test",
              TEST_SEGMENT_INTERVAL,
              String.valueOf(i),
              Collections.emptyMap(),
              Collections.emptyList(),
              Collections.emptyList(),
              null,
              0,
              10L
          )
      );
    }
    
    for (List<DataSegment> segments : segmentList) {
      serverHolders.add(
          new ServerHolder(
              new ImmutableDruidServer(
                  new DruidServerMetadata("id", "host", null, 10000000L, ServerType.HISTORICAL, "hot", 1),
                  3000L,
                  ImmutableMap.of("test", new ImmutableDruidDataSource("test", Collections.emptyMap(), segments)),
                  segments.size()
              ),
              new LoadQueuePeonTester()
          )
      );
    }
  }

  @Benchmark
  public void pickSegmentsToMove(Blackhole blackhole)
  {
    Iterator<BalancerSegmentHolder> iterator;
    if (useBatchedSegmentSampler) {
      iterator = balancerStrategy.pickSegmentsToMove(
          serverHolders,
          Collections.emptySet(),
          reservoirSize
      );
    } else {
      iterator = balancerStrategy.pickSegmentsToMove(
          serverHolders,
          Collections.emptySet(),
          percentOfSegmentsToConsider
      );
    }

    for (int i = 0; i < maxSegmentsToMove && iterator.hasNext(); i++) {
      blackhole.consume(iterator.next());
    }
  }
}
