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

package org.apache.druid.benchmark;

import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.joda.time.Interval;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(1)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 10)
@Measurement(iterations = 50)
public class DataSourcesSnapshotBenchmark
{
  private static Interval TEST_SEGMENT_INTERVAL = Intervals.of("2012-03-15T00:00:00.000/2012-03-16T00:00:00.000");

  @Param({"500", "1000"})
  private int numDataSources;

  @Param({"1000", "2000"})
  private int numSegmentPerDataSource;

  private DataSourcesSnapshot snapshot;

  @Setup
  public void setUp()
  {
    long start = System.currentTimeMillis();

    Map<String, ImmutableDruidDataSource> dataSources = new HashMap<>();

    for (int i = 0; i < numDataSources; i++) {
      String dataSource = StringUtils.format("ds-%d", i);
      List<DataSegment> segments = new ArrayList<>();

      for (int j = 0; j < numSegmentPerDataSource; j++) {
        segments.add(
            new DataSegment(
                dataSource,
                TEST_SEGMENT_INTERVAL,
                String.valueOf(j),
                Collections.emptyMap(),
                Collections.emptyList(),
                Collections.emptyList(),
                NoneShardSpec.instance(),
                0,
                10L
            )
        );
      }

      dataSources.put(dataSource, new ImmutableDruidDataSource(dataSource, Collections.emptyMap(), segments));
    }

    snapshot = new DataSourcesSnapshot(dataSources);

    System.out.println("Setup Time " + (System.currentTimeMillis() - start) + " ms");
  }

  @Benchmark
  public void iterateUsing_iterateAllUsedSegmentsInSnapshot(Blackhole blackhole)
  {
    long totalSize = 0;
    for (DataSegment segment : snapshot.iterateAllUsedSegmentsInSnapshot()) {
      totalSize += segment.getSize();
    }
    blackhole.consume(totalSize);
  }

  @Benchmark
  public void iterateUsing_forloops(Blackhole blackhole)
  {
    long totalSize = 0;
    for (ImmutableDruidDataSource dataSource : snapshot.getDataSourcesWithAllUsedSegments()) {
      for (DataSegment segment : dataSource.getSegments()) {
        totalSize += segment.getSize();
      }
    }
    blackhole.consume(totalSize);
  }
}

