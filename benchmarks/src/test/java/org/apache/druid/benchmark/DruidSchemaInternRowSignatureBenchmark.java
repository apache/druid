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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.druid.client.BrokerInternalQueryConfig;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.metadata.metadata.ColumnAnalysis;
import org.apache.druid.query.metadata.metadata.SegmentAnalysis;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.security.Escalator;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.SegmentMetadataCacheConfig;
import org.apache.druid.sql.calcite.schema.SegmentMetadataCache;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.easymock.EasyMock;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 10)
public class DruidSchemaInternRowSignatureBenchmark
{
  private SegmentMetadataCacheForBenchmark cache;

  private static class SegmentMetadataCacheForBenchmark extends SegmentMetadataCache
  {
    public SegmentMetadataCacheForBenchmark(
        final QueryLifecycleFactory queryLifecycleFactory,
        final TimelineServerView serverView,
        final SegmentManager segmentManager,
        final JoinableFactory joinableFactory,
        final PlannerConfig config,
        final Escalator escalator,
        final BrokerInternalQueryConfig brokerInternalQueryConfig
    )
    {
      super(
          queryLifecycleFactory,
          serverView,
          segmentManager,
          joinableFactory,
          SegmentMetadataCacheConfig.create(),
          escalator,
          brokerInternalQueryConfig
      );
    }

    // Overriding here so that it can be called explicitly to benchmark
    @Override
    public Set<SegmentId> refreshSegments(final Set<SegmentId> segments) throws IOException
    {
      return super.refreshSegments(segments);
    }

    @Override
    public void addSegment(final DruidServerMetadata server, final DataSegment segment)
    {
      super.addSegment(server, segment);
    }

    @Override
    protected Sequence<SegmentAnalysis> runSegmentMetadataQuery(Iterable<SegmentId> segments)
    {
      final int numColumns = 1000;
      LinkedHashMap<String, ColumnAnalysis> columnToAnalysisMap = new LinkedHashMap<>();
      for (int i = 0; i < numColumns; ++i) {
        columnToAnalysisMap.put(
            "col" + i,
            new ColumnAnalysis(
                ColumnType.STRING,
                null,
                false,
                false,
                40,
                null,
                null,
                null,
                null
            )
        );
      }
      return Sequences.simple(
          Lists.transform(
              Lists.newArrayList(segments),
              (segment) -> new SegmentAnalysis(
                  segment.toString(),
                  ImmutableList.of(segment.getInterval()),
                  columnToAnalysisMap,
                  40,
                  40,
                  null,
                  null,
                  null,
                  false
              )
          )
      );
    }
  }

  @State(Scope.Thread)
  public static class MyState
  {
    Set<SegmentId> segmentIds;

    @Setup(Level.Iteration)
    public void setup()
    {
      ImmutableSet.Builder<SegmentId> segmentIdsBuilder = ImmutableSet.builder();
      for (int i = 0; i < 10000; ++i) {
        segmentIdsBuilder.add(SegmentId.of("dummy", Intervals.of(i + "/" + (i + 1)), "1", new LinearShardSpec(0)));
      }
      segmentIds = segmentIdsBuilder.build();
    }

    @TearDown(Level.Iteration)
    public void teardown()
    {
      segmentIds = null;
    }
  }

  @Setup
  public void setup()
  {
    cache = new SegmentMetadataCacheForBenchmark(
        EasyMock.mock(QueryLifecycleFactory.class),
        EasyMock.mock(TimelineServerView.class),
        null,
        null,
        EasyMock.mock(PlannerConfig.class),
        null,
        null
    );
    DruidServerMetadata serverMetadata = new DruidServerMetadata(
        "dummy",
        "dummy",
        "dummy",
        42,
        ServerType.HISTORICAL,
        "tier-0",
        0
    );

    DataSegment.Builder builder = DataSegment.builder()
                                             .dataSource("dummy")
                                             .shardSpec(new LinearShardSpec(0))
                                             .dimensions(ImmutableList.of("col1", "col2", "col3", "col4"))
                                             .version("1")
                                             .size(0);
    for (int i = 0; i < 10000; ++i) {
      DataSegment dataSegment = builder.interval(Intervals.of(i + "/" + (i + 1)))
                                       .build();
      cache.addSegment(serverMetadata, dataSegment);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void addSegments(MyState state, Blackhole blackhole) throws IOException
  {
    blackhole.consume(cache.refreshSegments(state.segmentIds));
  }
}
