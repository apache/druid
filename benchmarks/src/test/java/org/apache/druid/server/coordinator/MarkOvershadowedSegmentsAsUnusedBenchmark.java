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
import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.balancer.RandomBalancerStrategy;
import org.apache.druid.server.coordinator.duty.MarkOvershadowedSegmentsAsUnused;
import org.apache.druid.server.coordinator.loading.SegmentLoadQueueManager;
import org.apache.druid.server.coordinator.loading.TestLoadQueuePeon;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks the {@code MarkOvershadowedSegmentsAsUnused} coordinator duty's {@code run} method against a cluster where
 * most datasources have no overshadowed segments (as in production). The duty builds segment timelines from the served
 * segments only for the datasources that actually have overshadowed segments, so the cost is dominated by how many
 * datasources are relevant relative to the total served.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
public class MarkOvershadowedSegmentsAsUnusedBenchmark
{
  private static final DateTime START = DateTimes.of("2024-01-01");

  /** Total datasources served by the cluster. */
  @Param({"1000"})
  private int numDatasources;

  /** Datasources that have overshadowed segments (an older version shadowed by a newer one). */
  @Param({"10", "100"})
  private int relevantDatasources;

  @Param({"4"})
  private int intervalsPerDatasource;

  private MarkOvershadowedSegmentsAsUnused duty;
  private DruidCoordinatorRuntimeParams params;

  @Setup(Level.Trial)
  public void setup()
  {
    final List<DataSegment> usedSegments = new ArrayList<>();
    final ImmutableMap.Builder<String, ImmutableDruidDataSource> dataSources = ImmutableMap.builder();

    for (int d = 0; d < numDatasources; d++) {
      final String datasource = "ds_" + d;
      final boolean hasOvershadowed = d < relevantDatasources;
      final List<DataSegment> dsSegments = new ArrayList<>();
      for (int i = 0; i < intervalsPerDatasource; i++) {
        final Interval interval = new Interval(START.plusDays(i), START.plusDays(i + 1));
        dsSegments.add(segment(datasource, interval, "v1"));
        if (hasOvershadowed) {
          // A newer version overshadows the v1 segment for the same interval.
          dsSegments.add(segment(datasource, interval, "v2"));
        }
      }
      usedSegments.addAll(dsSegments);
      dataSources.put(datasource, new ImmutableDruidDataSource(datasource, ImmutableMap.of(), dsSegments));
    }

    final ImmutableDruidServer server = new ImmutableDruidServer(
        new DruidServerMetadata("server", "host", null, 1L << 40, null, ServerType.HISTORICAL, "_default_tier", 0),
        0L,
        dataSources.build(),
        usedSegments.size()
    );
    final DruidCluster cluster = DruidCluster
        .builder()
        .add(new ServerHolder(server, new TestLoadQueuePeon()))
        .build();

    params = DruidCoordinatorRuntimeParams
        .builder()
        .withDataSourcesSnapshot(DataSourcesSnapshot.fromUsedSegments(usedSegments))
        .withDruidCluster(cluster)
        .withDynamicConfigs(CoordinatorDynamicConfig.builder().withMarkSegmentAsUnusedDelayMillis(0).build())
        .withBalancerStrategy(new RandomBalancerStrategy())
        .withSegmentAssignerUsing(new SegmentLoadQueueManager(null, null))
        .build();

    // A no-op delete handler so the benchmark measures the timeline build + overshadow check, not the metadata write.
    duty = new MarkOvershadowedSegmentsAsUnused((datasource, segmentIds) -> segmentIds.size());
  }

  @Benchmark
  public DruidCoordinatorRuntimeParams run()
  {
    return duty.run(params);
  }

  private static DataSegment segment(String datasource, Interval interval, String version)
  {
    return DataSegment.builder()
                      .dataSource(datasource)
                      .interval(interval)
                      .version(version)
                      .size(1)
                      .build();
  }
}
