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

package org.apache.druid.sql.calcite.schema;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.calcite.DataContext;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.druid.client.BrokerSegmentWatcherConfig;
import org.apache.druid.client.InternalQueryConfig;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.client.coordinator.NoopCoordinatorClient;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.security.AllowAllAuthorizer;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Escalator;
import org.apache.druid.sql.calcite.planner.CatalogResolver;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.SegmentStatusInCluster;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.easymock.EasyMock;
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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Validates the performance gain of answering the console's per-datasource aggregate over
 * {@code sys.segments} from the {@link SegmentsRollup} instead of scanning every segment.
 *
 * <ul>
 *   <li>{@link #scanAndAggregate} - the pre-acceleration per-query cost: scan every segment and
 *       group-by-datasource. O(#segments).</li>
 *   <li>{@link #rollupRead} - the accelerated per-query cost: read the precomputed rollup and emit one
 *       row per datasource. O(#datasources).</li>
 *   <li>{@link #rollupFold} - the one-time, amortized cost of (re)building the rollup, which happens
 *       once per poll off the query path (not per query).</li>
 * </ul>
 *
 * The available-segment cache is left empty so the published path is measured in isolation.
 */
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsAppend = {"-Xmx12g"})
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class SysSegmentsRollupBenchmark
{
  private static final int COL_DATASOURCE = 1;
  private static final int COL_SIZE = 4;
  private static final int COL_NUM_ROWS = 8;
  private static final int COL_IS_ACTIVE = 9;

  @Param({"1000000", "10000000"})
  private int numSegments;

  @Param({"1000"})
  private int numDataSources;

  private SystemSchema.SegmentsTable segmentsTable;
  private SegmentsRollup rollup;
  private ImmutableSortedMap<String, DatasourceSegmentStats> prebuiltStats;
  private DataContext dataContext;
  private List<SegmentStatusInCluster> publishedSegments;

  // Cube-cell masks for the console's columns (built once, like the optimizer rule would).
  private boolean[] maskActive;
  private boolean[] maskAvailNonRealtime;

  private static boolean[] mask(final java.util.function.IntPredicate cellMatches)
  {
    final boolean[] m = new boolean[DatasourceSegmentStats.NUM_CELLS];
    for (int key = 0; key < DatasourceSegmentStats.NUM_CELLS; key++) {
      m[key] = cellMatches.test(key);
    }
    return m;
  }

  private static class EmptyBrokerSegmentMetadataCache extends BrokerSegmentMetadataCache
  {
    EmptyBrokerSegmentMetadataCache()
    {
      super(
          EasyMock.mock(QueryLifecycleFactory.class),
          EasyMock.mock(TimelineServerView.class),
          BrokerSegmentMetadataCacheConfig.create(),
          EasyMock.mock(Escalator.class),
          EasyMock.mock(InternalQueryConfig.class),
          new NoopServiceEmitter(),
          new PhysicalDatasourceMetadataFactory(
              EasyMock.mock(JoinableFactory.class),
              EasyMock.mock(SegmentManager.class)
          ),
          new NoopCoordinatorClient(),
          CentralizedDatasourceSchemaConfig.create()
      );
    }
  }

  @Setup(Level.Trial)
  public void setup()
  {
    final List<SegmentStatusInCluster> segments = new ArrayList<>(numSegments);
    for (int i = 0; i < numSegments; i++) {
      final String dataSource = StringUtils.format("datasource_%d", i % numDataSources);
      final int dayOffset = i / numDataSources;
      final SegmentId segmentId = SegmentId.of(
          dataSource,
          Intervals.utc(dayOffset * 86_400_000L, (dayOffset + 1) * 86_400_000L),
          "1",
          new LinearShardSpec(0)
      );
      final DataSegment segment = DataSegment.builder(segmentId).size(1000L).build();
      segments.add(new SegmentStatusInCluster(segment, false, 1, 100L, false));
    }
    segments.sort(Comparator.naturalOrder());
    publishedSegments = ImmutableList.copyOf(segments);

    final NoopCoordinatorClient coordinatorClient = new NoopCoordinatorClient()
    {
      @Override
      public ListenableFuture<CloseableIterator<SegmentStatusInCluster>> fetchAllUsedSegmentsWithOvershadowedStatus(
          Set<String> watchedDataSources,
          boolean includeRealtimeSegments
      )
      {
        return Futures.immediateFuture(CloseableIterators.withEmptyBaggage(publishedSegments.iterator()));
      }
    };

    final BrokerSegmentMetadataCacheConfig config = new DefaultObjectMapper().convertValue(
        ImmutableMap.of("metadataSegmentCacheEnable", false),
        BrokerSegmentMetadataCacheConfig.class
    );

    final MetadataSegmentView metadataView = new MetadataSegmentView(
        coordinatorClient,
        new BrokerSegmentWatcherConfig(),
        config,
        new NoopServiceEmitter()
    );

    final BrokerSegmentMetadataCache availableCache = new EmptyBrokerSegmentMetadataCache();
    final DruidSchema druidSchema = new DruidSchema(availableCache, null, CatalogResolver.NULL_RESOLVER);

    final AuthorizerMapper authorizerMapper = new AuthorizerMapper(null)
    {
      @Override
      public Authorizer getAuthorizer(String name)
      {
        return new AllowAllAuthorizer(null);
      }
    };

    segmentsTable = new SystemSchema.SegmentsTable(druidSchema, metadataView, new DefaultObjectMapper(), authorizerMapper);

    rollup = new SegmentsRollup(metadataView, availableCache, config, new NoopServiceEmitter());
    rollup.poll();
    prebuiltStats = rollup.getStatsIfReady();

    maskActive = mask(DatasourceSegmentStats::cellIsActive);
    maskAvailNonRealtime = mask(
        key -> DatasourceSegmentStats.cellIsAvailable(key) && !DatasourceSegmentStats.cellIsRealtime(key)
    );

    final AuthenticationResult authenticationResult = new AuthenticationResult("benchmark", "benchmark", null, null);
    dataContext = new DataContext()
    {
      @Override
      public SchemaPlus getRootSchema()
      {
        return null;
      }

      @Override
      public JavaTypeFactoryImpl getTypeFactory()
      {
        return null;
      }

      @Override
      public QueryProvider getQueryProvider()
      {
        return null;
      }

      @Override
      public Object get(String name)
      {
        return PlannerContext.DATA_CTX_AUTHENTICATION_RESULT.equals(name) ? authenticationResult : null;
      }
    };
  }

  /** Pre-acceleration cost: scan every segment, then group-by-datasource. O(#segments). */
  @Benchmark
  public void scanAndAggregate(Blackhole blackhole)
  {
    final Map<String, long[]> byDatasource = new HashMap<>();
    for (Object[] row : segmentsTable.scan(dataContext, ImmutableList.of(), null)) {
      if (((Number) row[COL_IS_ACTIVE]).longValue() == 1L) {
        final long[] acc = byDatasource.computeIfAbsent((String) row[COL_DATASOURCE], ds -> new long[3]);
        acc[0]++;
        acc[1] += ((Number) row[COL_SIZE]).longValue();
        acc[2] += ((Number) row[COL_NUM_ROWS]).longValue();
      }
    }
    blackhole.consume(byDatasource);
  }

  /** Accelerated per-query cost: read the precomputed rollup (fold cube cells) per datasource. */
  @Benchmark
  public void rollupRead(Blackhole blackhole)
  {
    long rows = 0;
    for (Map.Entry<String, DatasourceSegmentStats> entry : prebuiltStats.entrySet()) {
      final DatasourceSegmentStats stat = entry.getValue();
      final long activeCount = stat.count(maskActive);
      blackhole.consume(new Object[]{
          entry.getKey(),
          activeCount,                                                    // num_segments
          stat.sumSize(maskActive),                                       // total_data_size
          stat.sumNumRows(maskActive),                                    // total_rows
          stat.sumReplicatedSize(maskActive),                             // replicated_size
          stat.minNumRows(maskAvailNonRealtime),                          // min_segment_rows
          stat.maxNumRows(maskAvailNonRealtime),                          // max_segment_rows
          stat.count(maskAvailNonRealtime) == 0
              ? null
              : stat.sumNumRows(maskAvailNonRealtime) / stat.count(maskAvailNonRealtime) // avg_segment_rows
      });
      rows++;
    }
    blackhole.consume(rows);
  }

  /** One-time, amortized cost of (re)building the rollup - happens once per poll, not per query. */
  @Benchmark
  public void rollupFold(Blackhole blackhole)
  {
    rollup.poll();
    blackhole.consume(rollup.getStatsIfReady());
  }
}
