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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.calcite.DataContext;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
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
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks representative {@code sys.segments} queries as they run through
 * {@link SystemSchema.SegmentsTable#scan}, against a synthetic cluster of {@code numSegments}
 * segments spread across {@code numDataSources} datasources.
 *
 * <p>The benchmark deliberately has NO in-code "pushdown on/off" switch: it simply issues each query
 * through the production scan path. To measure the effect of the datasource filter push-down, run
 * this benchmark on the patched code and again on the reverted code (e.g. {@code git stash} the
 * SystemSchema / MetadataSegmentView / AbstractSegmentMetadataCache changes, rebuild, rerun) and
 * compare. Pre-patch, {@link #SINGLE_DATASOURCE_EQUALS}/{@link #MULTI_DATASOURCE_IN} return every segment because
 * {@code scan} ignores its filters; post-patch they return only the matching datasources. That
 * difference in materialized work per query IS the optimization being measured.
 *
 * <p>Sample queries:
 * <ul>
 *   <li>{@code BASE_SCAN} - {@code SELECT * FROM sys.segments} (the unfiltered console/aggregate baseline)</li>
 *   <li>{@code SINGLE_DATASOURCE_EQUALS} - {@code WHERE datasource = 'datasource_0'} (single-datasource drill-down)</li>
 *   <li>{@code MULTI_DATASOURCE_IN} - {@code WHERE datasource IN ('datasource_0','datasource_1','datasource_2')}</li>
 * </ul>
 *
 * <p>The available-segment cache is left empty so the benchmark isolates the published-segment path,
 * which dominates cost on large clusters.
 */
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsAppend = {"-Xmx12g"})
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class SysSegmentsTableBenchmark
{
  public enum Query
  {
    BASE_SCAN,
    SINGLE_DATASOURCE_EQUALS,
    MULTI_DATASOURCE_IN
  }

  @Param({"1000000", "10000000"})
  private int numSegments;

  @Param({"1000"})
  private int numDataSources;

  @Param
  private Query query;

  private SystemSchema.SegmentsTable segmentsTable;
  private List<SegmentStatusInCluster> publishedSegments;
  private DataContext dataContext;
  private Map<Query, List<RexNode>> filtersByQuery;

  /**
   * An empty {@link BrokerSegmentMetadataCache}: the benchmark never announces segments to it, so the
   * available-segment side of the scan contributes nothing and the published path is measured in
   * isolation. Mirrors the null-arg construction proven in DruidSchemaInternRowSignatureBenchmark.
   */
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
    // sys.segments serves published segments sorted by SegmentId (datasource-prefixed).
    segments.sort(Comparator.naturalOrder());
    publishedSegments = ImmutableList.copyOf(segments);

    // Returns a fresh iterator over the synthetic segments on every fetch (cache disabled below).
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

    final DruidSchema druidSchema =
        new DruidSchema(new EmptyBrokerSegmentMetadataCache(), null, CatalogResolver.NULL_RESOLVER);

    final AuthorizerMapper authorizerMapper = new AuthorizerMapper(null)
    {
      @Override
      public Authorizer getAuthorizer(String name)
      {
        return new AllowAllAuthorizer(null);
      }
    };

    segmentsTable = new SystemSchema.SegmentsTable(druidSchema, metadataView, new DefaultObjectMapper(), authorizerMapper);

    filtersByQuery = buildFilters();

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

  /**
   * Builds the filter list for each sample query. The datasource input-ref is given the literal's
   * type so Calcite does not wrap the literal in a CAST (which would defeat the push-down extractor).
   * "datasource" is column index 1 in SEGMENTS_SIGNATURE.
   */
  private static Map<Query, List<RexNode>> buildFilters()
  {
    final RexBuilder rexBuilder = new RexBuilder(new JavaTypeFactoryImpl());
    final RexLiteral ds0 = (RexLiteral) rexBuilder.makeLiteral("datasource_0");
    final RexLiteral ds1 = (RexLiteral) rexBuilder.makeLiteral("datasource_1");
    final RexLiteral ds2 = (RexLiteral) rexBuilder.makeLiteral("datasource_2");
    final RexNode dsRef = rexBuilder.makeInputRef(ds0.getType(), 1);

    final Map<Query, List<RexNode>> filters = new EnumMap<>(Query.class);
    filters.put(Query.BASE_SCAN, ImmutableList.of());
    filters.put(
        Query.SINGLE_DATASOURCE_EQUALS,
        ImmutableList.of(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, dsRef, ds0))
    );
    filters.put(
        Query.MULTI_DATASOURCE_IN,
        ImmutableList.of(rexBuilder.makeIn(dsRef, ImmutableList.of(ds0, ds1, ds2)))
    );
    return filters;
  }

  @Benchmark
  public void scan(Blackhole blackhole)
  {
    long rows = 0;
    for (Object[] row : segmentsTable.scan(dataContext, filtersByQuery.get(query), null)) {
      blackhole.consume(row);
      rows++;
    }
    blackhole.consume(rows);
  }
}
