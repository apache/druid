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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.client.BrokerInternalQueryConfig;
import org.apache.druid.client.BrokerSegmentWatcherConfig;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.NonnullPair;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.metadata.metadata.SegmentAnalysis;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.loading.SegmentLoader;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.planner.SegmentsTableConfig;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.CalciteTests.FakeDruidNodeDiscoveryProvider;
import org.apache.druid.sql.calcite.util.CalciteTests.FakeHttpClient;
import org.apache.druid.sql.calcite.util.CalciteTests.FakeServerInventoryView;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.sql.calcite.util.TestServerInventoryView;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.SegmentWithOvershadowedStatus;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.easymock.EasyMock;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

public abstract class SegmentsTableQueryTestSuite
{
  static {
    NullHandling.initializeForTests();
    Calcites.setSystemProperties();
  }

  protected static final List<String> QUERIES = ImmutableList.of(
      "select count(*) from sys.segments", // 0
      "WITH s AS (SELECT\n"  // 1
      + "\"segment_id\",\n"
      + "\"datasource\",\n"
      + "\"start\",\n"
      + "\"end\",\n"
      + "\"version\",\n"
      + "CASE\n"
      + "  WHEN \"start\" LIKE '%-01-01T00:00:00.000Z' AND \"end\" LIKE '%-01-01T00:00:00.000Z' THEN 'Year'\n"
      + "  WHEN \"start\" LIKE '%-01T00:00:00.000Z' AND \"end\" LIKE '%-01T00:00:00.000Z' THEN 'Month'\n"
      + "  WHEN \"start\" LIKE '%T00:00:00.000Z' AND \"end\" LIKE '%T00:00:00.000Z' THEN 'Day'\n"
      + "  WHEN \"start\" LIKE '%:00:00.000Z' AND \"end\" LIKE '%:00:00.000Z' THEN 'Hour'\n"
      + "  WHEN \"start\" LIKE '%:00.000Z' AND \"end\" LIKE '%:00.000Z' THEN 'Minute'\n"
      + "  ELSE 'Sub minute'\n"
      + "END AS \"time_span\",\n"
      + "CASE\n"
      + "  WHEN \"shard_spec\" LIKE '%\"type\":\"numbered\"%' THEN 'dynamic'\n"
      + "  WHEN \"shard_spec\" LIKE '%\"type\":\"hashed\"%' THEN 'hashed'\n"
      + "  WHEN \"shard_spec\" LIKE '%\"type\":\"single\"%' THEN 'single_dim'\n"
      + "  WHEN \"shard_spec\" LIKE '%\"type\":\"none\"%' THEN 'none'\n"
      + "  WHEN \"shard_spec\" LIKE '%\"type\":\"linear\"%' THEN 'linear'\n"
      + "  WHEN \"shard_spec\" LIKE '%\"type\":\"numbered_overwrite\"%' THEN 'numbered_overwrite'\n"
      + "  ELSE '-'\n"
      + "END AS \"partitioning\",\n"
      + "\"partition_num\",\n"
      + "\"size\",\n"
      + "\"num_rows\",\n"
      + "\"num_replicas\",\n"
      + "\"is_published\",\n"
      + "\"is_available\",\n"
      + "\"is_realtime\",\n"
      + "\"is_overshadowed\"\n"
      + "FROM sys.segments)\n"
      + "SELECT *\n"
      + "FROM s\n"
      + "ORDER BY \"start\" DESC\n"
      + "LIMIT 25"
  );

  protected static PlannerFactory plannerFactory;
  protected static Closer closer;

  protected static void setupBenchmark(
      PlannerConfig plannerConfig,
      SegmentsTableConfig segmentsTableConfig,
      String segmentGranularity,
      String availableSegmentsInterval,
      String publishedSegmentsInterval,
      int numSegmentsPerInterval
  )
  {
    closer = Closer.create();
    final QueryRunnerFactoryConglomerate conglomerate = QueryStackTests.createQueryRunnerFactoryConglomerate(closer);
    final SpecificSegmentsQuerySegmentWalker walker = new SpecificSegmentsQuerySegmentWalker(conglomerate);

    final Granularity granularity = Granularity.fromString(segmentGranularity);
    final IncrementalIndexSchema schema = createIndexSchema();
    final List<DataSegment> availableSegments = new ArrayList<>();
    final List<SegmentAnalysis> availableSegmentAnalyses = new ArrayList<>();
    for (Interval timeChunk : granularity.getIterable(Intervals.of(availableSegmentsInterval))) {
      for (int i = 0; i < numSegmentsPerInterval; i++) {
        NonnullPair<DataSegment, SegmentAnalysis> pair = createSegment(
            timeChunk,
            i,
            schema.getDimensionsSpec().getDimensionNames(),
            Arrays.stream(schema.getMetrics()).map(AggregatorFactory::getName).collect(Collectors.toList())
        );
        availableSegments.add(pair.lhs);
        availableSegmentAnalyses.add(pair.rhs);
      }
    }

    final SortedSet<SegmentWithOvershadowedStatus> publishedSegments = new TreeSet<>();
    for (Interval timeChunk : granularity.getIterable(Intervals.of(publishedSegmentsInterval))) {
      for (int i = 0; i < numSegmentsPerInterval; i++) {
        DataSegment segment = createDataSegment(
            timeChunk,
            i,
            schema.getDimensionsSpec().getDimensionNames(),
            Arrays.stream(schema.getMetrics()).map(AggregatorFactory::getName).collect(Collectors.toList())
        );
        publishedSegments.add(new SegmentWithOvershadowedStatus(segment, false));
      }
    }

    final DruidSchema druidSchema = createDruidSchema(
        conglomerate,
        walker,
        plannerConfig,
        availableSegments,
        availableSegmentAnalyses
    );
    final MetadataSegmentView metadataSegmentView = new FixedMetadataSegmentView(
        new DefaultObjectMapper(),
        plannerConfig,
        publishedSegments
    );
    metadataSegmentView.start();
    final SystemSchema systemSchema = createMockSystemSchema(
        segmentsTableConfig,
        druidSchema,
        metadataSegmentView,
        availableSegments,
        CalciteTests.TEST_AUTHORIZER_MAPPER
    );
    final DruidSchemaCatalog rootSchema = CalciteTests.createMockRootSchema(
        plannerConfig,
        druidSchema,
        systemSchema,
        CalciteTests.TEST_AUTHORIZER_MAPPER
    );
    plannerFactory = new PlannerFactory(
        rootSchema,
        CalciteTests.createMockQueryMakerFactory(walker, conglomerate),
        CalciteTests.createOperatorTable(),
        CalciteTests.createExprMacroTable(),
        plannerConfig,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        CalciteTests.getJsonMapper(),
        CalciteTests.DRUID_SCHEMA_NAME
    );
    metadataSegmentView.waitForFirstPollToComplete();
    metadataSegmentView.stop();
  }

  private static DruidSchema createDruidSchema(
      final QueryRunnerFactoryConglomerate conglomerate,
      final QuerySegmentWalker walker,
      final PlannerConfig plannerConfig,
      final List<DataSegment> segments,
      final List<SegmentAnalysis> analyses
  )
  {
    final DruidSchema schema = new DruidSchema(
        CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
        new TestServerInventoryView(segments),
        new SegmentManager(EasyMock.createMock(SegmentLoader.class)),
        CalciteTests.createDefaultJoinableFactory(),
        plannerConfig,
        CalciteTests.TEST_AUTHENTICATOR_ESCALATOR,
        new BrokerInternalQueryConfig()
    )
    {
      @Override
      protected Sequence<SegmentAnalysis> runSegmentMetadataQuery(
          Iterable<SegmentId> segments
      )
      {
        try {
          // Emulate the query latency
          Thread.sleep(100);
        }
        catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        return Sequences.simple(analyses);
      }
    };

    try {
      schema.start();
      schema.awaitInitialization();
    }
    catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    schema.stop();
    return schema;
  }

  private static SystemSchema createMockSystemSchema(
      final SegmentsTableConfig segmentsTableConfig,
      final DruidSchema druidSchema,
      final MetadataSegmentView metadataSegmentView,
      final List<DataSegment> segments,
      final AuthorizerMapper authorizerMapper
  )
  {
    final DruidNodeDiscoveryProvider provider = new FakeDruidNodeDiscoveryProvider(ImmutableMap.of());
    final DruidLeaderClient leaderClient = new DruidLeaderClient(
        new FakeHttpClient(),
        provider,
        NodeRole.COORDINATOR,
        "/simple/leader"
    );
    return new SystemSchema(
        segmentsTableConfig,
        druidSchema,
        metadataSegmentView,
        new TestServerInventoryView(segments),
        new FakeServerInventoryView(),
        authorizerMapper,
        leaderClient,
        leaderClient,
        provider,
        new ObjectMapper()
    );
  }

  private static IncrementalIndexSchema createIndexSchema()
  {
    List<DimensionSchema> dimensions = new ArrayList<>(30);
    for (int i = 0; i < 30; i++) {
      dimensions.add(new StringDimensionSchema("dim_" + i));
    }
    return new IncrementalIndexSchema.Builder()
        .withDimensionsSpec(new DimensionsSpec(dimensions))
        .withMetrics(new CountAggregatorFactory("cnt"))
        .withRollup(false)
        .build();
  }

  private static NonnullPair<DataSegment, SegmentAnalysis> createSegment(
      Interval interval,
      int partitionId,
      List<String> dimensions,
      List<String> metrics
  )
  {
    DataSegment segment = createDataSegment(interval, partitionId, dimensions, metrics);
    SegmentAnalysis analysis = new SegmentAnalysis(
        segment.getId().toString(),
        ImmutableList.of(segment.getInterval()),
        new TreeMap<>(),
        10L,
        100L,
        null,
        null,
        null,
        false
    );
    return new NonnullPair<>(segment, analysis);
  }

  private static DataSegment createDataSegment(
      Interval interval,
      int partitionId,
      List<String> dimensions,
      List<String> metrics
  )
  {
    return new DataSegment(
        "datasource",
        interval,
        "version",
        ImmutableMap.of(),
        dimensions,
        metrics,
        new NumberedShardSpec(partitionId, 0),
        0,
        10L
    );
  }

  protected static void tearDownBenchmark() throws IOException
  {
    closer.close();
  }

  private static class FixedMetadataSegmentView extends MetadataSegmentView
  {
    private final SortedSet<SegmentWithOvershadowedStatus> publishedSegments;

    private FixedMetadataSegmentView(
        ObjectMapper jsonMapper,
        PlannerConfig plannerConfig,
        SortedSet<SegmentWithOvershadowedStatus> publishedSegments
    )
    {
      super(null, jsonMapper, new BrokerSegmentWatcherConfig(), plannerConfig);
      this.publishedSegments = publishedSegments;
    }

    @Override
    protected Iterator<SegmentWithOvershadowedStatus> getMetadataSegments(
        DruidLeaderClient coordinatorClient,
        ObjectMapper jsonMapper,
        Set<String> watchedDataSources
    )
    {
      return publishedSegments.iterator();
    }
  }
}
