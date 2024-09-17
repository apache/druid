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

package org.apache.druid.benchmark.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.multibindings.MapBinder;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.testutil.FrameTestUtil;
import org.apache.druid.guice.ExpressionModule;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.SegmentWranglerModule;
import org.apache.druid.guice.StartupInjectorBuilder;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.aggregation.datasketches.hll.sql.HllSketchApproxCountDistinctSqlAggregator;
import org.apache.druid.query.aggregation.datasketches.hll.sql.HllSketchApproxCountDistinctUtf8SqlAggregator;
import org.apache.druid.query.aggregation.datasketches.quantiles.sql.DoublesSketchApproxQuantileSqlAggregator;
import org.apache.druid.query.aggregation.datasketches.quantiles.sql.DoublesSketchObjectSqlAggregator;
import org.apache.druid.query.aggregation.datasketches.theta.sql.ThetaSketchApproxCountDistinctSqlAggregator;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.segment.AutoTypeColumnSchema;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexCursorFactory;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.column.StringEncodingStrategy;
import org.apache.druid.segment.generator.GeneratorBasicSchemas;
import org.apache.druid.segment.generator.GeneratorSchemaInfo;
import org.apache.druid.segment.generator.SegmentGenerator;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.sql.calcite.aggregation.ApproxCountDistinctSqlAggregator;
import org.apache.druid.sql.calcite.aggregation.SqlAggregationModule;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.aggregation.builtin.CountSqlAggregator;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.QueryLookupOperatorConversion;
import org.apache.druid.sql.calcite.planner.CalciteRulesManager;
import org.apache.druid.sql.calcite.planner.CatalogResolver;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.planner.DruidPlanner;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.planner.PlannerResult;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.schema.DruidSchemaCatalog;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.LookylooModule;
import org.apache.druid.sql.calcite.util.QueryFrameworkUtils;
import org.apache.druid.sql.calcite.util.testoperator.CalciteTestOperatorModule;
import org.apache.druid.sql.hook.DruidHookDispatcher;
import org.apache.druid.timeline.DataSegment;
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
import org.openjdk.jmh.infra.Blackhole;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Benchmark that tests various SQL queries.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class SqlWindowFunctionsBenchmark
{
  static {
    NullHandling.initializeForTests();
  }

  private static final Logger log = new Logger(SqlWindowFunctionsBenchmark.class);

  private static final String STORAGE_MMAP = "mmap";
  private static final String STORAGE_FRAME_ROW = "frame-row";
  private static final String STORAGE_FRAME_COLUMNAR = "frame-columnar";

  @Param({"2000000"})
  private int rowsPerSegment;

  @Param({
      "auto"
  })
  private String schema;

  // Can be STORAGE_MMAP, STORAGE_FRAME_ROW, or STORAGE_FRAME_COLUMNAR
  @Param({STORAGE_MMAP})
  private String storageType;

  private SqlEngine engine;

  @Nullable
  private PlannerFactory plannerFactory;
  private final Closer closer = Closer.create();

  private static final DruidProcessingConfig PROCESSING_CONFIG = new DruidProcessingConfig() {

    @Override
    public int getNumMergeBuffers()
    {
      return 3;
    }

    @Override
    public int intermediateComputeSizeBytes()
    {
      return 200_000_000;
    }
  };

  @Setup(Level.Trial)
  public void setup()
  {
    final GeneratorSchemaInfo schemaInfo = GeneratorBasicSchemas.SCHEMA_MAP.get("basic");
    final DataSegment dataSegment = schemaInfo.makeSegmentDescriptor("foo");
    final SegmentGenerator segmentGenerator = closer.register(new SegmentGenerator());

    log.info("Starting benchmark setup using cacheDir[%s], rows[%,d].", segmentGenerator.getCacheDir(), rowsPerSegment);
    final QueryableIndex index;
    if ("auto".equals(schema)) {
      List<DimensionSchema> columnSchemas = schemaInfo.getDimensionsSpec()
                                                      .getDimensions()
                                                      .stream()
                                                      .map(x -> new AutoTypeColumnSchema(x.getName(), null))
                                                      .collect(Collectors.toList());
      index = segmentGenerator.generate(
          dataSegment,
          schemaInfo,
          DimensionsSpec.builder().setDimensions(columnSchemas).build(),
          TransformSpec.NONE,
          IndexSpec.builder().withStringDictionaryEncoding(getStringEncodingStrategy()).build(),
          Granularities.NONE,
          rowsPerSegment
      );
    } else {
      index = segmentGenerator.generate(dataSegment, schemaInfo, Granularities.NONE, rowsPerSegment);
    }

    final Pair<PlannerFactory, SqlEngine> sqlSystem = createSqlSystem(
        ImmutableMap.of(dataSegment, index),
        Collections.emptyMap(),
        null,
        closer
    );

    plannerFactory = sqlSystem.lhs;
    engine = sqlSystem.rhs;
  }

  private StringEncodingStrategy getStringEncodingStrategy()
  {
    return new StringEncodingStrategy.Utf8();
  }

  public static Pair<PlannerFactory, SqlEngine> createSqlSystem(
      final Map<DataSegment, QueryableIndex> segmentMap,
      final Map<String, LookupExtractor> lookupMap,
      @Nullable final String storageType,
      final Closer closer
  )
  {
    final QueryRunnerFactoryConglomerate conglomerate = QueryStackTests.createQueryRunnerFactoryConglomerate(closer, PROCESSING_CONFIG);
    final SpecificSegmentsQuerySegmentWalker walker = SpecificSegmentsQuerySegmentWalker.createWalker(conglomerate);
    final PlannerConfig plannerConfig = new PlannerConfig();

    for (final Map.Entry<DataSegment, QueryableIndex> segmentEntry : segmentMap.entrySet()) {
      addSegmentToWalker(walker, segmentEntry.getKey(), segmentEntry.getValue(), storageType);
    }

    // Child injector that adds additional lookups.
    final Injector injector = new StartupInjectorBuilder()
        .withEmptyProperties()
        .add(
            new ExpressionModule(),
            new SegmentWranglerModule(),
            new LookylooModule(),
            new SqlAggregationModule(),
            new CalciteTestOperatorModule(),
            binder -> {
              for (Map.Entry<String, LookupExtractor> entry : lookupMap.entrySet()) {
                MapBinder.newMapBinder(binder, String.class, LookupExtractor.class)
                         .addBinding(entry.getKey())
                         .toProvider(entry::getValue)
                         .in(LazySingleton.class);
              }
            }
        )
        .build();

    final DruidSchemaCatalog rootSchema =
        QueryFrameworkUtils.createMockRootSchema(
            injector,
            conglomerate,
            walker,
            plannerConfig,
            AuthTestUtils.TEST_AUTHORIZER_MAPPER
        );

    final SqlEngine engine = CalciteTests.createMockSqlEngine(walker, conglomerate);

    final PlannerFactory plannerFactory = new PlannerFactory(
        rootSchema,
        createOperatorTable(injector),
        injector.getInstance(ExprMacroTable.class),
        plannerConfig,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        injector.getInstance(Key.get(ObjectMapper.class, Json.class)),
        CalciteTests.DRUID_SCHEMA_NAME,
        new CalciteRulesManager(ImmutableSet.of()),
        new JoinableFactoryWrapper(QueryFrameworkUtils.createDefaultJoinableFactory(injector)),
        CatalogResolver.NULL_RESOLVER,
        new AuthConfig(),
        new DruidHookDispatcher()
    );

    return Pair.of(plannerFactory, engine);
  }

  private static void addSegmentToWalker(
      final SpecificSegmentsQuerySegmentWalker walker,
      final DataSegment descriptor,
      final QueryableIndex index,
      @Nullable final String storageType
  )
  {
    if (storageType == null || STORAGE_MMAP.equals(storageType)) {
      walker.add(descriptor, new QueryableIndexSegment(index, descriptor.getId()));
    } else if (STORAGE_FRAME_ROW.equals(storageType)) {
      walker.add(
          descriptor,
          FrameTestUtil.cursorFactoryToFrameSegment(
              new QueryableIndexCursorFactory(index),
              FrameType.ROW_BASED,
              descriptor.getId()
          )
      );
    } else if (STORAGE_FRAME_COLUMNAR.equals(storageType)) {
      walker.add(
          descriptor,
          FrameTestUtil.cursorFactoryToFrameSegment(
              new QueryableIndexCursorFactory(index),
              FrameType.COLUMNAR,
              descriptor.getId()
          )
      );
    } else {
      throw new IAE("Invalid storageType[%s]", storageType);
    }
  }

  private static DruidOperatorTable createOperatorTable(final Injector injector)
  {
    try {
      final Set<SqlOperatorConversion> extractionOperators = new HashSet<>();
      extractionOperators.add(injector.getInstance(QueryLookupOperatorConversion.class));
      final ApproxCountDistinctSqlAggregator countDistinctSqlAggregator =
          new ApproxCountDistinctSqlAggregator(new HllSketchApproxCountDistinctSqlAggregator());
      final Set<SqlAggregator> aggregators = new HashSet<>(
          ImmutableList.of(
              new DoublesSketchApproxQuantileSqlAggregator(),
              new DoublesSketchObjectSqlAggregator(),
              new HllSketchApproxCountDistinctSqlAggregator(),
              new HllSketchApproxCountDistinctUtf8SqlAggregator(),
              new ThetaSketchApproxCountDistinctSqlAggregator(),
              new CountSqlAggregator(countDistinctSqlAggregator),
              countDistinctSqlAggregator
          )
      );
      return new DruidOperatorTable(aggregators, extractionOperators);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @TearDown(Level.Trial)
  public void tearDown() throws Exception
  {
    closer.close();
  }

  public void querySql(String sql, Blackhole blackhole)
  {
    final Map<String, Object> context = ImmutableMap.of(
        PlannerContext.CTX_ENABLE_WINDOW_FNS, true,
        QueryContexts.MAX_SUBQUERY_BYTES_KEY, "disabled",
        QueryContexts.MAX_SUBQUERY_ROWS_KEY, -1
    );
    try (final DruidPlanner planner = plannerFactory.createPlannerForTesting(engine, sql, context)) {
      final PlannerResult plannerResult = planner.plan();
      final Sequence<Object[]> resultSequence = plannerResult.run().getResults();
      final Object[] lastRow = resultSequence.accumulate(null, (accumulated, in) -> in);
      blackhole.consume(lastRow);
    }
  }

  @Benchmark
  public void groupByWithoutWindow(Blackhole blackhole)
  {
    String sql = "SELECT SUM(dimSequentialHalfNull) "
                 + "FROM foo "
                 + "GROUP BY dimUniform";
    querySql(sql, blackhole);
  }

  @Benchmark
  public void groupByWithWindow(Blackhole blackhole)
  {
    String sql = "SELECT SUM(SUM(dimSequentialHalfNull)) "
                 + "OVER (ORDER BY dimUniform) "
                 + "FROM foo "
                 + "GROUP BY dimUniform";
    querySql(sql, blackhole);
  }

  @Benchmark
  public void simpleWindow(Blackhole blackhole)
  {
    String sql = "SELECT ROW_NUMBER() "
                 + "OVER (PARTITION BY dimUniform ORDER BY dimSequential) "
                 + "FROM foo";
    querySql(sql, blackhole);
  }

  @Benchmark
  public void simpleWindowUnbounded(Blackhole blackhole)
  {
    String sql = "SELECT COUNT(*) "
                 + "OVER (PARTITION BY dimUniform RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) "
                 + "FROM foo";
    querySql(sql, blackhole);
  }

  @Benchmark
  public void windowTillCurrentRow(Blackhole blackhole)
  {
    String sql = "SELECT COUNT(*) "
                 + "OVER (PARTITION BY dimUniform ORDER BY dimSequential RANGE UNBOUNDED PRECEDING) "
                 + "FROM foo";
    querySql(sql, blackhole);
  }

  @Benchmark
  public void windowFromCurrentRow(Blackhole blackhole)
  {
    String sql = "SELECT COUNT(*) "
                 + "OVER (PARTITION BY dimUniform ORDER BY dimSequential RANGE UNBOUNDED FOLLOWING) "
                 + "FROM foo";
    querySql(sql, blackhole);
  }

  @Benchmark
  public void windowWithSorter(Blackhole blackhole)
  {
    String sql = "SELECT COUNT(*) "
                 + "OVER (PARTITION BY dimUniform ORDER BY dimSequential) "
                 + "FROM foo "
                 + "GROUP BY dimSequential, dimUniform";
    querySql(sql, blackhole);
  }

  @Benchmark
  public void windowWithoutSorter(Blackhole blackhole)
  {
    String sql = "SELECT COUNT(*) "
                 + "OVER (PARTITION BY dimUniform ORDER BY dimSequential) "
                 + "FROM foo "
                 + "GROUP BY dimUniform, dimSequential";
    querySql(sql, blackhole);
  }

  @Benchmark
  public void windowWithGroupbyTime(Blackhole blackhole)
  {
    String sql = "SELECT "
                 + "SUM(dimSequentialHalfNull) + SUM(dimHyperUnique), "
                 + "LAG(SUM(dimSequentialHalfNull + dimHyperUnique)) OVER (PARTITION BY dimUniform ORDER BY dimSequential) "
                 + "FROM foo "
                 + "GROUP BY __time, dimUniform, dimSequential";
    querySql(sql, blackhole);
  }
}
