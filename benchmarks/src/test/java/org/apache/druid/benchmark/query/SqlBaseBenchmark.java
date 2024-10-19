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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.multibindings.MapBinder;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.segment.FrameSegment;
import org.apache.druid.frame.testutil.FrameTestUtil;
import org.apache.druid.guice.ExpressionModule;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.SegmentWranglerModule;
import org.apache.druid.guice.StartupInjectorBuilder;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchModule;
import org.apache.druid.query.aggregation.datasketches.hll.sql.HllSketchApproxCountDistinctSqlAggregator;
import org.apache.druid.query.aggregation.datasketches.hll.sql.HllSketchApproxCountDistinctUtf8SqlAggregator;
import org.apache.druid.query.aggregation.datasketches.hll.sql.HllSketchEstimateOperatorConversion;
import org.apache.druid.query.aggregation.datasketches.quantiles.DoublesSketchModule;
import org.apache.druid.query.aggregation.datasketches.quantiles.sql.DoublesSketchApproxQuantileSqlAggregator;
import org.apache.druid.query.aggregation.datasketches.quantiles.sql.DoublesSketchObjectSqlAggregator;
import org.apache.druid.query.aggregation.datasketches.quantiles.sql.DoublesSketchQuantileOperatorConversion;
import org.apache.druid.query.aggregation.datasketches.quantiles.sql.DoublesSketchQuantilesOperatorConversion;
import org.apache.druid.query.aggregation.datasketches.theta.SketchModule;
import org.apache.druid.query.aggregation.datasketches.theta.sql.ThetaSketchApproxCountDistinctSqlAggregator;
import org.apache.druid.query.aggregation.datasketches.theta.sql.ThetaSketchEstimateOperatorConversion;
import org.apache.druid.query.aggregation.datasketches.tuple.ArrayOfDoublesSketchModule;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.PhysicalSegmentInspector;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexCursorFactory;
import org.apache.druid.segment.QueryableIndexPhysicalSegmentInspector;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.column.StringEncodingStrategy;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.FrontCodedIndexed;
import org.apache.druid.segment.generator.SegmentGenerator;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.sql.calcite.SqlVectorizedExpressionSanityTest;
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
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


@State(Scope.Benchmark)
public class SqlBaseBenchmark
{
  private static final Logger log = new Logger(SqlBaseBenchmark.class);
  private static final String STORAGE_MMAP = "MMAP";
  private static final String STORAGE_INCREMENTAL = "INCREMENTAL";
  private static final String STORAGE_FRAME_ROW = "FRAME_ROW";
  private static final String STORAGE_FRAME_COLUMNAR = "FRAME_COLUMNAR";

  static {
    NullHandling.initializeForTests();
    ExpressionProcessing.initializeForTests();
    HllSketchModule.registerSerde();
    SketchModule.registerSerde();
    DoublesSketchModule.registerSerde();

    CalciteTests.getJsonMapper()
                .registerModules(new HllSketchModule().getJacksonModules())
                .registerModules(new SketchModule().getJacksonModules())
                .registerModules(new DoublesSketchModule().getJacksonModules())
                .registerModules(new ArrayOfDoublesSketchModule().getJacksonModules());
  }

  public enum BenchmarkStorage
  {
    MMAP,
    INCREMENTAL,
    FRAME_COLUMNAR,
    FRAME_ROW
  }

  public enum BenchmarkStringEncodingStrategy
  {
    UTF8,
    FRONT_CODED_DEFAULT_V1,
    FRONT_CODED_16_V1
  }


  @Param({"1500000"})
  protected int rowsPerSegment;

  @Param({
      "false",
      "force"
  })
  protected String vectorize;

  @Param({
      "UTF8",
      "FRONT_CODED_DEFAULT_V1",
      "FRONT_CODED_16_V1"
  })
  protected BenchmarkStringEncodingStrategy stringEncoding;

  @Param({
      "none",
      "lz4"
  })
  protected String complexCompression;

  @Param({
      "explicit",
      "auto"
  })
  protected String schemaType;

  // Can be STORAGE_MMAP, STORAGE_INCREMENTAL, STORAGE_FRAME_ROW, or STORAGE_FRAME_COLUMNAR
  @Param({
      STORAGE_MMAP,
      STORAGE_INCREMENTAL,
      STORAGE_FRAME_ROW,
      STORAGE_FRAME_COLUMNAR
  })
  protected BenchmarkStorage storageType;

  protected SqlEngine engine;
  @Nullable
  protected PlannerFactory plannerFactory;
  private final Closer closer = Closer.create();

  protected QueryContexts.Vectorize vectorizeContext;


  public String getQuery()
  {
    throw new UnsupportedOperationException("getQuery not implemented");
  }

  public List<String> getDatasources()
  {
    throw new UnsupportedOperationException("getDatasources not implemented");
  }

  protected Map<String, Object> getContext()
  {
    final Map<String, Object> context = ImmutableMap.of(
        QueryContexts.VECTORIZE_KEY, vectorize,
        QueryContexts.VECTORIZE_VIRTUAL_COLUMNS_KEY, vectorize
    );
    return context;
  }

  protected IndexSpec getIndexSpec()
  {
    return IndexSpec.builder()
                    .withStringDictionaryEncoding(getStringEncodingStrategy())
                    .withComplexMetricCompression(
                        CompressionStrategy.valueOf(StringUtils.toUpperCase(complexCompression))
                    )
                    .build();
  }

  @Setup(Level.Trial)
  public void setup() throws JsonProcessingException
  {
    vectorizeContext = QueryContexts.Vectorize.fromString(vectorize);
    checkIncompatibleParameters();

    Map<DataSegment, IncrementalIndex> realtimeSegments = new HashMap<>();
    Map<DataSegment, QueryableIndex> segments = new HashMap<>();
    for (String dataSource : getDatasources()) {
      final SqlBenchmarkDatasets.BenchmarkSchema schema;
      if ("auto".equals(schemaType)) {
        schema = SqlBenchmarkDatasets.getSchema(dataSource).asAutoDimensions();
      } else {
        schema = SqlBenchmarkDatasets.getSchema(dataSource);
      }

      for (DataSegment dataSegment : schema.getDataSegments()) {
        final SegmentGenerator segmentGenerator = closer.register(new SegmentGenerator());
        log.info(
            "Starting benchmark setup using cacheDir[%s], rows[%,d].",
            segmentGenerator.getCacheDir(),
            rowsPerSegment
        );

        if (BenchmarkStorage.INCREMENTAL == storageType) {
          final IncrementalIndex index = segmentGenerator.generateIncrementalIndex(
              dataSegment,
              schema.getGeneratorSchemaInfo(),
              schema.getDimensionsSpec(),
              schema.getTransformSpec(),
              schema.getAggregators(),
              getIndexSpec(),
              schema.getQueryGranularity(),
              schema.getProjections(),
              rowsPerSegment,
              CalciteTests.getJsonMapper()
          );
          log.info(
              "Segment metadata: %s",
              CalciteTests.getJsonMapper().writerWithDefaultPrettyPrinter().writeValueAsString(index.getMetadata())
          );
          realtimeSegments.put(dataSegment, index);
        } else {
          final QueryableIndex index = segmentGenerator.generate(
              dataSegment,
              schema.getGeneratorSchemaInfo(),
              schema.getDimensionsSpec(),
              schema.getTransformSpec(),
              getIndexSpec(),
              schema.getQueryGranularity(),
              schema.getProjections(),
              rowsPerSegment,
              CalciteTests.getJsonMapper()
          );
          log.info(
              "Segment metadata: %s",
              CalciteTests.getJsonMapper().writerWithDefaultPrettyPrinter().writeValueAsString(index.getMetadata())
          );
          segments.put(dataSegment, index);
        }
      }
    }

    final Pair<PlannerFactory, SqlEngine> sqlSystem = createSqlSystem(
        segments,
        realtimeSegments,
        Collections.emptyMap(),
        storageType,
        closer
    );

    plannerFactory = sqlSystem.lhs;
    engine = sqlSystem.rhs;
    final ObjectMapper jsonMapper = CalciteTests.getJsonMapper();
    try (final DruidPlanner planner = plannerFactory.createPlannerForTesting(
        engine,
        "EXPLAIN PLAN FOR " + getQuery(),
        ImmutableMap.<String, Object>builder()
                    .putAll(getContext())
                    .put(
                        "useNativeQueryExplain",
                        true
                    )
                    .build()
    )) {
      final PlannerResult plannerResult = planner.plan();
      final Sequence<Object[]> resultSequence = plannerResult.run().getResults();
      final Object[] planResult = resultSequence.toList().get(0);
      log.info("Native query plan:\n" +
               jsonMapper.writerWithDefaultPrettyPrinter()
                         .writeValueAsString(jsonMapper.readValue((String) planResult[0], List.class))
      );
    }
    catch (JsonProcessingException ex) {
      log.warn(ex, "explain failed");
    }

    try (final DruidPlanner planner = plannerFactory.createPlannerForTesting(engine, getQuery(), getContext())) {
      final PlannerResult plannerResult = planner.plan();
      final Sequence<Object[]> resultSequence = plannerResult.run().getResults();
      final int rowCount = resultSequence.toList().size();
      log.info("Total result row count:" + rowCount);
    }
    catch (Throwable ex) {
      log.warn(ex, "failed to count rows");
    }


    if (vectorizeContext.shouldVectorize(true)) {
      try {
        SqlVectorizedExpressionSanityTest.sanityTestVectorizedSqlQueries(
            engine,
            plannerFactory,
            getQuery()
        );
        log.info("non-vectorized and vectorized results match");
      }
      catch (Throwable ex) {
        log.warn(ex, "non-vectorized and vectorized results do not match");
      }
    }
  }

  private void checkIncompatibleParameters()
  {
    // if running as fork 0, maybe don't use these combinations since it will kill everything
    if (stringEncoding != BenchmarkStringEncodingStrategy.UTF8 && storageType != BenchmarkStorage.MMAP) {
      System.exit(0);
    }
    // complex compression only applies to mmap segments, dont bother otherwise
    if (!"none".equals(complexCompression) && storageType != BenchmarkStorage.MMAP) {
      System.exit(0);
    }
    // vectorize only works for mmap and frame column segments, bail out if
    if (vectorizeContext.shouldVectorize(true) && !(storageType == BenchmarkStorage.MMAP || storageType == BenchmarkStorage.FRAME_COLUMNAR)) {
      System.exit(0);
    }
  }

  private StringEncodingStrategy getStringEncodingStrategy()
  {
    if (stringEncoding == BenchmarkStringEncodingStrategy.FRONT_CODED_DEFAULT_V1) {
      return new StringEncodingStrategy.FrontCoded(null, FrontCodedIndexed.V1);
    } else if (stringEncoding == BenchmarkStringEncodingStrategy.FRONT_CODED_16_V1) {
      return new StringEncodingStrategy.FrontCoded(16, FrontCodedIndexed.V1);
    } else {
      return new StringEncodingStrategy.Utf8();
    }
  }

  public static Pair<PlannerFactory, SqlEngine> createSqlSystem(
      final Map<DataSegment, QueryableIndex> segmentMap,
      final Map<DataSegment, IncrementalIndex> realtimeSegmentsMap,
      final Map<String, LookupExtractor> lookupMap,
      @Nullable final BenchmarkStorage storageType,
      final Closer closer
  )
  {
    final QueryRunnerFactoryConglomerate conglomerate = QueryStackTests.createQueryRunnerFactoryConglomerate(closer);
    final SpecificSegmentsQuerySegmentWalker walker = SpecificSegmentsQuerySegmentWalker.createWalker(conglomerate);
    final PlannerConfig plannerConfig = new PlannerConfig();

    for (final Map.Entry<DataSegment, QueryableIndex> segmentEntry : segmentMap.entrySet()) {
      addSegmentToWalker(walker, segmentEntry.getKey(), segmentEntry.getValue(), storageType);
    }

    for (final Map.Entry<DataSegment, IncrementalIndex> segmentEntry : realtimeSegmentsMap.entrySet()) {
      walker.add(
          segmentEntry.getKey(),
          new IncrementalIndexSegment(segmentEntry.getValue(), segmentEntry.getKey().getId())
      );
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
            },
            new HllSketchModule(),
            new SketchModule(),
            new DoublesSketchModule(),
            binder -> {

            }
        )
        .build();
    ObjectMapper injected = injector.getInstance(Key.get(ObjectMapper.class, Json.class));
    injected.registerModules(new HllSketchModule().getJacksonModules());

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
      @Nullable final BenchmarkStorage storageType
  )
  {
    if (storageType == null || BenchmarkStorage.MMAP == storageType) {
      walker.add(descriptor, new QueryableIndexSegment(index, descriptor.getId()));
    } else if (BenchmarkStorage.FRAME_ROW == storageType) {
      QueryableIndexCursorFactory cursorFactory = new QueryableIndexCursorFactory(index);
      walker.add(
          descriptor,
          new FrameSegment(
              FrameTestUtil.cursorFactoryToFrame(cursorFactory, FrameType.ROW_BASED),
              FrameReader.create(cursorFactory.getRowSignature()),
              descriptor.getId()
          )
          {
            @Nullable
            @Override
            public <T> T as(@Nonnull Class<T> clazz)
            {
              // computed sql schema uses segment metadata, which relies on physical inspector, use the underlying index
              if (clazz.equals(PhysicalSegmentInspector.class)) {
                return (T) new QueryableIndexPhysicalSegmentInspector(index);
              }
              return super.as(clazz);
            }
          }
      );
    } else if (BenchmarkStorage.FRAME_COLUMNAR == storageType) {
      QueryableIndexCursorFactory cursorFactory = new QueryableIndexCursorFactory(index);
      walker.add(
          descriptor,
          new FrameSegment(
              FrameTestUtil.cursorFactoryToFrame(cursorFactory, FrameType.COLUMNAR),
              FrameReader.create(cursorFactory.getRowSignature()),
              descriptor.getId()
          )
          {
            @Nullable
            @Override
            public <T> T as(@Nonnull Class<T> clazz)
            {
              // computed sql schema uses segment metadata, which relies on physical inspector, use the underlying index
              if (clazz.equals(PhysicalSegmentInspector.class)) {
                return (T) new QueryableIndexPhysicalSegmentInspector(index);
              }
              return super.as(clazz);
            }
          }
      );
    } else {
      throw new IAE("Invalid storageType[%s]", storageType);
    }
  }

  private static DruidOperatorTable createOperatorTable(final Injector injector)
  {
    try {
      final Set<SqlOperatorConversion> operators = new HashSet<>();
      operators.add(injector.getInstance(QueryLookupOperatorConversion.class));
      operators.addAll(
          ImmutableList.of(
              new HllSketchEstimateOperatorConversion(),
              new ThetaSketchEstimateOperatorConversion(),
              new DoublesSketchQuantileOperatorConversion(),
              new DoublesSketchQuantilesOperatorConversion()
          )
      );
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
      return new DruidOperatorTable(aggregators, operators);
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
}
