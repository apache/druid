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
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.aggregation.datasketches.hll.sql.HllSketchApproxCountDistinctSqlAggregator;
import org.apache.druid.query.aggregation.datasketches.hll.sql.HllSketchApproxCountDistinctUtf8SqlAggregator;
import org.apache.druid.query.aggregation.datasketches.quantiles.sql.DoublesSketchApproxQuantileSqlAggregator;
import org.apache.druid.query.aggregation.datasketches.quantiles.sql.DoublesSketchObjectSqlAggregator;
import org.apache.druid.query.aggregation.datasketches.theta.sql.ThetaSketchApproxCountDistinctSqlAggregator;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.column.StringEncodingStrategy;
import org.apache.druid.segment.data.FrontCodedIndexed;
import org.apache.druid.segment.generator.GeneratorBasicSchemas;
import org.apache.druid.segment.generator.GeneratorSchemaInfo;
import org.apache.druid.segment.generator.SegmentGenerator;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
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
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.planner.PlannerResult;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.schema.DruidSchemaCatalog;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.LookylooModule;
import org.apache.druid.sql.calcite.util.QueryFrameworkUtils;
import org.apache.druid.sql.calcite.util.testoperator.CalciteTestOperatorModule;
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

/**
 * Benchmark that tests various SQL queries.
 */
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class SqlBenchmark
{
  static {
    NullHandling.initializeForTests();
  }

  private static final Logger log = new Logger(SqlBenchmark.class);

  private static final String STORAGE_MMAP = "mmap";
  private static final String STORAGE_FRAME_ROW = "frame-row";
  private static final String STORAGE_FRAME_COLUMNAR = "frame-columnar";

  private static final List<String> QUERIES = ImmutableList.of(
      // 0, 1, 2, 3: Timeseries, unfiltered
      "SELECT COUNT(*) FROM foo",
      "SELECT COUNT(DISTINCT hyper) FROM foo",
      "SELECT SUM(sumLongSequential), SUM(sumFloatNormal) FROM foo",
      "SELECT FLOOR(__time TO MINUTE), SUM(sumLongSequential), SUM(sumFloatNormal) FROM foo GROUP BY 1",

      // 4: Timeseries, low selectivity filter (90% of rows match)
      "SELECT SUM(sumLongSequential), SUM(sumFloatNormal) FROM foo WHERE dimSequential NOT LIKE '%3'",

      // 5: Timeseries, high selectivity filter (0.1% of rows match)
      "SELECT SUM(sumLongSequential), SUM(sumFloatNormal) FROM foo WHERE dimSequential = '311'",

      // 6: Timeseries, mixing low selectivity index-capable filter (90% of rows match) + cursor filter
      "SELECT SUM(sumLongSequential), SUM(sumFloatNormal) FROM foo\n"
      + "WHERE dimSequential NOT LIKE '%3' AND maxLongUniform > 10",

      // 7: Timeseries, low selectivity toplevel filter (90%), high selectivity filtered aggregator (0.1%)
      "SELECT\n"
      + "  SUM(sumLongSequential) FILTER(WHERE dimSequential = '311'),\n"
      + "  SUM(sumFloatNormal)\n"
      + "FROM foo\n"
      + "WHERE dimSequential NOT LIKE '%3'",

      // 8: Timeseries, no toplevel filter, various filtered aggregators with clauses repeated.
      "SELECT\n"
      + "  SUM(sumLongSequential) FILTER(WHERE dimSequential = '311'),\n"
      + "  SUM(sumLongSequential) FILTER(WHERE dimSequential <> '311'),\n"
      + "  SUM(sumLongSequential) FILTER(WHERE dimSequential LIKE '%3'),\n"
      + "  SUM(sumLongSequential) FILTER(WHERE dimSequential NOT LIKE '%3'),\n"
      + "  SUM(sumLongSequential),\n"
      + "  SUM(sumFloatNormal) FILTER(WHERE dimSequential = '311'),\n"
      + "  SUM(sumFloatNormal) FILTER(WHERE dimSequential <> '311'),\n"
      + "  SUM(sumFloatNormal) FILTER(WHERE dimSequential LIKE '%3'),\n"
      + "  SUM(sumFloatNormal) FILTER(WHERE dimSequential NOT LIKE '%3'),\n"
      + "  SUM(sumFloatNormal),\n"
      + "  COUNT(*) FILTER(WHERE dimSequential = '311'),\n"
      + "  COUNT(*) FILTER(WHERE dimSequential <> '311'),\n"
      + "  COUNT(*) FILTER(WHERE dimSequential LIKE '%3'),\n"
      + "  COUNT(*) FILTER(WHERE dimSequential NOT LIKE '%3'),\n"
      + "  COUNT(*)\n"
      + "FROM foo",

      // 9: Timeseries, toplevel time filter, time-comparison filtered aggregators
      "SELECT\n"
      + "  SUM(sumLongSequential)\n"
      + "    FILTER(WHERE __time >= TIMESTAMP '2000-01-01 00:00:00' AND __time < TIMESTAMP '2000-01-01 12:00:00'),\n"
      + "  SUM(sumLongSequential)\n"
      + "    FILTER(WHERE __time >= TIMESTAMP '2000-01-01 12:00:00' AND __time < TIMESTAMP '2000-01-02 00:00:00')\n"
      + "FROM foo\n"
      + "WHERE __time >= TIMESTAMP '2000-01-01 00:00:00' AND __time < TIMESTAMP '2000-01-02 00:00:00'",

      // 10, 11: GroupBy two strings, unfiltered, unordered
      "SELECT dimSequential, dimZipf, SUM(sumLongSequential) FROM foo GROUP BY 1, 2",
      "SELECT dimSequential, dimZipf, SUM(sumLongSequential), COUNT(*) FROM foo GROUP BY 1, 2",

      // 12, 13, 14: GroupBy one string, unfiltered, various aggregator configurations
      "SELECT dimZipf FROM foo GROUP BY 1",
      "SELECT dimZipf, COUNT(*) FROM foo GROUP BY 1 ORDER BY COUNT(*) DESC",
      "SELECT dimZipf, SUM(sumLongSequential), COUNT(*) FROM foo GROUP BY 1 ORDER BY COUNT(*) DESC",

      // 15, 16: GroupBy long, unfiltered, unordered; with and without aggregators
      "SELECT maxLongUniform FROM foo GROUP BY 1",
      "SELECT maxLongUniform, SUM(sumLongSequential), COUNT(*) FROM foo GROUP BY 1",

      // 17, 18: GroupBy long, filter by long, unordered; with and without aggregators
      "SELECT maxLongUniform FROM foo WHERE maxLongUniform > 10 GROUP BY 1",
      "SELECT maxLongUniform, SUM(sumLongSequential), COUNT(*) FROM foo WHERE maxLongUniform > 10 GROUP BY 1",
      // 19: ultra mega union matrix
      "WITH matrix (dimZipf, dimSequential) AS (\n"
      + "  (\n"
      + "    SELECT '100', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '100'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '110', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '110'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '120', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '120'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '130', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '130'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '140', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '140'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '150', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '150'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '160', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '160'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '170', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '170'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '180', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '180'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '190', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '190'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '200', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '200'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '210', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '210'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '220', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '220'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '230', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '230'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '240', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '240'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '250', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '250'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '260', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '260'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '270', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '270'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '280', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '280'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '290', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '290'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '300', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '300'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '310', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '310'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '320', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '320'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '330', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '330'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '340', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '340'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '350', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '350'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '360', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '360'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '370', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '370'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '380', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '380'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT 'other', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE\n"
      + "      dimZipf NOT IN (\n"
      + "        '100', '110', '120', '130', '140', '150', '160', '170', '180', '190',\n"
      + "        '200', '210', '220', '230', '240', '250', '260', '270', '280', '290',\n"
      + "        '300', '310', '320', '330', '340', '350', '360', '370', '380'\n"
      + "      )\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + ")\n"
      + "SELECT * FROM matrix",

      // 20: GroupBy, doubles sketches
      "SELECT dimZipf, APPROX_QUANTILE_DS(sumFloatNormal, 0.5), DS_QUANTILES_SKETCH(maxLongUniform) "
      + "FROM foo "
      + "GROUP BY 1",

      // 21, 22: stringy stuff
      "SELECT dimSequential, dimZipf, SUM(sumLongSequential) FROM foo WHERE dimUniform NOT LIKE '%3' GROUP BY 1, 2",
      "SELECT dimZipf, SUM(sumLongSequential) FROM foo WHERE dimSequential = '311' GROUP BY 1 ORDER BY 1",
      // 23: full scan
      "SELECT * FROM foo",
      "SELECT * FROM foo WHERE dimSequential IN ('1', '2', '3', '4', '5', '10', '11', '20', '21', '23', '40', '50', '64', '70', '100')",
      "SELECT * FROM foo WHERE dimSequential > '10' AND dimSequential < '8500'",
      "SELECT dimSequential, dimZipf, SUM(sumLongSequential) FROM foo WHERE dimSequential IN ('1', '2', '3', '4', '5', '10', '11', '20', '21', '23', '40', '50', '64', '70', '100') GROUP BY 1, 2",
      "SELECT dimSequential, dimZipf, SUM(sumLongSequential) FROM foo WHERE dimSequential > '10' AND dimSequential < '8500' GROUP BY 1, 2",

      // 28, 29, 30, 31: Approximate count distinct of strings
      "SELECT APPROX_COUNT_DISTINCT_BUILTIN(dimZipf) FROM foo",
      "SELECT APPROX_COUNT_DISTINCT_DS_HLL(dimZipf) FROM foo",
      "SELECT APPROX_COUNT_DISTINCT_DS_HLL_UTF8(dimZipf) FROM foo",
      "SELECT APPROX_COUNT_DISTINCT_DS_THETA(dimZipf) FROM foo",
      // 32: LATEST aggregator long
      "SELECT LATEST(long1) FROM foo",
      // 33: LATEST aggregator double
      "SELECT LATEST(double4) FROM foo",
      // 34: LATEST aggregator double
      "SELECT LATEST(float3) FROM foo",
      // 35: LATEST aggregator double
      "SELECT LATEST(float3), LATEST(long1), LATEST(double4) FROM foo",
      // 36,37: filter numeric nulls
      "SELECT SUM(long5) FROM foo WHERE long5 IS NOT NULL",
      "SELECT string2, SUM(long5) FROM foo WHERE long5 IS NOT NULL GROUP BY 1",
      // 38: EARLIEST aggregator long
      "SELECT EARLIEST(long1) FROM foo",
      // 39: EARLIEST aggregator double
      "SELECT EARLIEST(double4) FROM foo",
      // 40: EARLIEST aggregator float
      "SELECT EARLIEST(float3) FROM foo"
  );

  @Param({"5000000"})
  private int rowsPerSegment;

  // Can be "false", "true", or "force"
  @Param({"force"})
  private String vectorize;

  // Can be "none" or "front-coded-N"
  @Param({"none", "front-coded-4"})
  private String stringEncoding;

  @Param({"28", "29", "30", "31"})
  private String query;

  // Can be STORAGE_MMAP, STORAGE_FRAME_ROW, or STORAGE_FRAME_COLUMNAR
  @Param({STORAGE_MMAP})
  private String storageType;

  private SqlEngine engine;

  @Nullable
  private PlannerFactory plannerFactory;
  private final Closer closer = Closer.create();

  @Setup(Level.Trial)
  public void setup()
  {
    final GeneratorSchemaInfo schemaInfo = GeneratorBasicSchemas.SCHEMA_MAP.get("basic");
    final DataSegment dataSegment = schemaInfo.makeSegmentDescriptor("foo");
    final SegmentGenerator segmentGenerator = closer.register(new SegmentGenerator());

    log.info("Starting benchmark setup using cacheDir[%s], rows[%,d].", segmentGenerator.getCacheDir(), rowsPerSegment);

    final QueryableIndex index = segmentGenerator.generate(
        dataSegment,
        schemaInfo,
        IndexSpec.builder().withStringDictionaryEncoding(getStringEncodingStrategy()).build(),
        Granularities.NONE,
        rowsPerSegment
    );

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
    if (stringEncoding.startsWith("front-coded")) {
      String[] split = stringEncoding.split("-");
      int bucketSize = Integer.parseInt(split[2]);
      return new StringEncodingStrategy.FrontCoded(bucketSize, FrontCodedIndexed.V1);
    } else {
      return new StringEncodingStrategy.Utf8();
    }
  }

  public static Pair<PlannerFactory, SqlEngine> createSqlSystem(
      final Map<DataSegment, QueryableIndex> segmentMap,
      final Map<String, LookupExtractor> lookupMap,
      @Nullable final String storageType,
      final Closer closer
  )
  {
    final QueryRunnerFactoryConglomerate conglomerate = QueryStackTests.createQueryRunnerFactoryConglomerate(closer);
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
        new AuthConfig()
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
          FrameTestUtil.adapterToFrameSegment(
              new QueryableIndexStorageAdapter(index),
              FrameType.ROW_BASED,
              descriptor.getId()
          )
      );
    } else if (STORAGE_FRAME_COLUMNAR.equals(storageType)) {
      walker.add(
          descriptor,
          FrameTestUtil.adapterToFrameSegment(
              new QueryableIndexStorageAdapter(index),
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

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void querySql(Blackhole blackhole)
  {
    final Map<String, Object> context = ImmutableMap.of(
        QueryContexts.VECTORIZE_KEY, vectorize,
        QueryContexts.VECTORIZE_VIRTUAL_COLUMNS_KEY, vectorize
    );
    final String sql = QUERIES.get(Integer.parseInt(query));
    try (final DruidPlanner planner = plannerFactory.createPlannerForTesting(engine, sql, context)) {
      final PlannerResult plannerResult = planner.plan();
      final Sequence<Object[]> resultSequence = plannerResult.run().getResults();
      final Object[] lastRow = resultSequence.accumulate(null, (accumulated, in) -> in);
      blackhole.consume(lastRow);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void planSql(Blackhole blackhole)
  {
    final Map<String, Object> context = ImmutableMap.of(
        QueryContexts.VECTORIZE_KEY, vectorize,
        QueryContexts.VECTORIZE_VIRTUAL_COLUMNS_KEY, vectorize
    );
    final String sql = QUERIES.get(Integer.parseInt(query));
    try (final DruidPlanner planner = plannerFactory.createPlannerForTesting(engine, sql, context)) {
      final PlannerResult plannerResult = planner.plan();
      blackhole.consume(plannerResult);
    }
  }
}
