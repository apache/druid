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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.AutoTypeColumnSchema;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.column.StringEncodingStrategy;
import org.apache.druid.segment.data.FrontCodedIndexed;
import org.apache.druid.segment.generator.GeneratorBasicSchemas;
import org.apache.druid.segment.generator.GeneratorSchemaInfo;
import org.apache.druid.segment.generator.SegmentGenerator;
import org.apache.druid.segment.transform.ExpressionTransform;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.sql.calcite.SqlVectorizedExpressionSanityTest;
import org.apache.druid.sql.calcite.planner.CalciteRulesManager;
import org.apache.druid.sql.calcite.planner.CatalogResolver;
import org.apache.druid.sql.calcite.planner.DruidPlanner;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.planner.PlannerResult;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.schema.DruidSchemaCatalog;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class SqlNestedDataBenchmark
{
  private static final Logger log = new Logger(SqlNestedDataBenchmark.class);

  static {
    NullHandling.initializeForTests();
    ExpressionProcessing.initializeForTests();
  }

  private static final DruidProcessingConfig PROCESSING_CONFIG = new DruidProcessingConfig()
  {
    @Override
    public int intermediateComputeSizeBytes()
    {
      return 512 * 1024 * 1024;
    }

    @Override
    public int getNumMergeBuffers()
    {
      return 3;
    }

    @Override
    public int getNumThreads()
    {
      return 1;
    }

    @Override
    public String getFormatString()
    {
      return "benchmarks-processing-%s";
    }
  };


  private static final List<String> QUERIES = ImmutableList.of(
      // ===========================
      // non-nested reference queries
      // ===========================
      // 0,1: timeseries, 1 columns
      "SELECT SUM(long1) FROM foo",
      "SELECT SUM(JSON_VALUE(nested, '$.long1' RETURNING BIGINT)) FROM foo",
      // 2,3: timeseries, 2 columns
      "SELECT SUM(long1), SUM(long2) FROM foo",
      "SELECT SUM(JSON_VALUE(nested, '$.long1' RETURNING BIGINT)), SUM(JSON_VALUE(nested, '$.nesteder.long2' RETURNING BIGINT)) FROM foo",
      // 4,5: timeseries, 3 columns
      "SELECT SUM(long1), SUM(long2), SUM(double3) FROM foo",
      "SELECT SUM(JSON_VALUE(nested, '$.long1' RETURNING BIGINT)), SUM(JSON_VALUE(nested, '$.nesteder.long2' RETURNING BIGINT)), SUM(JSON_VALUE(nested, '$.nesteder.double3' RETURNING DOUBLE)) FROM foo",
      // 6,7: group by string with 1 agg
      "SELECT string1, SUM(long1) FROM foo GROUP BY 1 ORDER BY 2",
      "SELECT JSON_VALUE(nested, '$.nesteder.string1'), SUM(JSON_VALUE(nested, '$.long1' RETURNING BIGINT)) FROM foo GROUP BY 1 ORDER BY 2",
      // 8,9: group by string with 2 agg
      "SELECT string1, SUM(long1), SUM(double3) FROM foo GROUP BY 1 ORDER BY 2",
      "SELECT JSON_VALUE(nested, '$.nesteder.string1'), SUM(JSON_VALUE(nested, '$.long1' RETURNING BIGINT)), SUM(JSON_VALUE(nested, '$.nesteder.double3' RETURNING DOUBLE)) FROM foo GROUP BY 1 ORDER BY 2",
      // 10,11: time-series filter string
      "SELECT SUM(long1) FROM foo WHERE string1 = '10000' OR string1 = '1000'",
      "SELECT SUM(JSON_VALUE(nested, '$.long1' RETURNING BIGINT)) FROM foo WHERE JSON_VALUE(nested, '$.nesteder.string1') = '10000' OR JSON_VALUE(nested, '$.nesteder.string1') = '1000'",
      // 12,13: time-series filter long
      "SELECT SUM(long1) FROM foo WHERE long2 = 10000 OR long2 = 1000",
      "SELECT SUM(JSON_VALUE(nested, '$.long1' RETURNING BIGINT)) FROM foo WHERE JSON_VALUE(nested, '$.nesteder.long2' RETURNING BIGINT) = 10000 OR JSON_VALUE(nested, '$.nesteder.long2' RETURNING BIGINT) = 1000",
      // 14,15: time-series filter double
      "SELECT SUM(long1) FROM foo WHERE double3 < 10000.0 AND double3 > 1000.0",
      "SELECT SUM(JSON_VALUE(nested, '$.long1' RETURNING BIGINT)) FROM foo WHERE JSON_VALUE(nested, '$.nesteder.double3' RETURNING DOUBLE) < 10000.0 AND JSON_VALUE(nested, '$.nesteder.double3' RETURNING DOUBLE) > 1000.0",
      // 16,17: group by long filter by string
      "SELECT long1, SUM(double3) FROM foo WHERE string1 = '10000' OR string1 = '1000' GROUP BY 1 ORDER BY 2",
      "SELECT JSON_VALUE(nested, '$.long1' RETURNING BIGINT), SUM(JSON_VALUE(nested, '$.nesteder.double3' RETURNING DOUBLE)) FROM foo WHERE JSON_VALUE(nested, '$.nesteder.string1') = '10000' OR JSON_VALUE(nested, '$.nesteder.string1') = '1000' GROUP BY 1 ORDER BY 2",
      // 18,19: group by string filter by long
      "SELECT string1, SUM(double3) FROM foo WHERE long2 < 10000 AND long2 > 1000 GROUP BY 1 ORDER BY 2",
      "SELECT JSON_VALUE(nested, '$.nesteder.string1'), SUM(JSON_VALUE(nested, '$.nesteder.double3' RETURNING DOUBLE)) FROM foo WHERE JSON_VALUE(nested, '$.nesteder.long2' RETURNING BIGINT) < 10000 AND JSON_VALUE(nested, '$.nesteder.long2' RETURNING BIGINT) > 1000 GROUP BY 1 ORDER BY 2",
      // 20,21: group by string filter by double
      "SELECT string1, SUM(double3) FROM foo WHERE double3 < 10000.0 AND double3 > 1000.0 GROUP BY 1 ORDER BY 2",
      "SELECT JSON_VALUE(nested, '$.nesteder.string1'), SUM(JSON_VALUE(nested, '$.nesteder.double3' RETURNING DOUBLE)) FROM foo WHERE JSON_VALUE(nested, '$.nesteder.double3' RETURNING DOUBLE) < 10000.0 AND JSON_VALUE(nested, '$.nesteder.double3' RETURNING DOUBLE) > 1000.0 GROUP BY 1 ORDER BY 2",
      // 22, 23:
      "SELECT long2 FROM foo WHERE long2 IN (1, 19, 21, 23, 25, 26, 46)",
      "SELECT JSON_VALUE(nested, '$.nesteder.long2' RETURNING BIGINT) FROM foo WHERE JSON_VALUE(nested, '$.nesteder.long2' RETURNING BIGINT) IN (1, 19, 21, 23, 25, 26, 46)",
      // 24, 25
      "SELECT long2 FROM foo WHERE long2 IN (1, 19, 21, 23, 25, 26, 46) GROUP BY 1",
      "SELECT JSON_VALUE(nested, '$.nesteder.long2' RETURNING BIGINT) FROM foo WHERE JSON_VALUE(nested, '$.nesteder.long2' RETURNING BIGINT) IN (1, 19, 21, 23, 25, 26, 46) GROUP BY 1",
      // 26, 27
      "SELECT SUM(long1) FROM foo WHERE double3 < 1005.0 AND double3 > 1000.0",
      "SELECT SUM(JSON_VALUE(nested, '$.long1' RETURNING BIGINT)) FROM foo WHERE JSON_VALUE(nested, '$.nesteder.double3' RETURNING DOUBLE) < 1005.0 AND JSON_VALUE(nested, '$.nesteder.double3' RETURNING DOUBLE) > 1000.0",
      // 28, 29
      "SELECT SUM(long1) FROM foo WHERE double3 < 2000.0 AND double3 > 1000.0",
      "SELECT SUM(JSON_VALUE(nested, '$.long1' RETURNING BIGINT)) FROM foo WHERE JSON_VALUE(nested, '$.nesteder.double3' RETURNING DOUBLE) < 2000.0 AND JSON_VALUE(nested, '$.nesteder.double3' RETURNING DOUBLE) > 1000.0",
      // 30, 31
      "SELECT SUM(long1) FROM foo WHERE double3 < 3000.0 AND double3 > 1000.0",
      "SELECT SUM(JSON_VALUE(nested, '$.long1' RETURNING BIGINT)) FROM foo WHERE JSON_VALUE(nested, '$.nesteder.double3' RETURNING DOUBLE) < 3000.0 AND JSON_VALUE(nested, '$.nesteder.double3' RETURNING DOUBLE) > 1000.0",
      // 32,33
      "SELECT SUM(long1) FROM foo WHERE double3 < 5000.0 AND double3 > 1000.0",
      "SELECT SUM(JSON_VALUE(nested, '$.long1' RETURNING BIGINT)) FROM foo WHERE JSON_VALUE(nested, '$.nesteder.double3' RETURNING DOUBLE) < 5000.0 AND JSON_VALUE(nested, '$.nesteder.double3' RETURNING DOUBLE) > 1000.0",
      // 34,35 smaller cardinality like range filter
      "SELECT SUM(long1) FROM foo WHERE string1 LIKE '1%'",
      "SELECT SUM(JSON_VALUE(nested, '$.long1' RETURNING BIGINT)) FROM foo WHERE JSON_VALUE(nested, '$.nesteder.string1') LIKE '1%'",
      // 36,37 smaller cardinality like predicate filter
      "SELECT SUM(long1) FROM foo WHERE string1 LIKE '%1%'",
      "SELECT SUM(JSON_VALUE(nested, '$.long1' RETURNING BIGINT)) FROM foo WHERE JSON_VALUE(nested, '$.nesteder.string1') LIKE '%1%'",
      // 38-39 moderate cardinality like range
      "SELECT SUM(long1) FROM foo WHERE string5 LIKE '1%'",
      "SELECT SUM(JSON_VALUE(nested, '$.long1' RETURNING BIGINT)) FROM foo WHERE JSON_VALUE(nested, '$.nesteder.string5') LIKE '1%'",
      // 40, 41 big cardinality lex range
      "SELECT SUM(long1) FROM foo WHERE string5 > '1'",
      "SELECT SUM(JSON_VALUE(nested, '$.long1' RETURNING BIGINT)) FROM foo WHERE JSON_VALUE(nested, '$.nesteder.string5') > '1'",
      // 42, 43 big cardinality like predicate filter
      "SELECT SUM(long1) FROM foo WHERE string5 LIKE '%1%'",
      "SELECT SUM(JSON_VALUE(nested, '$.long1' RETURNING BIGINT)) FROM foo WHERE JSON_VALUE(nested, '$.nesteder.string5') LIKE '%1%'",
      // 44, 45 big cardinality like filter + selector filter
      "SELECT SUM(long1) FROM foo WHERE string5 LIKE '%1%' AND string1 = '1000'",
      "SELECT SUM(JSON_VALUE(nested, '$.long1' RETURNING BIGINT)) FROM foo WHERE JSON_VALUE(nested, '$.nesteder.string5') LIKE '%1%' AND JSON_VALUE(nested, '$.nesteder.string1') = '1000'"
  );

  @Param({"5000000"})
  private int rowsPerSegment;

  @Param({
      "false",
      "force"
  })
  private String vectorize;

  @Param({
      "none",
      "front-coded-4",
      "front-coded-16"
  })
  private String stringEncoding;

  @Param({
      "0",
      "1",
      "2",
      "3",
      "4",
      "5",
      "6",
      "7",
      "8",
      "9",
      "10",
      "11",
      "12",
      "13",
      "14",
      "15",
      "16",
      "17",
      "18",
      "19",
      "20",
      "21",
      "22",
      "23",
      "24",
      "25",
      "26",
      "27",
      "28",
      "29",
      "30",
      "31",
      "32",
      "33",
      "34",
      "35",
      "36",
      "37",
      "38",
      "39",
      "40",
      "41",
      "42",
      "43",
      "44",
      "45"
  })
  private String query;

  private SqlEngine engine;
  @Nullable
  private PlannerFactory plannerFactory;
  private Closer closer = Closer.create();

  @Setup(Level.Trial)
  public void setup()
  {
    final GeneratorSchemaInfo schemaInfo = GeneratorBasicSchemas.SCHEMA_MAP.get("expression-testbench");

    final DataSegment dataSegment = DataSegment.builder()
                                               .dataSource("foo")
                                               .interval(schemaInfo.getDataInterval())
                                               .version("1")
                                               .shardSpec(new LinearShardSpec(0))
                                               .size(0)
                                               .build();


    final PlannerConfig plannerConfig = new PlannerConfig();

    final SegmentGenerator segmentGenerator = closer.register(new SegmentGenerator());
    log.info("Starting benchmark setup using cacheDir[%s], rows[%,d].", segmentGenerator.getCacheDir(), rowsPerSegment);

    TransformSpec transformSpec = new TransformSpec(
        null,
        ImmutableList.of(
            new ExpressionTransform(
                "nested",
                "json_object('long1', long1, 'nesteder', json_object('string1', string1, 'long2', long2, 'double3',double3, 'string5', string5))",
                TestExprMacroTable.INSTANCE
            )
        )
    );
    List<DimensionSchema> dims = ImmutableList.<DimensionSchema>builder()
                                              .addAll(schemaInfo.getDimensionsSpec().getDimensions())
                                              .add(new AutoTypeColumnSchema("nested", null))
                                              .build();
    DimensionsSpec dimsSpec = new DimensionsSpec(dims);


    StringEncodingStrategy encodingStrategy;
    if (stringEncoding.startsWith("front-coded")) {
      String[] split = stringEncoding.split("-");
      int bucketSize = Integer.parseInt(split[2]);
      encodingStrategy = new StringEncodingStrategy.FrontCoded(bucketSize, FrontCodedIndexed.V1);
    } else {
      encodingStrategy = new StringEncodingStrategy.Utf8();
    }
    final QueryableIndex index = segmentGenerator.generate(
        dataSegment,
        schemaInfo,
        dimsSpec,
        transformSpec,
        IndexSpec.builder().withStringDictionaryEncoding(encodingStrategy).build(),
        Granularities.NONE,
        rowsPerSegment
    );

    final QueryRunnerFactoryConglomerate conglomerate = QueryStackTests.createQueryRunnerFactoryConglomerate(
        closer,
        PROCESSING_CONFIG
    );

    final SpecificSegmentsQuerySegmentWalker walker = SpecificSegmentsQuerySegmentWalker.createWalker(conglomerate).add(
        dataSegment,
        index
    );
    closer.register(walker);

    final DruidSchemaCatalog rootSchema =
        CalciteTests.createMockRootSchema(conglomerate, walker, plannerConfig, AuthTestUtils.TEST_AUTHORIZER_MAPPER);
    engine = CalciteTests.createMockSqlEngine(walker, conglomerate);
    plannerFactory = new PlannerFactory(
        rootSchema,
        CalciteTests.createOperatorTable(),
        CalciteTests.createExprMacroTable(),
        plannerConfig,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        CalciteTests.getJsonMapper(),
        CalciteTests.DRUID_SCHEMA_NAME,
        new CalciteRulesManager(ImmutableSet.of()),
        CalciteTests.createJoinableFactoryWrapper(),
        CatalogResolver.NULL_RESOLVER,
        new AuthConfig()
    );

    try {
      SqlVectorizedExpressionSanityTest.sanityTestVectorizedSqlQueries(
          plannerFactory,
          QUERIES.get(Integer.parseInt(query))
      );
    }
    catch (Throwable ignored) {
      // the show must go on
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
}
