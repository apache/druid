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
import com.google.common.collect.ImmutableSet;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.guice.NestedDataModule;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.AutoTypeColumnSchema;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.column.StringEncodingStrategy;
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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class SqlGroupByBenchmark
{
  static {
    NullHandling.initializeForTests();
    ExpressionProcessing.initializeForTests();
    NestedDataModule.registerHandlersAndSerde();
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

  @Param({
      "string-Sequential-100_000",
      "string-Sequential-10_000_000",
      // "string-Sequential-1_000_000_000",
      "string-ZipF-1_000_000",
      "string-Uniform-1_000_000",

      "multi-string-Sequential-100_000",
      "multi-string-Sequential-10_000_000",
      // "multi-string-Sequential-1_000_000_000",
      "multi-string-ZipF-1_000_000",
      "multi-string-Uniform-1_000_000",

      "long-Sequential-100_000",
      "long-Sequential-10_000_000",
      // "long-Sequential-1_000_000_000",
      "long-ZipF-1_000_000",
      "long-Uniform-1_000_000",

      "double-ZipF-1_000_000",
      "double-Uniform-1_000_000",

      "float-ZipF-1_000_000",
      "float-Uniform-1_000_000",

      "stringArray-Sequential-100_000",
      "stringArray-Sequential-3_000_000",
      // "stringArray-Sequential-1_000_000_000",
      "stringArray-ZipF-1_000_000",
      "stringArray-Uniform-1_000_000",

      "longArray-Sequential-100_000",
      "longArray-Sequential-3_000_000",
      // "longArray-Sequential-1_000_000_000",
      "longArray-ZipF-1_000_000",
      "longArray-Uniform-1_000_000",

      "nested-Sequential-100_000",
      "nested-Sequential-3_000_000",
      // "nested-Sequential-1_000_000_000",
      "nested-ZipF-1_000_000",
      "nested-Uniform-1_000_000",
  })
  private String groupingDimension;

  private SqlEngine engine;
  @Nullable
  private PlannerFactory plannerFactory;
  private Closer closer = Closer.create();

  @Setup(Level.Trial)
  public void setup()
  {
    final GeneratorSchemaInfo schemaInfo = GeneratorBasicSchemas.SCHEMA_MAP.get("groupBy-testbench");

    final DataSegment dataSegment = DataSegment.builder()
                                               .dataSource("foo")
                                               .interval(schemaInfo.getDataInterval())
                                               .version("1")
                                               .shardSpec(new LinearShardSpec(0))
                                               .size(0)
                                               .build();
    final DataSegment dataSegment2 = DataSegment.builder()
                                               .dataSource("foo")
                                               .interval(schemaInfo.getDataInterval())
                                               .version("1")
                                               .shardSpec(new LinearShardSpec(1))
                                               .size(0)
                                               .build();


    final PlannerConfig plannerConfig = new PlannerConfig();

    String columnCardinalityWithUnderscores = groupingDimension.substring(groupingDimension.lastIndexOf('-') + 1);
    int rowsPerSegment = Integer.parseInt(StringUtils.replace(columnCardinalityWithUnderscores, "_", ""));

    final SegmentGenerator segmentGenerator = closer.register(new SegmentGenerator());

    TransformSpec transformSpec = new TransformSpec(
        null,
        ImmutableList.of(
            // string array dims
            new ExpressionTransform(
                "stringArray-Sequential-100_000",
                "array(\"string-Sequential-100_000\")",
                TestExprMacroTable.INSTANCE
            ),
            new ExpressionTransform(
                "stringArray-Sequential-3_000_000",
                "array(\"string-Sequential-10_000_000\")",
                TestExprMacroTable.INSTANCE
            ),
            /*
            new ExpressionTransform(
                "stringArray-Sequential-1_000_000_000",
                "array(\"string-Sequential-1_000_000_000\")",
                TestExprMacroTable.INSTANCE
            ),*/
            new ExpressionTransform(
                "stringArray-ZipF-1_000_000",
                "array(\"string-ZipF-1_000_000\")",
                TestExprMacroTable.INSTANCE
            ),
            new ExpressionTransform(
                "stringArray-Uniform-1_000_000",
                "array(\"string-Uniform-1_000_000\")",
                TestExprMacroTable.INSTANCE
            ),

            // long array dims
            new ExpressionTransform(
                "longArray-Sequential-100_000",
                "array(\"long-Sequential-100_000\")",
                TestExprMacroTable.INSTANCE
            ),
            new ExpressionTransform(
                "longArray-Sequential-3_000_000",
                "array(\"long-Sequential-10_000_000\")",
                TestExprMacroTable.INSTANCE
            ),
            /*
            new ExpressionTransform(
                "longArray-Sequential-1_000_000_000",
                "array(\"long-Sequential-1_000_000_000\")",
                TestExprMacroTable.INSTANCE
            ),*/
            new ExpressionTransform(
                "longArray-ZipF-1_000_000",
                "array(\"long-ZipF-1_000_000\")",
                TestExprMacroTable.INSTANCE
            ),
            new ExpressionTransform(
                "longArray-Uniform-1_000_000",
                "array(\"long-Uniform-1_000_000\")",
                TestExprMacroTable.INSTANCE
            ),

            // nested complex json dim
            new ExpressionTransform(
                "nested-Sequential-100_000",
                "json_object('long1', \"long-Sequential-100_000\", 'nesteder', json_object('long1', \"long-Sequential-100_000\"))",
                TestExprMacroTable.INSTANCE
            ),
            new ExpressionTransform(
                "nested-Sequential-3_000_000",
                "json_object('long1', \"long-Sequential-10_000_000\", 'nesteder', json_object('long1', \"long-Sequential-10_000_000\"))",
                TestExprMacroTable.INSTANCE
            ),
            /*new ExpressionTransform(
                "nested-Sequential-1_000_000_000",
                "json_object('long1', \"long-Sequential-1_000_000_000\", 'nesteder', json_object('long1', \"long-Sequential-1_000_000_000\"))",
                TestExprMacroTable.INSTANCE
            ),*/
            new ExpressionTransform(
                "nested-ZipF-1_000_000",
                "json_object('long1', \"long-ZipF-1_000_000\", 'nesteder', json_object('long1', \"long-ZipF-1_000_000\"))",
                TestExprMacroTable.INSTANCE
            ),
            new ExpressionTransform(
                "nested-Uniform-1_000_000",
                "json_object('long1', \"long-Uniform-1_000_000\", 'nesteder', json_object('long1', \"long-Uniform-1_000_000\"))",
                TestExprMacroTable.INSTANCE
            )
        )
    );

    List<DimensionSchema> columnSchemas = schemaInfo.getDimensionsSpec()
                                                    .getDimensions()
                                                    .stream()
                                                    .map(x -> new AutoTypeColumnSchema(x.getName(), null))
                                                    .collect(Collectors.toList());

    List<DimensionSchema> transformSchemas = transformSpec
        .getTransforms()
        .stream()
        .map(
            transform -> new AutoTypeColumnSchema(transform.getName(), null)
        )
        .collect(Collectors.toList());



    final QueryableIndex index = segmentGenerator.generate(
        dataSegment,
        schemaInfo,
        DimensionsSpec.builder()
                      .setDimensions(ImmutableList.<DimensionSchema>builder()
                                                  .addAll(columnSchemas)
                                                  .addAll(transformSchemas)
                                                  .build()
                      )
                      .build(),
        transformSpec,
        IndexSpec.builder().withStringDictionaryEncoding(new StringEncodingStrategy.Utf8()).build(),
        Granularities.NONE,
        rowsPerSegment
    );

    final QueryRunnerFactoryConglomerate conglomerate = QueryStackTests.createQueryRunnerFactoryConglomerate(
        closer,
        PROCESSING_CONFIG
    );

    final SpecificSegmentsQuerySegmentWalker walker = SpecificSegmentsQuerySegmentWalker.createWalker(conglomerate)
                                                                                        .add(dataSegment, index)
                                                                                        .add(dataSegment2, index);
    closer.register(walker);

    // Hacky and pollutes global namespace, but it is fine since benchmarks are run in isolation. Wasn't able
    // to work up a cleaner way of doing it by modifying the injector.
    CalciteTests.getJsonMapper().registerModules(NestedDataModule.getJacksonModulesList());

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
          sqlQuery(groupingDimension)
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
    final String sql = sqlQuery(groupingDimension);
    try (final DruidPlanner planner = plannerFactory.createPlannerForTesting(engine, sql, Collections.emptyMap())) {
      final PlannerResult plannerResult = planner.plan();
      final Sequence<Object[]> resultSequence = plannerResult.run().getResults();
      final Object[] lastRow = resultSequence.accumulate(null, (accumulated, in) -> in);
      blackhole.consume(lastRow);
    }
  }

  private static String sqlQuery(String groupingDimension)
  {
    return StringUtils.format("SELECT \"%s\", COUNT(*) FROM foo GROUP BY 1", groupingDimension);
  }
}
