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

package org.apache.druid.sql.calcite;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.druid.com.google.common.collect.ImmutableList;
import org.apache.druid.com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.generator.GeneratorBasicSchemas;
import org.apache.druid.segment.generator.GeneratorSchemaInfo;
import org.apache.druid.segment.generator.SegmentGenerator;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.DruidPlanner;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.planner.PlannerResult;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RunWith(Parameterized.class)
public class SqlVectorizedExpressionSanityTest extends InitializedNullHandlingTest
{
  private static final Logger log = new Logger(SqlVectorizedExpressionSanityTest.class);

  private static final List<String> QUERIES = ImmutableList.of(
      "SELECT SUM(long1 * long2) FROM foo",
      "SELECT SUM((long1 * long2) / double1) FROM foo",
      "SELECT SUM(float3 + ((long1 * long4)/double1)) FROM foo",
      "SELECT SUM(long5 - (float3 + ((long1 * long4)/double1))) FROM foo",
      "SELECT cos(double2) FROM foo",
      "SELECT SUM(-long4) FROM foo",
      "SELECT SUM(PARSE_LONG(string1)) FROM foo",
      "SELECT SUM(PARSE_LONG(string3)) FROM foo",
      "SELECT TIME_FLOOR(__time, 'PT1H'), string2, SUM(long1 * double4) FROM foo GROUP BY 1,2 ORDER BY 3",
      "SELECT TIME_FLOOR(__time, 'PT1H'), string2, SUM(long1 * double4) FROM foo WHERE string2 = '10' GROUP BY 1,2 ORDER BY 3",
      "SELECT TIME_FLOOR(__time, 'PT1H'), SUM(long1 * long4) FROM foo GROUP BY 1 ORDER BY 1",
      "SELECT TIME_FLOOR(__time, 'PT1H'), SUM(long1 * long4) FROM foo GROUP BY 1 ORDER BY 2",
      "SELECT TIME_FLOOR(TIMESTAMPADD(DAY, -1, __time), 'PT1H'), SUM(long1 * long4) FROM foo GROUP BY 1 ORDER BY 1",
      "SELECT (long1 * long2), SUM(double1) FROM foo GROUP BY 1 ORDER BY 2",
      "SELECT string2, SUM(long1 * long4) FROM foo GROUP BY 1 ORDER BY 2"
  );

  private static final int ROWS_PER_SEGMENT = 100_000;

  private static QueryableIndex INDEX;
  private static Closer CLOSER;
  private static QueryRunnerFactoryConglomerate CONGLOMERATE;
  private static SpecificSegmentsQuerySegmentWalker WALKER;
  @Nullable
  private static PlannerFactory PLANNER_FACTORY;

  @BeforeClass
  public static void setupClass()
  {
    Calcites.setSystemProperties();
    CLOSER = Closer.create();

    final GeneratorSchemaInfo schemaInfo = GeneratorBasicSchemas.SCHEMA_MAP.get("expression-testbench");

    final DataSegment dataSegment = DataSegment.builder()
                                               .dataSource("foo")
                                               .interval(schemaInfo.getDataInterval())
                                               .version("1")
                                               .shardSpec(new LinearShardSpec(0))
                                               .size(0)
                                               .build();

    final SegmentGenerator segmentGenerator = CLOSER.register(new SegmentGenerator());
    INDEX = CLOSER.register(
        segmentGenerator.generate(dataSegment, schemaInfo, Granularities.HOUR, ROWS_PER_SEGMENT)
    );
    CONGLOMERATE = QueryStackTests.createQueryRunnerFactoryConglomerate(CLOSER);

    WALKER = new SpecificSegmentsQuerySegmentWalker(CONGLOMERATE).add(
        dataSegment,
        INDEX
    );
    CLOSER.register(WALKER);

    final PlannerConfig plannerConfig = new PlannerConfig();
    final SchemaPlus rootSchema =
        CalciteTests.createMockRootSchema(CONGLOMERATE, WALKER, plannerConfig, AuthTestUtils.TEST_AUTHORIZER_MAPPER);
    PLANNER_FACTORY = new PlannerFactory(
        rootSchema,
        CalciteTests.createMockQueryLifecycleFactory(WALKER, CONGLOMERATE),
        CalciteTests.createOperatorTable(),
        CalciteTests.createExprMacroTable(),
        plannerConfig,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        CalciteTests.getJsonMapper(),
        CalciteTests.DRUID_SCHEMA_NAME
    );
  }

  @AfterClass
  public static void teardownClass() throws IOException
  {
    CLOSER.close();
  }

  @Parameterized.Parameters(name = "query = {0}")
  public static Iterable<?> constructorFeeder()
  {
    return QUERIES.stream().map(x -> new Object[]{x}).collect(Collectors.toList());
  }

  private String query;

  public SqlVectorizedExpressionSanityTest(String query)
  {
    this.query = query;
  }

  @Test
  public void testQuery() throws SqlParseException, RelConversionException, ValidationException
  {
    sanityTestVectorizedSqlQueries(PLANNER_FACTORY, query);
  }

  public static void sanityTestVectorizedSqlQueries(PlannerFactory plannerFactory, String query)
      throws ValidationException, RelConversionException, SqlParseException
  {
    final Map<String, Object> vector = ImmutableMap.of(
        QueryContexts.VECTORIZE_KEY, "force",
        QueryContexts.VECTORIZE_VIRTUAL_COLUMNS_KEY, "force"
    );
    final Map<String, Object> nonvector = ImmutableMap.of(
        QueryContexts.VECTORIZE_KEY, "false",
        QueryContexts.VECTORIZE_VIRTUAL_COLUMNS_KEY, "false"
    );

    try (
        final DruidPlanner vectorPlanner = plannerFactory.createPlannerForTesting(vector, query);
        final DruidPlanner nonVectorPlanner = plannerFactory.createPlannerForTesting(nonvector, query)
    ) {
      final PlannerResult vectorPlan = vectorPlanner.plan(query);
      final PlannerResult nonVectorPlan = nonVectorPlanner.plan(query);
      final Sequence<Object[]> vectorSequence = vectorPlan.run();
      final Sequence<Object[]> nonVectorSequence = nonVectorPlan.run();
      Yielder<Object[]> vectorizedYielder = Yielders.each(vectorSequence);
      Yielder<Object[]> nonVectorizedYielder = Yielders.each(nonVectorSequence);
      int row = 0;
      int misMatch = 0;
      while (!vectorizedYielder.isDone() && !nonVectorizedYielder.isDone()) {
        Object[] vectorGet = vectorizedYielder.get();
        Object[] nonVectorizedGet = nonVectorizedYielder.get();

        try {
          Assert.assertEquals(vectorGet.length, nonVectorizedGet.length);
          for (int i = 0; i < vectorGet.length; i++) {
            Object nonVectorObject = nonVectorizedGet[i];
            Object vectorObject = vectorGet[i];
            if (vectorObject instanceof Float || vectorObject instanceof Double) {
              Assert.assertEquals(
                  StringUtils.format(
                      "Double results differed at row %s (%s : %s)",
                      row,
                      nonVectorObject,
                      vectorObject
                  ),
                  ((Double) nonVectorObject).doubleValue(),
                  ((Double) vectorObject).doubleValue(),
                  0.01
              );
            } else {
              Assert.assertEquals(
                  StringUtils.format(
                      "Results differed at row %s (%s : %s)",
                      row,
                      nonVectorObject,
                      vectorObject
                  ),
                  nonVectorObject,
                  vectorObject
              );
            }
          }
        }
        catch (Throwable t) {
          log.warn(t.getMessage());
          misMatch++;
        }
        vectorizedYielder = vectorizedYielder.next(vectorGet);
        nonVectorizedYielder = nonVectorizedYielder.next(nonVectorizedGet);
        row++;
      }
      Assert.assertEquals("Expected no mismatched results", 0, misMatch);
      Assert.assertTrue(vectorizedYielder.isDone());
      Assert.assertTrue(nonVectorizedYielder.isDone());
    }
  }
}
