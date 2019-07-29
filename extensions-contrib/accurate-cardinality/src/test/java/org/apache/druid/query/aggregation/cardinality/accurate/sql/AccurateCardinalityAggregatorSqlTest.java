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

package org.apache.druid.query.aggregation.cardinality.accurate.sql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.tools.ValidationException;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.TimeAndDimsParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.cardinality.accurate.AccurateCardinalityModule;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.server.security.NoopEscalator;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.planner.DruidPlanner;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.planner.PlannerResult;
import org.apache.druid.sql.calcite.schema.DruidSchema;
import org.apache.druid.sql.calcite.schema.SystemSchema;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.QueryLogHook;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class AccurateCardinalityAggregatorSqlTest
{
  private static final String DATA_SOURCE = "foo";
  private static final String TIMESTAMP_COLUMN = "t";
  private static final Map<String, Object> QUERY_CONTEXT_DEFAULT = ImmutableMap.of(
      "remoterAddr", "127.0.0.1", "realIP", "127.0.0.1"
  );

  private static final List<DimensionSchema> dimensions = new ArrayList<DimensionSchema>();

  private static QueryRunnerFactoryConglomerate conglomerate;
  private static Closer resourceCloser;

  static {
    dimensions.add(new LongDimensionSchema("clientid"));
    dimensions.addAll(DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim1", "dim2")));
  }

  private static final InputRowParser<Map<String, Object>> PARSER = new MapInputRowParser(
      new TimeAndDimsParseSpec(
          new TimestampSpec(TIMESTAMP_COLUMN, "iso", null),
          new DimensionsSpec(
              dimensions,
              null,
              null
          )
      )
  );

  public static InputRow createRow(final ImmutableMap<String, ?> map)
  {
    return PARSER.parseBatch((Map<String, Object>) map).get(0);
  }

  public static final List<InputRow> ROWS1 = ImmutableList.of(
      createRow(
          ImmutableMap.of("t", "2000-01-01", "m1", "1.0", "clientid", 1000L, "dim1", "", "dim2", ImmutableList.of("a"))
      ),
      createRow(
          ImmutableMap.of(
              "t",
              "2000-01-02",
              "m1",
              "2.0",
              "clientid",
              1001L,
              "dim1",
              "10.1",
              "dim2",
              ImmutableList.of()
          )
      ),
      createRow(
          ImmutableMap.of("t", "2000-01-03", "m1", "3.0", "clientid", 1001L, "dim1", "2", "dim2", ImmutableList.of(""))
      ),
      createRow(
          ImmutableMap.of(
              "t",
              "2001-01-01",
              "m1",
              "4.0",
              "clientid",
              1002L,
              "dim1",
              "1",
              "dim2",
              ImmutableList.of("a")
          )
      ),
      createRow(
          ImmutableMap.of(
              "t",
              "2001-01-02",
              "m1",
              "5.0",
              "clientid",
              1003L,
              "dim1",
              "def",
              "dim2",
              ImmutableList.of("abc")
          )
      ),
      createRow(
          ImmutableMap.of("t", "2001-01-03", "m1", "6.0", "clientid", 1003L, "dim1", "abc")
      )
  );

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public QueryLogHook queryLogHook = QueryLogHook.create();

  private SpecificSegmentsQuerySegmentWalker walker;
  private PlannerFactory plannerFactory;

  @BeforeClass
  public static void setUpClass()
  {
    final Pair<QueryRunnerFactoryConglomerate, Closer> conglomerateCloserPair = CalciteTests
        .createQueryRunnerFactoryConglomerate();
    conglomerate = conglomerateCloserPair.lhs;
    resourceCloser = conglomerateCloserPair.rhs;
  }

  @AfterClass
  public static void tearDownClass() throws IOException
  {
    resourceCloser.close();
  }

  @Before
  public void setUp() throws Exception
  {
    Calcites.setSystemProperties();

    // Note: this is needed in order to properly register the serde for Histogram.
    new AccurateCardinalityModule().configure(null);

    final QueryableIndex index = IndexBuilder.create()
                                             .tmpDir(temporaryFolder.newFolder())
                                             .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                                             .schema(
                                                 new IncrementalIndexSchema.Builder()
                                                     .withDimensionsSpec(
                                                         new DimensionsSpec(
                                                             ImmutableList.of(new LongDimensionSchema("clientid")),
                                                             null,
                                                             null
                                                         )
                                                     )
                                                     .withMetrics(
                                                         new CountAggregatorFactory("cnt"),
                                                         new DoubleSumAggregatorFactory("m1", "m1")
                                                     )
                                                     .withRollup(true)
                                                     .build()
                                             )
                                             .rows(ROWS1)
                                             .buildMMappedIndex();

    walker = new SpecificSegmentsQuerySegmentWalker(conglomerate).add(
        DataSegment.builder()
                   .dataSource(DATA_SOURCE)
                   .interval(index.getDataInterval())
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .build(),
        index
    );

    final PlannerConfig plannerConfig = new PlannerConfig();
    final DruidSchema druidSchema = CalciteTests.createMockSchema(
        conglomerate,
        walker,
        plannerConfig
    );
    final SystemSchema systemSchema = CalciteTests.createMockSystemSchema(druidSchema, walker, plannerConfig);
    final DruidOperatorTable operatorTable = new DruidOperatorTable(
        ImmutableSet.of(new AccurateCardinalitySqlAggregator()),
        ImmutableSet.of()
    );

    plannerFactory = new PlannerFactory(
        druidSchema,
        systemSchema,
        CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
        operatorTable,
        CalciteTests.createExprMacroTable(),
        plannerConfig,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        CalciteTests.getJsonMapper()
    );
  }

  @After
  public void tearDown() throws Exception
  {
    walker.close();
    walker = null;
  }

  @Test
  public void testAccurateCardinalityAggregatorOnLongs() throws Exception
  {
    final DruidPlanner planner = plannerFactory.createPlanner(
        QUERY_CONTEXT_DEFAULT,
        NoopEscalator.getInstance()
                     .createEscalatedAuthenticationResult()
    );
    final String sql = "SELECT\n"
                       + "ACCURATE_CARDINALITY(clientid),\n"
                       + "ACCURATE_CARDINALITY(clientid) FILTER(WHERE dim1 = 'abc')\n"
                       + "FROM foo";

    final PlannerResult plannerResult = planner.plan(sql);
    final List<Object[]> results = plannerResult.run().toList();
    final List<Object[]> expectedResults = ImmutableList.of(
        new Object[]{
            4L,
            1L
        }
    );

    Assert.assertEquals(expectedResults.size(), results.size());

    for (int i = 0; i < expectedResults.size(); i++) {
      Assert.assertArrayEquals(expectedResults.get(i), results.get(i));
    }
  }

  @Test(expected = ValidationException.class)
  public void testAccurateCardinalityAggregatorOnString() throws Exception
  {
    final DruidPlanner planner = plannerFactory.createPlanner(
        QUERY_CONTEXT_DEFAULT,
        NoopEscalator.getInstance()
                     .createEscalatedAuthenticationResult()
    );
    final String sql = "SELECT\n"
                       + "ACCURATE_CARDINALITY(dim1)\n"
                       + "FROM foo";

    final PlannerResult plannerResult = planner.plan(sql);
    plannerResult.run().toList();
  }
}
