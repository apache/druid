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

package org.apache.druid.query.aggregation.collectset.sql;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.aggregation.collectset.CollectSetAggregatorFactory;
import org.apache.druid.query.aggregation.collectset.CollectSetDruidModule;
import org.apache.druid.query.aggregation.collectset.CollectSetTestHelper;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.sql.SqlLifecycle;
import org.apache.druid.sql.SqlLifecycleFactory;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.schema.DruidSchema;
import org.apache.druid.sql.calcite.schema.SystemSchema;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.druid.sql.calcite.util.CalciteTests.createRow;

public class CollectSetSqlAggregatorTest extends CalciteTestBase
{
  private static final String DATA_SOURCE = "foo";

  private static QueryRunnerFactoryConglomerate conglomerate;
  private static Closer resourceCloser;
  private static AuthenticationResult authenticationResult = CalciteTests.REGULAR_USER_AUTH_RESULT;
  private static final Map<String, Object> QUERY_CONTEXT_DEFAULT = ImmutableMap.of(
      PlannerContext.CTX_SQL_QUERY_ID, "dummy"
  );

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

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public QueryLogHook queryLogHook = QueryLogHook.create();

  private SpecificSegmentsQuerySegmentWalker walker;
  private SqlLifecycleFactory sqlLifecycleFactory;

  @Before
  public void setUp() throws Exception
  {
    for (Module mod : new CollectSetDruidModule().getJacksonModules()) {
      CalciteTests.getJsonMapper().registerModule(mod);
    }

    List<InputRow> inputRows = new ArrayList<>();
    for (String[] row : CollectSetTestHelper.ROWS) {
      inputRows.add(
          createRow(
              ImmutableMap.<String, Object>builder()
                  .put(CollectSetTestHelper.DATETIME, row[0])
                  .put(CollectSetTestHelper.DIMENSIONS[0], row[1])
                  .put(CollectSetTestHelper.DIMENSIONS[1], row[2])
                  .put(CollectSetTestHelper.DIMENSIONS[2], row[3])
                  .build()
          )
      );
    }

    final QueryableIndex index = IndexBuilder.create()
        .tmpDir(temporaryFolder.newFolder())
        .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
        .schema(
            new IncrementalIndexSchema.Builder()
                .withRollup(false)
                .build()
        )
        .rows(inputRows)
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
    final DruidSchema druidSchema = CalciteTests.createMockSchema(conglomerate, walker, plannerConfig);
    final SystemSchema systemSchema = CalciteTests.createMockSystemSchema(druidSchema, walker, plannerConfig);
    final DruidOperatorTable operatorTable = new DruidOperatorTable(
        ImmutableSet.of(new CollectSetSqlAggregator()),
        ImmutableSet.of()
    );

    sqlLifecycleFactory = CalciteTests.createSqlLifecycleFactory(
        new PlannerFactory(
            druidSchema,
            systemSchema,
            CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
            operatorTable,
            CalciteTests.createExprMacroTable(),
            plannerConfig,
            AuthTestUtils.TEST_AUTHORIZER_MAPPER,
            CalciteTests.getJsonMapper()
        )
    );
  }

  @After
  public void tearDown() throws Exception
  {
    walker.close();
    walker = null;
  }

  @Test
  public void testCollectSetSqlTimeseries() throws Exception
  {
    SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();

    final String sql = "SELECT\n"
        + "  COLLECT_SET(dim1) AS dim1_set,\n"
        + "  COLLECT_SET(dim2) AS dim2_set,\n"
        + "  COLLECT_SET(dim3) AS dim3_set\n"
        + "FROM druid.foo";

    // Verify results
    final List<Object[]> results = sqlLifecycle.runSimple(sql, QUERY_CONTEXT_DEFAULT, authenticationResult).toList();

    final List<Object[]> expectedResults = ImmutableList.of(
        new Object[]{
            "[\"0\",\"1\",\"2\"]",
            "[\"android\",\"iphone\"]",
            "[\"image\",\"video\",\"text\"]"
        }
    );

    Assert.assertEquals(expectedResults.size(), results.size());
    for (int i = 0; i < expectedResults.size(); i++) {
      Assert.assertArrayEquals(expectedResults.get(i), results.get(i));
    }

    // Verify query
    Assert.assertEquals(
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
              .granularity(Granularities.ALL)
              .aggregators(
                  ImmutableList.of(
                      new CollectSetAggregatorFactory("a0", "dim1"),
                      new CollectSetAggregatorFactory("a1", "dim2"),
                      new CollectSetAggregatorFactory("a2", "dim3")
                  )
              )
              .context(ImmutableMap.of("skipEmptyBuckets", true, PlannerContext.CTX_SQL_QUERY_ID, "dummy"))
              .build(),
        Iterables.getOnlyElement(queryLogHook.getRecordedQueries())
    );
  }

  @Test
  public void testCollectSetSqlGroupBy() throws Exception
  {
    SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();

    final String sql = "SELECT\n"
        + "  dim1,\n"
        + "  dim2,\n"
        + "  COLLECT_SET(dim3) AS dim3_set\n"
        + "FROM druid.foo\n"
        + "GROUP BY dim1, dim2\n"
        + "ORDER BY dim1 ASC, dim2 ASC\n"
        + "LIMIT 10";

    // Verify results
    final List<Object[]> results = sqlLifecycle.runSimple(sql, QUERY_CONTEXT_DEFAULT, authenticationResult).toList();

    final List<Object[]> expectedResults = ImmutableList.of(
        new Object[]{
            "0",
            "android",
            "[\"image\"]"
        },
        new Object[]{
            "0",
            "iphone",
            "[\"video\",\"text\"]"
        },
        new Object[]{
            "1",
            "iphone",
            "[\"video\"]"
        },
        new Object[]{
            "2",
            "android",
            "[\"video\"]"
        }
    );

    Assert.assertEquals(expectedResults.size(), results.size());
    for (int i = 0; i < expectedResults.size(); i++) {
      Assert.assertArrayEquals(expectedResults.get(i), results.get(i));
    }

    // Verify query
    Assert.assertEquals(
        new GroupByQuery.Builder()
            .setDataSource(CalciteTests.DATASOURCE1)
            .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
            .setDimensions(
                new DefaultDimensionSpec(
                    "dim1",
                    "d0"
                ),
                new DefaultDimensionSpec(
                    "dim2",
                    "d1"
                )
            )
            .setInterval(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
            .setLimitSpec(
                new DefaultLimitSpec(
                    Arrays.asList(
                        new OrderByColumnSpec("d0", OrderByColumnSpec.Direction.ASCENDING),
                        new OrderByColumnSpec("d1", OrderByColumnSpec.Direction.ASCENDING)
                    ),
                    10
                )
            )
            .setAggregatorSpecs(new CollectSetAggregatorFactory("a0", "dim3"))
            .setContext(ImmutableMap.of(PlannerContext.CTX_SQL_QUERY_ID, "dummy"))
            .build(),
        Iterables.getOnlyElement(queryLogHook.getRecordedQueries())
    );
  }
}
