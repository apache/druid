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
import com.google.common.collect.Sets;
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
import java.util.Objects;
import java.util.Set;

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

    final QueryableIndex index = IndexBuilder.create()
                                             .tmpDir(temporaryFolder.newFolder())
                                             .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                                             .schema(
                                                 new IncrementalIndexSchema.Builder()
                                                     .withRollup(false)
                                                     .build()
                                             )
                                             .rows(CollectSetTestHelper.INPUT_ROWS)
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
                       + "  COLLECT_SET(dim3) AS dim3_set,\n"
                       + "  COLLECT_SET(dim3, 2) AS dim3a_set,\n"
                       + "  COLLECT_SET(dim4) AS dim4_set,\n"
                       + "  COLLECT_SET(dim4, 4) AS dim4a_set,\n"
                       + "  COLLECT_SET(dim4, 0) AS dim4b_set,\n"
                       + "  COLLECT_SET(dim4, -2) AS dim4c_set\n"
                       + "FROM druid.foo";

    class ResultRecord
    {
      private Set<String> rec1Set;
      private Set<String> rec2Set;
      private Set<String> rec3Set;
      private Set<String> rec4Set;
      private Set<String> rec5Set;
      private Set<String> rec6Set;
      private Set<String> rec7Set;
      private Set<String> rec8Set;

      public ResultRecord(Set<String> rec1Set, Set<String> rec2Set, Set<String> rec3Set, Set<String> rec4Set,
                          Set<String> rec5Set, Set<String> rec6Set, Set<String> rec7Set, Set<String> rec8Set)
      {
        this.rec1Set = rec1Set;
        this.rec2Set = rec2Set;
        this.rec3Set = rec3Set;
        this.rec4Set = rec4Set;
        this.rec5Set = rec5Set;
        this.rec6Set = rec6Set;
        this.rec7Set = rec7Set;
        this.rec8Set = rec8Set;
      }

      @Override
      public boolean equals(Object o)
      {
        if (this == o) {
          return true;
        }
        if (o == null || getClass() != o.getClass()) {
          return false;
        }
        ResultRecord that = (ResultRecord) o;
        return Objects.equals(rec1Set, that.rec1Set) &&
               Objects.equals(rec2Set, that.rec2Set) &&
               Objects.equals(rec3Set, that.rec3Set) &&
               Objects.equals(rec4Set, that.rec4Set) &&
               Objects.equals(rec5Set, that.rec5Set) &&
               Objects.equals(rec6Set, that.rec6Set) &&
               Objects.equals(rec7Set, that.rec7Set) &&
               Objects.equals(rec8Set, that.rec8Set);
      }

      @Override
      public int hashCode()
      {
        return Objects.hash(rec1Set, rec2Set, rec3Set, rec4Set, rec5Set, rec6Set, rec7Set, rec8Set);
      }
    }

    // Verify results
    final List<Object[]> rawResults = sqlLifecycle.runSimple(sql, QUERY_CONTEXT_DEFAULT, authenticationResult).toList();
    List<ResultRecord> results = new ArrayList<>();
    for (Object[] result : rawResults) {
      results.add(
          new ResultRecord(CalciteTests.getJsonMapper().readValue((String) result[0], Set.class),
                           CalciteTests.getJsonMapper().readValue((String) result[1], Set.class),
                           CalciteTests.getJsonMapper().readValue((String) result[2], Set.class),
                           CalciteTests.getJsonMapper().readValue((String) result[3], Set.class),
                           CalciteTests.getJsonMapper().readValue((String) result[4], Set.class),
                           CalciteTests.getJsonMapper().readValue((String) result[5], Set.class),
                           CalciteTests.getJsonMapper().readValue((String) result[6], Set.class),
                           CalciteTests.getJsonMapper().readValue((String) result[7], Set.class))
      );
    }
    List<ResultRecord> expectedResults = ImmutableList.of(
        new ResultRecord(
            Sets.newHashSet("0", "1", "2"),
            Sets.newHashSet("android", "iphone"),
            Sets.newHashSet("image", "text", "video"),
            Sets.newHashSet("image", "text"),
            Sets.newHashSet("tag1", "tag2", "tag3", "tag4", "tag5", "tag6", "tag7", "tag8"),
            Sets.newHashSet("tag1", "tag4", "tag5", "tag6"),
            ImmutableSet.of(),
            Sets.newHashSet("tag1", "tag2", "tag3", "tag4", "tag5", "tag6", "tag7", "tag8"))
    );

    Assert.assertEquals(expectedResults.size(), results.size());

    for (int i = 0; i < expectedResults.size(); i++) {
      Assert.assertEquals(expectedResults.get(i), results.get(i));
    }

    // Verify query
    Assert.assertEquals(
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
              .granularity(Granularities.ALL)
              .aggregators(
                  ImmutableList.of(
                      new CollectSetAggregatorFactory("a0", "dim1", null),
                      new CollectSetAggregatorFactory("a1", "dim2", null),
                      new CollectSetAggregatorFactory("a2", "dim3", null),
                      new CollectSetAggregatorFactory("a3", "dim3", 2),
                      new CollectSetAggregatorFactory("a4", "dim4", null),
                      new CollectSetAggregatorFactory("a5", "dim4", 4),
                      new CollectSetAggregatorFactory("a6", "dim4", 0),
                      new CollectSetAggregatorFactory("a7", "dim4", -2)
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
                       + "  COLLECT_SET(dim3) AS dim3_set,\n"
                       + "  COLLECT_SET(dim3, 2) AS dim3a_set,\n"
                       + "  COLLECT_SET(dim4) AS dim4_set,\n"
                       + "  COLLECT_SET(dim4, 2) AS dim4a_set\n"
                       + "FROM druid.foo\n"
                       + "GROUP BY dim1, dim2\n"
                       + "ORDER BY dim1 ASC, dim2 ASC\n"
                       + "LIMIT 10";


    class ResultRecord
    {
      private String rec1;
      private String rec2;
      private Set<String> rec3Set;
      private Set<String> rec4Set;
      private Set<String> rec5Set;
      private Set<String> rec6Set;

      private ResultRecord(String rec1, String rec2, Set<String> rec3Set, Set<String> rec4Set,
                           Set<String> rec5Set, Set<String> rec6Set)
      {
        this.rec1 = rec1;
        this.rec2 = rec2;
        this.rec3Set = rec3Set;
        this.rec4Set = rec4Set;
        this.rec5Set = rec5Set;
        this.rec6Set = rec6Set;
      }

      @Override
      public boolean equals(Object o)
      {
        if (this == o) {
          return true;
        }
        if (o == null || getClass() != o.getClass()) {
          return false;
        }
        ResultRecord that = (ResultRecord) o;
        return Objects.equals(rec1, that.rec1) &&
               Objects.equals(rec2, that.rec2) &&
               Objects.equals(rec3Set, that.rec3Set) &&
               Objects.equals(rec4Set, that.rec4Set) &&
               Objects.equals(rec5Set, that.rec5Set) &&
               Objects.equals(rec6Set, that.rec6Set);
      }

      @Override
      public int hashCode()
      {
        return Objects.hash(rec1, rec2, rec3Set, rec4Set, rec5Set, rec6Set);
      }
    }

    // Verify results
    final List<Object[]> rawResults = sqlLifecycle.runSimple(sql, QUERY_CONTEXT_DEFAULT, authenticationResult).toList();
    List<ResultRecord> results = new ArrayList<>();
    for (Object[] result : rawResults) {
      results.add(new ResultRecord((String) result[0], (String) result[1],
                                   CalciteTests.getJsonMapper().readValue((String) result[2], Set.class),
                                   CalciteTests.getJsonMapper().readValue((String) result[3], Set.class),
                                   CalciteTests.getJsonMapper().readValue((String) result[4], Set.class),
                                   CalciteTests.getJsonMapper().readValue((String) result[5], Set.class)));
    }

    final List<ResultRecord> expectedResults = ImmutableList.of(
        new ResultRecord("0", "android",
                         Sets.newHashSet("image"),
                         Sets.newHashSet("image"),
                         Sets.newHashSet("tag1", "tag4", "tag5", "tag6"),
                         Sets.newHashSet("tag1", "tag4")),
        new ResultRecord("0", "iphone",
                         Sets.newHashSet("text", "video"),
                         Sets.newHashSet("text", "video"),
                         Sets.newHashSet("tag1", "tag2", "tag3", "tag4", "tag5", "tag7", "tag8"),
                         Sets.newHashSet("tag4", "tag5")),
        new ResultRecord("1", "iphone",
                         Sets.newHashSet("video"),
                         Sets.newHashSet("video"),
                         ImmutableSet.of(),
                         ImmutableSet.of()),
        new ResultRecord("2", "android",
                         Sets.newHashSet("video"),
                         Sets.newHashSet("video"),
                         Sets.newHashSet("tag2"),
                         Sets.newHashSet("tag2"))
    );

    Assert.assertEquals(expectedResults.size(), results.size());
    for (int i = 0; i < expectedResults.size(); i++) {
      Assert.assertEquals(expectedResults.get(i), results.get(i));
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
            .setAggregatorSpecs(new CollectSetAggregatorFactory("a0", "dim3", null),
                                new CollectSetAggregatorFactory("a1", "dim3", 2),
                                new CollectSetAggregatorFactory("a2", "dim4", null),
                                new CollectSetAggregatorFactory("a3", "dim4", 2))
            .setContext(ImmutableMap.of(PlannerContext.CTX_SQL_QUERY_ID, "dummy"))
            .build(),
        Iterables.getOnlyElement(queryLogHook.getRecordedQueries())
    );
  }
}
