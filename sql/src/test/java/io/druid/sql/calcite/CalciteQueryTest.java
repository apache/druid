/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.sql.calcite;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.druid.granularity.QueryGranularities;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.Druids;
import io.druid.query.Query;
import io.druid.query.QueryDataSource;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.DoubleMaxAggregatorFactory;
import io.druid.query.aggregation.DoubleMinAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.FilteredAggregatorFactory;
import io.druid.query.aggregation.LongMaxAggregatorFactory;
import io.druid.query.aggregation.LongMinAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HLLCV1;
import io.druid.query.aggregation.hyperloglog.HyperUniqueFinalizingPostAggregator;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.query.aggregation.post.ArithmeticPostAggregator;
import io.druid.query.aggregation.post.ConstantPostAggregator;
import io.druid.query.aggregation.post.ExpressionPostAggregator;
import io.druid.query.aggregation.post.FieldAccessPostAggregator;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.ExtractionDimensionSpec;
import io.druid.query.extraction.BucketExtractionFn;
import io.druid.query.extraction.CascadeExtractionFn;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.extraction.StrlenExtractionFn;
import io.druid.query.extraction.SubstringDimExtractionFn;
import io.druid.query.extraction.TimeFormatExtractionFn;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.InDimFilter;
import io.druid.query.filter.LikeDimFilter;
import io.druid.query.filter.NotDimFilter;
import io.druid.query.filter.OrDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.having.DimFilterHavingSpec;
import io.druid.query.groupby.orderby.DefaultLimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.ordering.StringComparator;
import io.druid.query.ordering.StringComparators;
import io.druid.query.select.PagingSpec;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.query.topn.DimensionTopNMetricSpec;
import io.druid.query.topn.InvertedTopNMetricSpec;
import io.druid.query.topn.NumericTopNMetricSpec;
import io.druid.query.topn.TopNQueryBuilder;
import io.druid.segment.column.Column;
import io.druid.sql.calcite.filtration.Filtration;
import io.druid.sql.calcite.planner.Calcites;
import io.druid.sql.calcite.planner.DruidOperatorTable;
import io.druid.sql.calcite.planner.PlannerConfig;
import io.druid.sql.calcite.planner.PlannerFactory;
import io.druid.sql.calcite.planner.PlannerResult;
import io.druid.sql.calcite.util.CalciteTests;
import io.druid.sql.calcite.util.QueryLogHook;
import io.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Planner;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class CalciteQueryTest
{
  private static final Logger log = new Logger(CalciteQueryTest.class);

  private static final PlannerConfig PLANNER_CONFIG_DEFAULT = new PlannerConfig();
  private static final PlannerConfig PLANNER_CONFIG_NO_TOPN = new PlannerConfig()
  {
    @Override
    public int getMaxTopNLimit()
    {
      return 0;
    }
  };
  private static final PlannerConfig PLANNER_CONFIG_SELECT_PAGING = new PlannerConfig()
  {
    @Override
    public int getSelectThreshold()
    {
      return 2;
    }
  };
  private static final PlannerConfig PLANNER_CONFIG_FALLBACK = new PlannerConfig()
  {
    @Override
    public boolean isUseFallback()
    {
      return true;
    }
  };
  private static final PlannerConfig PLANNER_CONFIG_SINGLE_NESTING_ONLY = new PlannerConfig()
  {
    @Override
    public int getMaxQueryCount()
    {
      return 2;
    }
  };
  private static final PlannerConfig PLANNER_CONFIG_NO_SUBQUERIES = new PlannerConfig()
  {
    @Override
    public int getMaxQueryCount()
    {
      return 1;
    }
  };

  public static final Map<String, Object> TIMESERIES_CONTEXT = ImmutableMap.<String, Object>of(
      "skipEmptyBuckets",
      true
  );
  private static final PagingSpec FIRST_PAGING_SPEC = new PagingSpec(null, 1000, true);

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public QueryLogHook queryLogHook = QueryLogHook.create();

  private SpecificSegmentsQuerySegmentWalker walker = null;

  @Before
  public void setUp() throws Exception
  {
    Calcites.setSystemProperties();
    walker = CalciteTests.createMockWalker(temporaryFolder.newFolder());
  }

  @After
  public void tearDown() throws Exception
  {
    walker.close();
    walker = null;
  }

  @Test
  public void testSelectConstantExpression() throws Exception
  {
    testQuery(
        "SELECT 1 + 1",
        ImmutableList.<Query>of(),
        ImmutableList.of(
            new Object[]{2}
        )
    );
  }

  @Test
  public void testExplainSelectConstantExpression() throws Exception
  {
    testQuery(
        "EXPLAIN PLAN FOR SELECT 1 + 1",
        ImmutableList.<Query>of(),
        ImmutableList.of(
            new Object[]{"BindableValues(tuples=[[{ 2 }]])\n"}
        )
    );
  }

  @Test
  public void testInformationSchemaSchemata() throws Exception
  {
    testQuery(
        "SELECT DISTINCT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA",
        ImmutableList.<Query>of(),
        ImmutableList.of(
            new Object[]{"druid"},
            new Object[]{"INFORMATION_SCHEMA"}
        )
    );
  }

  @Test
  public void testInformationSchemaTables() throws Exception
  {
    testQuery(
        "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE\n"
        + "FROM INFORMATION_SCHEMA.TABLES\n"
        + "WHERE TABLE_TYPE IN ('SYSTEM_TABLE', 'TABLE')",
        ImmutableList.<Query>of(),
        ImmutableList.of(
            new Object[]{"druid", "foo", "TABLE"},
            new Object[]{"druid", "foo2", "TABLE"},
            new Object[]{"INFORMATION_SCHEMA", "COLUMNS", "SYSTEM_TABLE"},
            new Object[]{"INFORMATION_SCHEMA", "SCHEMATA", "SYSTEM_TABLE"},
            new Object[]{"INFORMATION_SCHEMA", "TABLES", "SYSTEM_TABLE"}
        )
    );
  }

  @Test
  public void testInformationSchemaColumns() throws Exception
  {
    testQuery(
        "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE\n"
        + "FROM INFORMATION_SCHEMA.COLUMNS\n"
        + "WHERE TABLE_SCHEMA = 'druid' AND TABLE_NAME = 'foo'",
        ImmutableList.<Query>of(),
        ImmutableList.of(
            new Object[]{"__time", "TIMESTAMP", "NO"},
            new Object[]{"cnt", "BIGINT", "NO"},
            new Object[]{"dim1", "VARCHAR", "NO"},
            new Object[]{"dim2", "VARCHAR", "NO"},
            new Object[]{"m1", "FLOAT", "NO"},
            new Object[]{"unique_dim1", "OTHER", "NO"}
        )
    );
  }

  @Test
  public void testExplainInformationSchemaColumns() throws Exception
  {
    testQuery(
        "EXPLAIN PLAN FOR\n"
        + "SELECT COLUMN_NAME, DATA_TYPE\n"
        + "FROM INFORMATION_SCHEMA.COLUMNS\n"
        + "WHERE TABLE_SCHEMA = 'druid' AND TABLE_NAME = 'foo'",
        ImmutableList.<Query>of(),
        ImmutableList.of(
            new Object[]{
                "BindableProject(COLUMN_NAME=[$3], DATA_TYPE=[$7])\n"
                + "  BindableFilter(condition=[AND(=(CAST($1):VARCHAR(5) CHARACTER SET \"UTF-16LE\" COLLATE \"UTF-16LE$en_US$primary\" NOT NULL, 'druid'), =(CAST($2):VARCHAR(3) CHARACTER SET \"UTF-16LE\" COLLATE \"UTF-16LE$en_US$primary\" NOT NULL, 'foo'))])\n"
                + "    BindableTableScan(table=[[INFORMATION_SCHEMA, COLUMNS]])\n"
            }
        )
    );
  }

  @Test
  public void testSelectStar() throws Exception
  {
    testQuery(
        "SELECT * FROM druid.foo",
        ImmutableList.<Query>of(
            Druids.newSelectQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Filtration.eternity()))
                  .granularity(QueryGranularities.ALL)
                  .pagingSpec(FIRST_PAGING_SPEC)
                  .build(),
            Druids.newSelectQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Filtration.eternity()))
                  .granularity(QueryGranularities.ALL)
                  .pagingSpec(
                      new PagingSpec(
                          ImmutableMap.of("foo_1970-01-01T00:00:00.000Z_2001-01-03T00:00:00.001Z_1", 5),
                          1000,
                          true
                      )
                  )
                  .build()
        ),
        ImmutableList.of(
            new Object[]{T("2000-01-01"), 1L, "", "a", 1.0, HLLCV1.class.getName()},
            new Object[]{T("2000-01-02"), 1L, "10.1", "", 2.0, HLLCV1.class.getName()},
            new Object[]{T("2000-01-03"), 1L, "2", "", 3.0, HLLCV1.class.getName()},
            new Object[]{T("2001-01-01"), 1L, "1", "a", 4.0, HLLCV1.class.getName()},
            new Object[]{T("2001-01-02"), 1L, "def", "abc", 5.0, HLLCV1.class.getName()},
            new Object[]{T("2001-01-03"), 1L, "abc", "", 6.0, HLLCV1.class.getName()}
        )
    );
  }

  @Test
  public void testUnqualifiedTableName() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM foo",
        ImmutableList.<Query>of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Filtration.eternity()))
                  .granularity(QueryGranularities.ALL)
                  .aggregators(AGGS(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L}
        )
    );
  }

  @Test
  public void testExplainSelectStar() throws Exception
  {
    testQuery(
        "EXPLAIN PLAN FOR SELECT * FROM druid.foo",
        ImmutableList.<Query>of(),
        ImmutableList.of(
            new Object[]{
                "DruidQueryRel(dataSource=[foo])\n"
            }
        )
    );
  }

  @Test
  public void testSelectStarWithLimit() throws Exception
  {
    testQuery(
        "SELECT * FROM druid.foo LIMIT 2",
        ImmutableList.<Query>of(
            Druids.newSelectQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Filtration.eternity()))
                  .granularity(QueryGranularities.ALL)
                  .pagingSpec(FIRST_PAGING_SPEC)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{T("2000-01-01"), 1L, "", "a", 1.0, HLLCV1.class.getName()},
            new Object[]{T("2000-01-02"), 1L, "10.1", "", 2.0, HLLCV1.class.getName()}
        )
    );
  }

  @Test
  public void testSelectStarWithLimitDescending() throws Exception
  {
    testQuery(
        "SELECT * FROM druid.foo ORDER BY __time DESC LIMIT 2",
        ImmutableList.<Query>of(
            Druids.newSelectQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Filtration.eternity()))
                  .granularity(QueryGranularities.ALL)
                  .descending(true)
                  .pagingSpec(FIRST_PAGING_SPEC)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{T("2001-01-03"), 1L, "abc", "", 6.0, HLLCV1.class.getName()},
            new Object[]{T("2001-01-02"), 1L, "def", "abc", 5.0, HLLCV1.class.getName()}
        )
    );
  }

  @Test
  public void testSelectSingleColumnTwice() throws Exception
  {
    testQuery(
        "SELECT dim2 x, dim2 y FROM druid.foo LIMIT 2",
        ImmutableList.<Query>of(
            Druids.newSelectQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Filtration.eternity()))
                  .dimensionSpecs(DIMS(
                      new DefaultDimensionSpec("dim2", "d1"),
                      new DefaultDimensionSpec("dim2", "d2")
                  ))
                  .granularity(QueryGranularities.ALL)
                  .descending(false)
                  .pagingSpec(FIRST_PAGING_SPEC)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{"a", "a"},
            new Object[]{"", ""}
        )
    );
  }

  @Test
  public void testSelectSingleColumnWithLimitDescending() throws Exception
  {
    testQuery(
        "SELECT dim1 FROM druid.foo ORDER BY __time DESC LIMIT 2",
        ImmutableList.<Query>of(
            Druids.newSelectQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Filtration.eternity()))
                  .dimensionSpecs(DIMS(new DefaultDimensionSpec("dim1", "d1")))
                  .granularity(QueryGranularities.ALL)
                  .descending(true)
                  .pagingSpec(FIRST_PAGING_SPEC)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{"abc"},
            new Object[]{"def"}
        )
    );
  }

  @Test
  public void testGroupBySingleColumnDescending() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_NO_TOPN,
        "SELECT dim1 FROM druid.foo GROUP BY dim1 ORDER BY dim1 DESC",
        ImmutableList.<Query>of(
            new GroupByQuery.Builder()
                .setDataSource(CalciteTests.DATASOURCE1)
                .setInterval(QSS(Filtration.eternity()))
                .setDimensions(DIMS(new DefaultDimensionSpec("dim1", "d0")))
                .setGranularity(QueryGranularities.ALL)
                .setLimitSpec(
                    new DefaultLimitSpec(
                        ImmutableList.of(
                            new OrderByColumnSpec(
                                "d0",
                                OrderByColumnSpec.Direction.DESCENDING,
                                StringComparators.LEXICOGRAPHIC
                            )
                        ),
                        Integer.MAX_VALUE
                    )
                )
                .build()
        ),
        ImmutableList.of(
            new Object[]{"def"},
            new Object[]{"abc"},
            new Object[]{"2"},
            new Object[]{"10.1"},
            new Object[]{"1"},
            new Object[]{""}
        )
    );
  }

  @Test
  public void testSelfJoinWithFallback() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_FALLBACK,
        "SELECT x.dim1, y.dim1, y.dim2\n"
        + "FROM\n"
        + "  druid.foo x INNER JOIN druid.foo y ON x.dim1 = y.dim2\n"
        + "WHERE\n"
        + "  x.dim1 <> ''",
        ImmutableList.<Query>of(
            Druids.newSelectQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Filtration.eternity()))
                  .granularity(QueryGranularities.ALL)
                  .pagingSpec(FIRST_PAGING_SPEC)
                  .build(),
            Druids.newSelectQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Filtration.eternity()))
                  .granularity(QueryGranularities.ALL)
                  .pagingSpec(
                      new PagingSpec(
                          ImmutableMap.of("foo_1970-01-01T00:00:00.000Z_2001-01-03T00:00:00.001Z_1", 5),
                          1000,
                          true
                      )
                  )
                  .build(),
            Druids.newSelectQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Filtration.eternity()))
                  .granularity(QueryGranularities.ALL)
                  .filters(NOT(SELECTOR("dim1", "", null)))
                  .pagingSpec(FIRST_PAGING_SPEC)
                  .build(),
            Druids.newSelectQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Filtration.eternity()))
                  .granularity(QueryGranularities.ALL)
                  .filters(NOT(SELECTOR("dim1", "", null)))
                  .pagingSpec(
                      new PagingSpec(
                          ImmutableMap.of("foo_1970-01-01T00:00:00.000Z_2001-01-03T00:00:00.001Z_1", 4),
                          1000,
                          true
                      )
                  )
                  .build()
        ),
        ImmutableList.of(
            new Object[]{"abc", "def", "abc"}
        )
    );
  }

  @Test
  public void testExplainSelfJoinWithFallback() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_FALLBACK,
        "EXPLAIN PLAN FOR\n"
        + "SELECT x.dim1, y.dim1, y.dim2\n"
        + "FROM\n"
        + "  druid.foo x INNER JOIN druid.foo y ON x.dim1 = y.dim2\n"
        + "WHERE\n"
        + "  x.dim1 <> ''",
        ImmutableList.<Query>of(),
        ImmutableList.of(
            new Object[]{
                "BindableProject(dim1=[$8], dim10=[$2], dim2=[$3])\n"
                + "  BindableJoin(condition=[=($8, $3)], joinType=[inner])\n"
                + "    DruidQueryRel(dataSource=[foo])\n"
                + "    DruidQueryRel(dataSource=[foo], filter=[!dim1 = ])\n"
            }
        )
    );
  }

  @Test
  public void testUnplannableQueries() throws Exception
  {
    // All of these queries are unplannable because they rely on features Druid doesn't support.
    // This test is here to confirm that we don't fall back to Calcite's interpreter or enumerable implementation.
    // It's also here so when we do support these features, we can have "real" tests for these queries.

    final List<String> queries = ImmutableList.of(
        "SELECT (dim1 || ' ' || dim2) AS cc, COUNT(*) FROM druid.foo GROUP BY dim1 || ' ' || dim2", // Concat two dims
        "SELECT dim1 FROM druid.foo ORDER BY dim1", // SELECT query with order by
        "SELECT TRIM(dim1) FROM druid.foo", // TRIM function
        "SELECT cnt, COUNT(*) FROM druid.foo GROUP BY cnt", // GROUP BY long
        "SELECT m1, COUNT(*) FROM druid.foo GROUP BY m1", // GROUP BY float
        "SELECT COUNT(*) FROM druid.foo WHERE m1 = 1.0", // Filter on float
        "SELECT COUNT(*) FROM druid.foo WHERE dim1 = dim2", // Filter on two columns equaling each other
        "SELECT COUNT(*) FROM druid.foo WHERE CHARACTER_LENGTH(dim1) = CHARACTER_LENGTH(dim2)", // Similar to above
        "SELECT CHARACTER_LENGTH(dim1) + 1 FROM druid.foo GROUP BY CHARACTER_LENGTH(dim1) + 1", // Group by math
        "SELECT COUNT(*) FROM druid.foo x, druid.foo y", // Self-join
        "SELECT\n"
        + "  (CAST(__time AS DATE) + EXTRACT(HOUR FROM __time) * INTERVAL '1' HOUR) AS t,\n"
        + "  SUM(cnt) AS cnt\n"
        + "FROM druid.foo\n"
        + "GROUP BY (CAST(__time AS DATE) + EXTRACT(HOUR FROM __time) * INTERVAL '1' HOUR)", // Time arithmetic
        "SELECT dim1, SUM(m1) AS m1_sum FROM druid.foo GROUP BY dim1 HAVING SUM(m1) > 1", // HAVING on float
        "SELECT SUBSTRING(dim1, 2) FROM druid.foo GROUP BY dim1", // Project a dimension from GROUP BY
        "SELECT dim1 FROM druid.foo GROUP BY dim1 ORDER BY SUBSTRING(dim1, 2)" // ORDER BY projection
    );

    for (final String query : queries) {
      assertQueryIsUnplannable(query);
    }
  }

  private void assertQueryIsUnplannable(final String sql)
  {
    assertQueryIsUnplannable(PLANNER_CONFIG_DEFAULT, sql);
  }

  private void assertQueryIsUnplannable(final PlannerConfig plannerConfig, final String sql)
  {
    Exception e = null;
    try {
      testQuery(plannerConfig, sql, ImmutableList.<Query>of(), ImmutableList.<Object[]>of());
    }
    catch (Exception e1) {
      e = e1;
    }

    if (!(e instanceof RelOptPlanner.CannotPlanException)) {
      log.error(e, "Expected CannotPlanException for query: %s", sql);
      Assert.fail(sql);
    }
  }

  @Test
  public void testSelectStarWithDimFilter() throws Exception
  {
    testQuery(
        "SELECT * FROM druid.foo WHERE dim1 > 'd' OR dim2 = 'a'",
        ImmutableList.<Query>of(
            Druids.newSelectQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Filtration.eternity()))
                  .granularity(QueryGranularities.ALL)
                  .pagingSpec(FIRST_PAGING_SPEC)
                  .filters(
                      OR(
                          BOUND("dim1", "d", null, true, false, null, StringComparators.LEXICOGRAPHIC),
                          SELECTOR("dim2", "a", null)
                      )
                  )
                  .build(),
            Druids.newSelectQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Filtration.eternity()))
                  .granularity(QueryGranularities.ALL)
                  .pagingSpec(
                      new PagingSpec(
                          ImmutableMap.of("foo_1970-01-01T00:00:00.000Z_2001-01-03T00:00:00.001Z_1", 2),
                          1000,
                          true
                      )
                  )
                  .filters(
                      OR(
                          BOUND("dim1", "d", null, true, false, null, StringComparators.LEXICOGRAPHIC),
                          SELECTOR("dim2", "a", null)
                      )
                  )
                  .build()
        ),
        ImmutableList.of(
            new Object[]{T("2000-01-01"), 1L, "", "a", 1.0, HLLCV1.class.getName()},
            new Object[]{T("2001-01-01"), 1L, "1", "a", 4.0, HLLCV1.class.getName()},
            new Object[]{T("2001-01-02"), 1L, "def", "abc", 5.0, HLLCV1.class.getName()}
        )
    );
  }

  @Test
  public void testSelectStarWithDimFilterAndPaging() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_SELECT_PAGING,
        "SELECT * FROM druid.foo WHERE dim1 > 'd' OR dim2 = 'a'",
        ImmutableList.<Query>of(
            Druids.newSelectQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Filtration.eternity()))
                  .granularity(QueryGranularities.ALL)
                  .pagingSpec(new PagingSpec(null, 2, true))
                  .filters(
                      OR(
                          BOUND("dim1", "d", null, true, false, null, StringComparators.LEXICOGRAPHIC),
                          SELECTOR("dim2", "a", null)
                      )
                  )
                  .build(),
            Druids.newSelectQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Filtration.eternity()))
                  .granularity(QueryGranularities.ALL)
                  .pagingSpec(
                      new PagingSpec(
                          ImmutableMap.of("foo_1970-01-01T00:00:00.000Z_2001-01-03T00:00:00.001Z_1", 1),
                          2,
                          true
                      )
                  )
                  .filters(
                      OR(
                          BOUND("dim1", "d", null, true, false, null, StringComparators.LEXICOGRAPHIC),
                          SELECTOR("dim2", "a", null)
                      )
                  )
                  .build(),
            Druids.newSelectQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Filtration.eternity()))
                  .granularity(QueryGranularities.ALL)
                  .pagingSpec(
                      new PagingSpec(
                          ImmutableMap.of("foo_1970-01-01T00:00:00.000Z_2001-01-03T00:00:00.001Z_1", 2),
                          2,
                          true
                      )
                  )
                  .filters(
                      OR(
                          BOUND("dim1", "d", null, true, false, null, StringComparators.LEXICOGRAPHIC),
                          SELECTOR("dim2", "a", null)
                      )
                  )
                  .build()
        ),
        ImmutableList.of(
            new Object[]{T("2000-01-01"), 1L, "", "a", 1.0, HLLCV1.class.getName()},
            new Object[]{T("2001-01-01"), 1L, "1", "a", 4.0, HLLCV1.class.getName()},
            new Object[]{T("2001-01-02"), 1L, "def", "abc", 5.0, HLLCV1.class.getName()}
        )
    );
  }

  @Test
  public void testGroupByNothingWithLiterallyFalseFilter() throws Exception
  {
    testQuery(
        "SELECT COUNT(*), MAX(cnt) FROM druid.foo WHERE 1 = 0",
        ImmutableList.<Query>of(),
        ImmutableList.of(
            new Object[]{0L, null}
        )
    );
  }

  @Test
  public void testGroupByOneColumnWithLiterallyFalseFilter() throws Exception
  {
    testQuery(
        "SELECT COUNT(*), MAX(cnt) FROM druid.foo WHERE 1 = 0 GROUP BY dim1",
        ImmutableList.<Query>of(),
        ImmutableList.<Object[]>of()
    );
  }

  @Test
  public void testGroupByWithFilterMatchingNothing() throws Exception
  {
    // This query should actually return [0, null] rather than an empty result set, but it doesn't.
    // This test just "documents" the current behavior.

    testQuery(
        "SELECT COUNT(*), MAX(cnt) FROM druid.foo WHERE dim1 = 'foobar'",
        ImmutableList.<Query>of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Filtration.eternity()))
                  .filters(SELECTOR("dim1", "foobar", null))
                  .granularity(QueryGranularities.ALL)
                  .aggregators(AGGS(
                      new CountAggregatorFactory("a0"),
                      new LongMaxAggregatorFactory("a1", "cnt")
                  ))
                  .context(TIMESERIES_CONTEXT)
                  .build()
        ),
        ImmutableList.<Object[]>of()
    );
  }

  @Test
  public void testGroupByWithFilterMatchingNothingWithGroupByLiteral() throws Exception
  {
    testQuery(
        "SELECT COUNT(*), MAX(cnt) FROM druid.foo WHERE dim1 = 'foobar' GROUP BY 'dummy'",
        ImmutableList.<Query>of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Filtration.eternity()))
                  .filters(SELECTOR("dim1", "foobar", null))
                  .granularity(QueryGranularities.ALL)
                  .aggregators(AGGS(
                      new CountAggregatorFactory("a0"),
                      new LongMaxAggregatorFactory("a1", "cnt")
                  ))
                  .context(TIMESERIES_CONTEXT)
                  .build()
        ),
        ImmutableList.<Object[]>of()
    );
  }

  @Test
  public void testCountStar() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo",
        ImmutableList.<Query>of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Filtration.eternity()))
                  .granularity(QueryGranularities.ALL)
                  .aggregators(AGGS(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L}
        )
    );
  }

  @Test
  public void testCountStarWithLikeFilter() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE dim1 like 'a%' OR dim2 like '%xb%' escape 'x'",
        ImmutableList.<Query>of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Filtration.eternity()))
                  .granularity(QueryGranularities.ALL)
                  .filters(
                      OR(
                          new LikeDimFilter("dim1", "a%", null, null),
                          new LikeDimFilter("dim2", "%xb%", "x", null)
                      )
                  )
                  .aggregators(AGGS(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{2L}
        )
    );
  }

  @Test
  public void testCountStarWithLongColumnFilters() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE cnt >= 3 OR cnt = 1",
        ImmutableList.<Query>of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Filtration.eternity()))
                  .granularity(QueryGranularities.ALL)
                  .filters(
                      OR(
                          BOUND("cnt", "3", null, false, false, null, StringComparators.NUMERIC),
                          SELECTOR("cnt", "1", null)
                      )
                  )
                  .aggregators(AGGS(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L}
        )
    );
  }

  @Test
  public void testCountStarWithLongColumnFiltersOnTwoPoints() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE cnt = 1 OR cnt = 2",
        ImmutableList.<Query>of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Filtration.eternity()))
                  .granularity(QueryGranularities.ALL)
                  .filters(IN("cnt", ImmutableList.of("1", "2"), null))
                  .aggregators(AGGS(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L}
        )
    );
  }

  @Test
  public void testFilterOnStringAsNumber() throws Exception
  {
    testQuery(
        "SELECT distinct dim1 FROM druid.foo WHERE "
        + "dim1 = 10 OR "
        + "(floor(CAST(dim1 AS float)) = 10.00 and CAST(dim1 AS float) > 9 and CAST(dim1 AS float) <= 10.5)",
        ImmutableList.<Query>of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(QSS(Filtration.eternity()))
                        .setGranularity(QueryGranularities.ALL)
                        .setDimensions(DIMS(new DefaultDimensionSpec("dim1", "d0")))
                        .setDimFilter(
                            OR(
                                SELECTOR("dim1", "10", null),
                                AND(
                                    NUMERIC_SELECTOR("dim1", "10.00", new BucketExtractionFn(1.0, 0.0)),
                                    BOUND("dim1", "9", "10.5", true, false, null, StringComparators.NUMERIC)
                                )
                            )
                        )
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"10.1"}
        )
    );
  }

  @Test
  public void testSimpleAggregations() throws Exception
  {
    testQuery(
        "SELECT COUNT(*), COUNT(cnt), COUNT(dim1), AVG(cnt), SUM(cnt), SUM(cnt) + MIN(cnt) + MAX(cnt) FROM druid.foo",
        ImmutableList.<Query>of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Filtration.eternity()))
                  .granularity(QueryGranularities.ALL)
                  .aggregators(
                      AGGS(
                          new CountAggregatorFactory("a0"),
                          new LongSumAggregatorFactory("A1:sum", "cnt"),
                          new CountAggregatorFactory("A1:count"),
                          new LongSumAggregatorFactory("a2", "cnt"),
                          new LongMinAggregatorFactory("a3", "cnt"),
                          new LongMaxAggregatorFactory("a4", "cnt")
                      )
                  )
                  .postAggregators(
                      ImmutableList.<PostAggregator>of(
                          new ArithmeticPostAggregator(
                              "a1",
                              "quotient",
                              ImmutableList.<PostAggregator>of(
                                  new FieldAccessPostAggregator(null, "A1:sum"),
                                  new FieldAccessPostAggregator(null, "A1:count")
                              )
                          ),
                          new ArithmeticPostAggregator(
                              "a5",
                              "+",
                              ImmutableList.<PostAggregator>of(
                                  new ArithmeticPostAggregator(
                                      null,
                                      "+",
                                      ImmutableList.<PostAggregator>of(
                                          new FieldAccessPostAggregator(null, "a2"),
                                          new FieldAccessPostAggregator(null, "a3")
                                      )
                                  ),
                                  new FieldAccessPostAggregator(null, "a4")
                              )
                          )
                      )
                  )
                  .context(TIMESERIES_CONTEXT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L, 6L, 6L, 1L, 6L, 8L}
        )
    );
  }

  @Test
  public void testGroupByWithSortOnPostAggregation() throws Exception
  {
    testQuery(
        "SELECT dim1, MIN(m1) + MAX(m1) AS x FROM druid.foo GROUP BY dim1 ORDER BY x LIMIT 3",
        ImmutableList.<Query>of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(QSS(Filtration.eternity()))
                .granularity(QueryGranularities.ALL)
                .dimension(new DefaultDimensionSpec("dim1", "d0"))
                .metric(new InvertedTopNMetricSpec(new NumericTopNMetricSpec("a2")))
                .aggregators(AGGS(
                    new DoubleMinAggregatorFactory("a0", "m1"),
                    new DoubleMaxAggregatorFactory("a1", "m1")
                ))
                .postAggregators(
                    ImmutableList.<PostAggregator>of(
                        new ArithmeticPostAggregator(
                            "a2",
                            "+",
                            ImmutableList.<PostAggregator>of(
                                new FieldAccessPostAggregator(null, "a0"),
                                new FieldAccessPostAggregator(null, "a1")
                            )
                        )
                    )
                )
                .threshold(3)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"", 2.0},
            new Object[]{"10.1", 4.0},
            new Object[]{"2", 6.0}
        )
    );
  }

  @Test
  public void testGroupByWithSortOnPostAggregationNoTopNConfig() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_NO_TOPN,
        "SELECT dim1, MIN(m1) + MAX(m1) AS x FROM druid.foo GROUP BY dim1 ORDER BY x LIMIT 3",
        ImmutableList.<Query>of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(QSS(Filtration.eternity()))
                        .setGranularity(QueryGranularities.ALL)
                        .setDimensions(DIMS(new DefaultDimensionSpec("dim1", "d0")))
                        .setAggregatorSpecs(
                            ImmutableList.of(
                                new DoubleMinAggregatorFactory("a0", "m1"),
                                new DoubleMaxAggregatorFactory("a1", "m1")
                            )
                        )
                        .setPostAggregatorSpecs(
                            ImmutableList.<PostAggregator>of(
                                new ArithmeticPostAggregator(
                                    "a2",
                                    "+",
                                    ImmutableList.<PostAggregator>of(
                                        new FieldAccessPostAggregator(null, "a0"),
                                        new FieldAccessPostAggregator(null, "a1")
                                    )
                                )
                            )
                        )
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(
                                    new OrderByColumnSpec(
                                        "a2",
                                        OrderByColumnSpec.Direction.ASCENDING,
                                        StringComparators.NUMERIC
                                    )
                                ),
                                3
                            )
                        )
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", 2.0},
            new Object[]{"10.1", 4.0},
            new Object[]{"2", 6.0}
        )
    );
  }

  @Test
  public void testFilteredAggregations() throws Exception
  {
    testQuery(
        "SELECT "
        + "SUM(case dim1 when 'abc' then cnt end), "
        + "SUM(case dim1 when 'abc' then null else cnt end), "
        + "SUM(case substring(dim1, 1, 1) when 'a' then cnt end), "
        + "COUNT(dim2) filter(WHERE dim1 <> '1'), "
        + "COUNT(CASE WHEN dim1 <> '1' THEN 'dummy' END), "
        + "SUM(CASE WHEN dim1 <> '1' THEN 1 ELSE 0 END), "
        + "SUM(cnt) filter(WHERE dim2 = 'a'), "
        + "SUM(case when dim1 <> '1' then cnt end) filter(WHERE dim2 = 'a') "
        + "FROM druid.foo",
        ImmutableList.<Query>of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Filtration.eternity()))
                  .granularity(QueryGranularities.ALL)
                  .aggregators(AGGS(
                      new FilteredAggregatorFactory(
                          new LongSumAggregatorFactory("a0", "cnt"),
                          SELECTOR("dim1", "abc", null)
                      ),
                      new FilteredAggregatorFactory(
                          new LongSumAggregatorFactory("a1", "cnt"),
                          NOT(SELECTOR("dim1", "abc", null))
                      ),
                      new FilteredAggregatorFactory(
                          new LongSumAggregatorFactory("a2", "cnt"),
                          SELECTOR("dim1", "a", new SubstringDimExtractionFn(0, 1))
                      ),
                      new FilteredAggregatorFactory(
                          new CountAggregatorFactory("a3"),
                          NOT(SELECTOR("dim1", "1", null))
                      ),
                      new FilteredAggregatorFactory(
                          new CountAggregatorFactory("a4"),
                          NOT(SELECTOR("dim1", "1", null))
                      ),
                      new FilteredAggregatorFactory(
                          new CountAggregatorFactory("a5"),
                          NOT(SELECTOR("dim1", "1", null))
                      ),
                      new FilteredAggregatorFactory(
                          new LongSumAggregatorFactory("a6", "cnt"),
                          SELECTOR("dim2", "a", null)
                      ),
                      new FilteredAggregatorFactory(
                          new LongSumAggregatorFactory("a7", "cnt"),
                          AND(
                              SELECTOR("dim2", "a", null),
                              NOT(SELECTOR("dim1", "1", null))
                          )
                      )
                  ))
                  .context(TIMESERIES_CONTEXT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L, 5L, 1L, 5L, 5L, 5, 2L, 1L}
        )
    );
  }

  @Test
  public void testExpressionAggregations() throws Exception
  {
    testQuery(
        "SELECT SUM(cnt * 3), LN(SUM(cnt) + SUM(m1)) FROM druid.foo",
        ImmutableList.<Query>of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Filtration.eternity()))
                  .granularity(QueryGranularities.ALL)
                  .aggregators(AGGS(
                      new LongSumAggregatorFactory("a0", null, "(\"cnt\" * 3)"),
                      new LongSumAggregatorFactory("a1", "cnt", null),
                      new DoubleSumAggregatorFactory("a2", "m1", null)
                  ))
                  .postAggregators(ImmutableList.<PostAggregator>of(
                      new ExpressionPostAggregator("a3", "log((\"a1\" + \"a2\"))")
                  ))
                  .context(TIMESERIES_CONTEXT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{18L, 3.295836866004329}
        )
    );
  }

  @Test
  public void testInFilter() throws Exception
  {
    testQuery(
        "SELECT dim1, COUNT(*) FROM druid.foo WHERE dim1 IN ('abc', 'def', 'ghi') GROUP BY dim1",
        ImmutableList.<Query>of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(QSS(Filtration.eternity()))
                        .setGranularity(QueryGranularities.ALL)
                        .setDimensions(DIMS(new DefaultDimensionSpec("dim1", "d0")))
                        .setDimFilter(new InDimFilter("dim1", ImmutableList.of("abc", "def", "ghi"), null))
                        .setAggregatorSpecs(
                            AGGS(
                                new CountAggregatorFactory("a0")
                            )
                        )
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"abc", 1L},
            new Object[]{"def", 1L}
        )
    );
  }

  @Test
  public void testCountStarWithDegenerateFilter() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE dim2 = 'a' and (dim1 > 'a' OR dim1 < 'b')",
        ImmutableList.<Query>of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Filtration.eternity()))
                  .granularity(QueryGranularities.ALL)
                  .filters(SELECTOR("dim2", "a", null))
                  .aggregators(AGGS(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{2L}
        )
    );
  }

  @Test
  public void testCountStarWithNotOfDegenerateFilter() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE dim2 = 'a' and not (dim1 > 'a' OR dim1 < 'b')",
        ImmutableList.<Query>of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS())
                  .granularity(QueryGranularities.ALL)
                  .filters(null)
                  .aggregators(AGGS(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT)
                  .build()
        ),
        ImmutableList.<Object[]>of()
    );
  }

  @Test
  public void testCountStarWithBoundFilterSimplifyOr() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE (dim1 >= 'a' and dim1 < 'b') OR dim1 = 'ab'",
        ImmutableList.<Query>of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Filtration.eternity()))
                  .granularity(QueryGranularities.ALL)
                  .filters(BOUND("dim1", "a", "b", false, true, null, StringComparators.LEXICOGRAPHIC))
                  .aggregators(AGGS(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }

  @Test
  public void testCountStarWithBoundFilterSimplifyAnd() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE (dim1 >= 'a' and dim1 < 'b') and dim1 = 'abc'",
        ImmutableList.<Query>of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Filtration.eternity()))
                  .granularity(QueryGranularities.ALL)
                  .filters(SELECTOR("dim1", "abc", null))
                  .aggregators(AGGS(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }

  @Test
  public void testCountStarWithFilterOnCastedString() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE CAST(dim1 AS bigint) = 2",
        ImmutableList.<Query>of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Filtration.eternity()))
                  .granularity(QueryGranularities.ALL)
                  .filters(NUMERIC_SELECTOR("dim1", "2", null))
                  .aggregators(AGGS(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }

  @Test
  public void testCountStarWithTimeFilter() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo "
        + "WHERE __time >= TIMESTAMP '2000-01-01 00:00:00' AND __time < TIMESTAMP '2001-01-01 00:00:00'",
        ImmutableList.<Query>of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(new Interval("2000-01-01/2001-01-01")))
                  .granularity(QueryGranularities.ALL)
                  .aggregators(AGGS(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @Test
  public void testCountStarWithSinglePointInTime() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE __time = TIMESTAMP '2000-01-01 00:00:00'",
        ImmutableList.<Query>of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(new Interval("2000-01-01/2000-01-01T00:00:00.001")))
                  .granularity(QueryGranularities.ALL)
                  .aggregators(AGGS(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }

  @Test
  public void testCountStarWithTwoPointsInTime() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE "
        + "__time = TIMESTAMP '2000-01-01 00:00:00' OR __time = TIMESTAMP '2000-01-01 00:00:00' + INTERVAL '1' DAY",
        ImmutableList.<Query>of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(
                      QSS(
                          new Interval("2000-01-01/2000-01-01T00:00:00.001"),
                          new Interval("2000-01-02/2000-01-02T00:00:00.001")
                      )
                  )
                  .granularity(QueryGranularities.ALL)
                  .aggregators(AGGS(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{2L}
        )
    );
  }

  @Test
  public void testCountStarWithComplexDisjointTimeFilter() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo "
        + "WHERE dim2 = 'a' and ("
        + "  (__time >= TIMESTAMP '2000-01-01 00:00:00' AND __time < TIMESTAMP '2001-01-01 00:00:00')"
        + "  OR ("
        + "    (__time >= TIMESTAMP '2002-01-01 00:00:00' AND __time < TIMESTAMP '2003-05-01 00:00:00')"
        + "    and (__time >= TIMESTAMP '2002-05-01 00:00:00' AND __time < TIMESTAMP '2004-01-01 00:00:00')"
        + "    and dim1 = 'abc'"
        + "  )"
        + ")",
        ImmutableList.<Query>of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(new Interval("2000/2001"), new Interval("2002-05-01/2003-05-01")))
                  .granularity(QueryGranularities.ALL)
                  .filters(
                      AND(
                          SELECTOR("dim2", "a", null),
                          OR(
                              TIME_BOUND("2000/2001"),
                              AND(
                                  SELECTOR("dim1", "abc", null),
                                  TIME_BOUND("2002-05-01/2003-05-01")
                              )
                          )
                      )
                  )
                  .aggregators(AGGS(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }

  @Test
  public void testCountStarWithNotOfComplexDisjointTimeFilter() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo "
        + "WHERE not (dim2 = 'a' and ("
        + "    (__time >= TIMESTAMP '2000-01-01 00:00:00' AND __time < TIMESTAMP '2001-01-01 00:00:00')"
        + "    OR ("
        + "      (__time >= TIMESTAMP '2002-01-01 00:00:00' AND __time < TIMESTAMP '2004-01-01 00:00:00')"
        + "      and (__time >= TIMESTAMP '2002-05-01 00:00:00' AND __time < TIMESTAMP '2003-05-01 00:00:00')"
        + "      and dim1 = 'abc'"
        + "    )"
        + "  )"
        + ")",
        ImmutableList.<Query>of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Filtration.eternity()))
                  .filters(
                      NOT(AND(
                          SELECTOR("dim2", "a", null),
                          OR(
                              TIME_BOUND("2000/2001"),
                              AND(
                                  SELECTOR("dim1", "abc", null),
                                  TIME_BOUND("2002-05-01/2003-05-01")
                              )
                          )
                      ))
                  )
                  .granularity(QueryGranularities.ALL)
                  .aggregators(AGGS(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{5L}
        )
    );
  }

  @Test
  public void testCountStarWithNotTimeFilter() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo "
        + "WHERE dim1 <> 'xxx' and not ("
        + "    (__time >= TIMESTAMP '2000-01-01 00:00:00' AND __time < TIMESTAMP '2001-01-01 00:00:00')"
        + "    OR (__time >= TIMESTAMP '2003-01-01 00:00:00' AND __time < TIMESTAMP '2004-01-01 00:00:00'))",
        ImmutableList.<Query>of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(
                      QSS(
                          new Interval(Filtration.eternity().getStart(), new DateTime("2000")),
                          new Interval("2001/2003"),
                          new Interval(new DateTime("2004"), Filtration.eternity().getEnd())
                      )
                  )
                  .filters(NOT(SELECTOR("dim1", "xxx", null)))
                  .granularity(QueryGranularities.ALL)
                  .aggregators(AGGS(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @Test
  public void testCountStarWithTimeAndDimFilter() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo "
        + "WHERE dim2 <> 'a' "
        + "and __time BETWEEN TIMESTAMP '2000-01-01 00:00:00' AND TIMESTAMP '2000-12-31 23:59:59.999'",
        ImmutableList.<Query>of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(new Interval("2000-01-01/2001-01-01")))
                  .filters(NOT(SELECTOR("dim2", "a", null)))
                  .granularity(QueryGranularities.ALL)
                  .aggregators(AGGS(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{2L}
        )
    );
  }

  @Test
  public void testCountStarWithTimeOrDimFilter() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo "
        + "WHERE dim2 <> 'a' "
        + "or __time BETWEEN TIMESTAMP '2000-01-01 00:00:00' AND TIMESTAMP '2000-12-31 23:59:59.999'",
        ImmutableList.<Query>of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Filtration.eternity()))
                  .filters(
                      OR(
                          NOT(SELECTOR("dim2", "a", null)),
                          BOUND(
                              "__time",
                              String.valueOf(T("2000-01-01")),
                              String.valueOf(T("2000-12-31T23:59:59.999")),
                              false,
                              false,
                              null,
                              StringComparators.NUMERIC
                          )
                      )
                  )
                  .granularity(QueryGranularities.ALL)
                  .aggregators(AGGS(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{5L}
        )
    );
  }

  @Test
  public void testCountStarWithTimeFilterOnLongColumn() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE "
        + "cnt >= EXTRACT(EPOCH FROM TIMESTAMP '1970-01-01 00:00:00') * 1000 "
        + "AND cnt < EXTRACT(EPOCH FROM TIMESTAMP '1970-01-02 00:00:00') * 1000",
        ImmutableList.<Query>of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Filtration.eternity()))
                  .granularity(QueryGranularities.ALL)
                  .filters(
                      BOUND(
                          "cnt",
                          String.valueOf(new DateTime("1970-01-01").getMillis()),
                          String.valueOf(new DateTime("1970-01-02").getMillis()),
                          false,
                          true,
                          null,
                          StringComparators.NUMERIC
                      )
                  )
                  .aggregators(AGGS(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L}
        )
    );
  }

  @Test
  public void testSelectDistinctWithCascadeExtractionFilter() throws Exception
  {
    testQuery(
        "SELECT distinct dim1 FROM druid.foo WHERE substring(substring(dim1, 2), 1, 1) = 'e' OR dim2 = 'a'",
        ImmutableList.<Query>of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(QSS(Filtration.eternity()))
                        .setGranularity(QueryGranularities.ALL)
                        .setDimensions(DIMS(new DefaultDimensionSpec("dim1", "d0")))
                        .setDimFilter(
                            OR(
                                SELECTOR(
                                    "dim1",
                                    "e",
                                    CASCADE(
                                        new SubstringDimExtractionFn(1, null),
                                        new SubstringDimExtractionFn(0, 1)
                                    )
                                ),
                                SELECTOR("dim2", "a", null)
                            )
                        )
                        .build()
        ),
        ImmutableList.of(
            new Object[]{""},
            new Object[]{"1"},
            new Object[]{"def"}
        )
    );
  }

  @Test
  public void testSelectDistinctWithStrlenFilter() throws Exception
  {
    testQuery(
        "SELECT distinct dim1 FROM druid.foo "
        + "WHERE CHARACTER_LENGTH(dim1) = 3 OR CAST(CHARACTER_LENGTH(dim1) AS varchar) = 3",
        ImmutableList.<Query>of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(QSS(Filtration.eternity()))
                        .setGranularity(QueryGranularities.ALL)
                        .setDimensions(DIMS(new DefaultDimensionSpec("dim1", "d0")))
                        .setDimFilter(
                            OR(
                                NUMERIC_SELECTOR("dim1", "3", StrlenExtractionFn.instance()),
                                SELECTOR("dim1", "3", StrlenExtractionFn.instance())
                            )
                        )
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"abc"},
            new Object[]{"def"}
        )
    );
  }

  @Test
  public void testSelectDistinctWithLimit() throws Exception
  {
    // Should use topN even if approximate topNs are off, because this query is exact.

    testQuery(
        "SELECT DISTINCT dim2 FROM druid.foo LIMIT 10",
        ImmutableList.<Query>of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(QSS(Filtration.eternity()))
                .granularity(QueryGranularities.ALL)
                .dimension(new DefaultDimensionSpec("dim2", "d0"))
                .metric(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC))
                .threshold(10)
                .build()
        ),
        ImmutableList.of(
            new Object[]{""},
            new Object[]{"a"},
            new Object[]{"abc"}
        )
    );
  }

  @Test
  public void testCountDistinct() throws Exception
  {
    testQuery(
        "SELECT SUM(cnt), COUNT(distinct dim2), COUNT(distinct unique_dim1) FROM druid.foo",
        ImmutableList.<Query>of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Filtration.eternity()))
                  .granularity(QueryGranularities.ALL)
                  .aggregators(
                      AGGS(
                          new LongSumAggregatorFactory("a0", "cnt"),
                          new CardinalityAggregatorFactory(
                              "a1",
                              null,
                              DIMS(new DefaultDimensionSpec("dim2", null)),
                              false
                          ),
                          new HyperUniquesAggregatorFactory("a2", "unique_dim1")
                      )
                  )
                  .context(TIMESERIES_CONTEXT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L, 3L, 6L}
        )
    );
  }

  @Test
  public void testApproxCountDistinct() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  SUM(cnt),\n"
        + "  APPROX_COUNT_DISTINCT(dim2),\n" // uppercase
        + "  approx_count_distinct(dim2) FILTER(WHERE dim2 <> ''),\n" // lowercase; also, filtered
        + "  approx_count_distinct(unique_dim1)\n" // on native hyperUnique column
        + "FROM druid.foo",
        ImmutableList.<Query>of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Filtration.eternity()))
                  .granularity(QueryGranularities.ALL)
                  .aggregators(
                      AGGS(
                          new LongSumAggregatorFactory("a0", "cnt"),
                          new CardinalityAggregatorFactory(
                              "a1",
                              null,
                              DIMS(new DefaultDimensionSpec("dim2", "dim2")),
                              false
                          ),
                          new FilteredAggregatorFactory(
                              new CardinalityAggregatorFactory(
                                  "a2",
                                  null,
                                  DIMS(new DefaultDimensionSpec("dim2", "dim2")),
                                  false
                              ),
                              NOT(SELECTOR("dim2", "", null))
                          ),
                          new HyperUniquesAggregatorFactory("a3", "unique_dim1")
                      )
                  )
                  .context(TIMESERIES_CONTEXT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L, 3L, 2L, 6L}
        )
    );
  }

  @Test
  public void testDoubleNestedGroupBy() throws Exception
  {
    testQuery(
        "SELECT SUM(cnt), COUNT(*) FROM (\n"
        + "  SELECT dim2, SUM(t1.cnt) cnt FROM (\n"
        + "    SELECT\n"
        + "      dim1,\n"
        + "      dim2,\n"
        + "      COUNT(*) cnt\n"
        + "    FROM druid.foo\n"
        + "    GROUP BY dim1, dim2\n"
        + "  ) t1\n"
        + "  GROUP BY dim2\n"
        + ") t2",
        null,
        ImmutableList.of(
            new Object[]{6L, 3L}
        )
    );
  }

  @Test
  public void testDoubleNestedGroupByForbiddenByConfig() throws Exception
  {
    assertQueryIsUnplannable(
        PLANNER_CONFIG_SINGLE_NESTING_ONLY,
        "SELECT SUM(cnt), COUNT(*) FROM (\n"
        + "  SELECT dim2, SUM(t1.cnt) cnt FROM (\n"
        + "    SELECT\n"
        + "      dim1,\n"
        + "      dim2,\n"
        + "      COUNT(*) cnt\n"
        + "    FROM druid.foo\n"
        + "    GROUP BY dim1, dim2\n"
        + "  ) t1\n"
        + "  GROUP BY dim2\n"
        + ") t2"
    );
  }

  @Test
  public void testExactCountDistinctUsingSubquery() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_SINGLE_NESTING_ONLY, // Sanity check; this query should work with a single level of nesting.
        "SELECT\n"
        + "  SUM(cnt),\n"
        + "  COUNT(*)\n"
        + "FROM (SELECT dim2, SUM(cnt) AS cnt FROM druid.foo GROUP BY dim2)",
        ImmutableList.<Query>of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                GroupByQuery.builder()
                                            .setDataSource(CalciteTests.DATASOURCE1)
                                            .setInterval(QSS(Filtration.eternity()))
                                            .setGranularity(QueryGranularities.ALL)
                                            .setDimensions(DIMS(new DefaultDimensionSpec("dim2", "d0")))
                                            .setAggregatorSpecs(AGGS(new LongSumAggregatorFactory("a0", "cnt")))
                                            .build()
                            )
                        )
                        .setInterval(QSS(Filtration.eternity()))
                        .setGranularity(QueryGranularities.ALL)
                        .setAggregatorSpecs(AGGS(
                            new LongSumAggregatorFactory("a0", "a0"),
                            new CountAggregatorFactory("a1")
                        ))
                        .build()
        ),
        ImmutableList.of(
            new Object[]{6L, 3L}
        )
    );
  }

  @Test
  public void testTopNFilterJoin() throws Exception
  {
    // Filters on top N values of some dimension by using an inner join.
    testQuery(
        "SELECT t1.dim1, SUM(t1.cnt)\n"
        + "FROM druid.foo t1\n"
        + "  INNER JOIN (\n"
        + "  SELECT\n"
        + "    SUM(cnt) AS sum_cnt,\n"
        + "    dim2\n"
        + "  FROM druid.foo\n"
        + "  GROUP BY dim2\n"
        + "  ORDER BY 1 DESC\n"
        + "  LIMIT 2\n"
        + ") t2 ON (t1.dim2 = t2.dim2)\n"
        + "GROUP BY t1.dim1\n"
        + "ORDER BY 1\n",
        ImmutableList.<Query>of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(QSS(Filtration.eternity()))
                .granularity(QueryGranularities.ALL)
                .dimension(new DefaultDimensionSpec("dim2", "d0"))
                .aggregators(AGGS(new LongSumAggregatorFactory("a0", "cnt")))
                .metric(new NumericTopNMetricSpec("a0"))
                .threshold(2)
                .build(),
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(QSS(Filtration.eternity()))
                        .setGranularity(QueryGranularities.ALL)
                        .setDimFilter(IN("dim2", ImmutableList.of("", "a"), null))
                        .setDimensions(DIMS(new DefaultDimensionSpec("dim1", "d0")))
                        .setAggregatorSpecs(AGGS(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(
                                    new OrderByColumnSpec(
                                        "d0",
                                        OrderByColumnSpec.Direction.ASCENDING,
                                        StringComparators.LEXICOGRAPHIC
                                    )
                                ),
                                Integer.MAX_VALUE
                            )
                        )
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", 1L},
            new Object[]{"1", 1L},
            new Object[]{"10.1", 1L},
            new Object[]{"2", 1L},
            new Object[]{"abc", 1L}
        )
    );
  }

  @Test
  public void testRemovableLeftJoin() throws Exception
  {
    // LEFT JOIN where the right-hand side can be ignored.

    testQuery(
        "SELECT t1.dim1, SUM(t1.cnt)\n"
        + "FROM druid.foo t1\n"
        + "  LEFT JOIN (\n"
        + "  SELECT\n"
        + "    SUM(cnt) AS sum_cnt,\n"
        + "    dim2\n"
        + "  FROM druid.foo\n"
        + "  GROUP BY dim2\n"
        + "  ORDER BY 1 DESC\n"
        + "  LIMIT 2\n"
        + ") t2 ON (t1.dim2 = t2.dim2)\n"
        + "GROUP BY t1.dim1\n"
        + "ORDER BY 1\n",
        ImmutableList.<Query>of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(QSS(Filtration.eternity()))
                        .setGranularity(QueryGranularities.ALL)
                        .setDimensions(DIMS(new DefaultDimensionSpec("dim1", "d0")))
                        .setAggregatorSpecs(AGGS(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(
                                    new OrderByColumnSpec(
                                        "d0",
                                        OrderByColumnSpec.Direction.ASCENDING,
                                        StringComparators.LEXICOGRAPHIC
                                    )
                                ),
                                Integer.MAX_VALUE
                            )
                        )
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", 1L},
            new Object[]{"1", 1L},
            new Object[]{"10.1", 1L},
            new Object[]{"2", 1L},
            new Object[]{"abc", 1L},
            new Object[]{"def", 1L}
        )
    );
  }

  @Test
  public void testExactCountDistinctOfSemiJoinResult() throws Exception
  {
    testQuery(
        "SELECT COUNT(*)\n"
        + "FROM (\n"
        + "  SELECT DISTINCT dim2\n"
        + "  FROM druid.foo\n"
        + "  WHERE SUBSTRING(dim2, 1, 1) IN (\n"
        + "    SELECT SUBSTRING(dim1, 1, 1) FROM druid.foo WHERE dim1 <> ''\n"
        + "  )\n"
        + ")",
        ImmutableList.<Query>of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(QSS(Filtration.eternity()))
                        .setGranularity(QueryGranularities.ALL)
                        .setDimFilter(NOT(SELECTOR("dim1", "", null)))
                        .setDimensions(DIMS(new ExtractionDimensionSpec(
                            "dim1",
                            "d0",
                            new SubstringDimExtractionFn(0, 1)
                        )))
                        .build(),
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                GroupByQuery.builder()
                                            .setDataSource(CalciteTests.DATASOURCE1)
                                            .setInterval(QSS(Filtration.eternity()))
                                            .setGranularity(QueryGranularities.ALL)
                                            .setDimFilter(IN(
                                                "dim2",
                                                ImmutableList.of("1", "2", "a", "d"),
                                                new SubstringDimExtractionFn(0, 1)
                                            ))
                                            .setDimensions(DIMS(new DefaultDimensionSpec("dim2", "d0")))
                                            .build()
                            )
                        )
                        .setInterval(QSS(Filtration.eternity()))
                        .setGranularity(QueryGranularities.ALL)
                        .setAggregatorSpecs(AGGS(
                            new CountAggregatorFactory("a0")
                        ))
                        .build()

        ),
        ImmutableList.of(
            new Object[]{2L}
        )
    );
  }

  @Test
  public void testExactCountDistinctUsingSubqueryWithWherePushDown() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  SUM(cnt),\n"
        + "  COUNT(*)\n"
        + "FROM (SELECT dim2, SUM(cnt) AS cnt FROM druid.foo GROUP BY dim2)\n"
        + "WHERE dim2 <> ''",
        ImmutableList.<Query>of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                GroupByQuery.builder()
                                            .setDataSource(CalciteTests.DATASOURCE1)
                                            .setInterval(QSS(Filtration.eternity()))
                                            .setDimFilter(NOT(SELECTOR("dim2", "", null)))
                                            .setGranularity(QueryGranularities.ALL)
                                            .setDimensions(DIMS(new DefaultDimensionSpec("dim2", "d0")))
                                            .setAggregatorSpecs(AGGS(new LongSumAggregatorFactory("a0", "cnt")))
                                            .build()
                            )
                        )
                        .setInterval(QSS(Filtration.eternity()))
                        .setGranularity(QueryGranularities.ALL)
                        .setAggregatorSpecs(AGGS(
                            new LongSumAggregatorFactory("a0", "a0"),
                            new CountAggregatorFactory("a1")
                        ))
                        .build()
        ),
        ImmutableList.of(
            new Object[]{3L, 2L}
        )
    );
  }

  @Test
  public void testExactCountDistinctUsingSubqueryWithWhereToOuterFilter() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  SUM(cnt),\n"
        + "  COUNT(*)\n"
        + "FROM (SELECT dim2, SUM(cnt) AS cnt FROM druid.foo GROUP BY dim2 LIMIT 1)"
        + "WHERE cnt > 0",
        ImmutableList.<Query>of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                GroupByQuery.builder()
                                            .setDataSource(CalciteTests.DATASOURCE1)
                                            .setInterval(QSS(Filtration.eternity()))
                                            .setGranularity(QueryGranularities.ALL)
                                            .setDimensions(DIMS(new DefaultDimensionSpec("dim2", "d0")))
                                            .setAggregatorSpecs(AGGS(new LongSumAggregatorFactory("a0", "cnt")))
                                            .setLimit(1)
                                            .build()
                            )
                        )
                        .setDimFilter(BOUND("a0", "0", null, true, false, null, StringComparators.NUMERIC))
                        .setInterval(QSS(Filtration.eternity()))
                        .setGranularity(QueryGranularities.ALL)
                        .setAggregatorSpecs(AGGS(
                            new LongSumAggregatorFactory("a0", "a0"),
                            new CountAggregatorFactory("a1")
                        ))
                        .build()
        ),
        ImmutableList.of(
            new Object[]{3L, 1L}
        )
    );
  }

  @Test
  public void testCompareExactAndApproximateCountDistinctUsingSubquery() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  COUNT(*) AS exact_count,\n"
        + "  COUNT(DISTINCT dim1) AS approx_count,\n"
        + "  (CAST(1 AS FLOAT) - COUNT(DISTINCT dim1) / COUNT(*)) * 100 AS error_pct\n"
        + "FROM (SELECT DISTINCT dim1 FROM druid.foo WHERE dim1 <> '')",
        ImmutableList.<Query>of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                GroupByQuery.builder()
                                            .setDataSource(CalciteTests.DATASOURCE1)
                                            .setInterval(QSS(Filtration.eternity()))
                                            .setGranularity(QueryGranularities.ALL)
                                            .setDimFilter(NOT(SELECTOR("dim1", "", null)))
                                            .setDimensions(DIMS(new DefaultDimensionSpec("dim1", "d0")))
                                            .build()
                            )
                        )
                        .setInterval(QSS(Filtration.eternity()))
                        .setGranularity(QueryGranularities.ALL)
                        .setAggregatorSpecs(AGGS(
                            new CountAggregatorFactory("a0"),
                            new CardinalityAggregatorFactory(
                                "a1",
                                DIMS(new DefaultDimensionSpec("d0", null)),
                                false
                            )
                        ))
                        .setPostAggregatorSpecs(
                            ImmutableList.<PostAggregator>of(
                                new ArithmeticPostAggregator(
                                    "a2",
                                    "*",
                                    ImmutableList.of(
                                        new ArithmeticPostAggregator(
                                            null,
                                            "-",
                                            ImmutableList.of(
                                                new ConstantPostAggregator(
                                                    null,
                                                    1
                                                ),
                                                new ArithmeticPostAggregator(
                                                    null,
                                                    "quotient",
                                                    ImmutableList.of(
                                                        new HyperUniqueFinalizingPostAggregator(
                                                            "a1",
                                                            "a1"
                                                        ),
                                                        new FieldAccessPostAggregator(
                                                            null,
                                                            "a0"
                                                        )
                                                    )
                                                )
                                            )
                                        ),
                                        new ConstantPostAggregator(null, 100)
                                    )
                                )
                            )
                        )
                        .build()
        ),
        ImmutableList.of(
            new Object[]{5L, 5L, -0.1222693591629298}
        )
    );
  }

  @Test
  public void testHistogramUsingSubquery() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  CAST(thecnt AS VARCHAR),\n"
        + "  COUNT(*)\n"
        + "FROM (SELECT dim2, SUM(cnt) AS thecnt FROM druid.foo GROUP BY dim2)\n"
        + "GROUP BY CAST(thecnt AS VARCHAR)",
        ImmutableList.<Query>of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                GroupByQuery.builder()
                                            .setDataSource(CalciteTests.DATASOURCE1)
                                            .setInterval(QSS(Filtration.eternity()))
                                            .setGranularity(QueryGranularities.ALL)
                                            .setDimensions(DIMS(new DefaultDimensionSpec("dim2", "d0")))
                                            .setAggregatorSpecs(AGGS(new LongSumAggregatorFactory("a0", "cnt")))
                                            .setPostAggregatorSpecs(ImmutableList.<PostAggregator>of(
                                                new FieldAccessPostAggregator("a1", "a0")
                                            ))
                                            .build()
                            )
                        )
                        .setInterval(QSS(Filtration.eternity()))
                        .setGranularity(QueryGranularities.ALL)
                        .setDimensions(DIMS(new DefaultDimensionSpec("a1", "d0")))
                        .setAggregatorSpecs(AGGS(
                            new CountAggregatorFactory("a0")
                        ))
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"1", 1L},
            new Object[]{"2", 1L},
            new Object[]{"3", 1L}
        )
    );
  }

  @Test
  public void testHistogramUsingSubqueryWithSort() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  CAST(thecnt AS VARCHAR),\n"
        + "  COUNT(*)\n"
        + "FROM (SELECT dim2, SUM(cnt) AS thecnt FROM druid.foo GROUP BY dim2)\n"
        + "GROUP BY CAST(thecnt AS VARCHAR) ORDER BY CAST(thecnt AS VARCHAR) LIMIT 2",
        ImmutableList.<Query>of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                GroupByQuery.builder()
                                            .setDataSource(CalciteTests.DATASOURCE1)
                                            .setInterval(QSS(Filtration.eternity()))
                                            .setGranularity(QueryGranularities.ALL)
                                            .setDimensions(DIMS(new DefaultDimensionSpec("dim2", "d0")))
                                            .setAggregatorSpecs(AGGS(new LongSumAggregatorFactory("a0", "cnt")))
                                            .setPostAggregatorSpecs(ImmutableList.<PostAggregator>of(
                                                new FieldAccessPostAggregator("a1", "a0")
                                            ))
                                            .build()
                            )
                        )
                        .setInterval(QSS(Filtration.eternity()))
                        .setGranularity(QueryGranularities.ALL)
                        .setDimensions(DIMS(new DefaultDimensionSpec("a1", "d0")))
                        .setAggregatorSpecs(AGGS(
                            new CountAggregatorFactory("a0")
                        ))
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(new OrderByColumnSpec(
                                    "d0",
                                    OrderByColumnSpec.Direction.ASCENDING,
                                    StringComparators.LEXICOGRAPHIC
                                )),
                                2
                            )
                        )
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"1", 1L},
            new Object[]{"2", 1L}
        )
    );
  }

  @Test
  public void testCountDistinctArithmetic() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  SUM(cnt),\n"
        + "  COUNT(DISTINCT dim2),\n"
        + "  CAST(COUNT(DISTINCT dim2) AS FLOAT),\n"
        + "  SUM(cnt) / COUNT(DISTINCT dim2),\n"
        + "  SUM(cnt) / COUNT(DISTINCT dim2) + 3,\n"
        + "  CAST(SUM(cnt) AS FLOAT) / CAST(COUNT(DISTINCT dim2) AS FLOAT) + 3\n"
        + "FROM druid.foo",
        ImmutableList.<Query>of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Filtration.eternity()))
                  .granularity(QueryGranularities.ALL)
                  .aggregators(
                      AGGS(
                          new LongSumAggregatorFactory("a0", "cnt"),
                          new CardinalityAggregatorFactory(
                              "a1",
                              null,
                              DIMS(new DefaultDimensionSpec("dim2", null)),
                              false
                          )
                      )
                  )
                  .postAggregators(ImmutableList.of(
                      new HyperUniqueFinalizingPostAggregator("a2", "a1"),
                      new ArithmeticPostAggregator("a3", "quotient", ImmutableList.of(
                          new FieldAccessPostAggregator(null, "a0"),
                          new HyperUniqueFinalizingPostAggregator(null, "a1")
                      )),
                      new ArithmeticPostAggregator("a4", "+", ImmutableList.of(
                          new ArithmeticPostAggregator(null, "quotient", ImmutableList.of(
                              new FieldAccessPostAggregator(null, "a0"),
                              new HyperUniqueFinalizingPostAggregator(null, "a1")
                          )),
                          new ConstantPostAggregator(null, 3)
                      )),
                      new ArithmeticPostAggregator("a5", "+", ImmutableList.of(
                          new ArithmeticPostAggregator(null, "quotient", ImmutableList.of(
                              new FieldAccessPostAggregator(null, "a0"),
                              new HyperUniqueFinalizingPostAggregator(null, "a1")
                          )),
                          new ConstantPostAggregator(null, 3)
                      ))
                  ))
                  .context(TIMESERIES_CONTEXT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L, 3L, 3.0021994137521975, 1L, 4L, 4.9985347983600805}
        )
    );
  }

  @Test
  public void testCountDistinctOfSubstring() throws Exception
  {
    testQuery(
        "SELECT COUNT(distinct substring(dim1, 1, 1)) FROM druid.foo WHERE dim1 <> ''",
        ImmutableList.<Query>of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Filtration.eternity()))
                  .filters(NOT(SELECTOR("dim1", "", null)))
                  .granularity(QueryGranularities.ALL)
                  .aggregators(
                      AGGS(
                          new CardinalityAggregatorFactory(
                              "a0",
                              DIMS(
                                  new ExtractionDimensionSpec(
                                      "dim1",
                                      null,
                                      new SubstringDimExtractionFn(0, 1)
                                  )
                              ),
                              false
                          )
                      )
                  )
                  .context(TIMESERIES_CONTEXT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{4L}
        )
    );
  }

  @Test
  public void testGroupBySortPushDown() throws Exception
  {
    testQuery(
        "SELECT dim1, dim2, SUM(cnt) FROM druid.foo GROUP BY dim1, dim2 ORDER BY dim2 LIMIT 4",
        ImmutableList.<Query>of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(QSS(Filtration.eternity()))
                        .setGranularity(QueryGranularities.ALL)
                        .setDimensions(
                            DIMS(
                                new DefaultDimensionSpec("dim2", "d1"),
                                new DefaultDimensionSpec("dim1", "d0")
                            )
                        )
                        .setAggregatorSpecs(
                            AGGS(
                                new LongSumAggregatorFactory("a0", "cnt")
                            )
                        )
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(
                                    new OrderByColumnSpec("d1", OrderByColumnSpec.Direction.ASCENDING)
                                ),
                                4
                            )
                        )
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"10.1", "", 1L},
            new Object[]{"2", "", 1L},
            new Object[]{"abc", "", 1L},
            new Object[]{"", "a", 1L}
        )
    );
  }

  @Test
  public void testGroupByLimitPushDownWithHavingOnLong() throws Exception
  {
    testQuery(
        "SELECT dim1, dim2, SUM(cnt) AS thecnt "
        + "FROM druid.foo "
        + "group by dim1, dim2 "
        + "having SUM(cnt) = 1 "
        + "order by dim2 "
        + "limit 4",
        ImmutableList.<Query>of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(QSS(Filtration.eternity()))
                        .setGranularity(QueryGranularities.ALL)
                        .setDimensions(
                            DIMS(
                                new DefaultDimensionSpec("dim2", "d1"),
                                new DefaultDimensionSpec("dim1", "d0")
                            )
                        )
                        .setAggregatorSpecs(
                            AGGS(
                                new LongSumAggregatorFactory("a0", "cnt")
                            )
                        )
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(
                                    new OrderByColumnSpec("d1", OrderByColumnSpec.Direction.ASCENDING)
                                ),
                                4
                            )
                        )
                        .setHavingSpec(new DimFilterHavingSpec(NUMERIC_SELECTOR("a0", "1", null)))
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"10.1", "", 1L},
            new Object[]{"2", "", 1L},
            new Object[]{"abc", "", 1L},
            new Object[]{"", "a", 1L}
        )
    );
  }

  @Test
  public void testFilterOnTimeFloor() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo "
        + "WHERE floor(__time TO month) = TIMESTAMP '2000-01-01 00:00:00'",
        ImmutableList.<Query>of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(new Interval("2000/P1M")))
                  .granularity(QueryGranularities.ALL)
                  .aggregators(AGGS(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @Test
  public void testFilterOnTimeFloorMisaligned() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo "
        + "WHERE floor(__time TO month) = TIMESTAMP '2000-01-01 00:00:01'",
        ImmutableList.<Query>of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS())
                  .granularity(QueryGranularities.ALL)
                  .aggregators(AGGS(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT)
                  .build()
        ),
        ImmutableList.<Object[]>of()
    );
  }

  @Test
  public void testGroupByFloor() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_NO_SUBQUERIES, // Sanity check; this simple query should work with subqueries disabled.
        "SELECT floor(CAST(dim1 AS float)), COUNT(*) FROM druid.foo GROUP BY floor(CAST(dim1 AS float))",
        ImmutableList.<Query>of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(QSS(Filtration.eternity()))
                        .setGranularity(QueryGranularities.ALL)
                        .setDimensions(DIMS(
                            new ExtractionDimensionSpec("dim1", "d0", new BucketExtractionFn(1.0, 0.0))
                        ))
                        .setAggregatorSpecs(AGGS(new CountAggregatorFactory("a0")))
                        .build()
        ),
        ImmutableList.of(
            new Object[]{null, 3L},
            new Object[]{1.0, 1L},
            new Object[]{10.0, 1L},
            new Object[]{2.0, 1L}
        )
    );
  }

  @Test
  public void testGroupByFloorWithOrderBy() throws Exception
  {
    testQuery(
        "SELECT floor(CAST(dim1 AS float)) AS fl, COUNT(*) FROM druid.foo GROUP BY floor(CAST(dim1 AS float)) ORDER BY fl DESC",
        ImmutableList.<Query>of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(QSS(Filtration.eternity()))
                        .setGranularity(QueryGranularities.ALL)
                        .setDimensions(
                            DIMS(
                                new ExtractionDimensionSpec("dim1", "d0", new BucketExtractionFn(1.0, 0.0))
                            )
                        )
                        .setAggregatorSpecs(AGGS(new CountAggregatorFactory("a0")))
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(
                                    new OrderByColumnSpec(
                                        "d0",
                                        OrderByColumnSpec.Direction.DESCENDING,
                                        StringComparators.NUMERIC
                                    )
                                ),
                                Integer.MAX_VALUE
                            )
                        )
                        .build()
        ),
        ImmutableList.of(
            new Object[]{10.0, 1L},
            new Object[]{2.0, 1L},
            new Object[]{1.0, 1L},
            new Object[]{null, 3L}
        )
    );
  }

  @Test
  public void testGroupByFloorTimeAndOneOtherDimensionWithOrderBy() throws Exception
  {
    testQuery(
        "SELECT floor(__time TO year), dim2, COUNT(*)"
        + " FROM druid.foo"
        + " GROUP BY floor(__time TO year), dim2"
        + " ORDER BY floor(__time TO year), dim2, COUNT(*) DESC",
        ImmutableList.<Query>of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(QSS(Filtration.eternity()))
                        .setGranularity(QueryGranularities.ALL)
                        .setDimensions(
                            DIMS(
                                new ExtractionDimensionSpec(
                                    "__time",
                                    "d0",
                                    new TimeFormatExtractionFn(null, null, null, QueryGranularities.YEAR, true)
                                ),
                                new DefaultDimensionSpec("dim2", "d1")
                            )
                        )
                        .setAggregatorSpecs(
                            AGGS(
                                new CountAggregatorFactory("a0")
                            )
                        )
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(
                                    new OrderByColumnSpec(
                                        "d0",
                                        OrderByColumnSpec.Direction.ASCENDING,
                                        StringComparators.NUMERIC
                                    ),
                                    new OrderByColumnSpec(
                                        "d1",
                                        OrderByColumnSpec.Direction.ASCENDING,
                                        StringComparators.LEXICOGRAPHIC
                                    ),
                                    new OrderByColumnSpec(
                                        "a0",
                                        OrderByColumnSpec.Direction.DESCENDING,
                                        StringComparators.NUMERIC
                                    )
                                ),
                                Integer.MAX_VALUE
                            )
                        )
                        .build()
        ),
        ImmutableList.of(
            new Object[]{T("2000"), "", 2L},
            new Object[]{T("2000"), "a", 1L},
            new Object[]{T("2001"), "", 1L},
            new Object[]{T("2001"), "a", 1L},
            new Object[]{T("2001"), "abc", 1L}
        )
    );
  }

  @Test
  public void testGroupByStringLength() throws Exception
  {
    testQuery(
        "SELECT CHARACTER_LENGTH(dim1), COUNT(*) FROM druid.foo GROUP BY CHARACTER_LENGTH(dim1)",
        ImmutableList.<Query>of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(QSS(Filtration.eternity()))
                        .setGranularity(QueryGranularities.ALL)
                        .setDimensions(
                            DIMS(
                                new ExtractionDimensionSpec(
                                    "dim1",
                                    "d0",
                                    StrlenExtractionFn.instance()
                                )
                            )
                        )
                        .setAggregatorSpecs(
                            AGGS(
                                new CountAggregatorFactory("a0")
                            )
                        )
                        .build()
        ),
        ImmutableList.of(
            new Object[]{0, 1L},
            new Object[]{1, 2L},
            new Object[]{3, 2L},
            new Object[]{4, 1L}
        )
    );
  }

  @Test
  public void testTimeseries() throws Exception
  {
    testQuery(
        "SELECT SUM(cnt), gran FROM (\n"
        + "  SELECT floor(__time TO month) AS gran,\n"
        + "  cnt FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "ORDER BY gran",
        ImmutableList.<Query>of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Filtration.eternity()))
                  .granularity(QueryGranularities.MONTH)
                  .aggregators(AGGS(new LongSumAggregatorFactory("a0", "cnt")))
                  .context(TIMESERIES_CONTEXT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L, T("2000-01-01")},
            new Object[]{3L, T("2001-01-01")}
        )
    );
  }

  @Test
  public void testTimeseriesDescending() throws Exception
  {
    testQuery(
        "SELECT gran, SUM(cnt) FROM (\n"
        + "  SELECT floor(__time TO month) AS gran,\n"
        + "  cnt FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "ORDER BY gran DESC",
        ImmutableList.<Query>of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(QSS(Filtration.eternity()))
                  .granularity(QueryGranularities.MONTH)
                  .aggregators(AGGS(new LongSumAggregatorFactory("a0", "cnt")))
                  .descending(true)
                  .context(TIMESERIES_CONTEXT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{T("2001-01-01"), 3L},
            new Object[]{T("2000-01-01"), 3L}
        )
    );
  }

  @Test
  public void testGroupByExtractYear() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  EXTRACT(YEAR FROM __time) AS \"year\",\n"
        + "  SUM(cnt)\n"
        + "FROM druid.foo\n"
        + "GROUP BY EXTRACT(YEAR FROM __time)\n"
        + "ORDER BY 1",
        ImmutableList.<Query>of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(QSS(Filtration.eternity()))
                        .setGranularity(QueryGranularities.ALL)
                        .setDimensions(
                            DIMS(
                                new ExtractionDimensionSpec(
                                    "__time",
                                    "d0",
                                    new TimeFormatExtractionFn("Y", null, null, QueryGranularities.NONE, true)
                                )
                            )
                        )
                        .setAggregatorSpecs(AGGS(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(
                                    new OrderByColumnSpec(
                                        "d0",
                                        OrderByColumnSpec.Direction.ASCENDING,
                                        StringComparators.NUMERIC
                                    )
                                ),
                                Integer.MAX_VALUE
                            )
                        )
                        .build()
        ),
        ImmutableList.of(
            new Object[]{2000L, 3L},
            new Object[]{2001L, 3L}
        )
    );
  }

  @Test
  public void testExtractFloorTime() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "EXTRACT(YEAR FROM FLOOR(__time TO YEAR)) AS \"year\", SUM(cnt)\n"
        + "FROM druid.foo\n"
        + "GROUP BY EXTRACT(YEAR FROM FLOOR(__time TO YEAR))",
        ImmutableList.<Query>of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(QSS(Filtration.eternity()))
                        .setGranularity(QueryGranularities.ALL)
                        .setDimensions(
                            DIMS(
                                new ExtractionDimensionSpec(
                                    "__time",
                                    "d0",
                                    CASCADE(
                                        new TimeFormatExtractionFn(null, null, null, QueryGranularities.YEAR, true),
                                        new TimeFormatExtractionFn("Y", null, null, QueryGranularities.NONE, true)
                                    )
                                )
                            )
                        )
                        .setAggregatorSpecs(AGGS(new LongSumAggregatorFactory("a0", "cnt")))
                        .build()
        ),
        ImmutableList.of(
            new Object[]{2000L, 3L},
            new Object[]{2001L, 3L}
        )
    );
  }

  @Test
  public void testTimeseriesWithLimitNoTopN() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_NO_TOPN,
        "SELECT gran, SUM(cnt)\n"
        + "FROM (\n"
        + "  SELECT floor(__time TO month) AS gran, cnt\n"
        + "  FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "ORDER BY gran\n"
        + "LIMIT 1",
        ImmutableList.<Query>of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(QSS(Filtration.eternity()))
                        .setGranularity(QueryGranularities.ALL)
                        .setDimensions(
                            DIMS(
                                new ExtractionDimensionSpec(
                                    "__time",
                                    "d0",
                                    new TimeFormatExtractionFn(null, null, null, QueryGranularities.MONTH, true)
                                )
                            )
                        )
                        .setAggregatorSpecs(AGGS(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(
                                    new OrderByColumnSpec(
                                        "d0",
                                        OrderByColumnSpec.Direction.ASCENDING,
                                        StringComparators.NUMERIC
                                    )
                                ),
                                1
                            )
                        )
                        .build()
        ),
        ImmutableList.of(
            new Object[]{T("2000-01-01"), 3L}
        )
    );
  }

  @Test
  public void testTimeseriesWithLimit() throws Exception
  {
    testQuery(
        "SELECT gran, SUM(cnt)\n"
        + "FROM (\n"
        + "  SELECT floor(__time TO month) AS gran, cnt\n"
        + "  FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "ORDER BY gran\n"
        + "LIMIT 1",
        ImmutableList.<Query>of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(QSS(Filtration.eternity()))
                .granularity(QueryGranularities.ALL)
                .dimension(
                    new ExtractionDimensionSpec(
                        "__time",
                        "d0",
                        new TimeFormatExtractionFn(null, null, null, QueryGranularities.MONTH, true)
                    )
                )
                .aggregators(AGGS(new LongSumAggregatorFactory("a0", "cnt")))
                .metric(new DimensionTopNMetricSpec(null, StringComparators.NUMERIC))
                .threshold(1)
                .build()
        ),
        ImmutableList.of(
            new Object[]{T("2000-01-01"), 3L}
        )
    );
  }

  @Test
  public void testGroupByTimeAndOtherDimension() throws Exception
  {
    testQuery(
        "SELECT dim2, gran, SUM(cnt)\n"
        + "FROM (SELECT FLOOR(__time TO MONTH) AS gran, dim2, cnt FROM druid.foo) AS x\n"
        + "GROUP BY dim2, gran\n"
        + "ORDER BY dim2, gran",
        ImmutableList.<Query>of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(QSS(Filtration.eternity()))
                        .setGranularity(QueryGranularities.ALL)
                        .setDimensions(
                            DIMS(
                                new DefaultDimensionSpec("dim2", "d1"),
                                new ExtractionDimensionSpec(
                                    "__time",
                                    "d0",
                                    new TimeFormatExtractionFn(null, null, null, QueryGranularities.MONTH, true)
                                )
                            )
                        )
                        .setAggregatorSpecs(AGGS(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(
                                    new OrderByColumnSpec("d1", OrderByColumnSpec.Direction.ASCENDING),
                                    new OrderByColumnSpec(
                                        "d0",
                                        OrderByColumnSpec.Direction.ASCENDING,
                                        StringComparators.NUMERIC
                                    )
                                ),
                                Integer.MAX_VALUE
                            )
                        )
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", T("2000-01-01"), 2L},
            new Object[]{"", T("2001-01-01"), 1L},
            new Object[]{"a", T("2000-01-01"), 1L},
            new Object[]{"a", T("2001-01-01"), 1L},
            new Object[]{"abc", T("2001-01-01"), 1L}
        )
    );
  }

  @Test
  public void testUsingSubqueryAsFilter() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_SINGLE_NESTING_ONLY, // Sanity check; this query should work with a single level of nesting.
        "SELECT dim1, dim2, COUNT(*) FROM druid.foo "
        + "WHERE dim2 IN (SELECT dim1 FROM druid.foo WHERE dim1 <> '')"
        + "AND dim1 <> 'xxx'"
        + "group by dim1, dim2 ORDER BY dim2",
        ImmutableList.<Query>of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(QSS(Filtration.eternity()))
                        .setGranularity(QueryGranularities.ALL)
                        .setDimFilter(NOT(SELECTOR("dim1", "", null)))
                        .setDimensions(DIMS(new DefaultDimensionSpec("dim1", "d0")))
                        .build(),
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(QSS(Filtration.eternity()))
                        .setGranularity(QueryGranularities.ALL)
                        .setDimFilter(
                            AND(
                                IN("dim2", ImmutableList.of("1", "10.1", "2", "abc", "def"), null),
                                NOT(SELECTOR("dim1", "xxx", null))
                            )
                        )
                        .setDimensions(
                            DIMS(
                                new DefaultDimensionSpec("dim2", "d1"),
                                new DefaultDimensionSpec("dim1", "d0")
                            )
                        )
                        .setAggregatorSpecs(AGGS(new CountAggregatorFactory("a0")))
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(new OrderByColumnSpec("d1", OrderByColumnSpec.Direction.ASCENDING)),
                                Integer.MAX_VALUE
                            )
                        )
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"def", "abc", 1L}
        )
    );
  }

  @Test
  public void testUsingSubqueryAsFilterForbiddenByConfig() throws Exception
  {
    assertQueryIsUnplannable(
        PLANNER_CONFIG_NO_SUBQUERIES,
        "SELECT dim1, dim2, COUNT(*) FROM druid.foo "
        + "WHERE dim2 IN (SELECT dim1 FROM druid.foo WHERE dim1 <> '')"
        + "AND dim1 <> 'xxx'"
        + "group by dim1, dim2 ORDER BY dim2"
    );
  }

  @Test
  public void testUsingSubqueryAsFilterOnTwoColumns() throws Exception
  {
    testQuery(
        "SELECT __time, cnt, dim1, dim2 FROM druid.foo "
        + " WHERE (dim1, dim2) IN ("
        + "   SELECT dim1, dim2 FROM ("
        + "     SELECT dim1, dim2, COUNT(*)"
        + "     FROM druid.foo"
        + "     WHERE dim2 = 'abc'"
        + "     GROUP BY dim1, dim2"
        + "     HAVING COUNT(*) = 1"
        + "   )"
        + " )",
        ImmutableList.<Query>of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(QSS(Filtration.eternity()))
                        .setGranularity(QueryGranularities.ALL)
                        .setDimFilter(SELECTOR("dim2", "abc", null))
                        .setDimensions(DIMS(
                            new DefaultDimensionSpec("dim1", "d0"),
                            new DefaultDimensionSpec("dim2", "d1")
                        ))
                        .setAggregatorSpecs(AGGS(new CountAggregatorFactory("a0")))
                        .setHavingSpec(new DimFilterHavingSpec(NUMERIC_SELECTOR("a0", "1", null)))
                        .build(),
            Druids.newSelectQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .dimensionSpecs(DIMS(
                      new DefaultDimensionSpec("dim1", "d1"),
                      new DefaultDimensionSpec("dim2", "d2")
                  ))
                  .metrics(ImmutableList.of("cnt"))
                  .intervals(QSS(Filtration.eternity()))
                  .granularity(QueryGranularities.ALL)
                  .filters(AND(SELECTOR("dim1", "def", null), SELECTOR("dim2", "abc", null)))
                  .pagingSpec(FIRST_PAGING_SPEC)
                  .build(),
            Druids.newSelectQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .dimensionSpecs(DIMS(
                      new DefaultDimensionSpec("dim1", "d1"),
                      new DefaultDimensionSpec("dim2", "d2")
                  ))
                  .metrics(ImmutableList.of("cnt"))
                  .intervals(QSS(Filtration.eternity()))
                  .granularity(QueryGranularities.ALL)
                  .filters(AND(SELECTOR("dim1", "def", null), SELECTOR("dim2", "abc", null)))
                  .pagingSpec(
                      new PagingSpec(
                          ImmutableMap.of("foo_1970-01-01T00:00:00.000Z_2001-01-03T00:00:00.001Z_1", 0),
                          1000,
                          true
                      )
                  )
                  .build()
        ),
        ImmutableList.of(
            new Object[]{T("2001-01-02"), 1L, "def", "abc"}
        )
    );
  }

  @Test
  public void testUsingSubqueryWithExtractionFns() throws Exception
  {
    testQuery(
        "SELECT dim2, COUNT(*) FROM druid.foo "
        + "WHERE substring(dim2, 1, 1) IN (SELECT substring(dim1, 1, 1) FROM druid.foo WHERE dim1 <> '')"
        + "group by dim2",
        ImmutableList.<Query>of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(QSS(Filtration.eternity()))
                        .setGranularity(QueryGranularities.ALL)
                        .setDimFilter(NOT(SELECTOR("dim1", "", null)))
                        .setDimensions(
                            DIMS(new ExtractionDimensionSpec("dim1", "d0", new SubstringDimExtractionFn(0, 1)))
                        )
                        .build(),
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(QSS(Filtration.eternity()))
                        .setGranularity(QueryGranularities.ALL)
                        .setDimFilter(
                            IN(
                                "dim2",
                                ImmutableList.of("1", "2", "a", "d"),
                                new SubstringDimExtractionFn(0, 1)
                            )
                        )
                        .setDimensions(DIMS(new DefaultDimensionSpec("dim2", "d0")))
                        .setAggregatorSpecs(AGGS(new CountAggregatorFactory("a0")))
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"a", 2L},
            new Object[]{"abc", 1L}
        )
    );
  }

  @Test
  public void testUnicodeFilterAndGroupBy() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  dim1,\n"
        + "  dim2,\n"
        + "  COUNT(*)\n"
        + "FROM foo2\n"
        + "WHERE\n"
        + "  dim1 LIKE U&'\u05D3\\05E8%'\n" // First char is actually in the string; second is a SQL U& escape
        + "  OR dim1 = 'друид'\n"
        + "GROUP BY dim1, dim2",
        ImmutableList.<Query>of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE2)
                        .setInterval(QSS(Filtration.eternity()))
                        .setGranularity(QueryGranularities.ALL)
                        .setDimFilter(OR(
                            new LikeDimFilter("dim1", "דר%", null, null),
                            new SelectorDimFilter("dim1", "друид", null)
                        ))
                        .setDimensions(DIMS(
                            new DefaultDimensionSpec("dim1", "d0"),
                            new DefaultDimensionSpec("dim2", "d1")
                        ))
                        .setAggregatorSpecs(AGGS(new CountAggregatorFactory("a0")))
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"друид", "ru", 1L},
            new Object[]{"דרואיד", "he", 1L}
        )
    );
  }

  private void testQuery(
      final String sql,
      final List<Query> expectedQueries,
      final List<Object[]> expectedResults
  ) throws Exception
  {
    testQuery(PLANNER_CONFIG_DEFAULT, sql, expectedQueries, expectedResults);
  }

  private void testQuery(
      final PlannerConfig plannerConfig,
      final String sql,
      final List<Query> expectedQueries,
      final List<Object[]> expectedResults
  ) throws Exception
  {
    log.info("SQL: %s", sql);
    queryLogHook.clearRecordedQueries();
    final List<Object[]> plannerResults = getResults(plannerConfig, sql);
    verifyResults(sql, expectedQueries, expectedResults, plannerResults);
  }

  private List<Object[]> getResults(
      final PlannerConfig plannerConfig,
      final String sql
  ) throws Exception
  {
    final SchemaPlus rootSchema = Calcites.createRootSchema(CalciteTests.createMockSchema(walker, plannerConfig));
    final DruidOperatorTable operatorTable = CalciteTests.createOperatorTable();

    try (Planner planner = new PlannerFactory(rootSchema, operatorTable, plannerConfig).createPlanner()) {
      final PlannerResult plan = Calcites.plan(planner, sql);
      return Sequences.toList(plan.run(), Lists.<Object[]>newArrayList());
    }
  }

  private void verifyResults(
      final String sql,
      final List<Query> expectedQueries,
      final List<Object[]> expectedResults,
      final List<Object[]> results
  )
  {
    for (int i = 0; i < results.size(); i++) {
      log.info("row #%d: %s", i, Arrays.toString(results.get(i)));
    }

    Assert.assertEquals(String.format("result count: %s", sql), expectedResults.size(), results.size());
    for (int i = 0; i < results.size(); i++) {
      Assert.assertArrayEquals(
          String.format("result #%d: %s", i + 1, sql),
          expectedResults.get(i),
          results.get(i)
      );
    }

    if (expectedQueries != null) {
      final List<Query> recordedQueries = queryLogHook.getRecordedQueries();

      Assert.assertEquals(
          String.format("query count: %s", sql),
          expectedQueries.size(),
          recordedQueries.size()
      );
      for (int i = 0; i < expectedQueries.size(); i++) {
        Assert.assertEquals(
            String.format("query #%d: %s", i + 1, sql),
            expectedQueries.get(i),
            recordedQueries.get(i)
        );
      }
    }
  }

  // Generate timestamps for expected results
  private static long T(final String timeString)
  {
    return new DateTime(timeString).getMillis();
  }

  private static QuerySegmentSpec QSS(final Interval... intervals)
  {
    return new MultipleIntervalSegmentSpec(Arrays.asList(intervals));
  }

  private static AndDimFilter AND(DimFilter... filters)
  {
    return new AndDimFilter(Arrays.asList(filters));
  }

  private static OrDimFilter OR(DimFilter... filters)
  {
    return new OrDimFilter(Arrays.asList(filters));
  }

  private static NotDimFilter NOT(DimFilter filter)
  {
    return new NotDimFilter(filter);
  }

  private static InDimFilter IN(String dimension, List<String> values, ExtractionFn extractionFn)
  {
    return new InDimFilter(dimension, values, extractionFn);
  }

  private static SelectorDimFilter SELECTOR(final String fieldName, final String value, final ExtractionFn extractionFn)
  {
    return new SelectorDimFilter(fieldName, value, extractionFn);
  }

  private static DimFilter NUMERIC_SELECTOR(
      final String fieldName,
      final String value,
      final ExtractionFn extractionFn
  )
  {
    // We use Bound filters for numeric equality to achieve "10.0" = "10"
    return BOUND(fieldName, value, value, false, false, extractionFn, StringComparators.NUMERIC);
  }

  private static BoundDimFilter BOUND(
      final String fieldName,
      final String lower,
      final String upper,
      final boolean lowerStrict,
      final boolean upperStrict,
      final ExtractionFn extractionFn,
      final StringComparator comparator
  )
  {
    return new BoundDimFilter(fieldName, lower, upper, lowerStrict, upperStrict, null, extractionFn, comparator);
  }

  private static BoundDimFilter TIME_BOUND(final Object intervalObj)
  {
    final Interval interval = new Interval(intervalObj);
    return new BoundDimFilter(
        Column.TIME_COLUMN_NAME,
        String.valueOf(interval.getStartMillis()),
        String.valueOf(interval.getEndMillis()),
        false,
        true,
        null,
        null,
        StringComparators.NUMERIC
    );
  }

  private static CascadeExtractionFn CASCADE(final ExtractionFn... fns)
  {
    return new CascadeExtractionFn(fns);
  }

  private static List<DimensionSpec> DIMS(final DimensionSpec... dimensionSpecs)
  {
    return Arrays.asList(dimensionSpecs);
  }

  private static List<AggregatorFactory> AGGS(final AggregatorFactory... aggregators)
  {
    return Arrays.asList(aggregators);
  }
}
