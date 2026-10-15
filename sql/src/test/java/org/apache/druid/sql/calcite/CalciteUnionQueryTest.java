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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.FilteredDataSource;
import org.apache.druid.query.LookupDataSource;
import org.apache.druid.query.ProjectDataSource;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.RestrictedDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.sql.calcite.NotYetSupported.Modes;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class CalciteUnionQueryTest extends BaseCalciteQueryTest
{
  @Test
  public void testUnionAllDifferentTablesWithMapping()
  {
    testQuery(
        "SELECT\n"
        + "dim1, dim2, SUM(m1), COUNT(*)\n"
        + "FROM (SELECT dim1, dim2, m1 FROM foo UNION ALL SELECT dim1, dim2, m1 FROM numfoo)\n"
        + "WHERE dim2 = 'a' OR dim2 = 'def'\n"
        + "GROUP BY 1, 2",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new UnionDataSource(
                                ImmutableList.of(
                                    new TableDataSource(CalciteTests.DATASOURCE1),
                                    new TableDataSource(CalciteTests.DATASOURCE3)
                                )
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimFilter(in("dim2", ImmutableList.of("def", "a")))
                        .setDimensions(
                            new DefaultDimensionSpec("dim1", "d0"),
                            new DefaultDimensionSpec("dim2", "d1")
                        )
                        .setAggregatorSpecs(
                            aggregators(
                                new DoubleSumAggregatorFactory("a0", "m1"),
                                new CountAggregatorFactory("a1")
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", "a", 2.0, 2L},
            new Object[]{"1", "a", 8.0, 2L}
        )
    );
  }

  @Test
  public void testJoinUnionAllDifferentTablesWithMapping()
  {
    testQuery(
        "SELECT\n"
        + "dim1, dim2, SUM(m1), COUNT(*)\n"
        + "FROM (SELECT dim1, dim2, m1 FROM foo UNION ALL SELECT dim1, dim2, m1 FROM numfoo)\n"
        + "WHERE dim2 = 'a' OR dim2 = 'def'\n"
        + "GROUP BY 1, 2",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new UnionDataSource(
                                ImmutableList.of(
                                    new TableDataSource(CalciteTests.DATASOURCE1),
                                    new TableDataSource(CalciteTests.DATASOURCE3)
                                )
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimFilter(in("dim2", ImmutableList.of("def", "a")))
                        .setDimensions(
                            new DefaultDimensionSpec("dim1", "d0"),
                            new DefaultDimensionSpec("dim2", "d1")
                        )
                        .setAggregatorSpecs(
                            aggregators(
                                new DoubleSumAggregatorFactory("a0", "m1"),
                                new CountAggregatorFactory("a1")
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", "a", 2.0, 2L},
            new Object[]{"1", "a", 8.0, 2L}
        )
    );
  }

  @Test
  public void testUnionAllTablesWithFilters()
  {
    // Each arm has its own filter, which becomes a FilteredDataSource so the arms can stay in the UnionDataSource
    // instead of turning into subqueries. Grouped so the row order is deterministic across engines.
    testQuery(
        "SELECT dim2, COUNT(*) FROM (\n"
        + "SELECT dim1, dim2, m1 FROM foo WHERE dim2 = 'a'\n"
        + "UNION ALL\n"
        + "SELECT dim1, dim2, m1 FROM numfoo WHERE dim2 = 'abc'\n"
        + ") GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new UnionDataSource(
                                ImmutableList.of(
                                    FilteredDataSource.create(
                                        new TableDataSource(CalciteTests.DATASOURCE1),
                                        equality("dim2", "a", ColumnType.STRING)
                                    ),
                                    FilteredDataSource.create(
                                        new TableDataSource(CalciteTests.DATASOURCE3),
                                        equality("dim2", "abc", ColumnType.STRING)
                                    )
                                )
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(new DefaultDimensionSpec("dim2", "d0"))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"a", 2L},
            new Object[]{"abc", 1L}
        )
    );
  }

  @Test
  public void testUnionAllTablesWithProjectedExpressions()
  {
    // Each arm computes an expression, which becomes a virtual column on its ProjectDataSource.
    skipVectorize();
    testBuilder()
        .sql(
            "SELECT SUM(x) FROM (\n"
            + "SELECT m1 * 2 AS x FROM foo WHERE dim2 = 'a'\n"
            + "UNION ALL\n"
            + "SELECT m1 * 2 AS x FROM numfoo WHERE dim2 = 'abc'\n"
            + ")"
        )
        .expectedResults(ImmutableList.of(new Object[]{20.0D}))
        .run();
  }

  @Test
  public void testUnionAllTablesWithDifferentColumnsAliasedTheSame()
  {
    // The arms read different columns, and the second presents its "dim2" as "dim1" through its ProjectDataSource.
    skipVectorize();
    testBuilder()
        .sql(
            "SELECT COUNT(*) FROM (\n"
            + "SELECT dim1 AS c FROM foo WHERE dim1 = 'abc'\n"
            + "UNION ALL\n"
            + "SELECT dim2 AS c FROM foo WHERE dim2 = 'abc'\n"
            + ") WHERE c = 'abc'"
        )
        .expectedResults(ImmutableList.of(new Object[]{2L}))
        .run();
  }

  @Test
  public void testUnionAllTablesWithTimeFilters()
  {
    // The arms' time filters are pulled up into the query's intervals, so the broker can prune segments rather than
    // fetching the whole timeline of both tables and filtering row by row.
    testQuery(
        "SELECT dim2, COUNT(*) FROM (\n"
        + "SELECT dim1, dim2 FROM foo WHERE __time >= TIMESTAMP '2001-01-01'\n"
        + "UNION ALL\n"
        + "SELECT dim1, dim2 FROM numfoo WHERE __time >= TIMESTAMP '2001-01-02'\n"
        + ") GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new UnionDataSource(
                                ImmutableList.of(
                                    FilteredDataSource.create(
                                        new TableDataSource(CalciteTests.DATASOURCE1),
                                        range(
                                            ColumnHolder.TIME_COLUMN_NAME,
                                            ColumnType.LONG,
                                            DateTimes.of("2001-01-01").getMillis(),
                                            null,
                                            false,
                                            false
                                        )
                                    ),
                                    FilteredDataSource.create(
                                        new TableDataSource(CalciteTests.DATASOURCE3),
                                        range(
                                            ColumnHolder.TIME_COLUMN_NAME,
                                            ColumnType.LONG,
                                            DateTimes.of("2001-01-02").getMillis(),
                                            null,
                                            false,
                                            false
                                        )
                                    )
                                )
                            )
                        )
                        .setInterval(querySegmentSpec(Intervals.of("2001-01-01/146140482-04-24T15:36:27.903Z")))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(new DefaultDimensionSpec("dim2", "d0"))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{null, 2L},
            new Object[]{"a", 1L},
            new Object[]{"abc", 2L}
        )
    );
  }

  @Test
  public void testUnionAllTopLevelWithDifferentTimeFilters()
  {
    // The arms read different time intervals, so each runs as its own query, with its own intervals. A single query
    // over a union datasource would read the union of the arms' intervals from every arm.
    final String sql =
        "SELECT dim1 FROM foo WHERE __time >= TIMESTAMP '2001-01-02'\n"
        + "UNION ALL\n"
        + "SELECT dim1 FROM numfoo";
    final List<Object[]> expectedResults = ImmutableList.of(
        new Object[]{"def"},
        new Object[]{"abc"},
        new Object[]{""},
        new Object[]{"10.1"},
        new Object[]{"2"},
        new Object[]{"1"},
        new Object[]{"def"},
        new Object[]{"abc"}
    );

    final QueryTestBuilder builder =
        testBuilder()
            .sql(sql)
            .expectedResults(new UnorderedResultsVerifier(expectedResults, ResultMatchMode.EQUALS, null));

    if (!builder.isDecoupledMode() && !isRunningMSQ()) {
      builder.expectedQueries(
          ImmutableList.of(
              newScanQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Intervals.of("2001-01-02/146140482-04-24T15:36:27.903Z")))
                  .columns("dim1")
                  .columnTypes(ColumnType.STRING)
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build(),
              newScanQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .columns("dim1")
                  .columnTypes(ColumnType.STRING)
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
          )
      );
    }

    builder.run();
  }

  @Test
  public void testUnnestFilteredUnionAllWithDisjointTimeFilters()
  {
    // The outer filter sits between the unnest and the union. The arms' disjoint time filters must still be combined
    // into the query intervals, not intersected, which would leave no intervals at all.
    skipVectorize();
    testBuilder()
        .sql(
            "SELECT s.dim1, u FROM (\n"
            + "  SELECT * FROM (\n"
            + "    SELECT dim1, dim3 FROM foo WHERE __time < TIMESTAMP '2000-01-02'\n"
            + "    UNION ALL\n"
            + "    SELECT dim1, dim3 FROM foo\n"
            + "    WHERE __time >= TIMESTAMP '2000-01-03' AND __time < TIMESTAMP '2001-01-01'\n"
            + "  ) WHERE dim1 <> 'zzz'\n"
            + ") AS s CROSS JOIN UNNEST(MV_TO_ARRAY(s.dim3)) AS t(u)"
        )
        .expectedResults(
            new UnorderedResultsVerifier(
                ImmutableList.of(
                    new Object[]{"", "a"},
                    new Object[]{"", "b"},
                    new Object[]{"2", "d"}
                ),
                ResultMatchMode.EQUALS,
                null
            )
        )
        .run();
  }

  @Test
  public void testUnionAllAliasedArmsReadTablesDirectly()
  {
    // Both arms read the same columns under the same aliases, so the union presents them under their own names and
    // the arms read their tables directly, with no projection in between.
    testQuery(
        "SELECT x, SUM(m1) FROM (\n"
        + "SELECT dim1 AS x, m1 FROM foo\n"
        + "UNION ALL\n"
        + "SELECT dim1 AS x, m1 FROM numfoo\n"
        + ") WHERE x = 'abc' OR x = 'def' GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new UnionDataSource(
                                ImmutableList.of(
                                    new TableDataSource(CalciteTests.DATASOURCE1),
                                    new TableDataSource(CalciteTests.DATASOURCE3)
                                )
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimFilter(in("dim1", ImmutableList.of("abc", "def")))
                        .setDimensions(new DefaultDimensionSpec("dim1", "d0"))
                        .setAggregatorSpecs(aggregators(new DoubleSumAggregatorFactory("a0", "m1")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"abc", 12.0D},
            new Object[]{"def", 10.0D}
        )
    );
  }

  @Test
  public void testUnionAllArmComputesTime()
  {
    // The second arm computes the column the first arm reads as its own __time. Query intervals apply to each arm's
    // own __time, so the union names this column something else, and the outer filter reads it like any other.
    testBuilder()
        .sql(
            "SELECT COUNT(*) FROM (\n"
            + "SELECT __time, dim1 FROM foo\n"
            + "UNION ALL\n"
            + "SELECT TIMESTAMPADD(YEAR, 1, __time) AS __time, dim1 FROM numfoo\n"
            + ") WHERE __time >= TIMESTAMP '2002-01-01'"
        )
        .expectedResults(ImmutableList.of(new Object[]{3L}))
        .run();
  }

  @Test
  public void testUnionAllArmComputesTimeGroupByYear()
  {
    // Likewise for granularity, which would otherwise bucket each arm by its own __time.
    testBuilder()
        .sql(
            "SELECT FLOOR(__time TO YEAR), COUNT(*) FROM (\n"
            + "SELECT __time, dim1 FROM foo\n"
            + "UNION ALL\n"
            + "SELECT TIMESTAMPADD(YEAR, 1, __time) AS __time, dim1 FROM numfoo\n"
            + ") GROUP BY 1"
        )
        .expectedResults(
            ImmutableList.of(
                new Object[]{DateTimes.of("2000-01-01").getMillis(), 3L},
                new Object[]{DateTimes.of("2001-01-01").getMillis(), 6L},
                new Object[]{DateTimes.of("2002-01-01").getMillis(), 3L}
            )
        )
        .run();
  }

  @Test
  public void testUnionAllArmComputesTimeMax()
  {
    testBuilder()
        .sql(
            "SELECT MAX(__time) FROM (\n"
            + "SELECT __time FROM foo\n"
            + "UNION ALL\n"
            + "SELECT TIMESTAMPADD(YEAR, 1, __time) AS __time FROM numfoo\n"
            + ")"
        )
        .expectedResults(ImmutableList.of(new Object[]{DateTimes.of("2002-01-03").getMillis()}))
        .run();
  }

  @Test
  public void testUnionAllArmTypesComeFromUnion()
  {
    // The first arm reads a LONG column, but the union's column is FLOAT, as the second arm's values need. Typing the
    // union after its first arm would truncate them.
    testBuilder()
        .sql(
            "SELECT x, COUNT(*) FROM (\n"
            + "SELECT cnt AS x FROM foo\n"
            + "UNION ALL\n"
            + "SELECT m1 / 4 AS x FROM foo\n"
            + ") GROUP BY 1"
        )
        .expectedResults(
            ImmutableList.of(
                new Object[]{0.25F, 1L},
                new Object[]{0.5F, 1L},
                new Object[]{0.75F, 1L},
                new Object[]{1.0F, 7L},
                new Object[]{1.25F, 1L},
                new Object[]{1.5F, 1L}
            )
        )
        .run();
  }

  @Test
  public void testUnionAllArmTypesWidenedForExpressions()
  {
    // The first arm's "l1" is a LONG, but the union's column is a DOUBLE. An expression over the union must see DOUBLE
    // values from every arm, or "x / 2" would divide integers in the first.
    testBuilder()
        .sql(
            "SELECT x / 2 FROM (\n"
            + "SELECT l1 AS x FROM numfoo\n"
            + "UNION ALL\n"
            + "SELECT dbl1 AS x FROM numfoo\n"
            + ") WHERE x = 7"
        )
        .expectedResults(ImmutableList.of(new Object[]{3.5D}))
        .run();
  }

  @NotYetSupported(Modes.NOT_ENOUGH_RULES)
  @Test
  public void testUnionAllLexicographicTopNOverFilteredRenamedArm()
  {
    // The second arm renames a column and filters rows, so its segments lack some of that column's dictionary ids.
    // A lexicographic topN that assumed every id was present would stop early and miss the second arm's "abc".
    testBuilder()
        .sql(
            "SELECT x, COUNT(*) FROM (\n"
            + "SELECT dim1 AS x FROM foo WHERE dim1 = 'abc'\n"
            + "UNION ALL\n"
            + "SELECT dim2 AS x FROM numfoo WHERE dim2 = 'abc'\n"
            + ") GROUP BY x ORDER BY x LIMIT 2"
        )
        .queryContext(
            ImmutableMap.of(PlannerConfig.CTX_KEY_USE_LEXICOGRAPHIC_TOPN, true, QueryContexts.MIN_TOP_N_THRESHOLD, 1)
        )
        .expectedResults(ImmutableList.of(new Object[]{"abc", 2L}))
        .run();
  }

  @Test
  public void testUnionAllArmFilterOnStringExpression()
  {
    // Where the union runs as a single query, as in MSQ, the arm's filter reads a virtual column, so its
    // FilteredDataSource goes on top of the ProjectDataSource that defines it. Underneath, a filter on "v0" would
    // match nothing and quietly lose the whole arm. The arms' rows may interleave.
    skipVectorize();
    testBuilder()
        .sql(
            "SELECT dim1 FROM foo WHERE UPPER(dim1) = 'ABC'\n"
            + "UNION ALL\n"
            + "SELECT dim1 FROM numfoo"
        )
        .expectedResults(
            new UnorderedResultsVerifier(
                ImmutableList.of(
                    new Object[]{"abc"},
                    new Object[]{""},
                    new Object[]{"10.1"},
                    new Object[]{"2"},
                    new Object[]{"1"},
                    new Object[]{"def"},
                    new Object[]{"abc"}
                ),
                ResultMatchMode.EQUALS,
                null
            )
        )
        .run();
  }

  @Test
  public void testUnionAllArmFilterOnArithmeticExpression()
  {
    skipVectorize();
    testBuilder()
        .sql(
            "SELECT dim1 FROM foo WHERE m1 * 2 > 5\n"
            + "UNION ALL\n"
            + "SELECT dim1 FROM numfoo"
        )
        .expectedResults(
            new UnorderedResultsVerifier(
                ImmutableList.of(
                    new Object[]{"2"},
                    new Object[]{"1"},
                    new Object[]{"def"},
                    new Object[]{"abc"},
                    new Object[]{""},
                    new Object[]{"10.1"},
                    new Object[]{"2"},
                    new Object[]{"1"},
                    new Object[]{"def"},
                    new Object[]{"abc"}
                ),
                ResultMatchMode.EQUALS,
                null
            )
        )
        .run();
  }

  @Test
  public void testUnionAllArmFiltersOnExpressionsUnderGroupBy()
  {
    // Each arm's filter reads a virtual column, so its FilteredDataSource goes on top of its ProjectDataSource. The
    // arms stay in the UnionDataSource, which is the only way a union can sit underneath a GROUP BY.
    testQuery(
        "SELECT dim1, COUNT(*) FROM (\n"
        + "SELECT dim1 FROM foo WHERE UPPER(dim1) = 'ABC'\n"
        + "UNION ALL\n"
        + "SELECT dim1 FROM numfoo WHERE m1 * 2 > 9\n"
        + ") GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new UnionDataSource(
                                ImmutableList.of(
                                    FilteredDataSource.create(
                                        new ProjectDataSource(
                                            new TableDataSource(CalciteTests.DATASOURCE1),
                                            VirtualColumns.create(
                                                expressionVirtualColumn("v0", "upper(\"dim1\")", ColumnType.STRING)
                                            )
                                        ),
                                        equality("v0", "ABC", ColumnType.STRING)
                                    ),
                                    FilteredDataSource.create(
                                        new ProjectDataSource(
                                            new TableDataSource(CalciteTests.DATASOURCE3),
                                            VirtualColumns.create(
                                                expressionVirtualColumn("v0", "(\"m1\" * 2)", ColumnType.FLOAT)
                                            )
                                        ),
                                        range("v0", ColumnType.FLOAT, 9.0f, null, true, false)
                                    )
                                )
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(new DefaultDimensionSpec("dim1", "d0"))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"abc", 2L},
            new Object[]{"def", 1L}
        )
    );
  }

  @Test
  public void testUnionAllArmFilterOnRenamedExpression()
  {
    // The first arm's filter reads the virtual column that it also presents, under the union's name "x". The rename
    // goes on top of the FilteredDataSource, which goes on top of the ProjectDataSource that computes the column.
    testBuilder()
        .sql(
            "SELECT x, COUNT(*) FROM (\n"
            + "SELECT UPPER(dim1) AS x FROM foo WHERE UPPER(dim1) IN ('ABC', 'DEF')\n"
            + "UNION ALL\n"
            + "SELECT dim2 AS x FROM numfoo WHERE dim2 = 'a'\n"
            + ") GROUP BY 1"
        )
        .expectedResults(
            ImmutableList.of(
                new Object[]{"ABC", 1L},
                new Object[]{"DEF", 1L},
                new Object[]{"a", 2L}
            )
        )
        .run();
  }

  @Test
  public void testUnionAllFilterComparingTwoRenamedColumns()
  {
    // An expression filter over renamed columns cannot be rewritten into the arms' namespace, so the projection
    // applies it above the cursor rather than pushing it down.
    skipVectorize();
    testBuilder()
        .sql(
            "SELECT COUNT(*) FROM (\n"
            + "SELECT dim1 AS x, dim2 AS y FROM foo\n"
            + "UNION ALL\n"
            + "SELECT dim2 AS x, dim1 AS y FROM foo\n"
            + ") WHERE x <> y"
        )
        .expectedResults(ImmutableList.of(new Object[]{8L}))
        .run();
  }

  @Test
  public void testUnionAllOrderByTimeWithRenamedArms()
  {
    // Renaming a column must not cost the arms their time ordering, which a time-ordered scan requires.
    skipVectorize();
    testBuilder()
        .sql(
            "SELECT c, __time FROM (\n"
            + "SELECT dim1 AS c, __time FROM foo WHERE dim1 = 'abc'\n"
            + "UNION ALL\n"
            + "SELECT dim2 AS c, __time FROM foo WHERE dim2 = 'abc'\n"
            + ") ORDER BY __time"
        )
        .expectedResults(
            ImmutableList.of(
                new Object[]{"abc", DateTimes.of("2001-01-02").getMillis()},
                new Object[]{"abc", DateTimes.of("2001-01-03").getMillis()}
            )
        )
        .run();
  }

  @Test
  public void testUnionAllSameColumnTwiceUnderDifferentAliases()
  {
    // The first arm reads one column into two of the union's columns, so it reports that column twice under one name.
    skipVectorize();
    testBuilder()
        .sql(
            "SELECT a, b, COUNT(*) FROM (\n"
            + "SELECT dim1 AS a, dim1 AS b FROM foo\n"
            + "UNION ALL\n"
            + "SELECT dim1, dim2 FROM numfoo\n"
            + ") GROUP BY a, b"
        )
        .expectedResults(
            ImmutableList.of(
                new Object[]{"", "", 1L},
                new Object[]{"", "a", 1L},
                new Object[]{"1", "1", 1L},
                new Object[]{"1", "a", 1L},
                new Object[]{"10.1", null, 1L},
                new Object[]{"10.1", "10.1", 1L},
                new Object[]{"2", "", 1L},
                new Object[]{"2", "2", 1L},
                new Object[]{"abc", null, 1L},
                new Object[]{"abc", "abc", 1L},
                new Object[]{"def", "abc", 1L},
                new Object[]{"def", "def", 1L}
            )
        )
        .run();
  }

  @Test
  public void testUnionAllTopLevelSameColumnTwice()
  {
    // Like testUnionAllSameColumnTwiceUnderDifferentAliases, but with the union at the top level.
    skipVectorize();
    testBuilder()
        .sql(
            "SELECT dim1, dim1 FROM foo WHERE dim1 = 'abc'\n"
            + "UNION ALL\n"
            + "SELECT dim1, dim2 FROM numfoo WHERE dim1 = 'def'"
        )
        .expectedResults(
            new UnorderedResultsVerifier(
                ImmutableList.of(new Object[]{"abc", "abc"}, new Object[]{"def", "abc"}),
                ResultMatchMode.EQUALS,
                null
            )
        )
        .run();
  }

  @Test
  public void testUnionAllWithLookupArm()
  {
    // A lookup cannot be an arm of a UnionDataSource, so this union runs one query per arm instead.
    skipVectorize();
    testBuilder()
        .sql(
            "SELECT k AS c FROM lookup.lookyloo WHERE k = 'abc'\n"
            + "UNION ALL\n"
            + "SELECT dim1 AS c FROM foo WHERE dim1 = 'abc'"
        )
        .expectedResults(
            ImmutableList.of(
                new Object[]{"abc"},
                new Object[]{"abc"}
            )
        )
        .run();
  }

  @Test
  public void testUnionAllWithValuesArm()
  {
    // Likewise for inline data, which is not read from the timeline.
    skipVectorize();
    testBuilder()
        .sql(
            "SELECT c FROM (VALUES ('x'), ('y')) AS t(c)\n"
            + "UNION ALL\n"
            + "SELECT dim1 AS c FROM foo WHERE dim1 = 'abc'"
        )
        .expectedResults(
            ImmutableList.of(
                new Object[]{"x"},
                new Object[]{"y"},
                new Object[]{"abc"}
            )
        )
        .run();
  }

  @Test
  public void testUnionAllWithRestrictedArmsExpression()
  {
    testBuilder()
        .sql(
            """
                SELECT COUNT(*), SUM(x) FROM (
                SELECT m1 AS x FROM foo
                UNION ALL
                SELECT CAST(m2 + 1 AS FLOAT) AS x FROM druid.restrictedDatasource_m1_is_6
                )"""
        )
        .authResult(CalciteTests.REGULAR_USER_AUTH_RESULT)
        .expectedQueries(
            ImmutableList.of(
                Druids.newTimeseriesQueryBuilder()
                      .dataSource(
                          new UnionDataSource(
                              ImmutableList.of(
                                  new ProjectDataSource(
                                      new TableDataSource(CalciteTests.DATASOURCE1),
                                      VirtualColumns.create(expressionVirtualColumn("u0", "\"m1\"", ColumnType.FLOAT))
                                  ),
                                  new ProjectDataSource(
                                      RestrictedDataSource.create(
                                          new TableDataSource(CalciteTests.RESTRICTED_DATASOURCE),
                                          CalciteTests.POLICY_RESTRICTION
                                      ),
                                      VirtualColumns.create(
                                          expressionVirtualColumn("v0", "(\"m2\" + 1)", ColumnType.FLOAT),
                                          expressionVirtualColumn("u0", "\"v0\"", ColumnType.FLOAT)
                                      )
                                  )
                              )
                          )
                      )
                      .intervals(querySegmentSpec(Filtration.eternity()))
                      .granularity(Granularities.ALL)
                      .aggregators(
                          aggregators(
                              new CountAggregatorFactory("a0"),
                              new DoubleSumAggregatorFactory("a1", "u0")
                          )
                      )
                      .context(QUERY_CONTEXT_DEFAULT)
                      .build()
            )
        )
        .expectedResults(ImmutableList.of(new Object[]{7L, 28.0}))
        .run();
  }

  @Test
  public void testUnionAllWithRestrictedArmsFilter()
  {
    // Each arm's filter applies along with the policy: the first arm keeps the policy's one row (m1 = 6), and the
    // second arm filters it out.
    testBuilder()
        .sql(
            """
                SELECT COUNT(*), SUM(m1) FROM (
                SELECT m1 FROM druid.restrictedDatasource_m1_is_6 WHERE m1 >= 5
                UNION ALL
                SELECT m1 FROM druid.restrictedDatasource_m1_is_6 WHERE m1 < 5
                )"""
        )
        .authResult(CalciteTests.REGULAR_USER_AUTH_RESULT)
        .expectedQueries(
            ImmutableList.of(
                Druids.newTimeseriesQueryBuilder()
                      .dataSource(
                          new UnionDataSource(
                              ImmutableList.of(
                                  FilteredDataSource.create(
                                      RestrictedDataSource.create(
                                          new TableDataSource(CalciteTests.RESTRICTED_DATASOURCE),
                                          CalciteTests.POLICY_RESTRICTION
                                      ),
                                      range("m1", ColumnType.FLOAT, 5.0f, null, false, false)
                                  ),
                                  FilteredDataSource.create(
                                      RestrictedDataSource.create(
                                          new TableDataSource(CalciteTests.RESTRICTED_DATASOURCE),
                                          CalciteTests.POLICY_RESTRICTION
                                      ),
                                      range("m1", ColumnType.FLOAT, null, 5.0f, false, true)
                                  )
                              )
                          )
                      )
                      .intervals(querySegmentSpec(Filtration.eternity()))
                      .granularity(Granularities.ALL)
                      .aggregators(
                          aggregators(
                              new CountAggregatorFactory("a0"),
                              new DoubleSumAggregatorFactory("a1", "m1")
                          )
                      )
                      .context(QUERY_CONTEXT_DEFAULT)
                      .build()
            )
        )
        .expectedResults(ImmutableList.of(new Object[]{1L, 6.0}))
        .run();
  }

  @Test
  public void testUnionAllWithRestrictedArmsJoin()
  {
    // The join sits on top of the union, so it applies to each arm after that arm's own projection and policy. The
    // policy keeps only the row where m1 = 6, which the second arm presents as "6", a key of the lookup.
    cannotVectorize();
    testBuilder()
        .sql(
            """
                SELECT l.v, COUNT(*)
                FROM (
                  SELECT dim1 AS x FROM foo
                  UNION ALL
                  SELECT CAST(CAST(m1 AS BIGINT) AS VARCHAR) AS x FROM druid.restrictedDatasource_m1_is_6
                ) u
                LEFT JOIN lookup.lookyloo l ON u.x = l.k
                GROUP BY 1"""
        )
        .authResult(CalciteTests.REGULAR_USER_AUTH_RESULT)
        .expectedQueries(
            ImmutableList.of(
                GroupByQuery.builder()
                            .setDataSource(
                                join(
                                    new UnionDataSource(
                                        ImmutableList.of(
                                            new TableDataSource(CalciteTests.DATASOURCE1),
                                            new ProjectDataSource(
                                                RestrictedDataSource.create(
                                                    new TableDataSource(CalciteTests.RESTRICTED_DATASOURCE),
                                                    CalciteTests.POLICY_RESTRICTION
                                                ),
                                                VirtualColumns.create(
                                                    expressionVirtualColumn(
                                                        "v0",
                                                        "CAST(CAST(\"m1\", 'LONG'), 'STRING')",
                                                        ColumnType.STRING
                                                    ),
                                                    expressionVirtualColumn("dim1", "\"v0\"", ColumnType.STRING)
                                                )
                                            )
                                        )
                                    ),
                                    new LookupDataSource("lookyloo"),
                                    "j0.",
                                    equalsCondition(makeColumnExpression("dim1"), makeColumnExpression("j0.k")),
                                    JoinType.LEFT
                                )
                            )
                            .setInterval(querySegmentSpec(Filtration.eternity()))
                            .setGranularity(Granularities.ALL)
                            .setDimensions(new DefaultDimensionSpec("j0.v", "d0"))
                            .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                            .setContext(QUERY_CONTEXT_DEFAULT)
                            .build()
            )
        )
        .expectedResults(
            ImmutableList.of(
                new Object[]{null, 5L},
                new Object[]{"x6", 1L},
                new Object[]{"xabc", 1L}
            )
        )
        .run();
  }

  @NotYetSupported(Modes.UNION_QUERY_OVER_UNION_DATASOURCE)
  @Test
  public void testUnionAllWithFilteredNestedUnion()
  {
    // The inner union has a filter on top, so it cannot be flattened into the outer union, which would lose the
    // filter. MSQ cannot plan the outer union without flattening it.
    msqIncompatible();
    testBuilder()
        .sql(
            "SELECT dim1 FROM (SELECT dim1 FROM foo UNION ALL SELECT dim1 FROM numfoo) WHERE dim1 = 'abc'\n"
            + "UNION ALL\n"
            + "SELECT dim1 FROM foo WHERE dim1 = 'def'"
        )
        .expectedResults(
            new UnorderedResultsVerifier(
                ImmutableList.of(new Object[]{"abc"}, new Object[]{"abc"}, new Object[]{"def"}),
                ResultMatchMode.EQUALS,
                null
            )
        )
        .run();
  }

  @Test
  public void testUnionAllTablesColumnCountMismatch()
  {
    try {
      testQuery(
          "SELECT\n"
          + "dim1, dim2, SUM(m1), COUNT(*)\n"
          + "FROM (SELECT * FROM foo UNION ALL SELECT * FROM numfoo)\n"
          + "WHERE dim2 = 'a' OR dim2 = 'def'\n"
          + "GROUP BY 1, 2",
          ImmutableList.of(),
          ImmutableList.of()
      );
      Assertions.fail("query execution should fail");
    }
    catch (DruidException e) {
      assertDruidException(e, invalidSqlIs("Column count mismatch in UNION ALL (line [3], column [42])"));
    }
  }

  @Test
  public void testUnionAllTablesColumnTypeMismatchFloatLong()
  {
    // "m1" has a different type in foo and foo2 (float vs long), but this query is OK anyway because they can both
    // be implicitly cast to double. The foo2 arm carries that cast in its ProjectDataSource, as "v0", which the foo
    // arm presents its "m1" as.

    testQuery(
        "SELECT\n"
        + "dim1, dim2, SUM(m1), COUNT(*)\n"
        + "FROM (SELECT dim1, dim2, m1 FROM foo2 UNION ALL SELECT dim1, dim2, m1 FROM foo)\n"
        + "WHERE dim2 = 'a' OR dim2 = 'en'\n"
        + "GROUP BY 1, 2",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new UnionDataSource(
                                ImmutableList.of(
                                    new ProjectDataSource(
                                        new TableDataSource(CalciteTests.DATASOURCE2),
                                        VirtualColumns.create(
                                            expressionVirtualColumn("v0", "CAST(\"m1\", 'DOUBLE')", ColumnType.FLOAT)
                                        )
                                    ),
                                    new ProjectDataSource(
                                        new TableDataSource(CalciteTests.DATASOURCE1),
                                        VirtualColumns.create(expressionVirtualColumn("v0", "\"m1\"", ColumnType.FLOAT))
                                    )
                                )
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimFilter(in("dim2", ImmutableList.of("en", "a")))
                        .setDimensions(
                            new DefaultDimensionSpec("dim1", "d0"),
                            new DefaultDimensionSpec("dim2", "d1")
                        )
                        .setAggregatorSpecs(
                            aggregators(
                                new DoubleSumAggregatorFactory("a0", "v0"),
                                new CountAggregatorFactory("a1")
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", "a", 1.0, 1L},
            new Object[]{"1", "a", 4.0, 1L},
            new Object[]{"druid", "en", 1.0, 1L}
        )
    );
  }

  @Test
  public void testUnionFlipFlop()
  {
    String sql = "SELECT\n"
        + "dim1,dim2,count(1) \n"
        + "FROM (SELECT dim1 as dim1, dim2 as dim2 FROM foo UNION ALL SELECT dim2 as dim1, dim1 as dim2 FROM foo)\n"
        + "WHERE dim1 = 'def' OR dim2 = 'def'\n"
        + "GROUP BY 1, 2";
    // The second arm renames dim1 and dim2 past each other, which its ProjectDataSource carries.
    skipVectorize();
    testBuilder()
        .sql(sql)
        .expectedResults(
            ImmutableList.of(
                new Object[] {"abc", "def", 1L},
                new Object[] {"def", "abc", 1L}
            )
        )
        .run();
  }


  @Test
  public void testUnionAllTablesColumnTypeMismatchStringLong()
  {
    String sql = "SELECT\n"
        + "dim3, dim2, SUM(m1), COUNT(*)\n"
        + "FROM (SELECT dim3, dim2, m1 FROM foo2 UNION ALL SELECT dim3, dim2, m1 FROM foo)\n"
        + "WHERE dim2 = 'a' OR dim2 = 'en'\n"
        + "GROUP BY 1, 2";
    // The cast on "dim3" becomes a virtual column on each arm's ProjectDataSource.
    skipVectorize();
    testBuilder()
        .sql(sql)
        .expectedResults(
            ImmutableList.of(
                new Object[] {"", "a", 4.0D, 1L},
                new Object[] {"11", "en", 1.0D, 1L},
                new Object[] {"a", "a", 1.0D, 1L},
                new Object[] {"b", "a", 1.0D, 1L}
            )
        )
        .run();
  }

  @Test
  public void testUnionAllTablesWhenMappingIsRequired()
  {
    String sql = "SELECT\n"
        + "c, COUNT(*)\n"
        + "FROM (SELECT dim1 AS c, m1 FROM foo UNION ALL SELECT dim2 AS c, m1 FROM numfoo)\n"
        + "WHERE c = 'a' OR c = 'def'\n"
        + "GROUP BY 1";
    // The numfoo arm presents its "dim2" as "dim1" through its ProjectDataSource.
    skipVectorize();
    testBuilder()
        .sql(sql)
        .expectedResults(
            ImmutableList.of(
                new Object[] {"a", 2L},
                new Object[] {"def", 1L}
            )
        )
        .run();
  }

  @Test
  public void testUnionDifferentColumnOrder()
  {
    String sql = "SELECT dim2, dim1, m1 FROM foo2 UNION SELECT dim1, dim2, m1 FROM foo";
    // UnionToDistinctRule, over arms whose ProjectDataSource lines their columns up. The foo arm supplies
    // its dim1 as the union's dim2, and vice versa.
    testBuilder()
        .sql(sql)
        .expectedResults(
            ResultMatchMode.RELAX_NULLS,
            ImmutableList.of(
                new Object[] {"", "a", 1.0F},
                new Object[] {"1", "a", 4.0F},
                new Object[] {"10.1", null, 2.0F},
                new Object[] {"2", "", 3.0F},
                new Object[] {"abc", null, 6.0F},
                new Object[] {"def", "abc", 5.0F},
                new Object[] {"en", "druid", 1.0F},
                new Object[] {"he", "\u05D3\u05E8\u05D5\u05D0\u05D9\u05D3", 1.0F},
                new Object[] {"ru", "\u0434\u0440\u0443\u0438\u0434", 1.0F}
            )
        )
        .run();
  }

  @Test
  public void testUnionAllTablesWhenCastAndMappingIsRequired()
  {
    String sql = "SELECT\n"
        + "c, COUNT(*)\n"
        + "FROM (SELECT dim1 AS c, m1 FROM foo UNION ALL SELECT cnt AS c, m1 FROM numfoo)\n"
        + "WHERE c = '1' OR c = 'def'\n"
        + "GROUP BY 1";
    // The cast on "cnt" becomes a virtual column on that arm's ProjectDataSource.
    skipVectorize();
    testBuilder()
        .sql(sql)
        .expectedResults(
            ImmutableList.of(
                new Object[]{"1", 7L},
                new Object[]{"def", 1L}
            )
        )
        .run();
  }

  @Test
  public void testUnionAllSameTableTwice()
  {
    testQuery(
        "SELECT\n"
        + "dim1, dim2, SUM(m1), COUNT(*)\n"
        + "FROM (SELECT * FROM foo UNION ALL SELECT * FROM foo)\n"
        + "WHERE dim2 = 'a' OR dim2 = 'def'\n"
        + "GROUP BY 1, 2",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new UnionDataSource(
                                ImmutableList.of(
                                    new TableDataSource(CalciteTests.DATASOURCE1),
                                    new TableDataSource(CalciteTests.DATASOURCE1)
                                )
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimFilter(in("dim2", ImmutableList.of("def", "a")))
                        .setDimensions(
                            new DefaultDimensionSpec("dim1", "d0"),
                            new DefaultDimensionSpec("dim2", "d1")
                        )
                        .setAggregatorSpecs(
                            aggregators(
                                new DoubleSumAggregatorFactory("a0", "m1"),
                                new CountAggregatorFactory("a1")
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", "a", 2.0, 2L},
            new Object[]{"1", "a", 8.0, 2L}
        )
    );
  }

  @Test
  public void testUnionAllSameTableTwiceWithSameMapping()
  {
    testQuery(
        "SELECT\n"
        + "dim1, dim2, SUM(m1), COUNT(*)\n"
        + "FROM (SELECT dim1, dim2, m1 FROM foo UNION ALL SELECT dim1, dim2, m1 FROM foo)\n"
        + "WHERE dim2 = 'a' OR dim2 = 'def'\n"
        + "GROUP BY 1, 2",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new UnionDataSource(
                                ImmutableList.of(
                                    new TableDataSource(CalciteTests.DATASOURCE1),
                                    new TableDataSource(CalciteTests.DATASOURCE1)
                                )
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimFilter(in("dim2", ImmutableList.of("def", "a")))
                        .setDimensions(
                            new DefaultDimensionSpec("dim1", "d0"),
                            new DefaultDimensionSpec("dim2", "d1")
                        )
                        .setAggregatorSpecs(
                            aggregators(
                                new DoubleSumAggregatorFactory("a0", "m1"),
                                new CountAggregatorFactory("a1")
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", "a", 2.0, 2L},
            new Object[]{"1", "a", 8.0, 2L}
        )
    );
  }

  @Test
  public void testUnionAllSameTableTwiceWithDifferentMapping()
  {
    String sql = "SELECT\n"
        + "dim1, dim2, SUM(m1), COUNT(*)\n"
        + "FROM (SELECT dim1, dim2, m1 FROM foo UNION ALL SELECT dim2, dim1, m1 FROM foo)\n"
        + "WHERE dim2 = 'a' OR dim2 = 'def'\n"
        + "GROUP BY 1, 2";
    // The second arm reads foo.dim2 as dim1 and foo.dim1 as dim2, through its ProjectDataSource.
    testBuilder()
        .sql(sql)
        .expectedResults(
            ImmutableList.of(
                new Object[] {"", "a", 1.0D, 1L},
                new Object[] {"1", "a", 4.0D, 1L},
                new Object[] {"abc", "def", 5.0D, 1L}
            )
        )
        .run();
  }

  @Test
  public void testUnionAllThreeTablesColumnCountMismatch1()
  {
    try {
      testQuery(
          "SELECT\n"
          + "dim1, dim2, SUM(m1), COUNT(*)\n"
          + "FROM (SELECT * FROM numfoo UNION ALL SELECT * FROM foo UNION ALL SELECT * from foo)\n"
          + "WHERE dim2 = 'a' OR dim2 = 'def'\n"
          + "GROUP BY 1, 2",
          ImmutableList.of(),
          ImmutableList.of()
      );
      Assertions.fail("query execution should fail");
    }
    catch (DruidException e) {
      assertDruidException(e, invalidSqlIs("Column count mismatch in UNION ALL (line [3], column [45])"));
    }
  }

  @Test
  public void testUnionAllThreeTablesColumnCountMismatch2()
  {
    try {
      testQuery(
          "SELECT\n"
          + "dim1, dim2, SUM(m1), COUNT(*)\n"
          + "FROM (SELECT * FROM numfoo UNION ALL SELECT * FROM foo UNION ALL SELECT * from foo)\n"
          + "WHERE dim2 = 'a' OR dim2 = 'def'\n"
          + "GROUP BY 1, 2",
          ImmutableList.of(),
          ImmutableList.of()
      );
      Assertions.fail("query execution should fail");
    }
    catch (DruidException e) {
      assertDruidException(e, invalidSqlIs("Column count mismatch in UNION ALL (line [3], column [45])"));
    }
  }

  @Test
  public void testUnionAllThreeTablesColumnCountMismatch3()
  {
    try {
      testQuery(
          "SELECT\n"
          + "dim1, dim2, SUM(m1), COUNT(*)\n"
          + "FROM (SELECT * FROM foo UNION ALL SELECT * FROM foo UNION ALL SELECT * from numfoo)\n"
          + "WHERE dim2 = 'a' OR dim2 = 'def'\n"
          + "GROUP BY 1, 2",
          ImmutableList.of(),
          ImmutableList.of()
      );
      Assertions.fail("query execution should fail");
    }
    catch (DruidException e) {
      assertDruidException(e, invalidSqlIs("Column count mismatch in UNION ALL (line [3], column [70])"));
    }
  }

}
