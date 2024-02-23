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
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.sql.calcite.NotYetSupported.Modes;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;

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
                        .setDimFilter(in("dim2", ImmutableList.of("def", "a"), null))
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
                        .setDimFilter(in("dim2", ImmutableList.of("def", "a"), null))
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
      Assert.fail("query execution should fail");
    }
    catch (DruidException e) {
      MatcherAssert.assertThat(e, invalidSqlIs("Column count mismatch in UNION ALL (line [3], column [42])"));
    }
  }

  @NotYetSupported(Modes.UNION_MORE_STRICT_ROWTYPE_CHECK)
  @Test
  public void testUnionAllTablesColumnTypeMismatchFloatLong()
  {
    // "m1" has a different type in foo and foo2 (float vs long), but this query is OK anyway because they can both
    // be implicitly cast to double.

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
                                    new TableDataSource(CalciteTests.DATASOURCE2),
                                    new TableDataSource(CalciteTests.DATASOURCE1)
                                )
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimFilter(in("dim2", ImmutableList.of("en", "a"), null))
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
            new Object[]{"", "a", 1.0, 1L},
            new Object[]{"1", "a", 4.0, 1L},
            new Object[]{"druid", "en", 1.0, 1L}
        )
    );
  }

  @NotYetSupported(Modes.ERROR_HANDLING)
  @Test
  public void testUnionAllTablesColumnTypeMismatchStringLong()
  {
    // "dim3" has a different type in foo and foo2 (string vs long), which requires a casting subquery, so this
    // query cannot be planned.

    assertQueryIsUnplannable(
        "SELECT\n"
        + "dim3, dim2, SUM(m1), COUNT(*)\n"
        + "FROM (SELECT dim3, dim2, m1 FROM foo2 UNION ALL SELECT dim3, dim2, m1 FROM foo)\n"
        + "WHERE dim2 = 'a' OR dim2 = 'en'\n"
        + "GROUP BY 1, 2",
        "SQL requires union between inputs that are not simple table scans and involve a " +
        "filter or aliasing. Or column types of tables being unioned are not of same type."
    );
  }

  @NotYetSupported(Modes.ERROR_HANDLING)
  @Test
  public void testUnionAllTablesWhenMappingIsRequired()
  {
    // Cannot plan this UNION ALL operation, because the column swap would require generating a subquery.

    assertQueryIsUnplannable(
        "SELECT\n"
        + "c, COUNT(*)\n"
        + "FROM (SELECT dim1 AS c, m1 FROM foo UNION ALL SELECT dim2 AS c, m1 FROM numfoo)\n"
        + "WHERE c = 'a' OR c = 'def'\n"
        + "GROUP BY 1",
        "SQL requires union between two tables " +
        "and column names queried for each table are different Left: [dim1], Right: [dim2]."
    );
  }

  @NotYetSupported(Modes.ERROR_HANDLING)
  @Test
  public void testUnionIsUnplannable()
  {
    // Cannot plan this UNION operation
    assertQueryIsUnplannable(
        "SELECT dim2, dim1, m1 FROM foo2 UNION SELECT dim1, dim2, m1 FROM foo",
        "SQL requires 'UNION' but only 'UNION ALL' is supported."
    );
  }

  @NotYetSupported(Modes.ERROR_HANDLING)
  @Test
  public void testUnionAllTablesWhenCastAndMappingIsRequired()
  {
    // Cannot plan this UNION ALL operation, because the column swap would require generating a subquery.
    assertQueryIsUnplannable(
        "SELECT\n"
        + "c, COUNT(*)\n"
        + "FROM (SELECT dim1 AS c, m1 FROM foo UNION ALL SELECT cnt AS c, m1 FROM numfoo)\n"
        + "WHERE c = 'a' OR c = 'def'\n"
        + "GROUP BY 1",
        "SQL requires union between inputs that are not simple table scans and involve " +
        "a filter or aliasing. Or column types of tables being unioned are not of same type."
    );
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
                        .setDimFilter(in("dim2", ImmutableList.of("def", "a"), null))
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
                        .setDimFilter(in("dim2", ImmutableList.of("def", "a"), null))
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

  @NotYetSupported(Modes.ERROR_HANDLING)
  @Test
  public void testUnionAllSameTableTwiceWithDifferentMapping()
  {
    // Cannot plan this UNION ALL operation, because the column swap would require generating a subquery.
    assertQueryIsUnplannable(
        "SELECT\n"
        + "dim1, dim2, SUM(m1), COUNT(*)\n"
        + "FROM (SELECT dim1, dim2, m1 FROM foo UNION ALL SELECT dim2, dim1, m1 FROM foo)\n"
        + "WHERE dim2 = 'a' OR dim2 = 'def'\n"
        + "GROUP BY 1, 2",
        "SQL requires union between two tables and column names queried for each table are different Left: [dim1, dim2, m1], Right: [dim2, dim1, m1]."
    );
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
      Assert.fail("query execution should fail");
    }
    catch (DruidException e) {
      MatcherAssert.assertThat(e, invalidSqlIs("Column count mismatch in UNION ALL (line [3], column [45])"));
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
      Assert.fail("query execution should fail");
    }
    catch (DruidException e) {
      MatcherAssert.assertThat(e, invalidSqlIs("Column count mismatch in UNION ALL (line [3], column [45])"));
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
      Assert.fail("query execution should fail");
    }
    catch (DruidException e) {
      MatcherAssert.assertThat(e, invalidSqlIs("Column count mismatch in UNION ALL (line [3], column [70])"));
    }
  }

}
