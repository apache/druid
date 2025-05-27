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

package org.apache.druid.msq.test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.NotYetSupported;
import org.apache.druid.sql.calcite.NotYetSupported.Modes;
import org.apache.druid.sql.calcite.QueryTestBuilder;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.jupiter.api.Test;

@SqlTestFrameworkConfig.ComponentSupplier(DartComponentSupplier.class)
public class CalciteDartTest extends BaseCalciteQueryTest
{
  @Override
  protected QueryTestBuilder testBuilder()
  {
    return new QueryTestBuilder(new CalciteTestConfig(true))
        .queryContext(
            ImmutableMap.<String, Object>builder()
                .put(QueryContexts.ENABLE_DEBUG, true)
                .build()
        )
        .skipVectorize(true)
        .verifyNativeQueries(new VerifyMSQSupportedNativeQueriesPredicate());
  }

  @Test
  public void testSelect1()
  {
    testBuilder()
        .sql("SELECT 1")
        .expectedResults(ImmutableList.of(new Object[] {1}))
        .run();
  }

  @Test
  public void testOrderBy()
  {
    testBuilder()
        .sql("SELECT 2 from foo order by dim1")
        .expectedResults(
            ImmutableList.of(
                new Object[] {2},
                new Object[] {2},
                new Object[] {2},
                new Object[] {2},
                new Object[] {2},
                new Object[] {2}
            )
        )
        .run();
  }

  @NotYetSupported(Modes.RESTRICTED_DATASOURCE_SUPPORT)
  @Test
  public void testSelectFromRestricted()
  {
    testBuilder()
        .sql("SELECT 2 from restrictedDatasource_m1_is_6")
        .expectedResults(
            ImmutableList.of(
                new Object[]{2}
            )
        )
        .run();
  }

  @Test
  public void testSelectFromRestricted_superuser()
  {
    testBuilder()
        .authResult(CalciteTests.SUPER_USER_AUTH_RESULT)
        .sql("SELECT 2 from restrictedDatasource_m1_is_6")
        .expectedResults(
            ImmutableList.of(
                new Object[]{2},
                new Object[]{2},
                new Object[]{2},
                new Object[]{2},
                new Object[]{2},
                new Object[]{2}
            )
        )
        .run();
  }

  @NotYetSupported(Modes.SUPPORT_SORT)
  @Test
  public void testSelectFromFooLimit2()
  {
    testBuilder()
        .sql("SELECT 2 from foo limit 2")
        .expectedResults(
            ImmutableList.of(
                new Object[] {2},
                new Object[] {2}
            )
        )
        .run();
  }

  @NotYetSupported(Modes.SUPPORT_AGGREGATE)
  @Test
  public void testCount()
  {
    testBuilder()
        .sql("SELECT count(1) from foo")
        .expectedResults(
            ImmutableList.of(
                new Object[] {6L}
            )
        )
        .run();
  }

  @Test
  public void testSelectDim1FromFoo11()
  {
    testBuilder()
        .sql("SELECT dim1 from foo")
        .expectedResults(
            ImmutableList.of(
                new Object[] {""},
                new Object[] {"10.1"},
                new Object[] {"2"},
                new Object[] {"1"},
                new Object[] {"def"},
                new Object[] {"abc"}
            )
        )
        .run();
  }

  @Test
  public void testFiltered()
  {
    testBuilder()
        .sql("SELECT dim1 from foo where dim1 = 'abc'")
        .expectedResults(
            ImmutableList.of(
                new Object[] {"abc"}
            )
        )
        .run();
  }

  @Test
  public void testColumnFromFoo()
  {
    testBuilder()
        .sql("SELECT dim1 from foo")
        .expectedResults(
            ImmutableList.of(
                new Object[] {""},
                new Object[] {"10.1"},
                new Object[] {"2"},
                new Object[] {"1"},
                new Object[] {"def"},
                new Object[] {"abc"}
            )
        )
        .run();
  }

  @Test
  public void testSelectStar()
  {
    testBuilder()
        .sql("SELECT * from numbers")
        .expectedResults(
            ImmutableList.of(
                new Object[]{946684800000L, 1L, 0L, 1.0D, "one", "[\"o\",\"n\",\"e\"]", 1L},
                new Object[]{946684800000L, 2L, 1L, 0.5D, "two", "[\"t\",\"w\",\"o\"]", 1L},
                new Object[]{946684800000L, 3L, 1L, 0.3333333333333333D, "three", "[\"t\",\"h\",\"r\",\"e\",\"e\"]", 1L},
                new Object[]{946684800000L, 4L, 0L, 0.25D, "four", "[\"f\",\"o\",\"u\",\"r\"]", 1L},
                new Object[]{946684800000L, 5L, 1L, 0.2D, "five", "[\"f\",\"i\",\"v\",\"e\"]", 1L},
                new Object[]{946684800000L, 6L, 0L, 0.16666666666666666D, "six", "[\"s\",\"i\",\"x\"]", 1L},
                new Object[]{946684800000L, 7L, 1L, 0.14285714285714285D, "seven", "[\"s\",\"e\",\"v\",\"e\",\"n\"]", 1L},
                new Object[]{946684800000L, 8L, 0L, 0.125D, "eight", "[\"e\",\"i\",\"g\",\"h\",\"t\"]", 1L},
                new Object[]{946684800000L, 9L, 0L, 0.1111111111111111D, "nine", "[\"n\",\"i\",\"n\",\"e\"]", 1L},
                new Object[]{946684800000L, 10L, 0L, 0.1D, "ten", "[\"t\",\"e\",\"n\"]", 1L}
            )
        )
        .run();
  }

  @NotYetSupported(Modes.SUPPORT_AGGREGATE)
  @Test
  public void testGroupBy()
  {
    testBuilder()
        .sql("SELECT dim2 from foo group by dim2")
        .expectedResults(
            ImmutableList.of(
                new Object[]{null},
                new Object[]{""},
                new Object[]{"a"},
                new Object[]{"abc"}
            )
        )
        .run();
  }

  @NotYetSupported(Modes.SUPPORT_AGGREGATE)
  @Test
  public void testSubQuery()
  {
    String sql = "SELECT dim1, COUNT(*) FROM druid.foo "
        + "WHERE dim1 NOT IN ('ghi', 'abc', 'def') AND dim1 IS NOT NULL "
        + "GROUP BY dim1";
    testBuilder()
        .sql(sql)
        .expectedResults(
            ImmutableList.of(
                new Object[] {"", 1L},
                new Object[] {"1", 1L},
                new Object[] {"10.1", 1L},
                new Object[] {"2", 1L}
            )
        )
        .run();
  }
}
