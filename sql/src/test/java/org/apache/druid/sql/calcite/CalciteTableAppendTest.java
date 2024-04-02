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
import org.apache.druid.query.Druids;
import org.apache.druid.query.scan.ScanQuery.ResultFormat;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;

public class CalciteTableAppendTest extends BaseCalciteQueryTest
{
  @Test
  public void testUnion()
  {
    testBuilder()
        .sql("select dim1,null as dim4 from foo union all select dim1,dim4 from numfoo")
        .expectedQueries(
            ImmutableList.of(
                Druids.newScanQueryBuilder()
                    .dataSource(CalciteTests.DATASOURCE1)
                    .intervals(querySegmentSpec(Filtration.eternity()))
                    .columns("dim1", "v0")
                    .context(QUERY_CONTEXT_DEFAULT)
                    .resultFormat(ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                    .virtualColumns(
                        expressionVirtualColumn("v0", "null", null)
                    )
                    .legacy(false)
                    .build(),
                Druids.newScanQueryBuilder()
                    .dataSource(CalciteTests.DATASOURCE3)
                    .intervals(querySegmentSpec(Filtration.eternity()))
                    .columns("dim1", "dim4")
                    .context(QUERY_CONTEXT_DEFAULT)
                    .resultFormat(ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                    .legacy(false)
                    .build()
            )
        )
        .expectedResults(
            ImmutableList.of(
                new Object[] {"", null},
                new Object[] {"10.1", null},
                new Object[] {"2", null},
                new Object[] {"1", null},
                new Object[] {"def", null},
                new Object[] {"abc", null},
                new Object[] {"", "a"},
                new Object[] {"10.1", "a"},
                new Object[] {"2", "a"},
                new Object[] {"1", "b"},
                new Object[] {"def", "b"},
                new Object[] {"abc", "b"}
            )
        )
        .run();
  }

  @Test
  public void testAppend2()
  {
    testBuilder()
        .sql("select dim1,dim4,d1,f1 from TABLE(APPEND('foo','numfoo')) u")
        .expectedQuery(
            Druids.newScanQueryBuilder()
                .dataSource(
                    unionDataSource(CalciteTests.DATASOURCE1, CalciteTests.DATASOURCE3)
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("d1", "dim1", "dim4", "f1")
                .context(QUERY_CONTEXT_DEFAULT)
                .resultFormat(ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .legacy(false)
                .build()
        )
        .expectedResults(
            ResultMatchMode.RELAX_NULLS,
            ImmutableList.of(
                new Object[] {"", null, null, null},
                new Object[] {"10.1", null, null, null},
                new Object[] {"2", null, null, null},
                new Object[] {"1", null, null, null},
                new Object[] {"def", null, null, null},
                new Object[] {"abc", null, null, null},
                new Object[] {"", "a", 1.0D, 1.0F},
                new Object[] {"10.1", "a", 1.7D, 0.1F},
                new Object[] {"2", "a", 0.0D, 0.0F},
                new Object[] {"1", "b", null, null},
                new Object[] {"def", "b", null, null},
                new Object[] {"abc", "b", null, null}
            )
        )
        .expectedLogicalPlan(
            ""
                + "LogicalProject(exprs=[[$1, $8, $11, $13]])\n"
                + "  LogicalTableScan(table=[[APPEND]])\n"
        )
        .run();
  }

  @Test
  public void testAppendSameTableMultipleTimes()
  {
    testBuilder()
        .sql("select dim1,dim4,d1,f1 from TABLE(APPEND('foo','numfoo','foo')) u where dim1='2'")
        .expectedQuery(
            Druids.newScanQueryBuilder()
                .dataSource(
                    unionDataSource(CalciteTests.DATASOURCE1, CalciteTests.DATASOURCE3, CalciteTests.DATASOURCE1)
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("d1", "dim1", "dim4", "f1")
                .context(QUERY_CONTEXT_DEFAULT)
                .resultFormat(ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .filters(equality("dim1", "2", ColumnType.STRING))
                .legacy(false)
                .build()
        )
        .expectedResults(
            ResultMatchMode.RELAX_NULLS,
            ImmutableList.of(
                new Object[] {"2", null, null, null},
                new Object[] {"2", null, null, null},
                new Object[] {"2", "a", 0.0D, 0.0F}
            )
        )
        .run();
  }

  @Test
  public void testAppendtSingleTableIsValid()
  {
    testBuilder()
        .sql("select dim1 from TABLE(APPEND('foo')) u")
        .expectedQuery(
            Druids.newScanQueryBuilder()
                .dataSource(
                    unionDataSource(CalciteTests.DATASOURCE1)
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("dim1")
                .context(QUERY_CONTEXT_DEFAULT)
                .resultFormat(ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .legacy(false)
                .build()
        )
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
  public void testAppendCompatibleColumns()
  {
    // dim3 has different type (string/long) in foo/foo2
    testBuilder()
        .sql("select dim3 from TABLE(APPEND('foo','foo2')) u")
        .expectedQuery(
            Druids.newScanQueryBuilder()
                .dataSource(
                    unionDataSource(CalciteTests.DATASOURCE1, CalciteTests.DATASOURCE2)
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("dim3")
                .context(QUERY_CONTEXT_DEFAULT)
                .resultFormat(ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .legacy(false)
                .build()
        )
        .expectedResults(
            ResultMatchMode.RELAX_NULLS,
            ImmutableList.of(
                new Object[] {"11"},
                new Object[] {"12"},
                new Object[] {"10"},
                new Object[] {"[\"a\",\"b\"]"},
                new Object[] {"[\"b\",\"c\"]"},
                new Object[] {"d"},
                new Object[] {""},
                new Object[] {null},
                new Object[] {null}
            )
        )
        .run();
  }

  @Test
  public void testAppendNoTableIsInvalid()
  {
    try {
      testBuilder()
          .sql("select dim1 from TABLE(APPEND()) u")
          .run();
      Assert.fail("query execution should fail");
    }
    catch (DruidException e) {
      assertThat(
          e,
          invalidSqlIs("No match found for function signature APPEND() (line [1], column [24])")
      );
    }
  }

  @Test
  public void testAppendtInvalidIntegerArg()
  {
    try {
      testBuilder()
          .sql("select dim1 from TABLE(APPEND('foo',111)) u")
          .run();
      Assert.fail("query execution should fail");
    }
    catch (DruidException e) {
      assertThat(
          e,
          invalidSqlIs(
              "All arguments to APPEND should be literal strings. Argument #2 is not string (line [1], column [37])"
          )
      );
    }
  }

  @Test
  public void testAppendtNullArg()
  {
    try {
      testBuilder()
          .sql("select dim1 from TABLE(APPEND('foo',null)) u")
          .run();
      Assert.fail("query execution should fail");
    }
    catch (DruidException e) {
      assertThat(
          e,
          invalidSqlIs(
              "All arguments to APPEND should be literal strings. Argument #2 is not string (line [1], column [37])"
          )
      );
    }
  }

  @Test
  public void testAppendtNonExistentTable()
  {
    try {
      testBuilder()
          .sql("select dim1 from TABLE(APPEND('foo','nonexistent')) u")
          .run();
      Assert.fail("query execution should fail");
    }
    catch (DruidException e) {
      assertThat(
          e,
          invalidSqlIs("Table [nonexistent] not found (line [1], column [37])")
      );
    }
  }

  @Test
  public void testAppendCTE()
  {
    // not supported right now - test is here that it doesn't hit any serious issues
    try {
      testBuilder()
          .sql("with t0 as (select * from foo) select dim3 from TABLE(APPEND('t0','foo')) u")
          .run();
      Assert.fail("query execution should fail");
    }
    catch (DruidException e) {
      assertThat(
          e,
          invalidSqlIs("Table [t0] not found (line [1], column [62])")
      );
    }
  }
}
