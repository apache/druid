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

package org.apache.druid.query.sql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.guice.ExpressionModule;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExprMacroTable.ExprMacro;
import org.apache.druid.query.Druids;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.expression.LookupExprMacro;
import org.apache.druid.query.expressions.SleepExprMacro;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.scan.ScanQuery.ResultFormat;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class SleepSqlTest extends BaseCalciteQueryTest
{
  @Override
  public DruidOperatorTable createOperatorTable()
  {
    return new DruidOperatorTable(
        ImmutableSet.of(),
        ImmutableSet.of(new SleepOperatorConversion())
    );
  }

  @Override
  public ExprMacroTable createMacroTable()
  {
    final List<ExprMacro> exprMacros = new ArrayList<>();
    for (Class<? extends ExprMacroTable.ExprMacro> clazz : ExpressionModule.EXPR_MACROS) {
      exprMacros.add(CalciteTests.INJECTOR.getInstance(clazz));
    }
    exprMacros.add(CalciteTests.INJECTOR.getInstance(LookupExprMacro.class));
    exprMacros.add(new SleepExprMacro());
    return new ExprMacroTable(exprMacros);
  }

  @Test
  public void testSleepFunction()
  {
    testQuery(
        "SELECT sleep(m1) from foo where m1 < 2.0",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(new TableDataSource("foo"))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .virtualColumns(
                      new ExpressionVirtualColumn(
                          "v0",
                          "sleep(\"m1\")",
                          ColumnType.STRING,
                          createMacroTable()
                      )
                  )
                  .columns("v0")
                  .filters(new BoundDimFilter("m1", null, "2.0", null, true, null, null, StringComparators.NUMERIC))
                  .resultFormat(ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{NullHandling.replaceWithDefault() ? "" : null}
        )
    );
  }
}
