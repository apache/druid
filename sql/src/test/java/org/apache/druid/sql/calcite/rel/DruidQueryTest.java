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

package org.apache.druid.sql.calcite.rel;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.ExpressionParserImpl;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.joda.time.Interval;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DruidQueryTest
{

  static {
    NullHandling.initializeForTests();
  }

  private final DimFilter selectorFilter = new SelectorDimFilter("column", "value", null);
  private final DimFilter otherFilter = new SelectorDimFilter("column_2", "value_2", null);
  private final DimFilter filterWithInterval = new AndDimFilter(
      selectorFilter,
      new BoundDimFilter("__time", "100", "200", false, true, null, null, StringComparators.NUMERIC)
  );

  @Test
  void filtration_no_join_and_interval()
  {
    DataSource dataSource = new TableDataSource("test");

    Pair<DataSource, Filtration> pair = DruidQuery.getFiltration(
        dataSource,
        selectorFilter,
        VirtualColumnRegistry.create(
            RowSignature.empty(),
            new ExpressionParserImpl(TestExprMacroTable.INSTANCE),
            false
        ),
        CalciteTests.createJoinableFactoryWrapper()
    );
    verify(pair, dataSource, selectorFilter, Intervals.ETERNITY);
  }

  @Test
  void filtration_interval_in_query_filter()
  {
    DataSource dataSource = new TableDataSource("test");
    Pair<DataSource, Filtration> pair = DruidQuery.getFiltration(
        dataSource,
        filterWithInterval,
        VirtualColumnRegistry.create(
            RowSignature.empty(),
            new ExpressionParserImpl(TestExprMacroTable.INSTANCE),
            false
        ),
        CalciteTests.createJoinableFactoryWrapper()
    );
    verify(pair, dataSource, selectorFilter, Intervals.utc(100, 200));
  }

  @Test
  void filtration_join_data_source_interval_in_query_filter()
  {
    DataSource dataSource = join(JoinType.INNER, otherFilter);
    Pair<DataSource, Filtration> pair = DruidQuery.getFiltration(
        dataSource,
        filterWithInterval,
        VirtualColumnRegistry.create(
            RowSignature.empty(),
            new ExpressionParserImpl(TestExprMacroTable.INSTANCE),
            false
        ),
        CalciteTests.createJoinableFactoryWrapper()
    );
    verify(pair, dataSource, selectorFilter, Intervals.utc(100, 200));
  }

  @Test
  void filtration_join_data_source_interval_in_base_table_filter_inner()
  {
    DataSource dataSource = join(JoinType.INNER, filterWithInterval);
    DataSource expectedDataSource = join(JoinType.INNER, selectorFilter);
    Pair<DataSource, Filtration> pair = DruidQuery.getFiltration(
        dataSource,
        otherFilter,
        VirtualColumnRegistry.create(
            RowSignature.empty(),
            new ExpressionParserImpl(TestExprMacroTable.INSTANCE),
            false
        ),
        CalciteTests.createJoinableFactoryWrapper()
    );
    verify(pair, expectedDataSource, otherFilter, Intervals.utc(100, 200));
  }

  @Test
  void filtration_join_data_source_interval_in_base_table_filter_left()
  {
    DataSource dataSource = join(JoinType.LEFT, filterWithInterval);
    DataSource expectedDataSource = join(JoinType.LEFT, selectorFilter);
    Pair<DataSource, Filtration> pair = DruidQuery.getFiltration(
        dataSource,
        otherFilter,
        VirtualColumnRegistry.create(
            RowSignature.empty(),
            new ExpressionParserImpl(TestExprMacroTable.INSTANCE),
            false
        ),
        CalciteTests.createJoinableFactoryWrapper()
    );
    verify(pair, expectedDataSource, otherFilter, Intervals.utc(100, 200));
  }

  @Test
  void filtration_join_data_source_interval_in_base_table_filter_right()
  {
    DataSource dataSource = join(JoinType.RIGHT, filterWithInterval);
    DataSource expectedDataSource = join(JoinType.RIGHT, selectorFilter);
    Pair<DataSource, Filtration> pair = DruidQuery.getFiltration(
        dataSource,
        otherFilter,
        VirtualColumnRegistry.create(
            RowSignature.empty(),
            new ExpressionParserImpl(TestExprMacroTable.INSTANCE),
            false
        ),
        CalciteTests.createJoinableFactoryWrapper()
    );
    verify(pair, expectedDataSource, otherFilter, Intervals.utc(100, 200));
  }

  @Test
  void filtration_join_data_source_interval_in_base_table_filter_full()
  {
    DataSource dataSource = join(JoinType.FULL, filterWithInterval);
    DataSource expectedDataSource = join(JoinType.FULL, selectorFilter);
    Pair<DataSource, Filtration> pair = DruidQuery.getFiltration(
        dataSource,
        otherFilter,
        VirtualColumnRegistry.create(
            RowSignature.empty(),
            new ExpressionParserImpl(TestExprMacroTable.INSTANCE),
            false
        ),
        CalciteTests.createJoinableFactoryWrapper()
    );
    verify(pair, expectedDataSource, otherFilter, Intervals.utc(100, 200));
  }

  @Test
  void filtration_intervals_in_both_filters()
  {
    DataSource dataSource = join(JoinType.INNER, filterWithInterval);
    DataSource expectedDataSource = join(JoinType.INNER, selectorFilter);
    DimFilter queryFilter = new AndDimFilter(
        otherFilter,
        new BoundDimFilter("__time", "150", "250", false, true, null, null, StringComparators.NUMERIC)

    );
    Pair<DataSource, Filtration> pair = DruidQuery.getFiltration(
        dataSource,
        queryFilter,
        VirtualColumnRegistry.create(
            RowSignature.empty(),
            new ExpressionParserImpl(TestExprMacroTable.INSTANCE),
            false
        ),
        CalciteTests.createJoinableFactoryWrapper()
    );
    verify(pair, expectedDataSource, otherFilter, Intervals.utc(150, 200));
  }

  private JoinDataSource join(JoinType joinType, DimFilter filter)
  {
    return JoinDataSource.create(
        new TableDataSource("left"),
        new TableDataSource("right"),
        "r.",
        "c == \"r.c\"",
        joinType,
        filter,
        ExprMacroTable.nil(),
        CalciteTests.createJoinableFactoryWrapper()
    );
  }

  private void verify(
      Pair<DataSource, Filtration> pair,
      DataSource dataSource,
      DimFilter columnFilter,
      Interval interval
  )
  {
    assertEquals(dataSource, pair.lhs);
    assertEquals(columnFilter, pair.rhs.getDimFilter(), "dim-filter: " + pair.rhs.getDimFilter());
    assertEquals(Collections.singletonList(interval), pair.rhs.getIntervals());
  }
}
