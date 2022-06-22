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
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class DruidQueryTest
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
  public void test_filtration_noJoinAndInterval()
  {
    DataSource dataSource = new TableDataSource("test");
    Pair<DataSource, Filtration> pair = DruidQuery.getFiltration(
        dataSource,
        selectorFilter,
        VirtualColumnRegistry.create(RowSignature.empty(), TestExprMacroTable.INSTANCE, false)
    );
    verify(pair, dataSource, selectorFilter, Intervals.ETERNITY);
  }

  @Test
  public void test_filtration_intervalInQueryFilter()
  {
    DataSource dataSource = new TableDataSource("test");
    Pair<DataSource, Filtration> pair = DruidQuery.getFiltration(
        dataSource,
        filterWithInterval,
        VirtualColumnRegistry.create(RowSignature.empty(), TestExprMacroTable.INSTANCE, false)
    );
    verify(pair, dataSource, selectorFilter, Intervals.utc(100, 200));
  }

  @Test
  public void test_filtration_joinDataSource_intervalInQueryFilter()
  {
    DataSource dataSource = join(JoinType.INNER, otherFilter);
    Pair<DataSource, Filtration> pair = DruidQuery.getFiltration(
        dataSource,
        filterWithInterval,
        VirtualColumnRegistry.create(RowSignature.empty(), TestExprMacroTable.INSTANCE, false)
    );
    verify(pair, dataSource, selectorFilter, Intervals.utc(100, 200));
  }

  @Test
  public void test_filtration_joinDataSource_intervalInBaseTableFilter_inner()
  {
    DataSource dataSource = join(JoinType.INNER, filterWithInterval);
    DataSource expectedDataSource = join(JoinType.INNER, selectorFilter);
    Pair<DataSource, Filtration> pair = DruidQuery.getFiltration(
        dataSource,
        otherFilter,
        VirtualColumnRegistry.create(RowSignature.empty(), TestExprMacroTable.INSTANCE, false)
    );
    verify(pair, expectedDataSource, otherFilter, Intervals.utc(100, 200));
  }

  @Test
  public void test_filtration_joinDataSource_intervalInBaseTableFilter_left()
  {
    DataSource dataSource = join(JoinType.LEFT, filterWithInterval);
    DataSource expectedDataSource = join(JoinType.LEFT, selectorFilter);
    Pair<DataSource, Filtration> pair = DruidQuery.getFiltration(
        dataSource,
        otherFilter,
        VirtualColumnRegistry.create(RowSignature.empty(), TestExprMacroTable.INSTANCE, false)
    );
    verify(pair, expectedDataSource, otherFilter, Intervals.utc(100, 200));
  }

  @Test
  public void test_filtration_joinDataSource_intervalInBaseTableFilter_right()
  {
    DataSource dataSource = join(JoinType.RIGHT, filterWithInterval);
    DataSource expectedDataSource = join(JoinType.RIGHT, selectorFilter);
    Pair<DataSource, Filtration> pair = DruidQuery.getFiltration(
        dataSource,
        otherFilter,
        VirtualColumnRegistry.create(RowSignature.empty(), TestExprMacroTable.INSTANCE, false)
    );
    verify(pair, expectedDataSource, otherFilter, Intervals.utc(100, 200));
  }

  @Test
  public void test_filtration_joinDataSource_intervalInBaseTableFilter_full()
  {
    DataSource dataSource = join(JoinType.FULL, filterWithInterval);
    DataSource expectedDataSource = join(JoinType.FULL, selectorFilter);
    Pair<DataSource, Filtration> pair = DruidQuery.getFiltration(
        dataSource,
        otherFilter,
        VirtualColumnRegistry.create(RowSignature.empty(), TestExprMacroTable.INSTANCE, false)
    );
    verify(pair, expectedDataSource, otherFilter, Intervals.utc(100, 200));
  }

  @Test
  public void test_filtration_intervalsInBothFilters()
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
        VirtualColumnRegistry.create(RowSignature.empty(), TestExprMacroTable.INSTANCE, false)
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
        ExprMacroTable.nil()
    );
  }

  private void verify(
      Pair<DataSource, Filtration> pair,
      DataSource dataSource,
      DimFilter columnFilter,
      Interval interval
  )
  {
    Assert.assertEquals(dataSource, pair.lhs);
    Assert.assertEquals("dim-filter: " + pair.rhs.getDimFilter(), columnFilter, pair.rhs.getDimFilter());
    Assert.assertEquals(Collections.singletonList(interval), pair.rhs.getIntervals());
  }
}
