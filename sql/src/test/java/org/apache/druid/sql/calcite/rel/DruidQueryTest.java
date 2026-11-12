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

import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Druids;
import org.apache.druid.query.FilteredDataSource;
import org.apache.druid.query.JoinAlgorithm;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.ExpressionParserImpl;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.joda.time.Interval;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class DruidQueryTest
{
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
        VirtualColumnRegistry.create(
            RowSignature.empty(),
            new ExpressionParserImpl(TestExprMacroTable.INSTANCE),
            false,
            Set.of()
        ),
        CalciteTests.createJoinableFactoryWrapper()
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
        VirtualColumnRegistry.create(
            RowSignature.empty(),
            new ExpressionParserImpl(TestExprMacroTable.INSTANCE),
            false,
            Set.of()
        ),
        CalciteTests.createJoinableFactoryWrapper()
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
        VirtualColumnRegistry.create(
            RowSignature.empty(),
            new ExpressionParserImpl(TestExprMacroTable.INSTANCE),
            false,
            Set.of()
        ),
        CalciteTests.createJoinableFactoryWrapper()
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
        VirtualColumnRegistry.create(
            RowSignature.empty(),
            new ExpressionParserImpl(TestExprMacroTable.INSTANCE),
            false,
            Set.of()
        ),
        CalciteTests.createJoinableFactoryWrapper()
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
        VirtualColumnRegistry.create(
            RowSignature.empty(),
            new ExpressionParserImpl(TestExprMacroTable.INSTANCE),
            false,
            Set.of()
        ),
        CalciteTests.createJoinableFactoryWrapper()
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
        VirtualColumnRegistry.create(
            RowSignature.empty(),
            new ExpressionParserImpl(TestExprMacroTable.INSTANCE),
            false,
            Set.of()
        ),
        CalciteTests.createJoinableFactoryWrapper()
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
        VirtualColumnRegistry.create(
            RowSignature.empty(),
            new ExpressionParserImpl(TestExprMacroTable.INSTANCE),
            false,
            Set.of()
        ),
        CalciteTests.createJoinableFactoryWrapper()
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
        VirtualColumnRegistry.create(
            RowSignature.empty(),
            new ExpressionParserImpl(TestExprMacroTable.INSTANCE),
            false,
            Set.of()
        ),
        CalciteTests.createJoinableFactoryWrapper()
    );
    verify(pair, expectedDataSource, otherFilter, Intervals.utc(150, 200));
  }

  @Test
  public void test_filtration_filteredUnionDataSource_intervalsInArmFilters()
  {
    final DataSource dataSource = FilteredDataSource.create(
        new UnionDataSource(
            List.of(
                FilteredDataSource.create(new TableDataSource("a"), filterWithInterval),
                FilteredDataSource.create(
                    new TableDataSource("b"),
                    new BoundDimFilter("__time", "300", "400", false, true, null, null, StringComparators.NUMERIC)
                )
            )
        ),
        selectorFilter
    );
    final Pair<DataSource, Filtration> pair = DruidQuery.getFiltration(
        dataSource,
        otherFilter,
        VirtualColumnRegistry.create(
            RowSignature.empty(),
            new ExpressionParserImpl(TestExprMacroTable.INSTANCE),
            false,
            Set.of()
        ),
        CalciteTests.createJoinableFactoryWrapper()
    );
    Assertions.assertEquals(dataSource, pair.lhs);
    Assertions.assertEquals(otherFilter, pair.rhs.getDimFilter());
    Assertions.assertEquals(List.of(Intervals.utc(100, 200), Intervals.utc(300, 400)), pair.rhs.getIntervals());
  }

  @Test
  public void test_filtration_timeCondition_noIntervalInQueryFilter()
  {
    DataSource dataSource = new TableDataSource("test");
    Pair<DataSource, Filtration> pair = DruidQuery.getFiltration(
        dataSource,
        DruidQuery.withTimeCondition(dataSource, selectorFilter, Collections.singletonList(Intervals.utc(50, 150))),
        VirtualColumnRegistry.create(
            RowSignature.empty(),
            new ExpressionParserImpl(TestExprMacroTable.INSTANCE),
            false,
            Set.of()
        ),
        CalciteTests.createJoinableFactoryWrapper()
    );
    verify(pair, dataSource, selectorFilter, Intervals.utc(50, 150));
  }

  @Test
  public void test_filtration_timeCondition_noQueryFilter()
  {
    DataSource dataSource = new TableDataSource("test");
    Pair<DataSource, Filtration> pair = DruidQuery.getFiltration(
        dataSource,
        DruidQuery.withTimeCondition(dataSource, null, Collections.singletonList(Intervals.utc(50, 150))),
        VirtualColumnRegistry.create(
            RowSignature.empty(),
            new ExpressionParserImpl(TestExprMacroTable.INSTANCE),
            false,
            Set.of()
        ),
        CalciteTests.createJoinableFactoryWrapper()
    );
    verify(pair, dataSource, null, Intervals.utc(50, 150));
  }

  @Test
  public void test_filtration_timeCondition_intervalInQueryFilter()
  {
    DataSource dataSource = new TableDataSource("test");
    Pair<DataSource, Filtration> pair = DruidQuery.getFiltration(
        dataSource,
        DruidQuery.withTimeCondition(dataSource, filterWithInterval, Collections.singletonList(Intervals.utc(150, 300))),
        VirtualColumnRegistry.create(
            RowSignature.empty(),
            new ExpressionParserImpl(TestExprMacroTable.INSTANCE),
            false,
            Set.of()
        ),
        CalciteTests.createJoinableFactoryWrapper()
    );
    verify(pair, dataSource, selectorFilter, Intervals.utc(150, 200));
  }

  @Test
  public void test_filtration_timeCondition_joinDataSource_intervalInBaseTableFilter()
  {
    DataSource dataSource = join(JoinType.INNER, filterWithInterval);
    DataSource expectedDataSource = join(JoinType.INNER, selectorFilter);
    Pair<DataSource, Filtration> pair = DruidQuery.getFiltration(
        dataSource,
        DruidQuery.withTimeCondition(dataSource, otherFilter, Collections.singletonList(Intervals.utc(50, 150))),
        VirtualColumnRegistry.create(
            RowSignature.empty(),
            new ExpressionParserImpl(TestExprMacroTable.INSTANCE),
            false,
            Set.of()
        ),
        CalciteTests.createJoinableFactoryWrapper()
    );
    verify(pair, expectedDataSource, otherFilter, Intervals.utc(100, 150));
  }

  @Test
  public void test_filtration_timeCondition_queryDataSource()
  {
    DataSource dataSource = new QueryDataSource(
        Druids.newScanQueryBuilder()
              .dataSource("test")
              .intervals(new MultipleIntervalSegmentSpec(Intervals.ONLY_ETERNITY))
              .build()
    );
    Pair<DataSource, Filtration> pair = DruidQuery.getFiltration(
        dataSource,
        DruidQuery.withTimeCondition(dataSource, selectorFilter, Collections.singletonList(Intervals.utc(150, 300))),
        VirtualColumnRegistry.create(
            RowSignature.empty(),
            new ExpressionParserImpl(TestExprMacroTable.INSTANCE),
            false,
            Set.of()
        ),
        CalciteTests.createJoinableFactoryWrapper()
    );

    // Time condition is not applied, since the datasource is not table-based.
    verify(pair, dataSource, selectorFilter, Intervals.ETERNITY);
  }

  @Test
  public void test_filtration_timeCondition_joinDataSource_subqueryOnLeft()
  {
    DataSource dataSource = JoinDataSource.create(
        new QueryDataSource(
            Druids.newScanQueryBuilder()
                  .dataSource("left")
                  .intervals(new MultipleIntervalSegmentSpec(Intervals.ONLY_ETERNITY))
                  .build()
        ),
        new TableDataSource("right"),
        "r.",
        "c == \"r.c\"",
        JoinType.INNER,
        null,
        ExprMacroTable.nil(),
        CalciteTests.createJoinableFactoryWrapper(),
        JoinAlgorithm.BROADCAST
    );
    Pair<DataSource, Filtration> pair = DruidQuery.getFiltration(
        dataSource,
        DruidQuery.withTimeCondition(dataSource, selectorFilter, Collections.singletonList(Intervals.utc(150, 300))),
        VirtualColumnRegistry.create(
            RowSignature.empty(),
            new ExpressionParserImpl(TestExprMacroTable.INSTANCE),
            false,
            Set.of()
        ),
        CalciteTests.createJoinableFactoryWrapper()
    );

    // Time condition is not applied, since the base of the join is a subquery, which is not table-based.
    verify(pair, dataSource, selectorFilter, Intervals.ETERNITY);
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
        CalciteTests.createJoinableFactoryWrapper(),
        JoinAlgorithm.BROADCAST
    );
  }

  private void verify(
      Pair<DataSource, Filtration> pair,
      DataSource dataSource,
      DimFilter columnFilter,
      Interval interval
  )
  {
    Assertions.assertEquals(dataSource, pair.lhs);
    Assertions.assertEquals(columnFilter, pair.rhs.getDimFilter(), "dim-filter: " + pair.rhs.getDimFilter());
    Assertions.assertEquals(Collections.singletonList(interval), pair.rhs.getIntervals());
  }
}
