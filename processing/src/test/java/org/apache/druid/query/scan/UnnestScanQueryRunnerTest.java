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

package org.apache.druid.query.scan;

import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Order;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnnestDataSource;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;


public class UnnestScanQueryRunnerTest extends InitializedNullHandlingTest
{
  public static final QuerySegmentSpec I_0112_0114 = ScanQueryRunnerTest.I_0112_0114;
  private static final ScanQueryQueryToolChest TOOL_CHEST = new ScanQueryQueryToolChest(
      DefaultGenericQueryMetricsFactory.instance()
  );
  private static final ScanQueryRunnerFactory FACTORY = new ScanQueryRunnerFactory(
      TOOL_CHEST,
      new ScanQueryEngine(),
      new ScanQueryConfig()
  );
  private final IncrementalIndex index = TestIndex.getIncrementalTestIndex();

  private Druids.ScanQueryBuilder newTestUnnestQuery()
  {
    return Druids.newScanQueryBuilder()
                 .dataSource(QueryRunnerTestHelper.UNNEST_DATA_SOURCE)
                 .columns(Collections.emptyList())
                 .eternityInterval()
                 .limit(3);
  }

  private Druids.ScanQueryBuilder newTestUnnestQueryWithFilterDataSource()
  {
    return Druids.newScanQueryBuilder()
                 .dataSource(QueryRunnerTestHelper.UNNEST_FILTER_DATA_SOURCE)
                 .columns(Collections.emptyList())
                 .eternityInterval()
                 .limit(3);
  }

  @Test
  public void testScanOnUnnest()
  {
    ScanQuery query = newTestUnnestQuery()
        .intervals(I_0112_0114)
        .columns(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST)
        .limit(3)
        .build();

    final QueryRunner queryRunner = QueryRunnerTestHelper.makeQueryRunnerWithSegmentMapFn(
        FACTORY,
        new IncrementalIndexSegment(
            index,
            QueryRunnerTestHelper.SEGMENT_ID
        ),
        query,
        "rtIndexvc"
    );

    Iterable<ScanResultValue> results = queryRunner.run(QueryPlus.wrap(query)).toList();
    String[] columnNames = new String[]{
        QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST
    };
    String[] values = new String[]{
        "a",
        "preferred",
        "b"
    };

    final List<List<Map<String, Object>>> events = ScanQueryRunnerTest.toEvents(columnNames, values);
    List<ScanResultValue> expectedResults = toExpected(
        events,
        Collections.singletonList(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST),
        0,
        3
    );
    ScanQueryRunnerTest.verify(expectedResults, results);
  }

  @Test
  public void testScanOnUnnestFilterDataSource()
  {
    ScanQuery query = newTestUnnestQueryWithFilterDataSource()
        .intervals(I_0112_0114)
        .columns(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST)
        .limit(3)
        .build();

    final QueryRunner queryRunner = QueryRunnerTestHelper.makeQueryRunnerWithSegmentMapFn(
        FACTORY,
        new IncrementalIndexSegment(
            index,
            QueryRunnerTestHelper.SEGMENT_ID
        ),
        query,
        "rtIndexvc"
    );

    Iterable<ScanResultValue> results = queryRunner.run(QueryPlus.wrap(query)).toList();
    String[] columnNames = new String[]{
        QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST
    };
    String[] values = new String[]{
        "a",
        "preferred",
        "b"
    };

    final List<List<Map<String, Object>>> events = ScanQueryRunnerTest.toEvents(columnNames, values);
    List<ScanResultValue> expectedResults = toExpected(
        events,
        Collections.singletonList(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST),
        0,
        3
    );
    ScanQueryRunnerTest.verify(expectedResults, results);
  }

  @Test
  public void testUnnestRunnerVirtualColumnsUsingSingleColumn()
  {
    ScanQuery query =
        Druids.newScanQueryBuilder()
              .intervals(I_0112_0114)
              .dataSource(UnnestDataSource.create(
                  new TableDataSource(QueryRunnerTestHelper.DATA_SOURCE),
                  new ExpressionVirtualColumn(
                      QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST,
                      "mv_to_array(placementish)",
                      ColumnType.STRING,
                      TestExprMacroTable.INSTANCE
                  ),
                  null
              ))
              .columns(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST)
              .eternityInterval()
              .limit(3)
              .build();

    QueryRunner vcrunner = QueryRunnerTestHelper.makeQueryRunnerWithSegmentMapFn(
        FACTORY,
        new IncrementalIndexSegment(
            index,
            QueryRunnerTestHelper.SEGMENT_ID
        ),
        query,
        "rtIndexvc"
    );
    Iterable<ScanResultValue> results = vcrunner.run(QueryPlus.wrap(query)).toList();
    String[] columnNames = new String[]{
        QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST
    };
    String[] values = new String[]{
        "a",
        "preferred",
        "b"
    };

    final List<List<Map<String, Object>>> events = ScanQueryRunnerTest.toEvents(columnNames, values);
    List<ScanResultValue> expectedResults = toExpected(
        events,
        Collections.singletonList(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST),
        0,
        3
    );
    ScanQueryRunnerTest.verify(expectedResults, results);
  }

  @Test
  public void testUnnestRunnerVirtualColumnsUsingMultipleColumn()
  {
    ScanQuery query =
        Druids.newScanQueryBuilder()
              .intervals(I_0112_0114)
              .dataSource(UnnestDataSource.create(
                  new TableDataSource(QueryRunnerTestHelper.DATA_SOURCE),
                  new ExpressionVirtualColumn(
                      QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST,
                      "array(\"market\",\"quality\")",
                      ColumnType.STRING,
                      TestExprMacroTable.INSTANCE
                  ),
                  null
              ))
              .columns(QueryRunnerTestHelper.MARKET_DIMENSION, QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST)
              .eternityInterval()
              .limit(4)
              .build();

    QueryRunner vcrunner = QueryRunnerTestHelper.makeQueryRunnerWithSegmentMapFn(
        FACTORY,
        new IncrementalIndexSegment(
            index,
            QueryRunnerTestHelper.SEGMENT_ID
        ),
        query,
        "rtIndexvc"
    );

    Iterable<ScanResultValue> results = vcrunner.run(QueryPlus.wrap(query)).toList();
    String[] columnNames = new String[]{
        QueryRunnerTestHelper.MARKET_DIMENSION,
        QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST
    };
    String[] values = new String[]{
        "spot\tspot",
        "spot\tautomotive",
        "spot\tspot",
        "spot\tbusiness"
    };

    final List<List<Map<String, Object>>> events = ScanQueryRunnerTest.toEvents(columnNames, values);
    List<ScanResultValue> expectedResults = toExpected(
        events,
        Lists.newArrayList(
            QueryRunnerTestHelper.MARKET_DIMENSION,
            QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST
        ),
        0,
        4
    );
    ScanQueryRunnerTest.verify(expectedResults, results);
  }

  @Test
  public void testUnnestRunnerWithFilter()
  {
    ScanQuery query = newTestUnnestQuery()
        .intervals(I_0112_0114)
        .columns(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST)
        .limit(3)
        .filters(new SelectorDimFilter(QueryRunnerTestHelper.MARKET_DIMENSION, "spot", null))
        .build();

    final QueryRunner queryRunner = QueryRunnerTestHelper.makeQueryRunnerWithSegmentMapFn(
        FACTORY,
        new IncrementalIndexSegment(
            index,
            QueryRunnerTestHelper.SEGMENT_ID
        ),
        query,
        "rtIndexvc"
    );

    Iterable<ScanResultValue> results = queryRunner.run(QueryPlus.wrap(query)).toList();
    String[] columnNames = new String[]{
        QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST
    };
    String[] values = new String[]{
        "a",
        "preferred",
        "b"
    };

    final List<List<Map<String, Object>>> events = ScanQueryRunnerTest.toEvents(columnNames, values);
    List<ScanResultValue> expectedResults = toExpected(
        events,
        Collections.singletonList(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST),
        0,
        3
    );
    ScanQueryRunnerTest.verify(expectedResults, results);
  }

  @Test
  public void testUnnestRunnerWithOrdering()
  {
    ScanQuery query = newTestUnnestQuery()
        .intervals(I_0112_0114)
        .columns(QueryRunnerTestHelper.TIME_DIMENSION, QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST)
        .limit(3)
        .filters(new SelectorDimFilter(QueryRunnerTestHelper.MARKET_DIMENSION, "spot", null))
        .order(Order.ASCENDING)
        .build();


    final QueryRunner queryRunner = QueryRunnerTestHelper.makeQueryRunnerWithSegmentMapFn(
        FACTORY,
        new IncrementalIndexSegment(
            index,
            QueryRunnerTestHelper.SEGMENT_ID
        ),
        query,
        "rtIndexvc"
    );

    Iterable<ScanResultValue> results = queryRunner.run(QueryPlus.wrap(query)).toList();
    String[] columnNames = new String[]{
        ColumnHolder.TIME_COLUMN_NAME,
        QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST
    };
    String[] values = new String[]{
        "2011-01-12T00:00:00.000Z\ta",
        "2011-01-12T00:00:00.000Z\tpreferred",
        "2011-01-12T00:00:00.000Z\tb"
    };

    final List<List<Map<String, Object>>> ascendingEvents = ScanQueryRunnerTest.toEvents(columnNames, values);

    for (List<Map<String, Object>> batch : ascendingEvents) {
      for (Map<String, Object> event : batch) {
        event.put("__time", (DateTimes.of((String) event.get("__time"))).getMillis());
      }
    }
    List<ScanResultValue> ascendingExpectedResults = toExpected(
        ascendingEvents,
        Lists.newArrayList(
            QueryRunnerTestHelper.TIME_DIMENSION,
            QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST
        ),
        0,
        3
    );

    ScanQueryRunnerTest.verify(ascendingExpectedResults, results);
  }

  private List<ScanResultValue> toExpected(
      List<List<Map<String, Object>>> targets,
      List<String> columns,
      final int offset,
      final int limit
  )
  {
    List<ScanResultValue> expected = Lists.newArrayListWithExpectedSize(targets.size());
    for (List<Map<String, Object>> group : targets) {
      List<Map<String, Object>> events = Lists.newArrayListWithExpectedSize(limit);
      int end = Math.min(group.size(), offset + limit);
      if (end == 0) {
        end = group.size();
      }
      events.addAll(group.subList(offset, end));
      expected.add(new ScanResultValue(QueryRunnerTestHelper.SEGMENT_ID.toString(), columns, events));
    }
    return expected;
  }
}
