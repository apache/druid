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

package org.apache.druid.msq.querykit.scan;

import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Order;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.column.ColumnHolder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class ScanLimitTrackerTest
{
  @Test
  public void test_create_noLimit_timeOrder_returnNoopTracker()
  {
    Assertions.assertSame(ScanLimitTracker.NONE, ScanLimitTracker.create(scanQuery().build()));
    Assertions.assertSame(
        ScanLimitTracker.NONE,
        ScanLimitTracker.create(scanQuery().orderBy(List.of(OrderBy.descending(ColumnHolder.TIME_COLUMN_NAME))).build())
    );
  }

  @Test
  public void test_create_limit_noOrder_returnRowCountTracker()
  {
    final ScanLimitTracker tracker = ScanLimitTracker.create(scanQuery().limit(10).offset(5).build());
    final RowCountScanLimitTracker rowCountTracker =
        Assertions.assertInstanceOf(RowCountScanLimitTracker.class, tracker);
    Assertions.assertEquals(15, rowCountTracker.getRowsNeeded());
    Assertions.assertEquals(Order.NONE, tracker.getTimeOrder());
  }

  @Test
  public void test_create_limit_noOrder_ingestionContextKey_returnRowCountTracker()
  {
    Assertions.assertInstanceOf(
        RowCountScanLimitTracker.class,
        ScanLimitTracker.create(scanQuery().limit(10).context(ingestionContext()).build())
    );
  }

  @Test
  public void test_create_limit_timeOrderDesc_returnTimeOrderTracker()
  {
    final ScanLimitTracker tracker = ScanLimitTracker.create(
        scanQuery().limit(10).offset(5).orderBy(List.of(OrderBy.descending(ColumnHolder.TIME_COLUMN_NAME))).build()
    );
    final TimeOrderedScanLimitTracker timeOrderedTracker =
        Assertions.assertInstanceOf(TimeOrderedScanLimitTracker.class, tracker);
    Assertions.assertEquals(15, timeOrderedTracker.getRowsNeeded());
    Assertions.assertEquals(Order.DESCENDING, tracker.getTimeOrder());
  }

  @Test
  public void test_create_limit_timeOrderAsc_returnTimeOrderTracker()
  {
    final ScanLimitTracker tracker = ScanLimitTracker.create(
        scanQuery().limit(10).orderBy(List.of(OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME))).build()
    );
    Assertions.assertInstanceOf(TimeOrderedScanLimitTracker.class, tracker);
    Assertions.assertEquals(Order.ASCENDING, tracker.getTimeOrder());
  }

  @Test
  public void test_create_limit_timeOrder_otherOrder_returnTimeOrderTracker()
  {
    final ScanLimitTracker tracker = ScanLimitTracker.create(
        scanQuery().limit(10)
                   .orderBy(List.of(OrderBy.descending(ColumnHolder.TIME_COLUMN_NAME), OrderBy.ascending("dim")))
                   .build()
    );
    Assertions.assertInstanceOf(TimeOrderedScanLimitTracker.class, tracker);
    Assertions.assertEquals(Order.DESCENDING, tracker.getTimeOrder());
  }

  @Test
  public void test_create_limit_otherOrder_timeOrder_returnNoopTracker()
  {
    Assertions.assertSame(
        ScanLimitTracker.NONE,
        ScanLimitTracker.create(scanQuery().limit(10).orderBy(List.of(OrderBy.ascending("dim"))).build())
    );
    Assertions.assertSame(
        ScanLimitTracker.NONE,
        ScanLimitTracker.create(
            scanQuery().limit(10)
                       .orderBy(List.of(OrderBy.ascending("dim"), OrderBy.descending(ColumnHolder.TIME_COLUMN_NAME)))
                       .build()
        )
    );
  }

  @Test
  public void test_create_limit_timeOrder_ingestionContextKey_returnNoopTracker()
  {
    Assertions.assertSame(
        ScanLimitTracker.NONE,
        ScanLimitTracker.create(
            scanQuery().limit(10)
                       .orderBy(List.of(OrderBy.descending(ColumnHolder.TIME_COLUMN_NAME)))
                       .context(ingestionContext())
                       .build()
        )
    );
  }

  @Test
  public void test_create_maxLimit_timeOrder_returnTimeOrderTracker()
  {
    Assertions.assertInstanceOf(
        TimeOrderedScanLimitTracker.class,
        ScanLimitTracker.create(
            scanQuery().limit(TimeOrderedScanLimitTracker.MAX_ROWS_TRACKED)
                       .orderBy(List.of(OrderBy.descending(ColumnHolder.TIME_COLUMN_NAME)))
                       .build()
        ),
        "offset + limit at the maximum"
    );
  }

  @Test
  public void test_create_overMaxLimit_timeOrder_returnNoopTracker()
  {
    Assertions.assertSame(
        ScanLimitTracker.NONE,
        ScanLimitTracker.create(
            scanQuery().limit(TimeOrderedScanLimitTracker.MAX_ROWS_TRACKED + 1)
                       .orderBy(List.of(OrderBy.descending(ColumnHolder.TIME_COLUMN_NAME)))
                       .build()
        ),
        "offset + limit over the maximum"
    );
  }

  @Test
  public void test_create_maxLimitPlusOffset_timeOrder_returnNoopTracker()
  {
    Assertions.assertSame(
        ScanLimitTracker.NONE,
        ScanLimitTracker.create(
            scanQuery().limit(TimeOrderedScanLimitTracker.MAX_ROWS_TRACKED).offset(1)
                       .orderBy(List.of(OrderBy.descending(ColumnHolder.TIME_COLUMN_NAME)))
                       .build()
        ),
        "offset + limit over the maximum"
    );
  }

  private static Map<String, Object> ingestionContext()
  {
    return Map.of(QueryContextParameters.SQL_INSERT_SEGMENT_GRANULARITY.getName(), "\"DAY\"");
  }

  private static Druids.ScanQueryBuilder scanQuery()
  {
    return Druids.newScanQueryBuilder()
                 .dataSource("test")
                 .intervals(new MultipleIntervalSegmentSpec(List.of(Intervals.ETERNITY)))
                 .columns(ColumnHolder.TIME_COLUMN_NAME, "dim");
  }
}
