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

package org.apache.druid.queryng.operators.scan;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.scan.ScanQuery.ResultFormat;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.queryng.fragment.FragmentManager;
import org.apache.druid.queryng.fragment.Fragments;
import org.apache.druid.queryng.operators.ConcatOperator;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.general.MockCursor;
import org.apache.druid.queryng.operators.general.MockStorageAdapter;
import org.apache.druid.queryng.operators.general.MockStorageAdapter.MockSegment;
import org.apache.druid.queryng.operators.scan.ScanEngineOperator.CursorDefinition;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnHolder;
import org.joda.time.Interval;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class ScanQueryOperatorTest
{
  /**
   * Segment that creates two cursors. Not very realistic, but realism
   * isn't needed: just the two cursors are needed.
   */
  private static class MockDualCursorSegment extends MockSegment
  {
    public MockDualCursorSegment(int segmentSize)
    {
      super(segmentSize);
    }

    @Override
    public StorageAdapter asStorageAdapter()
    {
      return new MockStorageAdapter(segmentSize)
      {
        @Override
        public Sequence<Cursor> makeCursors(
            Filter filter,
            Interval interval,
            VirtualColumns virtualColumns,
            Granularity gran,
            boolean descending,
            QueryMetrics<?> queryMetrics)
        {
          return Sequences.simple(Arrays.asList(
              new MockCursor(interval, segmentSize),
              new MockCursor(interval, segmentSize)
          ));
        }
      };
    }
  }

  private CursorDefinition mockCursor(Segment segment)
  {
    return new CursorDefinition(
        segment,
        MockStorageAdapter.MOCK_INTERVAL, // Whole segment
        null, // No filter
        VirtualColumns.EMPTY,
        false,
        Granularities.ALL,
        null // No query metrics
    );
  }

  private CursorDefinition mockCursor(int rowCount)
  {
    return mockCursor(new MockSegment(rowCount));
  }

  @Test
  public void testWildcard()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    Operator<ScanResultValue> op = new ScanEngineOperator(
        fragment,
        mockCursor(20),
        10, // Batch size
        false, // Not legacy
        null, // No columns AKA "wildcard"
        ScanEngineOperator.Order.NONE,
        Long.MAX_VALUE, // No limit
        ResultFormat.RESULT_FORMAT_COMPACTED_LIST,
        Long.MAX_VALUE // No timeout
    );
    fragment.registerRoot(op);
    List<ScanResultValue> results = fragment.toList();
    assertEquals(2, results.size());
    ScanResultValue first = results.get(0);
    assertEquals(3, first.getColumns().size());

    // Column order should be dimensions then measures.
    assertEquals(ColumnHolder.TIME_COLUMN_NAME, first.getColumns().get(0));
    assertEquals("page", first.getColumns().get(1));
    assertEquals("delta", first.getColumns().get(2));

    // Sample one row
    List<List<Object>> rows = first.getRows();
    assertEquals(10, rows.size());
    List<Object> row = rows.get(0);
    assertEquals((Long) 1442062800000L, (Long) row.get(0));
    assertEquals("row 1", (String) row.get(1));
    assertEquals((Long) 0L, (Long) row.get(2));

    // Second batch
    rows = results.get(1).getRows();
    assertEquals(10, rows.size());
    row = rows.get(0);
    assertEquals((Long) 10L, (Long) row.get(2));

    // Close twice: benign
    op.close(false);

    // Context was updated
    assertEquals((Long) 20L, fragment.responseContext().getRowScanCount());
  }

  @Test
  public void testProjection()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    Operator<ScanResultValue> op = new ScanEngineOperator(
        fragment,
        mockCursor(5),
        10, // Batch size
        false, // Not legacy
        // Projection, omit metrics, add unknown column
        Arrays.asList("page", ColumnHolder.TIME_COLUMN_NAME, "bogus"),
        ScanEngineOperator.Order.NONE,
        Long.MAX_VALUE, // No limit
        ResultFormat.RESULT_FORMAT_COMPACTED_LIST,
        Long.MAX_VALUE // No timeout
    );
    fragment.registerRoot(op);
    List<ScanResultValue> results = fragment.toList();
    assertEquals(1, results.size());
    ScanResultValue first = results.get(0);
    assertEquals(3, first.getColumns().size());

    // Column order from the projection list.
    assertEquals("page", first.getColumns().get(0));
    assertEquals(ColumnHolder.TIME_COLUMN_NAME, first.getColumns().get(1));
    assertEquals("bogus", first.getColumns().get(2));

    // Sample one row
    List<List<Object>> rows = first.getRows();
    assertEquals(5, rows.size());
    List<Object> row = rows.get(0);
    assertEquals("row 1", (String) row.get(0));
    assertEquals((Long) 1442062800000L, (Long) row.get(1));
    assertNull(row.get(2));
  }

  /**
   * Simulate a filter that matches no rows
   */
  @Test
  public void testZeroRows()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    Operator<ScanResultValue> op = new ScanEngineOperator(
        fragment,
        mockCursor(0),
        10, // Batch size
        false, // Not legacy
        null, // No columns AKA "wildcard"
        ScanEngineOperator.Order.NONE,
        Long.MAX_VALUE, // No limit
        ResultFormat.RESULT_FORMAT_COMPACTED_LIST,
        Long.MAX_VALUE // No timeout
    );
    fragment.registerRoot(op);
    List<ScanResultValue> results = fragment.toList();
    assertTrue(results.isEmpty());
  }

  @Test
  public void testNoSegment()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    Operator<ScanResultValue> op = new ScanEngineOperator(
        fragment,
        mockCursor(-1), // simulate no segment
        10, // Batch size
        false, // Not legacy
        null, // No columns AKA "wildcard"
        ScanEngineOperator.Order.NONE,
        Long.MAX_VALUE, // No limit
        ResultFormat.RESULT_FORMAT_COMPACTED_LIST,
        Long.MAX_VALUE // No timeout
    );
    fragment.registerRoot(op);
    assertThrows(ISE.class, () -> fragment.toList());
  }

  @Test
  public void testTwoCursors()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    Operator<ScanResultValue> op = new ScanEngineOperator(
        fragment,
        mockCursor(new MockDualCursorSegment(20)),
        100, // Batch size
        false, // Not legacy
        null, // No columns AKA "wildcard"
        ScanEngineOperator.Order.NONE,
        Long.MAX_VALUE, // No limit
        ResultFormat.RESULT_FORMAT_COMPACTED_LIST,
        Long.MAX_VALUE // No timeout
    );
    fragment.registerRoot(op);
    List<ScanResultValue> results = fragment.toList();
    assertEquals(2, results.size());
    assertEquals(20, results.get(0).getRows().size());
    assertEquals(20, results.get(1).getRows().size());
  }

  @Test
  public void testMultipleCursorsZeroRows()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    Operator<ScanResultValue> op = new ScanEngineOperator(
        fragment,
        mockCursor(new MockDualCursorSegment(0)),
        10, // Batch size
        false, // Not legacy
        null, // No columns AKA "wildcard"
        ScanEngineOperator.Order.NONE,
        Long.MAX_VALUE, // No limit
        ResultFormat.RESULT_FORMAT_COMPACTED_LIST,
        Long.MAX_VALUE // No timeout
    );
    fragment.registerRoot(op);
    List<ScanResultValue> results = fragment.toList();
    assertTrue(results.isEmpty());
  }

  private List<ScanResultValue> opWithLimit(int limit)
  {
    FragmentManager fragment = Fragments.defaultFragment();
    Operator<ScanResultValue> op = new ScanEngineOperator(
        fragment,
        mockCursor(20),
        10, // Batch size
        false, // Not legacy
        null, // No columns AKA "wildcard"
        ScanEngineOperator.Order.NONE,
        limit,
        ResultFormat.RESULT_FORMAT_COMPACTED_LIST,
        Long.MAX_VALUE // No timeout
    );
    fragment.registerRoot(op);
    return fragment.toList();
  }

  @Test
  public void testLimit()
  {
    // Limit on first batch
    List<ScanResultValue> results = opWithLimit(5);
    assertEquals(1, results.size());
    assertEquals(5, results.get(0).getRows().size());

    // Limit on first batch boundary
    results = opWithLimit(10);
    assertEquals(1, results.size());
    assertEquals(10, results.get(0).getRows().size());

    // Limit in second batch
    results = opWithLimit(15);
    assertEquals(2, results.size());
    assertEquals(10, results.get(0).getRows().size());
    assertEquals(5, results.get(1).getRows().size());
  }

  @Test
  public void testLimitOnSecondCursor()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    Operator<ScanResultValue> op = new ScanEngineOperator(
        fragment,
        mockCursor(new MockDualCursorSegment(20)),
        10, // Batch size
        false, // Not legacy
        null, // No columns AKA "wildcard"
        ScanEngineOperator.Order.NONE,
        25, // Limit in second cursor
        ResultFormat.RESULT_FORMAT_COMPACTED_LIST,
        Long.MAX_VALUE // No timeout
    );
    fragment.registerRoot(op);
    List<ScanResultValue> results = fragment.toList();
    assertEquals(3, results.size());
    assertEquals(10, results.get(0).getRows().size());
    assertEquals(10, results.get(1).getRows().size());
    assertEquals(5, results.get(2).getRows().size());
  }

  /**
   * Test the case when there are two distinct segment scans within a
   * single fragment. The row count is carried from one to the next
   * using the response context.
   * <p>
   * Note: using the response context emulates the current approach, but
   * is not very satisfying in an operator context: better to provide a
   * distinct operator to do the work.
   */
  @Test
  public void testLimitOnSecondScan()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    Operator<ScanResultValue> op1 = new ScanEngineOperator(
        fragment,
        mockCursor(20),
        10, // Batch size
        false, // Not legacy
        null, // No columns AKA "wildcard"
        ScanEngineOperator.Order.NONE,
        25, // Limit in second scan
        ResultFormat.RESULT_FORMAT_COMPACTED_LIST,
        Long.MAX_VALUE // No timeout
    );
    Operator<ScanResultValue> op2 = new ScanEngineOperator(
        fragment,
        mockCursor(20),
        10, // Batch size
        false, // Not legacy
        null, // No columns AKA "wildcard"
        ScanEngineOperator.Order.NONE,
        25, // Limit in second scan
        ResultFormat.RESULT_FORMAT_COMPACTED_LIST,
        Long.MAX_VALUE // No timeout
    );
    ConcatOperator<ScanResultValue> concat = new ConcatOperator<>(
        fragment,
        Arrays.asList(op1, op2)
    );
    fragment.registerRoot(concat);
    List<ScanResultValue> results = fragment.toList();
    assertEquals(3, results.size());
    assertEquals(10, results.get(0).getRows().size());
    assertEquals(10, results.get(1).getRows().size());
    assertEquals(5, results.get(2).getRows().size());
  }

  /**
   * DAG has two scans, but the limit is satisfied on the first.
   * An outer operator would normally omit calling the second,
   * but the converted code does handle the case anyway.
   */
  @Test
  public void testLimitOnFirstScan()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    Operator<ScanResultValue> op1 = new ScanEngineOperator(
        fragment,
        mockCursor(20),
        10, // Batch size
        false, // Not legacy
        null, // No columns AKA "wildcard"
        ScanEngineOperator.Order.NONE,
        15, // Limit in second scan
        ResultFormat.RESULT_FORMAT_COMPACTED_LIST,
        Long.MAX_VALUE // No timeout
    );
    Operator<ScanResultValue> op2 = new ScanEngineOperator(
        fragment,
        mockCursor(20),
        10, // Batch size
        false, // Not legacy
        null, // No columns AKA "wildcard"
        ScanEngineOperator.Order.NONE,
        15, // Limit in second scan
        ResultFormat.RESULT_FORMAT_COMPACTED_LIST,
        Long.MAX_VALUE // No timeout
    );
    ConcatOperator<ScanResultValue> concat = new ConcatOperator<>(
        fragment,
        Arrays.asList(op1, op2)
    );
    fragment.registerRoot(concat);
    List<ScanResultValue> results = fragment.toList();
    assertEquals(2, results.size());
    assertEquals(10, results.get(0).getRows().size());
    assertEquals(5, results.get(1).getRows().size());
  }

  /**
   * Test an overall limit on ordered results, but each scan applies
   * the entire order separately. A merge, not shown here, would create
   * the final list that can be limited.
   */
  @Test
  public void testLocalLimitWhenOrdered()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    Operator<ScanResultValue> op1 = new ScanEngineOperator(
        fragment,
        mockCursor(20),
        10, // Batch size
        false, // Not legacy
        null, // No columns AKA "wildcard"
        ScanEngineOperator.Order.ASCENDING,
        25, // Limit in second scan
        ResultFormat.RESULT_FORMAT_COMPACTED_LIST,
        Long.MAX_VALUE // No timeout
    );
    Operator<ScanResultValue> op2 = new ScanEngineOperator(
        fragment,
        mockCursor(20),
        10, // Batch size
        false, // Not legacy
        null, // No columns AKA "wildcard"
        ScanEngineOperator.Order.ASCENDING,
        25, // Limit in second scan
        ResultFormat.RESULT_FORMAT_COMPACTED_LIST,
        Long.MAX_VALUE // No timeout
    );
    ConcatOperator<ScanResultValue> concat = new ConcatOperator<>(
        fragment,
        Arrays.asList(op1, op2)
    );
    fragment.registerRoot(concat);
    List<ScanResultValue> results = fragment.toList();
    assertEquals(4, results.size());
    assertEquals(10, results.get(0).getRows().size());
    assertEquals(10, results.get(1).getRows().size());
    assertEquals(10, results.get(2).getRows().size());
    assertEquals(10, results.get(3).getRows().size());
  }
}
