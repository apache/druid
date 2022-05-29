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

import com.google.common.base.Strings;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.scan.ScanQuery.ResultFormat;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.fragment.FragmentManager;
import org.apache.druid.queryng.fragment.Fragments;
import org.apache.druid.queryng.operators.Iterators;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.OperatorTest;
import org.apache.druid.queryng.operators.OperatorTests;
import org.apache.druid.queryng.operators.ResultIterator;
import org.apache.druid.segment.column.ColumnHolder;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Category(OperatorTest.class)
public class ScanQueryOperatorsTest
{
  private MockScanResultReader scan(
      FragmentContext context,
      int columnCount,
      int rowCount,
      int batchSize)
  {
    return new MockScanResultReader(
        context,
        columnCount,
        rowCount,
        batchSize,
        MockScanResultReader.interval(0));
  }

  private MockScanResultReader scan(
      FragmentContext context,
      int columnCount,
      int rowCount,
      int batchSize,
      ResultFormat rowFormat)
  {
    return new MockScanResultReader(
        context,
        columnCount,
        rowCount,
        batchSize,
        MockScanResultReader.interval(0),
        rowFormat);
  }

  // Tests for the mock reader used to power tests without the overhead
  // of using an actual scan operator. The parent operators don't know
  // the difference.
  @Test
  public void testMockReaderNull()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    Operator<ScanResultValue> op = scan(fragment, 0, 0, 3);
    fragment.registerRoot(op);
    ResultIterator<ScanResultValue> iter = fragment.run();
    OperatorTests.assertEof(iter);
    fragment.close();
  }

  @Test
  public void testMockReaderEmpty() throws ResultIterator.EofException
  {
    FragmentManager fragment = Fragments.defaultFragment();
    MockScanResultReader scan = scan(fragment, 0, 1, 3);
    assertFalse(Strings.isNullOrEmpty(scan.segmentId));
    fragment.registerRoot(scan);
    ResultIterator<ScanResultValue> iter = fragment.run();
    ScanResultValue value = iter.next();
    assertTrue(value.getColumns().isEmpty());
    List<List<String>> events = value.getRows();
    assertEquals(1, events.size());
    assertTrue(events.get(0).isEmpty());
    OperatorTests.assertEof(iter);
    fragment.close();
  }

  @Test
  public void testMockReader()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    Operator<ScanResultValue> scan = scan(fragment, 3, 10, 4);
    fragment.registerRoot(scan);
    ResultIterator<ScanResultValue> iter = fragment.run();
    int rowCount = 0;
    for (ScanResultValue value : Iterators.toIterable(iter)) {
      assertEquals(3, value.getColumns().size());
      assertEquals(ColumnHolder.TIME_COLUMN_NAME, value.getColumns().get(0));
      assertEquals("Column1", value.getColumns().get(1));
      assertEquals("Column2", value.getColumns().get(2));
      List<List<Object>> events = value.getRows();
      assertTrue(events.size() <= 4);
      long prevTs = 0;
      for (List<Object> row : events) {
        for (int i = 0; i < 3; i++) {
          Object colValue = row.get(i);
          assertNotNull(row.get(i));
          if (i == 0) {
            assertTrue(colValue instanceof Long);
            long ts = (Long) colValue;
            assertTrue(ts > prevTs);
            prevTs = ts;
          } else {
            assertTrue(colValue instanceof String);
          }
        }
        rowCount++;
      }
    }
    assertEquals(10, rowCount);
    fragment.close();
  }

  /**
   * Test the offset operator for various numbers of input rows, spread across
   * multiple batches. Tests for offsets that fall on a batch boundary
   * and within a batch.
   */
  @Test
  public void testOffset()
  {
    final int totalRows = 10;
    for (int offset = 1; offset < 2 * totalRows; offset++) {
      FragmentManager fragment = Fragments.defaultFragment();
      Operator<ScanResultValue> scan = scan(fragment, 3, totalRows, 4);
      Operator<ScanResultValue> root =
          new ScanResultOffsetOperator(fragment, scan, offset);
      int rowCount = 0;
      final String firstVal = StringUtils.format("Value %d.1", offset);
      fragment.registerRoot(root);
      ResultIterator<ScanResultValue> iter = fragment.run();
      for (ScanResultValue row : Iterators.toIterable(iter)) {
        ScanResultValue value = row;
        List<List<Object>> events = value.getRows();
        if (rowCount == 0) {
          assertEquals(firstVal, events.get(0).get(1));
        }
        rowCount += events.size();
      }
      assertEquals(Math.max(0, totalRows - offset), rowCount);
      fragment.close();
    }
  }

  /**
   * Test the limit operator for various numbers of input rows, spread across
   * multiple batches. Tests for limits that fall on a batch boundary
   * and within a batch.
   */
  @Test
  public void testGroupedLimit()
  {
    final int totalRows = 10;
    for (int limit = 0; limit < totalRows + 1; limit++) {
      FragmentManager fragment = Fragments.defaultFragment();
      Operator<ScanResultValue> scan = scan(fragment, 3, totalRows, 4);
      Operator<ScanResultValue> root =
          new GroupedScanResultLimitOperator(fragment, scan, limit);
      fragment.registerRoot(root);
      ResultIterator<ScanResultValue> iter = fragment.run();
      int rowCount = 0;
      for (ScanResultValue row : Iterators.toIterable(iter)) {
        ScanResultValue value = row;
        rowCount += value.rowCount();
      }
      assertEquals(Math.min(totalRows, limit), rowCount);
      fragment.close();
    }
  }

  @Test
  public void testUngroupedLimit()
  {
    final int totalRows = 10;
    for (int limit = 0; limit < totalRows + 1; limit++) {
      FragmentManager fragment = Fragments.defaultFragment();
      Operator<ScanResultValue> scan = scan(fragment, 3, totalRows, 1);
      Operator<ScanResultValue> root =
          new UngroupedScanResultLimitOperator(fragment, scan, limit, 4);
      fragment.registerRoot(root);
      ResultIterator<ScanResultValue> iter = fragment.run();
      int rowCount = 0;
      for (ScanResultValue row : Iterators.toIterable(iter)) {
        ScanResultValue value = row;
        rowCount += value.rowCount();
      }
      assertEquals(Math.min(totalRows, limit), rowCount);
      fragment.close();
    }
  }

  @Test
  public void testBatchToRow()
  {
    {
      FragmentManager fragment = Fragments.defaultFragment();
      MockScanResultReader scan = scan(fragment, 3, 25, 4, ResultFormat.RESULT_FORMAT_COMPACTED_LIST);
      Operator<List<Object>> op =
          new ScanBatchToRowOperator<List<Object>>(fragment, scan);
      Operator<Object[]> root =
          new ScanCompactListToArrayOperator(fragment, op, scan.columns);
      fragment.registerRoot(root);
      List<Object[]> results = fragment.toList();
      assertEquals(25, results.size());
    }
    {
      FragmentManager fragment = Fragments.defaultFragment();
      MockScanResultReader scan = scan(fragment, 3, 25, 4, ResultFormat.RESULT_FORMAT_LIST);
      Operator<Map<String, Object>> op =
          new ScanBatchToRowOperator<Map<String, Object>>(fragment, scan);
      Operator<Object[]> root =
          new ScanListToArrayOperator(fragment, op, scan.columns);
      fragment.registerRoot(root);
      List<Object[]> results = fragment.toList();
      assertEquals(25, results.size());
    }
  }
}
