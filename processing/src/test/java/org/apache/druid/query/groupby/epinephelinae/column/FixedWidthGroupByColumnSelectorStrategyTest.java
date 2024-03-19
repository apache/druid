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

package org.apache.druid.query.groupby.epinephelinae.column;

import com.google.common.collect.ImmutableList;
import junitparams.converters.Nullable;
import org.apache.druid.query.IterableRowsCursorHelper;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.GroupByColumnSelectorStrategyFactory;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.List;

@RunWith(Enclosed.class)
public class FixedWidthGroupByColumnSelectorStrategyTest extends InitializedNullHandlingTest
{
  private static final List<Object[]> DATASOURCE_ROWS = ImmutableList.of(
      new Object[]{1L, 1.0f, 1.0d},
      new Object[]{2L, 2.0f, 2.0d},
      new Object[]{null, null, null},
      new Object[]{3L, 3.0f, 3.0d}
  );
  private static final GroupByColumnSelectorStrategyFactory STRATEGY_FACTORY = new GroupByColumnSelectorStrategyFactory();
  private static final ByteBuffer BUFFER1 = ByteBuffer.allocate(10);
  private static final ByteBuffer BUFFER2 = ByteBuffer.allocate(10);
  private static final String LONG_COLUMN = "long";
  private static final String FLOAT_COLUMN = "float";
  private static final String DOUBLE_COLUMN = "double";

  public static class LongGroupByColumnSelectorStrategyTest
  {
    private static final GroupByColumnSelectorStrategy STRATEGY =
        STRATEGY_FACTORY.makeColumnSelectorStrategy(
            createCursor().getColumnSelectorFactory().getColumnCapabilities(LONG_COLUMN),
            createCursor().getColumnSelectorFactory().makeColumnValueSelector(LONG_COLUMN)
        );

    @Test
    public void testKeySize()
    {
      Assert.assertEquals(Byte.BYTES + Long.BYTES, STRATEGY.getGroupingKeySizeBytes());
    }

    @Test
    public void testWriteToKeyBuffer()
    {
      Cursor cursor = createCursor();
      ResultRow resultRow = ResultRow.create(1);
      ColumnValueSelector columnValueSelector = cursor.getColumnSelectorFactory().makeColumnValueSelector(LONG_COLUMN);
      GroupByColumnSelectorPlus groupByColumnSelectorPlus = Mockito.mock(GroupByColumnSelectorPlus.class);
      Mockito.when(groupByColumnSelectorPlus.getResultRowPosition()).thenReturn(0);

      int rowNum = 0;
      while (!cursor.isDone()) {
        // Check if the round trip serde produces the same results
        int sizeIncrease = STRATEGY.writeToKeyBuffer(0, columnValueSelector, BUFFER1);
        STRATEGY.processValueFromGroupingKey(groupByColumnSelectorPlus, BUFFER1, resultRow, 0);
        // There shouldn't be any internal size increase associated with the fixed width types
        Assert.assertEquals(0, sizeIncrease);
        Assert.assertEquals(DATASOURCE_ROWS.get(rowNum)[0], resultRow.get(0));
        cursor.advance();
        ++rowNum;
      }
    }

    @Test
    public void testInitColumnValues()
    {
      Cursor cursor = createCursor();
      ColumnValueSelector columnValueSelector = cursor.getColumnSelectorFactory().makeColumnValueSelector(LONG_COLUMN);
      Object[] valuess = new Object[1];

      int rowNum = 0;
      while (!cursor.isDone()) {
        int sizeIncrease = STRATEGY.initColumnValues(columnValueSelector, 0, valuess);
        Assert.assertEquals(0, sizeIncrease);
        Assert.assertEquals(DATASOURCE_ROWS.get(rowNum)[0], valuess[0]);
        cursor.advance();
        ++rowNum;
      }
    }

    @Test
    public void testBufferComparator()
    {
      // lhs < rhs
      writeGroupingKeyToBuffer(BUFFER1, 100L);
      writeGroupingKeyToBuffer(BUFFER2, 200L);
      Assert.assertEquals(-1, STRATEGY.bufferComparator(0, null).compare(BUFFER1, BUFFER2, 0, 0));

      // lhs == rhs
      writeGroupingKeyToBuffer(BUFFER1, 100L);
      writeGroupingKeyToBuffer(BUFFER2, 100L);
      Assert.assertEquals(0, STRATEGY.bufferComparator(0, null).compare(BUFFER1, BUFFER2, 0, 0));

      // lhs > rhs
      writeGroupingKeyToBuffer(BUFFER1, 200L);
      writeGroupingKeyToBuffer(BUFFER2, 100L);
      Assert.assertEquals(1, STRATEGY.bufferComparator(0, null).compare(BUFFER1, BUFFER2, 0, 0));

      // lhs is null
      writeGroupingKeyToBuffer(BUFFER1, null);
      writeGroupingKeyToBuffer(BUFFER2, 0L);
      Assert.assertEquals(-1, STRATEGY.bufferComparator(0, null).compare(BUFFER1, BUFFER2, 0, 0));

      // rhs is null
      writeGroupingKeyToBuffer(BUFFER1, 0L);
      writeGroupingKeyToBuffer(BUFFER2, null);
      Assert.assertEquals(1, STRATEGY.bufferComparator(0, null).compare(BUFFER1, BUFFER2, 0, 0));

      // lhs and rhs are null
      writeGroupingKeyToBuffer(BUFFER1, null);
      writeGroupingKeyToBuffer(BUFFER2, null);
      Assert.assertEquals(0, STRATEGY.bufferComparator(0, null).compare(BUFFER1, BUFFER2, 0, 0));

      // stringComparator is provided, for lexicographic comparator "2" > "100"
      writeGroupingKeyToBuffer(BUFFER1, 2L);
      writeGroupingKeyToBuffer(BUFFER2, 100L);
      Assert.assertEquals(
          1,
          STRATEGY.bufferComparator(0, StringComparators.LEXICOGRAPHIC)
                  .compare(BUFFER1, BUFFER2, 0, 0)
      );

      // stringComparator is provided, for alphanumeric comparator number("2") < number("100")
      writeGroupingKeyToBuffer(BUFFER1, 2L);
      writeGroupingKeyToBuffer(BUFFER2, 100L);
      Assert.assertEquals(
          -1,
          STRATEGY.bufferComparator(0, StringComparators.ALPHANUMERIC)
                  .compare(BUFFER1, BUFFER2, 0, 0)
      );
    }

    private static void writeGroupingKeyToBuffer(final ByteBuffer buffer, @Nullable Long key)
    {
      ColumnValueSelector columnValueSelector1 = Mockito.mock(ColumnValueSelector.class);

      Mockito.when(columnValueSelector1.getObject()).thenReturn(key);
      Mockito.when(columnValueSelector1.isNull()).thenReturn(key == null);

      Assert.assertEquals(0, STRATEGY.writeToKeyBuffer(0, columnValueSelector1, buffer));
    }

    @Test
    public void testMultiValueHandling()
    {
      // Returns false, because fixed width strategy doesn't handle multi-value dimensions, therefore even index-0 is
      // flagged
      Assert.assertFalse(STRATEGY.checkRowIndexAndAddValueToGroupingKey(0, 1L, 0, BUFFER1));
      Assert.assertFalse(STRATEGY.checkRowIndexAndAddValueToGroupingKey(0, 1L, 10, BUFFER1));



    }
  }

  private static Cursor createCursor()
  {
    Cursor cursor = IterableRowsCursorHelper.getCursorFromIterable(
        DATASOURCE_ROWS,
        RowSignature.builder()
                    .add("long", ColumnType.LONG)
                    .add("float", ColumnType.FLOAT)
                    .add("double", ColumnType.DOUBLE)
                    .build()
    ).lhs;
    return cursor;
  }
}