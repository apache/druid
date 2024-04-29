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
import com.google.common.collect.ImmutableMap;
import org.apache.druid.guice.NestedDataModule;
import org.apache.druid.query.IterableRowsCursorHelper;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.GroupByColumnSelectorStrategyFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.nested.StructuredData;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Serves as tests for class {@link DictionaryBuildingGroupByColumnSelectorStrategy} when a complex type is specified
 */
public class NestedColumnGroupByColumnSelectorStrategyTest extends InitializedNullHandlingTest
{
  static {
    NestedDataModule.registerHandlersAndSerde();
  }

  private static final GroupByColumnSelectorStrategyFactory STRATEGY_FACTORY = new GroupByColumnSelectorStrategyFactory();

  // No datasource would exist like this, however the inline datasource is an easy way to create the required column value selectors
  private static final List<Object[]> DATASOURCE_ROWS = ImmutableList.of(
      new Object[]{StructuredData.wrap(ImmutableList.of("x", "y", "z"))},
      new Object[]{StructuredData.wrap(ImmutableMap.of("x", 1.1, "y", 2L))},
      new Object[]{null},
      new Object[]{StructuredData.wrap("hello")}
  );

  // Dictionary ids alloted to each object, in the column-0 of the DATASOURCE_ROWS, when building from scratch.
  // null's dictionary id would be -1
  private static final int[] DICT_IDS = new int[]{0, 1, -1, 2};

  private static final String NESTED_COLUMN = "nested";
  /**
   * Row with null value in the column
   */
  private static final int NULL_ROW_NUMBER = 2;
  private static final ByteBuffer BUFFER1 = ByteBuffer.allocate(10);
  private static final ByteBuffer BUFFER2 = ByteBuffer.allocate(10);

  @Test
  public void testKeySize()
  {
    Assert.assertEquals(Integer.BYTES, createStrategy().getGroupingKeySizeBytes());
  }

  @Test
  public void testInitColumnValues()
  {
    GroupByColumnSelectorStrategy strategy = createStrategy();
    Cursor cursor = createCursor();
    ColumnValueSelector columnValueSelector = cursor.getColumnSelectorFactory().makeColumnValueSelector(NESTED_COLUMN);
    GroupByColumnSelectorPlus groupByColumnSelectorPlus = Mockito.mock(GroupByColumnSelectorPlus.class);
    Mockito.when(groupByColumnSelectorPlus.getResultRowPosition()).thenReturn(0);
    Object[] valuess = new Object[1];
    int rowNum = 0;
    while (!cursor.isDone()) {
      int sz = strategy.initColumnValues(columnValueSelector, 0, valuess);
      // While adding the values for the first time, the initialisation should have a non-zero footprint, apart from the
      // row with the null value
      if (DATASOURCE_ROWS.get(rowNum)[0] == null) {
        Assert.assertEquals(0, sz);
      } else {
        Assert.assertTrue(sz > 0);
      }
      Assert.assertEquals(DICT_IDS[rowNum], valuess[0]);

      cursor.advance();
      ++rowNum;
    }

    cursor = createCursor();
    columnValueSelector = cursor.getColumnSelectorFactory().makeColumnValueSelector(NESTED_COLUMN);
    rowNum = 0;
    while (!cursor.isDone()) {
      int sz = strategy.initColumnValues(columnValueSelector, 0, valuess);
      // While adding the values for the first time, the initialisation should have a non-zero footprint
      Assert.assertEquals(0, sz);
      Assert.assertEquals(DICT_IDS[rowNum], valuess[0]);

      cursor.advance();
      ++rowNum;
    }
  }

  @Test
  public void testWriteToKeyBuffer()
  {
    GroupByColumnSelectorStrategy strategy = createStrategy();
    ResultRow resultRow = ResultRow.create(1);
    GroupByColumnSelectorPlus groupByColumnSelectorPlus = Mockito.mock(GroupByColumnSelectorPlus.class);
    Mockito.when(groupByColumnSelectorPlus.getResultRowPosition()).thenReturn(0);
    Cursor cursor = createCursor();
    ColumnValueSelector columnValueSelector = cursor.getColumnSelectorFactory().makeColumnValueSelector(NESTED_COLUMN);

    int rowNum = 0;
    while (!cursor.isDone()) {
      int sz = strategy.writeToKeyBuffer(0, columnValueSelector, BUFFER1);
      if (DATASOURCE_ROWS.get(rowNum)[0] == null) {
        Assert.assertEquals(0, sz);
      } else {
        Assert.assertTrue(sz > 0);
      }
      // null is represented by GROUP_BY_MISSING_VALUE on the buffer, even though it gets its own dictionaryId in the dictionary
      Assert.assertEquals(DICT_IDS[rowNum], BUFFER1.getInt(0));
      // Readback the value
      strategy.processValueFromGroupingKey(groupByColumnSelectorPlus, BUFFER1, resultRow, 0);
      Assert.assertEquals(DATASOURCE_ROWS.get(rowNum)[0], resultRow.get(0));

      cursor.advance();
      ++rowNum;
    }
  }

  @Test
  public void testInitGroupingKeyColumnValue()
  {
    GroupByColumnSelectorStrategy strategy = createStrategy();
    GroupByColumnSelectorPlus groupByColumnSelectorPlus = Mockito.mock(GroupByColumnSelectorPlus.class);
    Mockito.when(groupByColumnSelectorPlus.getResultRowPosition()).thenReturn(0);
    int[] stack = new int[1];
    ResultRow resultRow = ResultRow.create(1);

    // Test nulls
    strategy.initGroupingKeyColumnValue(0, 0, -1, BUFFER1, stack);
    Assert.assertEquals(0, stack[0]);
    strategy.processValueFromGroupingKey(groupByColumnSelectorPlus, BUFFER1, resultRow, 0);
    Assert.assertNull(resultRow.get(0));
  }

  // test reset works fine

  private static GroupByColumnSelectorStrategy createStrategy()
  {
    return STRATEGY_FACTORY.makeColumnSelectorStrategy(
        createCursor().getColumnSelectorFactory().getColumnCapabilities(NESTED_COLUMN),
        createCursor().getColumnSelectorFactory().makeColumnValueSelector(NESTED_COLUMN),
        "dimension"
    );
  }


  private static Cursor createCursor()
  {
    return IterableRowsCursorHelper.getCursorFromIterable(
        DATASOURCE_ROWS,
        RowSignature.builder()
                    .add(NESTED_COLUMN, ColumnType.NESTED_DATA)
                    .build()
    ).lhs;
  }
}
