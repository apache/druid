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
import org.apache.druid.query.IterableRowsCursorHelper;
import org.apache.druid.query.groupby.epinephelinae.GroupByColumnSelectorStrategyFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.nested.StructuredData;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Serves as tests for class {@link DictionaryBuildingGroupByColumnSelectorStrategy} when a complex type is specified
 */
public class NestedColumnGroupByColumnSelectorStrategyTest extends InitializedNullHandlingTest
{
  private static final GroupByColumnSelectorStrategyFactory STRATEGY_FACTORY = new GroupByColumnSelectorStrategyFactory();

  // No datasource would exist like this, however the inline datasource is an easy way to create the required column value selectors
  private static final List<Object[]> DATASOURCE_ROWS = ImmutableList.of(
      new Object[]{StructuredData.wrap(ImmutableList.of("x", "y", "z"))},
      new Object[]{StructuredData.wrap(ImmutableMap.of("x", 1.1, "y", 2L))},
      new Object[]{null},
      new Object[]{StructuredData.wrap("hello")}
  );

  private static final String NESTED_COLUMN = "nested";
  /**
   * Row with null value in the column
   */
  private static final int NULL_ROW_NUMBER = 2;
  private static final ByteBuffer BUFFER1 = ByteBuffer.allocate(10);
  private static final ByteBuffer BUFFER2 = ByteBuffer.allocate(10);

  @Test
  public void testInitColumnValues()
  {
    final GroupByColumnSelectorStrategy strategy = createStrategy();

    Cursor cursor = createCursor();
    ColumnValueSelector columnValueSelector = cursor.getColumnSelectorFactory().makeColumnValueSelector(NESTED_COLUMN);
    Object[] valuess = new Object[1];
    int rowNum = 0;
    while (!cursor.isDone()) {
      int sz = strategy.initColumnValues(columnValueSelector, 0, valuess);
      // While adding the values for the first time, the initialisation should have a non-zero footprint
      Assert.assertTrue(sz > 0);
      Assert.assertEquals(DATASOURCE_ROWS.get(rowNum)[0], valuess[0]);

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
      Assert.assertEquals(DATASOURCE_ROWS.get(rowNum)[0], valuess[0]);

      cursor.advance();
      ++rowNum;
    }
  }

  @Test
  public void testWriteToKeyBuffer()
  {
    final GroupByColumnSelectorStrategy strategy = createStrategy();

    Cursor cursor = createCursor();
    ColumnValueSelector columnValueSelector = cursor.getColumnSelectorFactory().makeColumnValueSelector(NESTED_COLUMN);

    int rowNum = 0;
    while (!cursor.isDone()) {
      int sz = strategy.writeToKeyBuffer(0, columnValueSelector, BUFFER1);
      Assert.assertTrue(sz > 0);
      // null is represented by GROUP_BY_MISSING_VALUE on the buffer, even though it gets its own dictionaryId in the dictionary
      Assert.assertEquals(rowNum == NULL_ROW_NUMBER ? -1 : rowNum, BUFFER1.getInt(0));
      cursor.advance();
      ++rowNum;
    }
  }

  @Test
  public void testKeySize()
  {
    Assert.assertEquals(Integer.BYTES, createStrategy().getGroupingKeySizeBytes());
  }

  private static GroupByColumnSelectorStrategy createStrategy()
  {
    return STRATEGY_FACTORY.makeColumnSelectorStrategy(
        createCursor().getColumnSelectorFactory().getColumnCapabilities(NESTED_COLUMN),
        createCursor().getColumnSelectorFactory().makeColumnValueSelector(NESTED_COLUMN)
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