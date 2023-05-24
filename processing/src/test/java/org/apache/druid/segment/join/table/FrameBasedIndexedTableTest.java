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

package org.apache.druid.segment.join.table;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.allocation.HeapMemoryAllocator;
import org.apache.druid.frame.allocation.SingleMemoryAllocatorFactory;
import org.apache.druid.frame.segment.FrameCursorUtils;
import org.apache.druid.frame.write.FrameWriterFactory;
import org.apache.druid.frame.write.FrameWriters;
import org.apache.druid.query.FrameBasedInlineDataSource;
import org.apache.druid.query.FrameSignaturePair;
import org.apache.druid.query.IterableRowsCursorHelper;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class FrameBasedIndexedTableTest extends InitializedNullHandlingTest
{
  private static final String STRING_COL_1 = "market";
  private static final String LONG_COL_1 = "longNumericNull";
  private static final String DOUBLE_COL_1 = "doubleNumericNull";
  private static final String FLOAT_COL_1 = "floatNumericNull";
  private static final String STRING_COL_2 = "market";
  private static final String MULTI_VALUE_COLUMN = "placementish";
  private static final String DIM_NOT_EXISTS = "DIM_NOT_EXISTS";

  private static final List<Object[]> DATASOURCE_ROWS =
      ImmutableList.<Object[]>builder()
                   .add(new Object[]{"spot", 1L, 1.1d, 3.1f, "preferred", new Object[]{"val1", "val2"}})
                   .add(new Object[]{"spot", 1L, 1.1d, 3.1f, "preferred", new Object[]{"val1", "val2"}})
                   .add(new Object[]{"spot", 2L, 1.3d, 3.1f, "preferred", new Object[]{"val1", "val2"}})
                   .add(new Object[]{"spot", 1L, 1.1d, 3.1f, "preferred", new Object[]{"val1", "val2"}})
                   .add(new Object[]{"spot", 1L, 1.1d, 3.1f, "preferred", new Object[]{"val1", "val2"}})
                   .add(new Object[]{"spot", 1L, 1.5d, 3.1f, "preferred", new Object[]{"val1", "val2"}})
                   .add(new Object[]{"spot", 4L, 1.1d, 3.1f, "preferred", new Object[]{"val1", "val2"}})
                   .add(new Object[]{"spot", 1L, 1.7d, 3.1f, "preferred", new Object[]{"val1", "val2"}})
                   .add(new Object[]{"spot", 5L, 1.1d, 3.1f, "preferred", new Object[]{"val1", "val2"}})
                   .build();

  private static final RowSignature ROW_SIGNATURE =
      RowSignature.builder()
                  .add(STRING_COL_1, ColumnType.STRING)
                  .add(LONG_COL_1, ColumnType.LONG)
                  .add(DOUBLE_COL_1, ColumnType.DOUBLE)
                  .add(FLOAT_COL_1, ColumnType.FLOAT)
                  .add(STRING_COL_1, ColumnType.STRING)
                  .add(MULTI_VALUE_COLUMN, ColumnType.STRING_ARRAY)
                  .build();

  private static final Set<String> KEY_COLUMNS = ImmutableSet.<String>builder()
                                                             .add(STRING_COL_1)
                                                             .add(LONG_COL_1)
                                                             .add(DOUBLE_COL_1)
                                                             .add(FLOAT_COL_1)
                                                             .add(MULTI_VALUE_COLUMN)
                                                             .add(DIM_NOT_EXISTS)
                                                             .build();


  private FrameBasedInlineDataSource dataSource;
  private FrameBasedIndexedTable frameBasedIndexedTable;

  @Before
  public void setup()
  {
    Cursor cursor = IterableRowsCursorHelper.getCursorFromIterable(DATASOURCE_ROWS, ROW_SIGNATURE);
    FrameWriterFactory frameWriterFactory = FrameWriters.makeFrameWriterFactory(
        FrameType.COLUMNAR,
        new SingleMemoryAllocatorFactory(HeapMemoryAllocator.unlimited()),
        ROW_SIGNATURE,
        new ArrayList<>()
    );
    Frame frame = Iterables.getOnlyElement(FrameCursorUtils.cursorToFrames(cursor, frameWriterFactory).toList());

    dataSource = new FrameBasedInlineDataSource(
        ImmutableList.of(new FrameSignaturePair(frame, ROW_SIGNATURE)),
        ROW_SIGNATURE
    );

    frameBasedIndexedTable = new FrameBasedIndexedTable(dataSource, KEY_COLUMNS, "test");

  }

  @Test
  public void testInitShouldGenerateCorrectTable()
  {
    Assert.assertEquals(9, frameBasedIndexedTable.numRows());
  }

  @Test
  public void testStringKeyColumn()
  {
    checkIndexAndReader(STRING_COL_1);
  }

  @Test
  public void testLongKeyColumn()
  {
    checkIndexAndReader(LONG_COL_1);
  }

  @Test
  public void testFloatKeyColumn()
  {
    checkIndexAndReader(FLOAT_COL_1);
  }

  @Test
  public void testDoubleKeyColumn()
  {
    checkIndexAndReader(DOUBLE_COL_1);
  }

  private void checkIndexAndReader(String columnName)
  {
    checkColumnReader(columnName);
  }

  private void checkColumnReader(String columnName)
  {
    int numRows = DATASOURCE_ROWS.size();

    int columnNumber = ROW_SIGNATURE.indexOf(columnName);
    IndexedTable.Reader reader = frameBasedIndexedTable.columnReader(columnNumber);
    for (int i = 0; i < numRows; ++i) {
      Assert.assertEquals(DATASOURCE_ROWS.get(i)[columnNumber], reader.read(i));
    }
  }
}
