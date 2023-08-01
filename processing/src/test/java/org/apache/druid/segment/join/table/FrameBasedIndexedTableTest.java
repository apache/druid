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
import it.unimi.dsi.fastutil.ints.IntBidirectionalIterator;
import it.unimi.dsi.fastutil.ints.IntSortedSet;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.allocation.HeapMemoryAllocator;
import org.apache.druid.frame.allocation.SingleMemoryAllocatorFactory;
import org.apache.druid.frame.segment.FrameCursorUtils;
import org.apache.druid.frame.write.FrameWriterFactory;
import org.apache.druid.frame.write.FrameWriters;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class FrameBasedIndexedTableTest extends InitializedNullHandlingTest
{
  private static final String STRING_COL_1 = "market";
  private static final String LONG_COL_1 = "longNumericNull";
  private static final String DOUBLE_COL_1 = "doubleNumericNull";
  private static final String FLOAT_COL_1 = "floatNumericNull";
  private static final String STRING_COL_2 = "partial_null_column";
  private static final String MULTI_VALUE_COLUMN = "placementish";
  private static final String NON_INDEXED_STRING_COL = "nonIndexedString";
  private static final String NON_INDEXED_LONG_COL = "nonIndexedNumeric";
  private static final String NON_INDEXED_DOUBLE_COL = "nonIndexedDouble";
  private static final String NON_INDEXED_FLOAT_COL = "nonIndexedFloat";
  private static final String DIM_NOT_EXISTS = "DIM_NOT_EXISTS";

  private static final List<Object[]> DATASOURCE_ROWS =
      ImmutableList.<Object[]>builder()
                   .add(
                       new Object[]{
                           "spot",
                           1L,
                           null,
                           3.1f,
                           "preferred",
                           new Object[]{"val1", "val2"},
                           "spot",
                           1L,
                           null,
                           3.1f
                       })
                   .add(new Object[]{
                       "total_market",
                       1L,
                       1.2d,
                       3.2f,
                       null,
                       new Object[]{"val1", "val2"},
                       "total_market",
                       1L,
                       1.2d,
                       3.2f
                   })
                   .add(new Object[]{
                       "spot",
                       2L,
                       1.3d,
                       3.1f,
                       "preferred",
                       new Object[]{"val1", "val2"},
                       "spot",
                       2L,
                       1.3d,
                       3.1f
                   })
                   .add(new Object[]{
                       "upfront",
                       1L,
                       1.5d,
                       3.5f,
                       "preferred",
                       new Object[]{"val1", "val2"},
                       "upfront",
                       1L,
                       1.5d,
                       3.5f
                   })
                   .add(new Object[]{
                       "total_market",
                       null,
                       1.1d,
                       3.1f,
                       null,
                       new Object[]{"val1", "val2"},
                       "total_market",
                       null,
                       1.1d,
                       3.1f
                   })
                   .add(new Object[]{
                       "upfront",
                       2L,
                       1.5d,
                       null,
                       "preferred",
                       new Object[]{"val1", "val2"},
                       "upfront",
                       2L,
                       1.5d,
                       null
                   })
                   .add(new Object[]{
                       "upfront",
                       4L,
                       1.1d,
                       3.9f,
                       "preferred",
                       new Object[]{"val1", "val2"},
                       "upfront",
                       4L,
                       1.1d,
                       3.9f
                   })
                   .add(new Object[]{
                       "total_market",
                       1L,
                       1.7d,
                       3.8f,
                       "preferred",
                       new Object[]{"val1", "val2"},
                       "total_market",
                       1L,
                       1.7d,
                       3.8f
                   })
                   .add(new Object[]{
                       "spot",
                       5L,
                       1.8d,
                       3.1f,
                       null,
                       new Object[]{"val1", "val2"},
                       "spot",
                       5L,
                       1.8d,
                       3.1f
                   })
                   .build();

  private static final RowSignature ROW_SIGNATURE =
      RowSignature.builder()
                  .add(STRING_COL_1, ColumnType.STRING)
                  .add(LONG_COL_1, ColumnType.LONG)
                  .add(DOUBLE_COL_1, ColumnType.DOUBLE)
                  .add(FLOAT_COL_1, ColumnType.FLOAT)
                  .add(STRING_COL_2, ColumnType.STRING)
                  .add(MULTI_VALUE_COLUMN, ColumnType.STRING_ARRAY)
                  .add(NON_INDEXED_STRING_COL, ColumnType.STRING)
                  .add(NON_INDEXED_LONG_COL, ColumnType.LONG)
                  .add(NON_INDEXED_DOUBLE_COL, ColumnType.DOUBLE)
                  .add(NON_INDEXED_FLOAT_COL, ColumnType.FLOAT)
                  .build();

  private static final Set<String> KEY_COLUMNS = ImmutableSet.<String>builder()
                                                             .add(STRING_COL_1)
                                                             .add(STRING_COL_2)
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
    final String[] vals = new String[]{"spot", "total_market", "upfront"};
    checkIndexAndReader(STRING_COL_1, vals);
  }

  @Test
  public void testNullableStringKeyColumn()
  {
    final String[] vals = new String[]{null, "preferred"};
    checkIndexAndReader(STRING_COL_2, vals);
  }

  @Test
  public void testMultiValueStringKeyColumn()
  {
    final Object[] nonMatchingVals = new Object[]{ImmutableList.of("a", "preferred")};
    checkIndexAndReader(MULTI_VALUE_COLUMN, new Object[0], nonMatchingVals);
  }

  @Test
  public void testLongKeyColumn()
  {
    final Long[] vals = new Long[]{NullHandling.replaceWithDefault() ? 0L : null, 1L, 2L, 4L, 5L};
    checkIndexAndReader(LONG_COL_1, vals);
  }

  @Test
  public void testFloatKeyColumn()
  {
    final Float[] vals = new Float[]{NullHandling.replaceWithDefault() ? 0.0f : null, 3.1f, 3.2f, 3.5f, 3.8f, 3.9f};
    checkIndexAndReader(FLOAT_COL_1, vals);
  }

  @Test
  public void testDoubleKeyColumn()
  {
    final Double[] vals = new Double[]{
        NullHandling.replaceWithDefault() ? 0.0 : null, 1.1d, 1.2d, 1.3d, 1.5d, 1.7d, 1.8d
    };
    checkIndexAndReader(DOUBLE_COL_1, vals);
  }

  @Test
  public void testStringNonKeyColumn()
  {
    checkNonIndexedReader(NON_INDEXED_STRING_COL);
  }

  @Test
  public void testLongNonKeyColumn()
  {
    checkNonIndexedReader(NON_INDEXED_LONG_COL);
  }

  @Test
  public void testFloatNonKeyColumn()
  {
    checkNonIndexedReader(NON_INDEXED_FLOAT_COL);
  }

  @Test
  public void testDoubleNonKeyColumn()
  {
    checkNonIndexedReader(NON_INDEXED_DOUBLE_COL);
  }

  @Test
  public void testIsCacheable()
  {
    Assert.assertFalse(frameBasedIndexedTable.isCacheable());
  }

  private void checkIndexAndReader(String columnName, Object[] vals)
  {
    checkIndexAndReader(columnName, vals, new Object[0]);
  }

  private void checkIndexAndReader(String columnName, Object[] vals, Object[] nonmatchingVals)
  {
    checkColumnReader(columnName);
    try (final Closer closer = Closer.create()) {
      final int columnIndex = ROW_SIGNATURE.indexOf(columnName);
      final IndexedTable.Reader reader = frameBasedIndexedTable.columnReader(columnIndex);
      closer.register(reader);
      final IndexedTable.Index valueIndex = frameBasedIndexedTable.columnIndex(columnIndex);

      for (Object val : vals) {
        final IntSortedSet valIndex = valueIndex.find(val);
        if (val == null) {
          Assert.assertEquals(0, valIndex.size());
        } else {
          Assert.assertTrue(valIndex.size() > 0);
          final IntBidirectionalIterator rowIterator = valIndex.iterator();
          while (rowIterator.hasNext()) {
            Assert.assertEquals(val, reader.read(rowIterator.nextInt()));
          }
        }
      }
      for (Object val : nonmatchingVals) {
        final IntSortedSet valIndex = valueIndex.find(val);
        Assert.assertEquals(0, valIndex.size());
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void checkNonIndexedReader(String columnName)
  {
    // make sure it doesn't have an index since it isn't a key column
    checkColumnReader(columnName);
    int columnIndex = ROW_SIGNATURE.indexOf(columnName);
    try {
      Assert.assertNull(frameBasedIndexedTable.columnIndex(columnIndex));
    }
    catch (IAE iae) {
      Assert.assertEquals(StringUtils.format("Column[%d] is not a key column", columnIndex), iae.getMessage());
    }
  }


  private void checkColumnReader(String columnName)
  {
    int numRows = DATASOURCE_ROWS.size();

    int columnNumber = ROW_SIGNATURE.indexOf(columnName);
    IndexedTable.Reader reader = frameBasedIndexedTable.columnReader(columnNumber);
    List<Object[]> originalRows = dataSource.getRowsAsSequence().toList();
    for (int i = 0; i < numRows; ++i) {
      final Object originalValue = originalRows.get(i)[columnNumber];
      final Object actualValue = reader.read(i);

      if (!Objects.deepEquals(originalValue, actualValue)) {
        // Call Assert.assertEquals, which we expect to fail, to get a nice failure message
        Assert.assertEquals(
            originalValue instanceof Object[] ? Arrays.toString((Object[]) originalValue) : originalValue,
            actualValue instanceof Object[] ? Arrays.toString((Object[]) actualValue) : actualValue
        );
      }
    }
  }
}
