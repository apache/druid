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

package org.apache.druid.query.rowsandcols;

import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.query.rowsandcols.concrete.ColumnBasedFrameRowsAndColumns;
import org.apache.druid.query.rowsandcols.concrete.ColumnBasedFrameRowsAndColumnsTest;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.function.Function;

public class CursorFactoryRowsAndColumnsTest extends RowsAndColumnsTestBase
{
  public CursorFactoryRowsAndColumnsTest()
  {
    super(CursorFactoryRowsAndColumns.class);
  }

  public static Function<MapOfColumnsRowsAndColumns, CursorFactoryRowsAndColumns> MAKER =
      CursorFactoryRowsAndColumnsTest::buildFrame;

  /// Exceeding a single frame's capacity must fail instead of returning a truncated row count.
  @Test
  public void testMaterializationFailsWhenFrameIsFull()
  {
    final CursorFactoryRowsAndColumns rows = smallMaterializer(32);
    final ResourceLimitExceededException exception = Assertions.assertThrows(
        ResourceLimitExceededException.class,
        rows::numRows
    );
    Assertions.assertTrue(exception.getMessage().contains("4,096"));
  }

  /// Rows that fit in the same bounded allocator are all retained with their original values.
  @Test
  public void testMaterializationWithinFrameCapacity()
  {
    final CursorFactoryRowsAndColumns rows = smallMaterializer(2);
    Assertions.assertEquals(2, rows.numRows());
    Assertions.assertEquals("x".repeat(512), rows.findColumn("value").toAccessor().getObject(1));
  }

  private static CursorFactoryRowsAndColumns smallMaterializer(final int numRows)
  {
    final RowSignature signature = RowSignature.builder()
                                               .add("__time", ColumnType.LONG)
                                               .add("value", ColumnType.STRING)
                                               .build();
    final Object[][] values = new Object[numRows][];
    for (int i = 0; i < numRows; i++) {
      values[i] = new Object[]{(long) i, "x".repeat(512)};
    }
    try (final ColumnBasedFrameRowsAndColumns frame = ColumnBasedFrameRowsAndColumnsTest.buildFrame(
        MapOfColumnsRowsAndColumns.fromRowObjects(values, signature)
    )) {
      return new CursorFactoryRowsAndColumns(frame.as(CursorFactory.class), 4_096);
    }
  }

  private static CursorFactoryRowsAndColumns buildFrame(MapOfColumnsRowsAndColumns input)
  {
    try (ColumnBasedFrameRowsAndColumns fRAC = ColumnBasedFrameRowsAndColumnsTest.buildFrame(input)) {
      return new CursorFactoryRowsAndColumns(fRAC.as(CursorFactory.class));
    }
  }
}
