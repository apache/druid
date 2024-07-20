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

package org.apache.druid.query.rowsandcols.semantic;

import org.apache.druid.frame.Frame;
import org.apache.druid.query.rowsandcols.ArrayListRowsAndColumnsTest;
import org.apache.druid.query.rowsandcols.MapOfColumnsRowsAndColumns;
import org.apache.druid.query.rowsandcols.column.ColumnAccessor;
import org.apache.druid.query.rowsandcols.concrete.ColumnBasedFrameRowsAndColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Assert;
import org.junit.Test;

public class FrameMakerTest
{
  public static RowSignature ROW_SIGNATURE = RowSignature.builder()
                                                         .add("dim1", ColumnType.STRING)
                                                         .add("dim2", ColumnType.STRING)
                                                         .add("dim3", ColumnType.STRING)
                                                         .add("m1", ColumnType.LONG)
                                                         .add("m2", ColumnType.LONG)
                                                         .build();

  @Test
  public void testFrameMaker()
  {
    final MapOfColumnsRowsAndColumns mapOfColumnsRowsAndColumns = MapOfColumnsRowsAndColumns
        .builder()
        .add("dim1", ColumnType.STRING, "a", "b", "c")
        .add("dim2", ColumnType.STRING, "m", "d", "e")
        .add("dim3", ColumnType.STRING, "a")
        .add("m1", ColumnType.LONG, 1L, 2L, 3L)
        .add("m2", ColumnType.LONG, 52L, 42L)
        .build();

    final FrameMaker frameMaker = FrameMaker.fromRAC(ArrayListRowsAndColumnsTest.MAKER.apply(mapOfColumnsRowsAndColumns));

    Assert.assertEquals(ROW_SIGNATURE, frameMaker.computeSignature());

    final Frame frame = frameMaker.toColumnBasedFrame();
    ColumnBasedFrameRowsAndColumns columnBasedFrameRowsAndColumns = new ColumnBasedFrameRowsAndColumns(
        frame,
        frameMaker.computeSignature()
    );
    for (String columnName : mapOfColumnsRowsAndColumns.getColumnNames()) {
      ColumnAccessor expectedColumn = mapOfColumnsRowsAndColumns.findColumn(columnName).toAccessor();
      ColumnAccessor actualColumn = columnBasedFrameRowsAndColumns.findColumn(columnName).toAccessor();

      for (int i = 0; i < expectedColumn.numRows(); i++) {
        Assert.assertEquals(
            expectedColumn.getObject(i),
            actualColumn.getObject(i)
        );
      }
    }
    Assert.assertEquals(3, frame.numRows());
  }
}
