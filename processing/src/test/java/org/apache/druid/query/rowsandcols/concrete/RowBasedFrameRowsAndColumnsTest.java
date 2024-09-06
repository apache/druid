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

package org.apache.druid.query.rowsandcols.concrete;

import org.apache.druid.frame.Frame;
import org.apache.druid.frame.allocation.ArenaMemoryAllocatorFactory;
import org.apache.druid.frame.write.FrameWriter;
import org.apache.druid.frame.write.FrameWriters;
import org.apache.druid.query.rowsandcols.MapOfColumnsRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumnsTestBase;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.semantic.ColumnSelectorFactoryMaker;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.RowSignature;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class RowBasedFrameRowsAndColumnsTest extends RowsAndColumnsTestBase
{
  public RowBasedFrameRowsAndColumnsTest()
  {
    super(RowBasedFrameRowsAndColumns.class);
  }

  public static Function<MapOfColumnsRowsAndColumns, RowBasedFrameRowsAndColumns> MAKER = RowBasedFrameRowsAndColumnsTest::buildFrame;

  private static RowBasedFrameRowsAndColumns buildFrame(MapOfColumnsRowsAndColumns rac)
  {
    final AtomicInteger rowId = new AtomicInteger(0);
    final int numRows = rac.numRows();
    final ColumnSelectorFactoryMaker csfm = ColumnSelectorFactoryMaker.fromRAC(rac);
    final ColumnSelectorFactory selectorFactory = csfm.make(rowId);

    final RowSignature.Builder sigBob = RowSignature.builder();
    final ArenaMemoryAllocatorFactory memFactory = new ArenaMemoryAllocatorFactory(200 << 20);


    for (String column : rac.getColumnNames()) {
      final Column racColumn = rac.findColumn(column);
      if (racColumn == null) {
        continue;
      }
      sigBob.add(column, racColumn.toAccessor().getType());
    }

    final RowSignature signature = sigBob.build();
    final FrameWriter frameWriter = FrameWriters.makeRowBasedFrameWriterFactory(
        memFactory,
        signature,
        Collections.emptyList(),
        false
    ).newFrameWriter(selectorFactory);

    rowId.set(0);
    for (; rowId.get() < numRows; rowId.incrementAndGet()) {
      frameWriter.addSelection();
    }

    return new RowBasedFrameRowsAndColumns(Frame.wrap(frameWriter.toByteArray()), signature);
  }
}
