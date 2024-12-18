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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.allocation.ArenaMemoryAllocatorFactory;
import org.apache.druid.frame.write.FrameWriter;
import org.apache.druid.frame.write.FrameWriterFactory;
import org.apache.druid.frame.write.FrameWriters;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.concrete.ColumnBasedFrameRowsAndColumns;
import org.apache.druid.segment.CloseableShapeshifter;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;

/**
 * Provides {@link RowsAndColumns} on top of a {@link CursorFactory}.
 */
public class CursorFactoryRowsAndColumns implements CloseableShapeshifter, RowsAndColumns
{
  private final CursorFactory cursorFactory;
  private final Supplier<RowsAndColumns> materialized;

  public CursorFactoryRowsAndColumns(CursorFactory cursorFactory)
  {
    this.cursorFactory = cursorFactory;
    this.materialized = Suppliers.memoize(() -> materialize(cursorFactory));
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T as(Class<T> clazz)
  {
    if (CursorFactory.class == clazz) {
      return (T) cursorFactory;
    }
    return RowsAndColumns.super.as(clazz);
  }

  @Override
  public Collection<String> getColumnNames()
  {
    return cursorFactory.getRowSignature().getColumnNames();
  }

  @Override
  public int numRows()
  {
    return materialized.get().numRows();
  }

  @Override
  public Column findColumn(String name)
  {
    return materialized.get().findColumn(name);
  }

  @Override
  public void close()
  {
  }

  @Nonnull
  private static RowsAndColumns materialize(CursorFactory cursorFactory)
  {
    try (final CursorHolder cursorHolder = cursorFactory.makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
      final Cursor cursor = cursorHolder.asCursor();
      final RowSignature rowSignature = cursorFactory.getRowSignature();

      if (cursor == null) {
        return new EmptyRowsAndColumns();
      }

      final ColumnSelectorFactory columnSelectorFactory = cursor.getColumnSelectorFactory();

      final FrameWriterFactory frameWriterFactory = FrameWriters.makeColumnBasedFrameWriterFactory(
          new ArenaMemoryAllocatorFactory(200 << 20), // 200 MB, because, why not?
          rowSignature,
          Collections.emptyList()
      );

      try (final FrameWriter writer = frameWriterFactory.newFrameWriter(columnSelectorFactory)) {
        while (!cursor.isDoneOrInterrupted()) {
          writer.addSelection();
          cursor.advance();
        }

        final byte[] bytes = writer.toByteArray();
        return new ColumnBasedFrameRowsAndColumns(Frame.wrap(bytes), rowSignature);
      }
    }
  }
}
