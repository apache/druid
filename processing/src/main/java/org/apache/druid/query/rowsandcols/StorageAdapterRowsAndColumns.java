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

import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.allocation.ArenaMemoryAllocatorFactory;
import org.apache.druid.frame.write.FrameWriter;
import org.apache.druid.frame.write.FrameWriterFactory;
import org.apache.druid.frame.write.FrameWriters;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.concrete.FrameRowsAndColumns;
import org.apache.druid.segment.CloseableShapeshifter;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;

/**
 * Provides {@link RowsAndColumns} on top of a {@link StorageAdapter}.
 */
public class StorageAdapterRowsAndColumns implements CloseableShapeshifter, RowsAndColumns
{
  private final StorageAdapter storageAdapter;
  private RowsAndColumns materialized;

  public StorageAdapterRowsAndColumns(StorageAdapter storageAdapter)
  {
    this.storageAdapter = storageAdapter;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T as(Class<T> clazz)
  {
    if (StorageAdapter.class == clazz) {
      return (T) storageAdapter;
    }
    return null;
  }

  @Override
  public Collection<String> getColumnNames()
  {
    return storageAdapter.getRowSignature().getColumnNames();
  }

  @Override
  public int numRows()
  {
    return storageAdapter.getNumRows();
  }

  @Override
  public Column findColumn(String name)
  {
    return getRealRAC().findColumn(name);
  }

  @Override
  public void close()
  {
  }

  protected RowsAndColumns getRealRAC()
  {
    if (materialized == null) {
      materialized = materialize(storageAdapter);
    }
    return materialized;
  }

  @Nonnull
  private static RowsAndColumns materialize(StorageAdapter as)
  {
    final Sequence<Cursor> cursors = as.makeCursors(
        null,
        Intervals.ETERNITY,
        VirtualColumns.EMPTY,
        Granularities.ALL,
        false,
        null
    );

    RowSignature rowSignature = as.getRowSignature();

    FrameWriter writer = cursors.accumulate(null, (accumulated, in) -> {
      if (accumulated != null) {
        // We should not get multiple cursors because we set the granularity to ALL.  So, this should never
        // actually happen, but it doesn't hurt us to defensive here, so we test against it.
        throw new ISE("accumulated[%s] non-null, why did we get multiple cursors?", accumulated);
      }

      final ColumnSelectorFactory columnSelectorFactory = in.getColumnSelectorFactory();

      final FrameWriterFactory frameWriterFactory = FrameWriters.makeFrameWriterFactory(
          FrameType.COLUMNAR,
          new ArenaMemoryAllocatorFactory(200 << 20), // 200 MB, because, why not?
          rowSignature,
          Collections.emptyList()
      );

      final FrameWriter frameWriter = frameWriterFactory.newFrameWriter(columnSelectorFactory);
      while (!in.isDoneOrInterrupted()) {
        frameWriter.addSelection();
        in.advance();
      }
      return frameWriter;
    });

    if (writer == null) {
      return new EmptyRowsAndColumns();
    } else {
      final byte[] bytes = writer.toByteArray();
      return new FrameRowsAndColumns(Frame.wrap(bytes), rowSignature);
    }
  }
}
