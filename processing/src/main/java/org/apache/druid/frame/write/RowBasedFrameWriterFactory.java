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

package org.apache.druid.frame.write;

import org.apache.druid.frame.allocation.AppendableMemory;
import org.apache.druid.frame.allocation.MemoryAllocator;
import org.apache.druid.frame.field.FieldWriter;
import org.apache.druid.frame.field.FieldWriters;
import org.apache.druid.frame.key.SortColumn;
import org.apache.druid.frame.read.FrameReaderUtils;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.utils.CloseableUtils;

import java.util.ArrayList;
import java.util.List;

public class RowBasedFrameWriterFactory implements FrameWriterFactory
{
  private final MemoryAllocator allocator;
  private final RowSignature signature;
  private final List<SortColumn> sortColumns;

  public RowBasedFrameWriterFactory(
      final MemoryAllocator allocator,
      final RowSignature signature,
      final List<SortColumn> sortColumns
  )
  {
    this.allocator = allocator;
    this.signature = signature;
    this.sortColumns = sortColumns;

    FrameWriterUtils.verifySortColumns(sortColumns, signature);
  }

  @Override
  public FrameWriter newFrameWriter(final ColumnSelectorFactory columnSelectorFactory)
  {
    // Only need rowOrderMemory if we are sorting.
    final AppendableMemory rowOrderMemory = sortColumns.isEmpty() ? null : AppendableMemory.create(allocator);
    final AppendableMemory rowOffsetMemory = AppendableMemory.create(allocator);
    final AppendableMemory dataMemory = AppendableMemory.create(
        allocator,
        RowBasedFrameWriter.BASE_DATA_ALLOCATION_SIZE
    );

    return new RowBasedFrameWriter(
        signature,
        sortColumns,
        makeFieldWriters(columnSelectorFactory),
        FrameReaderUtils.makeRowMemorySupplier(columnSelectorFactory, signature),
        rowOrderMemory,
        rowOffsetMemory,
        dataMemory
    );
  }

  @Override
  public long allocatorCapacity()
  {
    return allocator.capacity();
  }

  /**
   * Returns field writers that source data from the provided {@link ColumnSelectorFactory}.
   *
   * The returned {@link FieldWriter} objects are not thread-safe, and should only be used with a
   * single frame writer.
   */
  private List<FieldWriter> makeFieldWriters(final ColumnSelectorFactory columnSelectorFactory)
  {
    final List<FieldWriter> fieldWriters = new ArrayList<>();

    try {
      for (int i = 0; i < signature.size(); i++) {
        final String column = signature.getColumnName(i);
        // note: null type won't work, but we'll get a nice error from FrameColumnWriters.create
        final ColumnType columnType = signature.getColumnType(i).orElse(null);
        fieldWriters.add(FieldWriters.create(columnSelectorFactory, column, columnType));
      }
    }
    catch (Throwable e) {
      throw CloseableUtils.closeAndWrapInCatch(e, () -> CloseableUtils.closeAll(fieldWriters));
    }

    return fieldWriters;
  }
}
