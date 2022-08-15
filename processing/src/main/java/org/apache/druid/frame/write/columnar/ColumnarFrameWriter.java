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

package org.apache.druid.frame.write.columnar;

import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.allocation.AppendableMemory;
import org.apache.druid.frame.allocation.MemoryRange;
import org.apache.druid.frame.key.SortColumn;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.write.FrameSort;
import org.apache.druid.frame.write.FrameWriter;
import org.apache.druid.frame.write.FrameWriterUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.util.List;

public class ColumnarFrameWriter implements FrameWriter
{
  private final RowSignature signature;
  private final List<SortColumn> sortColumns;
  @Nullable // Null if frame will not need permutation
  private final AppendableMemory rowOrderMemory;
  private final List<FrameColumnWriter> columnWriters;
  private int numRows = 0;
  private boolean written = false;

  public ColumnarFrameWriter(
      final RowSignature signature,
      final List<SortColumn> sortColumns,
      @Nullable final AppendableMemory rowOrderMemory,
      final List<FrameColumnWriter> columnWriters
  )
  {
    this.signature = signature;
    this.sortColumns = sortColumns;
    this.rowOrderMemory = rowOrderMemory;
    this.columnWriters = columnWriters;
  }

  @Override
  public boolean addSelection()
  {
    if (written) {
      throw new ISE("Cannot modify after writing");
    }

    if (numRows == Integer.MAX_VALUE) {
      return false;
    }

    if (rowOrderMemory != null && !rowOrderMemory.reserveAdditional(Integer.BYTES)) {
      return false;
    }

    int i = 0;
    for (; i < columnWriters.size(); i++) {
      if (!columnWriters.get(i).addSelection()) {
        break;
      }
    }

    if (i < columnWriters.size()) {
      // Add failed, clean up.
      for (int j = 0; j < i; j++) {
        columnWriters.get(j).undo();
      }

      return false;
    } else {
      if (rowOrderMemory != null) {
        final MemoryRange<WritableMemory> rowOrderCursor = rowOrderMemory.cursor();
        rowOrderCursor.memory().putInt(rowOrderCursor.start(), numRows);
        rowOrderMemory.advanceCursor(Integer.BYTES);
      }

      numRows++;
      return true;
    }
  }

  @Override
  public int getNumRows()
  {
    return numRows;
  }

  @Override
  public long getTotalSize()
  {
    return Frame.HEADER_SIZE + computeRowOrderSize() + computeRegionMapSize() + computeDataSize();
  }

  @Override
  public long writeTo(final WritableMemory memory, final long startPosition)
  {
    if (written) {
      throw new ISE("Cannot write twice");
    }

    final long totalSize = getTotalSize();
    long currentPosition = startPosition;

    currentPosition += FrameWriterUtils.writeFrameHeader(
        memory,
        startPosition,
        FrameType.COLUMNAR,
        totalSize,
        numRows,
        columnWriters.size(),
        mustSort()
    );

    if (mustSort()) {
      currentPosition += rowOrderMemory.writeTo(memory, currentPosition);
    }

    // Write region (column) ending positions.
    long columnEndPosition = Frame.HEADER_SIZE + computeRowOrderSize() + computeRegionMapSize();

    for (FrameColumnWriter writer : columnWriters) {
      columnEndPosition += writer.size();
      memory.putLong(currentPosition, columnEndPosition);
      currentPosition += Long.BYTES;
    }

    // Write regions (columns).
    for (FrameColumnWriter writer : columnWriters) {
      currentPosition += writer.writeTo(memory, currentPosition);
    }

    // Sanity check.
    if (currentPosition != totalSize) {
      throw new ISE("Expected to write [%,d] bytes, but wrote [%,d] bytes.", totalSize, currentPosition);
    }

    if (mustSort()) {
      FrameSort.sort(Frame.wrap(memory), FrameReader.create(signature), sortColumns);
    }

    written = true;
    return totalSize;
  }

  @Override
  public void close()
  {
    if (rowOrderMemory != null) {
      rowOrderMemory.close();
    }

    columnWriters.forEach(FrameColumnWriter::close);
  }

  /**
   * Computes the size of the row-order-permutation section of the frame.
   */
  private long computeRowOrderSize()
  {
    return mustSort() ? rowOrderMemory.size() : 0;
  }

  /**
   * Computes the size of the region-map section of the frame. It has an ending offset for each region, and since
   * we have one column per region, that's one long for every entry in {@link #columnWriters}.
   */
  private long computeRegionMapSize()
  {
    return (long) columnWriters.size() * Long.BYTES;
  }

  /**
   * Computes the size of the data section of the frame, which contains all of the columnar data.
   */
  private long computeDataSize()
  {
    return columnWriters.stream().mapToLong(FrameColumnWriter::size).sum();
  }

  /**
   * Returns whether this writer must sort during {@link #writeTo}.
   */
  private boolean mustSort()
  {
    return rowOrderMemory != null;
  }
}
