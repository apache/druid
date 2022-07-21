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

import com.google.common.primitives.Ints;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.allocation.AppendableMemory;
import org.apache.druid.frame.allocation.MemoryRange;
import org.apache.druid.frame.field.FieldWriter;
import org.apache.druid.frame.key.SortColumn;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Write row-based frames: type {@link FrameType#ROW_BASED}.
 *
 * Row-based frames have two regions: an offset region and data region.
 *
 * The offset region has one integer per row containing the ending position of each row within the data region.
 *
 * The data region has each row laid out back to back. Each row has a header section with one int per field, containing
 * the end position of that field relative to the start of the row. This header is followed by the fields.
 */
public class RowBasedFrameWriter implements FrameWriter
{
  public static final int ROW_OFFSET_REGION = 0;
  public static final int ROW_DATA_REGION = 1;
  public static final int NUM_REGIONS = 2;

  /**
   * Base allocation size for "dataWriter".
   */
  static final int BASE_DATA_ALLOCATION_SIZE = 1 << 13; // 8 KB

  private final RowSignature signature;
  private final List<SortColumn> sortColumns;
  private final List<FieldWriter> fieldWriters;
  private final Supplier<MemoryRange<Memory>> rowMemorySupplier;
  @Nullable // Null if frame will not need permutation
  private final AppendableMemory rowOrderMemory;
  private final AppendableMemory rowOffsetMemory;
  private final AppendableMemory dataMemory;
  private int numRows = 0;
  private boolean written = false;

  public RowBasedFrameWriter(
      final RowSignature signature,
      final List<SortColumn> sortColumns,
      final List<FieldWriter> fieldWriters,
      @Nullable final Supplier<MemoryRange<Memory>> rowMemorySupplier,
      @Nullable final AppendableMemory rowOrderMemory,
      final AppendableMemory rowOffsetMemory,
      final AppendableMemory dataMemory
  )
  {
    this.signature = signature;
    this.sortColumns = sortColumns;
    this.rowMemorySupplier = rowMemorySupplier;
    this.fieldWriters = fieldWriters;
    this.rowOrderMemory = rowOrderMemory;
    this.rowOffsetMemory = rowOffsetMemory;
    this.dataMemory = dataMemory;

    FrameWriterUtils.verifySortColumns(sortColumns, signature);

    // Check for disallowed field names.
    final Set<String> disallowedFieldNames = FrameWriterUtils.findDisallowedFieldNames(signature);
    if (!disallowedFieldNames.isEmpty()) {
      throw new IAE("Disallowed field names: %s", disallowedFieldNames);
    }
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

    if (!rowOffsetMemory.reserveAdditional(Long.BYTES)) {
      return false;
    }

    if (!writeData()) {
      return false;
    }

    final MemoryRange<WritableMemory> rowOffsetCursor = rowOffsetMemory.cursor();
    rowOffsetCursor.memory().putLong(rowOffsetCursor.start(), dataMemory.size());
    rowOffsetMemory.advanceCursor(Long.BYTES);

    if (rowOrderMemory != null) {
      final MemoryRange<WritableMemory> rowOrderCursor = rowOrderMemory.cursor();
      rowOrderCursor.memory().putInt(rowOrderCursor.start(), numRows);
      rowOrderMemory.advanceCursor(Integer.BYTES);
    }

    numRows++;
    return true;
  }

  @Override
  public int getNumRows()
  {
    return numRows;
  }

  @Override
  public long getTotalSize()
  {
    return Frame.HEADER_SIZE
           + computeRowOrderSize()
           + ((long) NUM_REGIONS * Long.BYTES)
           + rowOffsetMemory.size()
           + dataMemory.size();
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
        FrameType.ROW_BASED,
        totalSize,
        numRows,
        NUM_REGIONS,
        mustSort()
    );

    if (mustSort()) {
      currentPosition += rowOrderMemory.writeTo(memory, currentPosition);
    }

    // Write region ending positions.
    long regionsStart = Frame.HEADER_SIZE + computeRowOrderSize() + ((long) NUM_REGIONS * Long.BYTES);
    memory.putLong(currentPosition, regionsStart + rowOffsetMemory.size());
    memory.putLong(currentPosition + Long.BYTES, regionsStart + rowOffsetMemory.size() + dataMemory.size());
    currentPosition += ((long) NUM_REGIONS * Long.BYTES);

    // Write regions.
    currentPosition += rowOffsetMemory.writeTo(memory, currentPosition);
    currentPosition += dataMemory.writeTo(memory, currentPosition);

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
    CloseableUtils.closeAndWrapExceptions(() -> {
      final Closer closer = Closer.create();

      closer.register(rowOrderMemory);
      closer.register(rowOffsetMemory);
      closer.register(dataMemory);

      if (fieldWriters != null) {
        closer.registerAll(fieldWriters);
      }

      closer.close();
    });
  }

  /**
   * Computes the size of the row-order-permutation section of the frame.
   */
  private long computeRowOrderSize()
  {
    return mustSort() ? rowOrderMemory.size() : 0;
  }

  /**
   * Returns whether this writer must sort during {@link #writeTo}.
   */
  private boolean mustSort()
  {
    return rowOrderMemory != null;
  }

  private boolean writeData()
  {
    if (rowMemorySupplier != null) {
      final MemoryRange<Memory> rowMemory = rowMemorySupplier.get();
      if (rowMemory != null) {
        return writeDataUsingRowMemory(rowMemory);
      }
    }

    return writeDataUsingFieldWriters();
  }

  /**
   * Helper for writing to {@link #dataMemory} using {@link #fieldWriters}.
   */
  private boolean writeDataUsingRowMemory(final MemoryRange<Memory> rowMemory)
  {
    if (!dataMemory.reserveAdditional(Ints.checkedCast(rowMemory.length()))) {
      return false;
    }

    final MemoryRange<WritableMemory> cursor = dataMemory.cursor();
    rowMemory.memory().copyTo(rowMemory.start(), cursor.memory(), cursor.start(), rowMemory.length());
    dataMemory.advanceCursor(Ints.checkedCast(rowMemory.length()));
    return true;
  }

  /**
   * Helper for writing to {@link #dataMemory} using {@link #fieldWriters}.
   */
  private boolean writeDataUsingFieldWriters()
  {
    assert fieldWriters != null;

    // One int per field, containing the end offset of that field.
    final long fieldPositionBytes = (long) fieldWriters.size() * Integer.BYTES;

    if (numRows == 0) {
      if (!dataMemory.reserveAdditional(Ints.checkedCast(Math.max(fieldPositionBytes, BASE_DATA_ALLOCATION_SIZE)))) {
        return false;
      }
    }

    MemoryRange<WritableMemory> dataCursor = dataMemory.cursor();
    long remainingInBlock = dataCursor.length();
    long bytesWritten = fieldPositionBytes;
    int reserveMultiple = 1;

    for (int i = 0; i < fieldWriters.size(); i++) {
      final FieldWriter fieldWriter = fieldWriters.get(i);
      final long writeResult;

      // May throw InvalidNullByteException; allow it to propagate upwards.
      writeResult = fieldWriter.writeTo(
          dataCursor.memory(),
          dataCursor.start() + bytesWritten,
          remainingInBlock - bytesWritten
      );

      if (writeResult < 0) {
        // Reset to beginning of loop.
        i = -1;

        // Try again with a bigger allocation.
        reserveMultiple *= 2;

        if (!dataMemory.reserveAdditional(Ints.checkedCast((long) BASE_DATA_ALLOCATION_SIZE * reserveMultiple))) {
          return false;
        }

        dataCursor = dataMemory.cursor();
        remainingInBlock = dataCursor.length();
        bytesWritten = fieldPositionBytes;
      } else {
        bytesWritten += writeResult;

        // Write field ending position.
        dataCursor.memory().putInt(dataCursor.start() + (long) i * Integer.BYTES, Ints.checkedCast(bytesWritten));
      }
    }

    dataMemory.advanceCursor(Ints.checkedCast(bytesWritten));
    return true;
  }
}
