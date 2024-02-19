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
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.allocation.AppendableMemory;
import org.apache.druid.frame.allocation.MemoryAllocator;
import org.apache.druid.frame.allocation.MemoryRange;
import org.apache.druid.frame.write.FrameWriterUtils;
import org.apache.druid.segment.ColumnValueSelector;

import java.util.List;

/**
 * Parent class for the family of writers writing numeric arrays in columnar frames. Since the numeric primitives are
 * fixed width, we don't need to store the width of each element. The memory layout of a column written by this writer
 * is as follows:
 *
 * n : Total number of rows
 * k : Total number of elements in all the rows, cumulative
 *
 * | Section | Length of the section | Denotion                                                                             |
 * |---------|-----------------------|--------------------------------------------------------------------------------------|
 * | 0       | 1                     | typeCode                                                                             |
 * | 1       | n * Integer.BYTES     | n integers, where i-th integer represents the cumulative length of the array         |
 * | 2       | k * Byte.BYTES        | k bytes, where i-th byte represent whether the i-th value from the start is null     |
 * | 3       | k * ELEMENT_SIZE      | k values, each representing the element, or null equivalent value (e.g 0 for double) |
 *
 * Note on cumulative lengths stored in section 1: Cumulative lengths are stored so that its fast to offset into the
 * elements of the array. We also use negative cumulative length to denote that the array itself is null (as opposed to
 * individual elements being null, which we store in section 2)
 */
public abstract class NumericArrayFrameColumnWriter implements FrameColumnWriter
{
  /**
   * Equivalent to {@link AppendableMemory#DEFAULT_INITIAL_ALLOCATION_SIZE} / 3, since the memory would be further split
   * up into three regions
   */
  private static final int INITIAL_ALLOCATION_SIZE = 120;

  public static final byte NULL_ELEMENT_MARKER = 0x00;
  public static final byte NON_NULL_ELEMENT_MARKER = 0x01;

  /**
   * A byte required at the beginning for type code
   */
  public static final long DATA_OFFSET = 1;

  final ColumnValueSelector selector;
  final byte typeCode;

  /**
   * Row lengths: one int per row with the number of values contained by that row and all previous rows.
   * Only written for multi-value and array columns. When the corresponding row is null itself, the length is
   * written as -(actual length) - 1. (Guaranteed to be a negative number even if "actual length" is zero.)
   */
  private final AppendableMemory cumulativeRowLengths;

  /**
   * Denotes if the element of the row is null or not
   */
  private final AppendableMemory rowNullityData;

  /**
   * Row data.
   */
  private final AppendableMemory rowData;

  private int lastCumulativeRowLength = 0;
  private int lastRowLength = -1;


  public NumericArrayFrameColumnWriter(
      final ColumnValueSelector selector,
      final MemoryAllocator allocator,
      final byte typeCode
  )
  {
    this.selector = selector;
    this.typeCode = typeCode;
    this.cumulativeRowLengths = AppendableMemory.create(allocator, INITIAL_ALLOCATION_SIZE);
    this.rowNullityData = AppendableMemory.create(allocator, INITIAL_ALLOCATION_SIZE);
    this.rowData = AppendableMemory.create(allocator, INITIAL_ALLOCATION_SIZE);
  }

  /**
   * Returns the size of the elements of the array
   */
  abstract int elementSizeBytes();

  /**
   * Inserts default null value in the given memory location at the provided offset.
   */
  abstract void putNull(WritableMemory memory, long offset);

  /**
   * Inserts the element value in the given memory location at the provided offset.
   */
  abstract void putArrayElement(WritableMemory memory, long offset, Number element);

  @Override
  public boolean addSelection()
  {
    List<? extends Number> numericArray = FrameWriterUtils.getNumericArrayFromObject(selector.getObject());
    int rowLength = numericArray == null ? 0 : numericArray.size();

    // Begin memory allocations before writing
    if ((long) lastCumulativeRowLength + rowLength > Integer.MAX_VALUE) {
      return false;
    }

    if (!cumulativeRowLengths.reserveAdditional(Integer.BYTES)) {
      return false;
    }

    if (!rowNullityData.reserveAdditional(rowLength * Byte.BYTES)) {
      return false;
    }

    if (!rowData.reserveAdditional(rowLength * elementSizeBytes())) {
      return false;
    }
    // Memory allocations completed

    final MemoryRange<WritableMemory> rowLengthsCursor = cumulativeRowLengths.cursor();

    if (numericArray == null) {
      rowLengthsCursor.memory().putInt(rowLengthsCursor.start(), -(lastCumulativeRowLength + rowLength) - 1);
    } else {
      rowLengthsCursor.memory().putInt(rowLengthsCursor.start(), lastCumulativeRowLength + rowLength);
    }
    cumulativeRowLengths.advanceCursor(Integer.BYTES);
    lastRowLength = rowLength;
    lastCumulativeRowLength += rowLength;

    final MemoryRange<WritableMemory> rowNullityDataCursor = rowLength > 0 ? rowNullityData.cursor() : null;
    final MemoryRange<WritableMemory> rowDataCursor = rowLength > 0 ? rowData.cursor() : null;

    for (int i = 0; i < rowLength; ++i) {
      final Number element = numericArray.get(i);
      final long memoryOffset = rowDataCursor.start() + ((long) elementSizeBytes() * i);
      if (element == null) {
        rowNullityDataCursor.memory()
                            .putByte(rowNullityDataCursor.start() + (long) Byte.BYTES * i, NULL_ELEMENT_MARKER);
        putNull(rowDataCursor.memory(), memoryOffset);
      } else {
        rowNullityDataCursor.memory()
                            .putByte(rowNullityDataCursor.start() + (long) Byte.BYTES * i, NON_NULL_ELEMENT_MARKER);
        putArrayElement(rowDataCursor.memory(), memoryOffset, element);
      }
    }

    if (rowLength > 0) {
      rowNullityData.advanceCursor(Byte.BYTES * rowLength);
      rowData.advanceCursor(elementSizeBytes() * rowLength);
    }

    return true;
  }

  @Override
  public void undo()
  {
    if (lastRowLength == -1) {
      throw DruidException.defensive("Nothing written to undo()");
    }

    cumulativeRowLengths.rewindCursor(Integer.BYTES);
    rowNullityData.rewindCursor(lastRowLength * Byte.BYTES);
    rowData.rewindCursor(lastRowLength * elementSizeBytes());

    lastCumulativeRowLength -= lastRowLength;
    // Multiple undo calls cannot be chained together
    lastRowLength = -1;
  }

  @Override
  public long size()
  {
    return DATA_OFFSET + cumulativeRowLengths.size() + rowNullityData.size() + rowData.size();
  }

  @Override
  public long writeTo(final WritableMemory memory, final long startPosition)
  {
    long currentPosition = startPosition;

    memory.putByte(currentPosition, typeCode);
    ++currentPosition;

    currentPosition += cumulativeRowLengths.writeTo(memory, currentPosition);
    currentPosition += rowNullityData.writeTo(memory, currentPosition);
    currentPosition += rowData.writeTo(memory, currentPosition);

    return currentPosition - startPosition;
  }

  @Override
  public void close()
  {
    cumulativeRowLengths.close();
    rowNullityData.close();
    rowData.close();
  }
}
