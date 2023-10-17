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

package org.apache.druid.frame.field;

import org.apache.datasketches.memory.Memory;
import org.apache.druid.frame.segment.row.ReadableFrameRowPointer;

/**
 * A {@link ReadableFieldPointer} that is derived from a row-based frame.
 *
 * Returns the position and the length of a field at a particular position for the row that the rowPointer is pointing
 * to at the time. It caches the values of the position and the length based on position of the rowPointer.
 * This method is not thread-safe
 */
public class RowMemoryFieldPointer implements ReadableFieldPointer
{
  private final Memory memory;
  private final ReadableFrameRowPointer rowPointer;
  private final int fieldNumber;
  private final int fieldCount;

  // Caching of position() calls
  private long rowWithCachedPosition = -1L;
  private long cachedPosition = -1L;

  // Caching of length() calls
  // We cache the length() calls separately, because not all field types call length(), therefore it's wasteful to
  // compute and cache length() if we are not reading it
  private long rowWithCachedLength = -1L;
  private long cachedLength = -1L;

  public RowMemoryFieldPointer(
      final Memory memory,
      final ReadableFrameRowPointer rowPointer,
      final int fieldNumber,
      final int fieldCount
  )
  {
    this.memory = memory;
    this.rowPointer = rowPointer;
    this.fieldNumber = fieldNumber;
    this.fieldCount = fieldCount;
  }

  @Override
  public long position()
  {
    updatePosition();
    return cachedPosition;
  }

  @Override
  public long length()
  {
    updatePositionAndLength();
    return cachedLength;
  }

  private void updatePosition()
  {
    long rowPointerPosition = rowPointer.position();
    if (rowPointerPosition == rowWithCachedPosition) {
      return;
    }
    // Update the cached position for position()
    rowWithCachedPosition = rowPointerPosition;

    // Update the start position
    cachedPosition = startPosition(fieldNumber);
  }

  // Not all field types call length(), and therefore there's no need to cache the length of the field. This method
  // updates both the position and the length.
  private void updatePositionAndLength()
  {
    updatePosition();

    // rowPointerPosition = rowPointer.position() = rowWithCachedPosition, since that was updated in the call to update
    // position above
    long rowPointerPosition = rowWithCachedPosition;

    if (rowPointerPosition == rowWithCachedLength) {
      return;
    }
    // Update the cached position for length()
    rowWithCachedLength = rowPointerPosition;

    if (fieldNumber == fieldCount - 1) {
      // If the field is the last field in the row, then the length of the field would be the end of the row minus the
      // start position of the field. End of the row is the start of the row plus the length of the row.
      cachedLength = (rowPointerPosition + rowPointer.length()) - cachedPosition;
    } else {
      // Else the length of the field would be the difference between the start position of the given field and
      // the subsequent field
      cachedLength = startPosition(fieldNumber + 1) - cachedPosition;
    }
  }

  /**
   * Given a fieldNumber, computes the start position of the field. Requires a memory access to read the start position,
   * therefore callers should cache the value for better efficiency.
   */
  private long startPosition(int fieldNumber)
  {
    if (fieldNumber == 0) {
      // First field starts after the field end pointers -- one integer per field.
      // (See RowReader javadocs for format details).
      return rowPointer.position() + (long) Integer.BYTES * fieldCount;
    } else {
      // Get position from the end pointer of the prior field.
      return rowPointer.position() + memory.getInt(rowPointer.position() + (long) Integer.BYTES * (fieldNumber - 1));
    }
  }
}
