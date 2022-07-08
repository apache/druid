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

package org.apache.druid.frame.segment.row;

import com.google.common.base.Preconditions;
import org.apache.datasketches.memory.Memory;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.write.RowBasedFrameWriter;
import org.apache.druid.segment.data.ReadableOffset;

/**
 * Row pointer based on a {@link ReadableOffset} containing a row number.
 */
public class CursorFrameRowPointer implements ReadableFrameRowPointer
{
  private final Frame frame;
  private final ReadableOffset offset;
  private final Memory rowPositions;

  // Cache row position across calls to "position" and "length" for the same offset.
  int cachedOffset = -1;
  long cachedPosition;
  long cachedLength;

  public CursorFrameRowPointer(final Frame frame, final ReadableOffset offset)
  {
    this.frame = FrameType.ROW_BASED.ensureType(frame);
    this.offset = Preconditions.checkNotNull(offset, "offset");
    this.rowPositions = frame.region(RowBasedFrameWriter.ROW_OFFSET_REGION);
  }

  @Override
  public long position()
  {
    update();
    return cachedPosition;
  }

  @Override
  public long length()
  {
    update();
    return cachedLength;
  }

  private void update()
  {
    final int rowNumber = offset.getOffset();

    if (cachedOffset != rowNumber) {
      // Cached position and length need to be updated.
      final long rowPosition;
      final int physicalRowNumber = frame.physicalRow(offset.getOffset());
      final long rowEndPosition = rowPositions.getLong((long) Long.BYTES * physicalRowNumber);

      if (physicalRowNumber == 0) {
        // The first row is located at the start of the memory region.
        rowPosition = 0;
      } else {
        // Use the rowPositions (pointers to end positions) to get pointers to subsequent rows.
        rowPosition = rowPositions.getLong((long) Long.BYTES * (physicalRowNumber - 1));
      }

      cachedOffset = rowNumber;
      cachedPosition = rowPosition;
      cachedLength = rowEndPosition - rowPosition;
    }
  }
}
